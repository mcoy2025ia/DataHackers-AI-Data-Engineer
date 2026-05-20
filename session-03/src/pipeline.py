"""
MarketPipeline — ETL pipeline for equity market data via Yahoo Finance.
Reto 1: Extraction with tenacity Exponential Backoff.
Reto 2: Quality validation — null % report + Z-Score outlier detection + DLQ quarantine.
Reto 3: Business transformation — RSI(14) + Realized Volatility + log returns.
Reto 4: Persistence — Parquet output with Snappy compression via pyarrow.
Migration note: Original provider blocked on free tier -> Yahoo Finance. See CLAUDE_LOG.md #008.
"""

import logging
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR            = Path(__file__).parent.parent
LOGS_DIR            = BASE_DIR / "logs"
DATA_RAW_DIR        = BASE_DIR / "data" / "raw"
DATA_QUARANTINE_DIR = BASE_DIR / "data" / "raw" / "quarantine"
DATA_OUTPUT_DIR     = BASE_DIR / "data" / "output"

# ── Quality constants ──────────────────────────────────────────────────────────
Z_SCORE_THRESHOLD = 3.0   # |Z| > 3σ → outlier
RSI_PERIOD        = 14    # Wilder's RSI period
HEALTH_THRESHOLD  = 0.90  # < 90% usable records → DEGRADED

# ── Advanced analytics windows ─────────────────────────────────────────────────
EMA_SPAN      = 20   # EMA span for Price Z-Score baseline
SHARPE_WINDOW = 14   # Rolling window for Sharpe Ratio
MDD_WINDOW    = 30   # Rolling window for Maximum Drawdown


# ── Logging ────────────────────────────────────────────────────────────────────
def _setup_logging() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("market_pipeline")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    fh = logging.FileHandler(LOGS_DIR / "pipeline.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # Caveman requirement: errors must be loud on console too
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    # Windows cp1252 consoles reject non-ASCII; reconfigure stream to UTF-8
    if hasattr(ch.stream, "reconfigure"):
        try:
            ch.stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


logger = _setup_logging()


# ── Quality Report ─────────────────────────────────────────────────────────────
@dataclass
class QualityReport:
    symbol: str
    total_records: int
    null_pct: dict
    outlier_count: int
    outlier_timestamps: list
    quarantine_path: Optional[Path]
    ohlcv_violations: int
    health_score: float
    is_healthy: bool

    def log_summary(self) -> None:
        status = "HEALTHY" if self.is_healthy else "*** DEGRADED ***"
        logger.info(f"[VALIDATE] --- Quality Report: {self.symbol} [{status}] ---")
        logger.info(f"[VALIDATE]   Total records    : {self.total_records}")
        logger.info(f"[VALIDATE]   Health score     : {self.health_score:.1%}")
        logger.info(f"[VALIDATE]   Z-Score outliers : {self.outlier_count} (rolling |Z|>{Z_SCORE_THRESHOLD})")
        logger.info(f"[VALIDATE]   OHLCV violations : {self.ohlcv_violations}")
        for col, pct in self.null_pct.items():
            level = logging.WARNING if pct > 0 else logging.DEBUG
            logger.log(level, f"[VALIDATE]   NULL% {col:<8s}: {pct:.1%}")
        if self.quarantine_path:
            logger.warning(f"[VALIDATE]   DLQ quarantine   : {self.quarantine_path.name}")


# ── Retry Policy (Exponential Backoff) ────────────────────────────────────────
def _is_retryable(exc: BaseException) -> bool:
    """Network and transient HTTP errors are retryable; data errors are terminal."""
    if isinstance(exc, requests.HTTPError):
        code = exc.response.status_code if exc.response is not None else 0
        return code in (429, 500, 502, 503)
    return isinstance(exc, (requests.ConnectionError, requests.Timeout, ConnectionError, TimeoutError))


def _before_sleep_masked(state: RetryCallState) -> None:
    exc = state.outcome.exception()
    msg = re.sub(r"token=[^&\s.]+", "token=[MASKED]", str(exc))
    wait = getattr(state.next_action, "sleep", "?")
    logger.warning(f"Retry {state.attempt_number} | sleeping {wait:.1f}s | {msg}")


_RETRY_POLICY = retry(
    retry=retry_if_exception(_is_retryable),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(5),
    before_sleep=_before_sleep_masked,
    reraise=True,
)


# ── RSI helper ─────────────────────────────────────────────────────────────────
def _rsi(series: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
    """
    Wilder's RSI using EWM (alpha = 1/period).
    First `period` values are NaN by design (insufficient lookback).
    """
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


# ── Pipeline ───────────────────────────────────────────────────────────────────
class MarketPipeline:
    """
    ETL pipeline for equity market data via Yahoo Finance.

    Reto 1 — extract()   : yfinance Ticker.history() with exponential backoff.
    Reto 2 — validate()  : null % + Z-Score outlier detection + DLQ quarantine.
    Reto 3 — transform() : RSI(14) + realized volatility + log returns + signals.
    Reto 4 — load()      : Parquet persistence with Snappy compression (pyarrow).
    """

    def __init__(self) -> None:
        DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
        DATA_QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        DATA_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        logger.info("=" * 60)
        logger.info("MarketPipeline initialized. Yahoo Finance engine ready.")
        logger.info("=" * 60)

    @_RETRY_POLICY
    def _download_yf(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        """yfinance Ticker.history() call — wrapped with tenacity for network resilience."""
        return yf.Ticker(symbol).history(start=start, end=end, interval="1d", auto_adjust=True)

    # ── Reto 1: Extraction ─────────────────────────────────────────────────────
    def extract(self, symbol: str, days: int = 30) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV daily candles for `symbol` over the last `days` calendar days.

        Returns a DataFrame with DatetimeIndex (UTC) and columns:
        open, high, low, close, volume — or None on failure.
        """
        end_dt   = datetime.now(tz=timezone.utc).date()
        start_dt = (datetime.now(tz=timezone.utc) - timedelta(days=days)).date()

        logger.info(
            f"[EXTRACT] >>> START | symbol={symbol} | "
            f"range={start_dt} -> {end_dt} ({days}d)"
        )

        try:
            raw = self._download_yf(symbol, str(start_dt), str(end_dt))
        except Exception as exc:
            logger.error(
                f"[EXTRACT] *** DOWNLOAD FAILED after all retries | "
                f"symbol={symbol} | {type(exc).__name__}: {exc}"
            )
            return None

        if raw is None or raw.empty:
            logger.error(
                f"[EXTRACT] *** NO DATA | symbol={symbol} | "
                f"yfinance returned empty DataFrame (ticker may be invalid or delisted)"
            )
            return None

        # Normalize to lowercase OHLCV columns; drop yfinance extras (Dividends, Stock Splits)
        raw.columns = [str(c).lower().replace(" ", "_") for c in raw.columns]
        required = {"open", "high", "low", "close", "volume"}
        missing = required - set(raw.columns)
        if missing:
            logger.error(f"[EXTRACT] *** MISSING COLUMNS | symbol={symbol} | missing={missing}")
            return None

        df = raw[list(required)].copy()
        df = df.dropna(how="all")

        # Ensure UTC timezone on DatetimeIndex
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")
        df.index.name = "timestamp"

        logger.info(f"[EXTRACT] <<< SUCCESS | symbol={symbol} | {len(df)} candles received")
        return df

    # ── Reto 2: Validation ─────────────────────────────────────────────────────
    def validate(
        self, input_df: pd.DataFrame, symbol: str
    ) -> tuple[QualityReport, pd.DataFrame]:
        """
        Quality gate: null % report + Z-Score outlier detection (on close price).

        Outlier strategy: QUARANTINE, not DROP.
        Outlier rows are written to data/raw/quarantine/ as JSON for audit.
        See CLAUDE_LOG.md #003 for the full decision rationale.

        Returns (QualityReport, clean_df).
        """
        logger.info(f"[VALIDATE] >>> START | symbol={symbol} | {len(input_df)} records")

        df = input_df.copy().sort_index()
        total = len(df)

        # ── 1. Null % per column (DE requirement) ─────────────────────────────
        null_pct = {col: float(df[col].isna().mean()) for col in df.columns}
        total_nulls = sum(1 for v in null_pct.values() if v > 0)
        if total_nulls:
            logger.warning(f"[VALIDATE] {total_nulls} column(s) contain NaN values")

        # ── 2. OHLCV integrity (Trader requirement) ────────────────────────────
        ohlcv_bad_mask = (
            (df["high"] < df["low"])
            | (df["high"] < df["close"])
            | (df["high"] < df["open"])
            | (df["low"] > df["close"])
            | (df["low"] > df["open"])
        )
        ohlcv_violations = int(ohlcv_bad_mask.sum())
        if ohlcv_violations:
            logger.warning(
                f"[VALIDATE] *** OHLCV violations detected: {ohlcv_violations} rows "
                f"(high < low or price outside high/low range)"
            )

        # ── 3. Z-Score outlier detection on close price ────────────────────────
        # Rolling Z-Score: detecta outliers relativos a la tendencia local,
        # no a la media global de la serie. Correcto para series con tendencia.
        # Ventana adaptativa: mínimo 5 observaciones, máximo 20.
        z_window = min(20, max(5, len(df) // 3))
        roll_mean = df["close"].rolling(window=z_window, min_periods=5).mean()
        roll_std  = df["close"].rolling(window=z_window, min_periods=5).std(ddof=1)

        if roll_std.replace(0.0, np.nan).isna().all():
            logger.warning("[VALIDATE] rolling std(close) = 0 — all prices identical, skipping Z-Score")
            df["_z"] = 0.0
        else:
            df["_z"] = (df["close"] - roll_mean) / roll_std.replace(0.0, np.nan)
            df["_z"] = df["_z"].fillna(0.0)  # primeras filas sin ventana completa → no outlier

        outlier_mask  = df["_z"].abs() > Z_SCORE_THRESHOLD
        outlier_df    = df[outlier_mask].copy()
        outlier_count = len(outlier_df)

        # ── 4. DLQ: quarantine outliers, never silently drop ───────────────────
        quarantine_path: Optional[Path] = None
        if outlier_count > 0:
            ts_tag = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
            quarantine_path = DATA_QUARANTINE_DIR / f"{symbol}_{ts_tag}_outliers.json"
            quarantine_records = outlier_df.drop(columns=["_z"]).reset_index()
            quarantine_records["timestamp"] = quarantine_records["timestamp"].astype(str)
            quarantine_records.to_json(quarantine_path, orient="records", indent=2)
            logger.warning(
                f"[VALIDATE] *** {outlier_count} outlier(s) -> quarantine: {quarantine_path.name}"
            )

        clean_df     = df[~outlier_mask].drop(columns=["_z"])
        health_score = len(clean_df) / total if total > 0 else 0.0

        report = QualityReport(
            symbol=symbol,
            total_records=total,
            null_pct=null_pct,
            outlier_count=outlier_count,
            outlier_timestamps=[str(ts) for ts in outlier_df.index.tolist()],
            quarantine_path=quarantine_path,
            ohlcv_violations=ohlcv_violations,
            health_score=health_score,
            is_healthy=health_score >= HEALTH_THRESHOLD,
        )
        report.log_summary()

        if not report.is_healthy:
            logger.error(
                f"[VALIDATE] *** Dataset health {health_score:.1%} is below "
                f"{HEALTH_THRESHOLD:.0%} threshold — downstream transforms degraded."
            )

        logger.info(
            f"[VALIDATE] <<< DONE | {len(clean_df)}/{total} records pass quality gate"
        )
        return report, clean_df

    # ── Reto 3: Transform ──────────────────────────────────────────────────────
    def transform(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Business transformations agreed by Business Analyst + Trader:

        Base indicators:
        - rsi_14       : RSI(14) via Wilder's EWM.
        - log_return   : ln(close_t / close_{t-1}).
        - volatility   : rolling std of log_return.
        - pct_change   : daily % change in close.
        - signal       : OVERBOUGHT / OVERSOLD / NEUTRAL / NO_SIGNAL.

        Advanced risk & mean-reversion (vectorised, zero Python loops):
        - price_zscore : (close - EMA_20) / rolling_std_20.
        - sharpe_14    : rolling 14d annualised Sharpe (rf=0): mu/sigma x sqrt(252).
        - drawdown     : (close - rolling_peak_30d) / rolling_peak_30d.
        """
        logger.info(f"[TRANSFORM] >>> START | symbol={symbol} | {len(df)} records")

        if len(df) < 2:
            logger.error(f"[TRANSFORM] *** Too few records ({len(df)}) to compute indicators.")
            return df

        result = df.copy()

        # ── RSI(14) ────────────────────────────────────────────────────────────
        result["rsi_14"] = _rsi(result["close"], period=RSI_PERIOD)
        valid_rsi = int(result["rsi_14"].notna().sum())
        logger.info(
            f"[TRANSFORM] RSI({RSI_PERIOD}) | {valid_rsi}/{len(result)} valid values "
            f"(first {RSI_PERIOD - 1} NaN by design)"
        )

        # ── Log returns ────────────────────────────────────────────────────────
        result["log_return"] = np.log(result["close"] / result["close"].shift(1))

        # ── Realized volatility ────────────────────────────────────────────────
        vol_window = min(21, max(2, len(result) // 2))
        result["volatility"] = (
            result["log_return"].rolling(window=vol_window, min_periods=2).std()
        )
        logger.info(
            f"[TRANSFORM] Volatility | window={vol_window}d | "
            f"{int(result['volatility'].notna().sum())} valid values"
        )

        # ── Daily % change ─────────────────────────────────────────────────────
        result["pct_change"] = result["close"].pct_change() * 100.0

        # ── Advanced: Price Z-Score vs EMA(20) ────────────────────────────────
        ema_20 = result["close"].ewm(span=EMA_SPAN, adjust=False).mean()
        std_20 = result["close"].rolling(window=EMA_SPAN, min_periods=5).std()
        result["price_zscore"] = (result["close"] - ema_20) / std_20.replace(0.0, np.nan)
        logger.info(
            f"[TRANSFORM] Price Z-Score | EMA_span={EMA_SPAN} | "
            f"{int(result['price_zscore'].notna().sum())} valid values"
        )

        # ── Advanced: Rolling Sharpe Ratio (14d, rf=0, annualised) ────────────
        roll_mean = result["log_return"].rolling(window=SHARPE_WINDOW, min_periods=5).mean()
        roll_std  = result["log_return"].rolling(window=SHARPE_WINDOW, min_periods=5).std()
        result["sharpe_14"] = (roll_mean / roll_std.replace(0.0, np.nan)) * np.sqrt(252)
        logger.info(
            f"[TRANSFORM] Sharpe({SHARPE_WINDOW}d ann.) | "
            f"{int(result['sharpe_14'].notna().sum())} valid values"
        )

        # ── Advanced: Rolling Maximum Drawdown (30d) ──────────────────────────
        rolling_peak = result["close"].rolling(window=MDD_WINDOW, min_periods=1).max()
        result["drawdown"] = (result["close"] - rolling_peak) / rolling_peak
        mdd_scalar = float(result["drawdown"].min())
        logger.info(f"[TRANSFORM] MDD (window={MDD_WINDOW}d) = {mdd_scalar:.2%}")

        # ── Trading signals ────────────────────────────────────────────────────
        result["signal"] = "NEUTRAL"
        has_rsi = result["rsi_14"].notna()
        result.loc[has_rsi & (result["rsi_14"] > 70), "signal"] = "OVERBOUGHT"
        result.loc[has_rsi & (result["rsi_14"] < 30), "signal"] = "OVERSOLD"
        result.loc[~has_rsi, "signal"] = "NO_SIGNAL"

        counts = result["signal"].value_counts().to_dict()
        logger.info(
            "[TRANSFORM] Signals -> "
            + " | ".join(f"{k}={v}" for k, v in counts.items())
        )

        last       = result.iloc[-1]
        rsi_str    = f"{last['rsi_14']:.1f}"       if pd.notna(last["rsi_14"])       else "N/A"
        vol_str    = f"{last['volatility']:.4f}"   if pd.notna(last["volatility"])   else "N/A"
        zscore_str = f"{last['price_zscore']:.2f}" if pd.notna(last["price_zscore"]) else "N/A"
        sharpe_str = f"{last['sharpe_14']:.2f}"    if pd.notna(last["sharpe_14"])    else "N/A"
        mdd_str    = f"{mdd_scalar:.2%}"
        logger.info(
            f"[TRANSFORM] Latest ({result.index[-1].date()}) | "
            f"close={last['close']:.2f} | RSI={rsi_str} | vol={vol_str} | "
            f"Z={zscore_str} | Sharpe={sharpe_str} | MDD={mdd_str} | signal={last['signal']}"
        )

        logger.info(f"[TRANSFORM] <<< DONE | {len(result)} enriched records")
        return result

    # ── Reto 4: Persistence ────────────────────────────────────────────────────
    def load(self, df: pd.DataFrame, symbol: str) -> Path:
        """
        Persist the enriched DataFrame to Parquet format.

        WHY PARQUET OVER CSV — Technical Justification (Data Engineer mandate):
        -----------------------------------------------------------------------
        1. COLUMNAR STORAGE: Parquet organises data by column, not by row.
           An analytical query reading only `close` and `rsi_14` scans just
           those two columns on disk; CSV forces a full row scan every time.
           For a 10-column OHLCV+indicators table, this is a ~5x I/O reduction
           on typical BI workloads.

        2. SCHEMA ENFORCEMENT: Parquet embeds metadata (dtypes, nullability,
           timezone on timestamps) inside the file. When you re-read it, pandas
           reconstructs `DatetimeTZDtype[ns, UTC]` automatically. CSV is a plain
           text contract that breaks silently on re-read.

        3. COMPRESSION EFFICIENCY (Snappy): Financial OHLCV data has high
           inter-row correlation. Parquet + Snappy achieves 4-8x vs raw CSV.

        4. INTEROPERABILITY: Parquet is the de-facto standard for Data Lakes
           on S3, GCS, and ADLS. Any downstream Spark, Athena, or dbt workload
           expects Parquet, not CSV.

        Compression choice: `snappy` — fast decompression for interactive queries.
        """
        output_path = DATA_OUTPUT_DIR / f"{symbol}_data.parquet"

        logger.info(f"[LOAD] >>> START | dest={output_path.relative_to(BASE_DIR)}")

        df_to_save = df.reset_index()  # promote timestamp index to column
        df_to_save.to_parquet(
            output_path,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )

        file_size_kb = output_path.stat().st_size / 1024
        logger.info(
            f"[LOAD] <<< SUCCESS | {len(df_to_save)} rows | "
            f"{len(df_to_save.columns)} cols | {file_size_kb:.1f} KB | "
            f"compression=snappy engine=pyarrow"
        )
        logger.debug(f"[LOAD] Schema: {dict(df_to_save.dtypes)}")
        return output_path


# ── Business Analyst summary ───────────────────────────────────────────────────
def _print_ba_summary(df: pd.DataFrame, symbol: str, parquet_path: Path) -> None:
    """Human-readable statistical summary for the Business Analyst."""
    analytics_cols = [c for c in [
        "close", "rsi_14", "volatility", "log_return", "pct_change",
        "price_zscore", "sharpe_14", "drawdown",
    ] if c in df.columns]
    desc = df[analytics_cols].describe().round(4)

    separator = "-" * 62
    print(f"\n{separator}")
    print(f"  BUSINESS ANALYST REPORT — {symbol}  ({len(df)} trading days)")
    print(f"  Output: {parquet_path.relative_to(parquet_path.parent.parent.parent)}")
    print(separator)
    print(desc.to_string())
    print(separator)

    if "signal" in df.columns:
        sig_counts = df["signal"].value_counts()
        print("\n  TRADING SIGNALS")
        for sig, count in sig_counts.items():
            print(f"    {sig:<12s} {'#' * count} ({count})")

    if "rsi_14" in df.columns:
        last_rsi = df["rsi_14"].dropna().iloc[-1] if df["rsi_14"].notna().any() else None
        if last_rsi is not None:
            condition = ("OVERBOUGHT (>70)" if last_rsi > 70 else
                         "OVERSOLD  (<30)" if last_rsi < 30 else "NEUTRAL")
            print(f"\n  Latest RSI(14) = {last_rsi:.2f}  -->  {condition}")

    if "volatility" in df.columns:
        last_vol = df["volatility"].dropna().iloc[-1] if df["volatility"].notna().any() else None
        if last_vol is not None:
            print(f"  Latest Volatility  = {last_vol:.4f} daily  (~{last_vol * (252 ** 0.5):.2%} ann.)")

    if "sharpe_14" in df.columns:
        last_s = df["sharpe_14"].dropna().iloc[-1] if df["sharpe_14"].notna().any() else None
        if last_s is not None:
            quality = "Strong (>1)" if last_s > 1 else "Negative (<0)" if last_s < 0 else "Moderate"
            print(f"  Latest Sharpe(14d) = {last_s:.2f}  ({quality})")

    if "price_zscore" in df.columns:
        last_z = df["price_zscore"].dropna().iloc[-1] if df["price_zscore"].notna().any() else None
        if last_z is not None:
            print(f"  Latest Price Z     = {last_z:.2f}  ({'STRETCHED' if abs(last_z) > 2 else 'Normal'})")

    if "drawdown" in df.columns and df["drawdown"].notna().any():
        print(f"  Max Drawdown (30d) = {float(df['drawdown'].min()):.2%}")

    print(f"{separator}\n")


# ── Entry Point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":

    # ── Portfolio: 18 high-conviction tickers across Tech, Crypto, Finance ──────
    TICKERS: list[str] = [
        "NVDA", "AAPL", "MSFT", "AMZN", "META",
        "TSLA", "GOOGL", "AMD",  "PLTR", "AVGO",
        "INTC", "NFLX", "COIN", "CRWD", "NOW",
        "CRM",  "JPM",  "RKLB",
    ]
    DAYS             = 252
    RATE_LIMIT_PAUSE = 1  # yfinance is permissive; SecOps mandates courtesy pauses

    logger.info("=" * 60)
    logger.info(f">>> BATCH PIPELINE START | {len(TICKERS)} tickers | {DAYS}d <<<")
    logger.info("=" * 60)

    pipeline = MarketPipeline()
    results: dict[str, str] = {}

    for idx, symbol in enumerate(TICKERS, start=1):
        logger.info(f"[BATCH] --- [{idx:02d}/{len(TICKERS)}] {symbol} ---")

        try:
            # Extract
            df = pipeline.extract(symbol=symbol, days=DAYS)
            if df is None:
                raise ValueError("extract() returned None")

            # Validate
            _, clean_df = pipeline.validate(df, symbol=symbol)
            if clean_df.empty:
                raise ValueError("No clean records after validation")

            # Transform + Load
            enriched_df  = pipeline.transform(clean_df, symbol=symbol)
            parquet_path = pipeline.load(enriched_df, symbol=symbol)

            results[symbol] = f"ok  | {len(enriched_df)} records | {parquet_path.name}"
            logger.info(f"[BATCH] [{idx:02d}/{len(TICKERS)}] {symbol} -> SUCCESS")

        except Exception as exc:
            # Caveman rule: log loud, skip, continue — one failure != full abort
            reason = f"{type(exc).__name__}: {exc}"
            results[symbol] = f"FAIL | {reason}"
            logger.error(
                f"[BATCH] [{idx:02d}/{len(TICKERS)}] {symbol} -> FAILED | {reason}"
            )

        # Courtesy pause after every ticker (including failures)
        if idx < len(TICKERS):
            logger.debug(f"[BATCH] Courtesy pause {RATE_LIMIT_PAUSE}s ...")
            time.sleep(RATE_LIMIT_PAUSE)

    # ── Batch summary ──────────────────────────────────────────────────────────
    ok_list   = [t for t, r in results.items() if r.startswith("ok")]
    fail_list = [t for t, r in results.items() if r.startswith("FAIL")]

    logger.info("=" * 60)
    logger.info(
        f">>> BATCH COMPLETE | OK={len(ok_list)} | FAILED={len(fail_list)} "
        f"| TOTAL={len(TICKERS)} <<<"
    )
    if ok_list:
        logger.info(f"[BATCH] Succeeded: {ok_list}")
    if fail_list:
        logger.warning(f"[BATCH] Failed   : {fail_list}")
    logger.info("=" * 60)

    # Compact stdout table — Caveman reads this to know if the run was clean
    sep = "-" * 64
    print(f"\n{sep}")
    print(f"  BATCH RUN SUMMARY  —  {len(TICKERS)} tickers  /  {DAYS}d window")
    print(sep)
    for ticker, result in results.items():
        print(f"  {ticker:<6s}  {result}")
    print(sep)
    print(f"  Processed: {len(ok_list)}   Failed: {len(fail_list)}")
    print(f"{sep}\n")

    logger.info(">>> PIPELINE RUN END <<<")
