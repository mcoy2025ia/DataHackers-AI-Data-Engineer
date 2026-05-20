"""
Market Pipeline Dashboard — Streamlit app.
Reads data/output/{symbol}_data.parquet produced by src/pipeline.py.
Views: Trader, Business Analyst, Pipeline Health, Advanced Analytics + Groq AI.
"""

import datetime
import json
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).parent.parent
OUTPUT_DIR     = BASE_DIR / "data" / "output"
QUARANTINE_DIR = BASE_DIR / "data" / "raw" / "quarantine"

# ── Ensure src/ is importable so agents/ and utils/ resolve correctly ──────────
_SRC_DIR = str(Path(__file__).parent)
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from agents.market_expert import MarketExpert  # noqa: E402
from utils.taxonomy import get_peers, get_sector  # noqa: E402

SIGNAL_PALETTE = {
    "OVERBOUGHT": "#ef4444",
    "OVERSOLD":   "#22c55e",
    "NEUTRAL":    "#3b82f6",
}


# ── Data helpers ───────────────────────────────────────────────────────────────
@st.cache_data
def load_parquet(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    if "timestamp" in df.columns:
        df = df.set_index("timestamp").sort_index()
    return df


def discover_symbols() -> list[str]:
    return sorted(
        p.stem.replace("_data", "")
        for p in OUTPUT_DIR.glob("*_data.parquet")
    )


@st.cache_data
def load_quarantine(symbol: str) -> pd.DataFrame:
    files = sorted(QUARANTINE_DIR.glob(f"{symbol}_*_outliers.json"), reverse=True)
    if not files:
        return pd.DataFrame()
    rows = []
    for f in files:
        for rec in json.loads(f.read_text(encoding="utf-8")):
            rec["_file"] = f.name
            rows.append(rec)
    return pd.DataFrame(rows)


# ── Chart builders ─────────────────────────────────────────────────────────────
def _chart_close(df: pd.DataFrame, symbol: str) -> alt.LayerChart:
    src = df[["close"]].reset_index()
    area = (
        alt.Chart(src)
        .mark_area(color="#93c5fd", opacity=0.25, line=False)
        .encode(
            x=alt.X("timestamp:T", title=None, axis=alt.Axis(format="%b %d")),
            y=alt.Y("close:Q", scale=alt.Scale(zero=False), title="Price (USD)"),
        )
    )
    line = (
        alt.Chart(src)
        .mark_line(color="#2563eb", strokeWidth=2)
        .encode(
            x="timestamp:T",
            y=alt.Y("close:Q", scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip("timestamp:T", title="Date", format="%Y-%m-%d"),
                alt.Tooltip("close:Q",     title="Close",  format="$.2f"),
            ],
        )
    )
    return (area + line).properties(
        title=f"{symbol} — Close Price", height=220
    )


def _chart_rsi(df: pd.DataFrame) -> alt.LayerChart:
    src = df[["rsi_14"]].dropna().reset_index()
    rsi_line = (
        alt.Chart(src)
        .mark_line(color="#f59e0b", strokeWidth=2)
        .encode(
            x=alt.X("timestamp:T", title=None, axis=alt.Axis(format="%b %d")),
            y=alt.Y("rsi_14:Q", scale=alt.Scale(domain=[0, 100]), title="RSI"),
            tooltip=[
                alt.Tooltip("timestamp:T", title="Date", format="%Y-%m-%d"),
                alt.Tooltip("rsi_14:Q",    title="RSI",  format=".1f"),
            ],
        )
    )
    rule_70 = (
        alt.Chart(pd.DataFrame({"y": [70]}))
        .mark_rule(color="#ef4444", strokeDash=[6, 3], strokeWidth=1.5)
        .encode(y="y:Q")
    )
    rule_30 = (
        alt.Chart(pd.DataFrame({"y": [30]}))
        .mark_rule(color="#22c55e", strokeDash=[6, 3], strokeWidth=1.5)
        .encode(y="y:Q")
    )
    return (rsi_line + rule_70 + rule_30).properties(
        title="RSI (14)  |  red line = overbought (70)  |  green line = oversold (30)",
        height=180,
    )


def _chart_zscore(df: pd.DataFrame) -> alt.LayerChart | None:
    if "price_zscore" not in df.columns or df["price_zscore"].notna().sum() < 2:
        return None
    src = df[["price_zscore"]].dropna().reset_index()

    # Shaded normal band between -2 and +2
    band = (
        alt.Chart(pd.DataFrame({"y1": [-2.0], "y2": [2.0]}))
        .mark_rect(color="#86efac", opacity=0.15)
        .encode(y="y1:Q", y2="y2:Q")
    )
    zero = (
        alt.Chart(pd.DataFrame({"y": [0.0]}))
        .mark_rule(color="#94a3b8", strokeDash=[4, 4], strokeWidth=1)
        .encode(y="y:Q")
    )
    z_line = (
        alt.Chart(src)
        .mark_line(color="#7c3aed", strokeWidth=2)
        .encode(
            x=alt.X("timestamp:T", title=None, axis=alt.Axis(format="%b %d")),
            y=alt.Y("price_zscore:Q", title="Z-Score (std devs)"),
            tooltip=[
                alt.Tooltip("timestamp:T",   title="Date",    format="%Y-%m-%d"),
                alt.Tooltip("price_zscore:Q", title="Z-Score", format=".2f"),
            ],
        )
    )
    # Red dots on reversal signals (|Z| > 2)
    reversal = (
        alt.Chart(src[src["price_zscore"].abs() > 2])
        .mark_point(color="#ef4444", size=100, filled=True)
        .encode(
            x="timestamp:T",
            y="price_zscore:Q",
            tooltip=[
                alt.Tooltip("timestamp:T",   title="Date",    format="%Y-%m-%d"),
                alt.Tooltip("price_zscore:Q", title="Z-Score", format=".2f"),
            ],
        )
    )
    return (band + zero + z_line + reversal).properties(
        title=(
            "Price Z-Score vs EMA(20)  |  "
            "green band = normal range [-2, +2]  |  red dot = mean-reversion signal"
        ),
        height=260,
    )


def _chart_sharpe(df: pd.DataFrame) -> alt.LayerChart | None:
    if "sharpe_14" not in df.columns or df["sharpe_14"].notna().sum() < 2:
        return None
    src = df[["sharpe_14"]].dropna().reset_index()
    zero = (
        alt.Chart(pd.DataFrame({"y": [0.0]}))
        .mark_rule(color="#94a3b8", strokeDash=[4, 4], strokeWidth=1)
        .encode(y="y:Q")
    )
    bars = (
        alt.Chart(src)
        .mark_bar(opacity=0.8)
        .encode(
            x=alt.X("timestamp:T", title=None, axis=alt.Axis(format="%b %d")),
            y=alt.Y("sharpe_14:Q", title="Sharpe (ann.)", axis=alt.Axis(format=".1f")),
            color=alt.condition(
                alt.datum.sharpe_14 >= 0,
                alt.value("#22c55e"),
                alt.value("#ef4444"),
            ),
            tooltip=[
                alt.Tooltip("timestamp:T", title="Date",   format="%Y-%m-%d"),
                alt.Tooltip("sharpe_14:Q", title="Sharpe", format=".2f"),
            ],
        )
    )
    return (zero + bars).properties(
        title="Rolling Sharpe Ratio (14d ann.)  |  green = positive return quality",
        height=210,
    )


def _chart_drawdown(df: pd.DataFrame) -> alt.Chart | None:
    if "drawdown" not in df.columns or df["drawdown"].notna().sum() < 2:
        return None
    src = df[["drawdown"]].dropna().reset_index()
    src["zero"] = 0.0
    return (
        alt.Chart(src)
        .mark_area(
            color="#ef4444", opacity=0.45,
            line={"color": "#dc2626", "strokeWidth": 1.5},
        )
        .encode(
            x=alt.X("timestamp:T", title=None, axis=alt.Axis(format="%b %d")),
            y=alt.Y("drawdown:Q", title="Drawdown", axis=alt.Axis(format=".1%"),
                    scale=alt.Scale(domainMax=0)),
            y2=alt.Y2("zero:Q"),
            tooltip=[
                alt.Tooltip("timestamp:T", title="Date",     format="%Y-%m-%d"),
                alt.Tooltip("drawdown:Q",  title="Drawdown", format=".2%"),
            ],
        )
        .properties(title="Rolling Max Drawdown (30d window)", height=210)
    )


# ── Trader Intelligence narrative ─────────────────────────────────────────────
def _trader_intelligence(
    z: float | None,
    sharpe: float | None,
    drawdown: float | None,
) -> None:
    """
    Automatic market narrative for the Trader persona.
    Caveman rule: one signal per line, direct language, zero filler.
    """
    st.markdown("#### >> Trader Intelligence")

    # ── Z-Score state ──────────────────────────────────────────────────────────
    if z is None:
        st.info("**[~] ESTADO: N/A** — Not enough data for Z-Score analysis.")
    elif z > 2.0:
        st.warning(
            f"**[!] ESTADO: EUPHORIA / STRETCHED** — Z = {z:+.2f}  \n"
            f"Precio {z:.1f}σ por encima de EMA(20). "
            "El precio esta estadisticamente lejos de su media: riesgo de pullback alto. "
            "Evitar nuevas entradas largas."
        )
    elif z < -2.0:
        st.success(
            f"**[+] ESTADO: MEAN REVERSION OPPORTUNITY** — Z = {z:+.2f}  \n"
            f"Precio {abs(z):.1f}σ por debajo de EMA(20). "
            "Activo subvaluado respecto a su tendencia reciente. "
            "Alta probabilidad estadistica de rebote hacia la media."
        )
    else:
        st.info(
            f"**[~] ESTADO: STABLE** — Z = {z:+.2f}  \n"
            "Precio dentro del rango normal [-2, +2]. Sin estiramiento extremo en este momento."
        )

    # ── Trend quality (only surfaces when truly elite) ─────────────────────────
    if sharpe is not None and sharpe > 3.0:
        st.markdown(
            f"**[*] TENDENCIA DE ALTA CALIDAD (ELITE)** — Sharpe {sharpe:.2f}  \n"
            "Retornos consistentes y eficientes: el riesgo esta bajo control. "
            "Menos del 5% de los activos alcanzan un Sharpe anualizado > 3.0."
        )

    # ── Drawdown context ───────────────────────────────────────────────────────
    if drawdown is not None:
        if drawdown == 0.0:
            st.markdown(
                "**[^] PRICE DISCOVERY** — Drawdown 0.00%  \n"
                "El activo cotiza en sus maximos de los ultimos 30 dias. "
                "No hay resistencia tecnica visible en esta ventana."
            )
        elif drawdown < -0.10:
            st.markdown(
                f"**[v] DRAWDOWN SIGNIFICATIVO** — {drawdown:.2%} desde el pico (30d)  \n"
                "Caida relevante desde el ultimo maximo. Zona de posible capitulacion o soporte."
            )
        else:
            st.markdown(
                f"**[-] DRAWDOWN ACTIVO** — {drawdown:.2%} desde el pico (30d)  \n"
                "Retroceso menor dentro de parametros normales de volatilidad."
            )


# ── View sections ──────────────────────────────────────────────────────────────
def view_trader(df: pd.DataFrame, symbol: str) -> None:
    st.subheader("Trader View")
    st.altair_chart(_chart_close(df, symbol), use_container_width=True)
    if df["rsi_14"].notna().any():
        st.altair_chart(_chart_rsi(df), use_container_width=True)
    else:
        st.info("Not enough data to compute RSI(14). Need at least 14 candles.")


def view_ba(df: pd.DataFrame) -> None:
    st.subheader("Business Analyst View")

    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last

    vol_now  = last["volatility"] if pd.notna(last["volatility"]) else 0.0
    ann_vol  = vol_now * (252 ** 0.5)
    rsi_now  = last["rsi_14"] if pd.notna(last["rsi_14"]) else None
    close_delta = last["close"] - prev["close"]
    sharpe_now = (
        last["sharpe_14"]
        if "sharpe_14" in df.columns and pd.notna(last["sharpe_14"])
        else None
    )

    # ── KPI strip ─────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Close Price",      f"${last['close']:.2f}", f"{close_delta:+.2f}")
    c2.metric("RSI (14)",         f"{rsi_now:.1f}" if rsi_now else "N/A",
              f"{rsi_now - 50:+.1f} vs neutral" if rsi_now else None)
    c3.metric("Volatility (daily)", f"{vol_now:.4f}",
              f"~{ann_vol:.1%} ann.", delta_color="off")
    if sharpe_now is not None:
        quality = "Strong" if sharpe_now > 1 else "Weak" if sharpe_now < 0 else "Moderate"
        c4.metric("Sharpe (14d ann.)", f"{sharpe_now:.2f}", quality,
                  delta_color="normal" if sharpe_now >= 0 else "inverse")
    else:
        c4.metric("Sharpe (14d ann.)", "N/A")

    # ── Signal breakdown ───────────────────────────────────────────────────────
    st.markdown("#### Signal Breakdown")
    active = df[df["signal"].isin(SIGNAL_PALETTE)]
    total  = len(active)

    for sig, color in SIGNAL_PALETTE.items():
        count = int((df["signal"] == sig).sum())
        pct   = count / total if total > 0 else 0.0
        ca, cb = st.columns([1, 4])
        ca.markdown(f"**{sig}**")
        cb.progress(pct, text=f"{count} day(s)  ({pct:.0%})")

    # ── Data table ─────────────────────────────────────────────────────────────
    st.markdown("#### Full Analytics Table")
    show = ["close", "rsi_14", "volatility", "pct_change",
            "price_zscore", "sharpe_14", "drawdown", "signal"]
    fmt  = {"close": "${:.2f}", "rsi_14": "{:.1f}", "volatility": "{:.4f}",
            "pct_change": "{:+.2f}%", "price_zscore": "{:.2f}",
            "sharpe_14": "{:.2f}", "drawdown": "{:.2%}"}
    cols = [c for c in show if c in df.columns]
    st.dataframe(
        df[cols].sort_index(ascending=False)
               .style.format({k: v for k, v in fmt.items() if k in cols}),
        use_container_width=True,
        height=300,
    )


def view_health(df: pd.DataFrame, symbol: str) -> None:
    st.subheader("Pipeline Health  (Caveman View)")

    q_df    = load_quarantine(symbol)
    q_count = len(q_df)
    p_count = len(df)
    total   = p_count + q_count
    health  = p_count / total if total > 0 else 1.0

    c1, c2, c3 = st.columns(3)
    c1.metric("Records in Production",    p_count)
    c2.metric("Records in DLQ (Quarantine)", q_count, delta_color="inverse")
    c3.metric("Dataset Health", f"{health:.1%}",
              delta="HEALTHY" if health >= 0.9 else "DEGRADED",
              delta_color="normal" if health >= 0.9 else "inverse")

    st.progress(health)

    if q_count > 0:
        st.warning(
            f"{q_count} record(s) flagged by Z-Score (|Z| > 3.0) were routed to "
            "`data/raw/quarantine/` instead of being silently dropped. "
            "Review before any backtesting run."
        )
        st.markdown("#### Quarantined Records")
        st.dataframe(q_df, use_container_width=True)
    else:
        st.success("No quarantined records — all data passed the quality gate.")

    with st.expander("What is the DLQ / Quarantine?"):
        st.markdown(
            "The **Dead Letter Queue** stores records that failed validation "
            "(Z-Score > 3.0 on close price). They are never deleted — kept as "
            "JSON in `data/raw/quarantine/` for audit and regulatory traceability. "
            "See `CLAUDE_LOG.md #003` for the full committee decision."
        )


def view_advanced(df: pd.DataFrame, symbol: str) -> None:
    st.subheader("Advanced Analytics — Risk & Mean Reversion")

    # ── Price Z-Score (full width) ─────────────────────────────────────────────
    st.markdown("#### Price Z-Score vs EMA(20)")
    st.caption(
        "How many standard deviations is the close price from its 20-day EMA? "
        "Values beyond **±2** suggest the price is stretched and likely to revert. "
        "Red dots are actionable mean-reversion signals."
    )
    z_chart = _chart_zscore(df)
    if z_chart:
        st.altair_chart(z_chart, use_container_width=True)
    else:
        st.info("Need at least 5 candles to compute Price Z-Score.")

    st.divider()

    # ── Sharpe + Drawdown side by side ─────────────────────────────────────────
    col_s, col_d = st.columns(2)

    with col_s:
        st.markdown("#### Rolling Sharpe Ratio (14d)")
        st.caption("( μ_returns / σ_returns ) × √252  |  rf = 0")
        s_chart = _chart_sharpe(df)
        if s_chart:
            st.altair_chart(s_chart, use_container_width=True)
        else:
            st.info("Need at least 5 candles for Sharpe.")

    with col_d:
        st.markdown("#### Rolling Drawdown (30d window)")
        st.caption("( close − rolling_peak ) / rolling_peak")
        d_chart = _chart_drawdown(df)
        if d_chart:
            st.altair_chart(d_chart, use_container_width=True)
        else:
            st.info("Not enough data for Drawdown.")

    # ── Summary metrics ────────────────────────────────────────────────────────
    st.divider()
    has_dd     = "drawdown" in df.columns and df["drawdown"].notna().any()
    has_sharpe = "sharpe_14" in df.columns and df["sharpe_14"].notna().any()
    has_z      = "price_zscore" in df.columns and df["price_zscore"].notna().any()

    cm1, cm2, cm3 = st.columns(3)

    if has_dd:
        mdd = float(df["drawdown"].min())
        cm1.metric("Max Drawdown (30d)", f"{mdd:.2%}", delta_color="inverse")

    if has_sharpe:
        latest_s = float(df["sharpe_14"].dropna().iloc[-1])
        quality  = "Strong" if latest_s > 1 else "Negative" if latest_s < 0 else "Moderate"
        cm2.metric("Latest Sharpe (14d)", f"{latest_s:.2f}", quality,
                   delta_color="normal" if latest_s >= 0 else "inverse")

    if has_z:
        latest_z   = float(df["price_zscore"].dropna().iloc[-1])
        z_label    = "Stretched (|Z|>2)" if abs(latest_z) > 2 else "Normal range"
        cm3.metric("Latest Price Z-Score", f"{latest_z:.2f}", z_label,
                   delta_color="off")

    # ── Trader Intelligence — narrative layer ──────────────────────────────────
    st.divider()
    _trader_intelligence(
        z        = float(df["price_zscore"].dropna().iloc[-1]) if has_z      else None,
        sharpe   = float(df["sharpe_14"].dropna().iloc[-1])    if has_sharpe else None,
        drawdown = float(df["drawdown"].dropna().iloc[-1])     if has_dd     else None,
    )

    # ── Groq Relative Value Analysis ───────────────────────────────────────────
    st.divider()
    sector = get_sector(symbol)
    peers  = get_peers(symbol)

    st.markdown("#### Analisis de Valor Relativo (Groq AI)")
    if sector:
        st.caption(f"Sector: **{sector}** — {len(peers)} competidor(es) en taxonomia")

    if not peers:
        st.info(
            f"**{symbol}** no tiene sector definido en la taxonomia. "
            "No es posible el análisis comparativo."
        )
        return

    # ── Collect available peer metrics ─────────────────────────────────────────
    peers_data:    list[dict] = []
    missing_peers: list[str]  = []

    for peer in peers:
        peer_path = OUTPUT_DIR / f"{peer}_data.parquet"
        if not peer_path.exists():
            missing_peers.append(peer)
            continue
        try:
            peer_df = pd.read_parquet(str(peer_path))
            if "timestamp" in peer_df.columns:
                peer_df = peer_df.set_index("timestamp").sort_index()
            peer_last = peer_df.iloc[-1]
            peers_data.append({
                "symbol": peer,
                "sharpe": float(peer_last["sharpe_14"])
                          if "sharpe_14" in peer_df.columns and pd.notna(peer_last["sharpe_14"])
                          else 0.0,
                "zscore": float(peer_last["price_zscore"])
                          if "price_zscore" in peer_df.columns and pd.notna(peer_last["price_zscore"])
                          else 0.0,
            })
        except Exception:
            missing_peers.append(peer)

    if missing_peers:
        st.caption(
            f"Sin datos: {', '.join(missing_peers)}. "
            "Ejecuta `python src/pipeline.py` para generarlos."
        )

    if not peers_data:
        st.warning(
            f"Ningún competidor de **{symbol}** tiene archivo Parquet en `data/output/`. "
            "Ejecuta el pipeline primero y vuelve a cargar el dashboard."
        )
        return

    # ── Peer snapshot table ────────────────────────────────────────────────────
    peers_preview = pd.DataFrame(peers_data).set_index("symbol")
    with st.expander(f"Metricas de pares cargados ({len(peers_data)})"):
        st.dataframe(
            peers_preview.style.format({"sharpe": "{:.2f}", "zscore": "{:.2f}"}),
            use_container_width=True,
        )

    # ── Groq trigger button ────────────────────────────────────────────────────
    if st.button("Ejecutar Analisis de Valor Relativo (Groq)",
                 use_container_width=True, type="primary"):

        target_metrics = {
            "rsi":      float(df["rsi_14"].dropna().iloc[-1])
                        if "rsi_14" in df.columns and df["rsi_14"].notna().any() else 0.0,
            "sharpe":   float(df["sharpe_14"].dropna().iloc[-1])
                        if has_sharpe else 0.0,
            "zscore":   float(df["price_zscore"].dropna().iloc[-1])
                        if has_z else 0.0,
            "drawdown": float(df["drawdown"].dropna().iloc[-1]) * 100.0
                        if has_dd else 0.0,
        }

        try:
            expert = MarketExpert()
        except EnvironmentError as exc:
            st.error(
                f"**Configuracion requerida:** {exc}  \n"
                "Agrega `GROQ_API_KEY=gsk_...` a tu archivo `.env` y recarga el dashboard."
            )
            return

        with st.spinner("Consultando con el comite de estrategia..."):
            report = expert.get_comparison_report(symbol, target_metrics, peers_data)

        st.chat_message("assistant").markdown(report)


# ── App layout ─────────────────────────────────────────────────────────────────
def main() -> None:
    st.set_page_config(
        page_title="Market Intelligence Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    with st.sidebar:
        st.title("Market Pipeline")
        st.caption("Mini-Proyecto 3 — Datapath")
        st.divider()

        symbols = discover_symbols()
        if not symbols:
            st.error(
                "No Parquet files found in `data/output/`.\n\n"
                "Run the pipeline first:\n```\npython src/pipeline.py\n```"
            )
            st.stop()

        symbol = st.selectbox("Symbol", symbols)
        parquet_path = OUTPUT_DIR / f"{symbol}_data.parquet"

        mtime = datetime.datetime.fromtimestamp(parquet_path.stat().st_mtime)
        st.caption(f"Last pipeline run: `{mtime.strftime('%Y-%m-%d %H:%M')}`")

        if st.button("Refresh", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.divider()
        st.markdown("**Stack**")
        st.caption("yfinance · tenacity · pyarrow · Streamlit · Altair · Groq")

    df = load_parquet(str(parquet_path))

    st.title(f"{symbol} — Market Intelligence Dashboard")
    last_ts = df.index[-1].strftime("%Y-%m-%d") if not df.empty else "N/A"
    st.caption(f"{len(df)} trading days  |  latest: {last_ts}")
    st.divider()

    tab_t, tab_ba, tab_h, tab_adv = st.tabs(
        ["Trader", "Business Analyst", "Pipeline Health", "Advanced Analytics"]
    )

    with tab_t:
        view_trader(df, symbol)

    with tab_ba:
        view_ba(df)

    with tab_h:
        view_health(df, symbol)

    with tab_adv:
        view_advanced(df, symbol)


if __name__ == "__main__":
    main()
