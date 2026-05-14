import os

from dotenv import load_dotenv
from groq import Groq

load_dotenv()


class MarketExpert:
    """Senior Quant Strategist — powered by Groq LLM (llama-3.3-70b-versatile)."""

    def __init__(self) -> None:
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise EnvironmentError(
                "GROQ_API_KEY not found. Add it to your .env file: GROQ_API_KEY=gsk_..."
            )
        self.client = Groq(api_key=api_key)
        self.model = "llama-3.3-70b-versatile"

    def get_comparison_report(
        self,
        target_ticker: str,
        target_metrics: dict,
        peers_data: list[dict],
    ) -> str:
        """
        Generate a strategic relative-value analysis comparing target_ticker to its sector peers.

        target_metrics: {"rsi": float, "sharpe": float, "zscore": float, "drawdown": float}
        peers_data:     [{"symbol": str, "sharpe": float, "zscore": float}, ...]
        """
        peers_context = "\n".join(
            f"- {p['symbol']}: Sharpe {p['sharpe']:.2f}, Z-Score {p['zscore']:.2f}"
            for p in peers_data
        )

        sector_sharpes = [p["sharpe"] for p in peers_data]
        avg_sharpe = sum(sector_sharpes) / len(sector_sharpes) if sector_sharpes else 0.0

        prompt = f"""
Eres un Senior Quant Strategist con 20 años en mercados institucionales.
Analiza la siguiente situación de mercado con precision y brevedad (estilo Bloomberg Terminal).

═══════════════════════════════════════
ACTIVO OBJETIVO: {target_ticker}
═══════════════════════════════════════
- RSI (14d):          {target_metrics['rsi']:.2f}
- Sharpe Ratio (14d): {target_metrics['sharpe']:.2f}
- Price Z-Score:      {target_metrics['zscore']:.2f}
- Max Drawdown (30d): {target_metrics['drawdown']:.2f}%

═══════════════════════════════════════
COMPETIDORES DEL SECTOR:
═══════════════════════════════════════
{peers_context}

Sharpe promedio del sector: {avg_sharpe:.2f}

═══════════════════════════════════════
TAREA:
═══════════════════════════════════════
1. VALOR RELATIVO: ¿Está {target_ticker} más caro o barato que sus pares según Z-Score?
2. CALIDAD DE TENDENCIA: ¿Su Sharpe supera al promedio sectorial ({avg_sharpe:.2f})?
3. RIESGO: ¿El drawdown y RSI actual configuran alguna señal de alerta?
4. CONCLUSIÓN: Recomendación táctica clara — HIGH / MEDIUM / LOW Conviction.

Formato: Markdown. Directo. Sin adornos. Máximo 250 palabras.
"""
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=512,
            )
            return completion.choices[0].message.content
        except Exception as exc:
            return f"**Error en el análisis:** {exc}"
