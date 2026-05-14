import os
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

class MarketExpert:
    def __init__(self):
        self.client = Groq(api_key=os.getenv("GROQ_API_KEY"))
        self.model = "llama-3.3-70b-versatile" # Modelo de alta capacidad de razonamiento

    def get_comparison_report(self, target_ticker, target_metrics, peers_data):
        \"\"\"
        Genera un análisis estratégico comparando el ticker objetivo con sus pares.
        target_metrics: dict con {rsi, sharpe, zscore, drawdown}
        peers_data: lista de dicts con métricas de competidores
        \"\"\"
        
        # Construcción del contexto para el LLM
        peers_context = ""
        for peer in peers_data:
            peers_context += f"- {peer['symbol']}: Sharpe {peer['sharpe']:.2f}, Z-Score {peer['zscore']:.2f}\\n"

        prompt = f\"\"\"
        Eres un Senior Quant Strategist. Analiza la siguiente situación de mercado:
        
        ACTIVO OBJETIVO: {target_ticker}
        MÉTRICAS:
        - RSI: {target_metrics['rsi']:.2f}
        - Sharpe Ratio (14d): {target_metrics['sharpe']:.2f}
        - Price Z-Score: {target_metrics['zscore']:.2f}
        - Max Drawdown: {target_metrics['drawdown']:.2f}%
        
        COMPETIDORES DEL SECTOR:
        {peers_context}
        
        TAREA:
        1. Compara el valor relativo: ¿Está el activo objetivo más 'caro' o 'barato' que sus pares según el Z-Score?
        2. Evalúa la calidad de la tendencia: ¿Su Sharpe Ratio es superior al promedio del sector?
        3. Concluye con una recomendación táctica (High/Medium/Low Conviction).
        
        Responde en Markdown, sé directo y profesional (estilo Terminal Bloomberg).
        \"\"\"

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2, # Baja temperatura para análisis técnico preciso
            )
            return completion.choices[0].message.content
        except Exception as e:
            return f"⚠️ Error en el análisis de IA: {str(e)}"
```"