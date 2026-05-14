# 🛠️ Guía de Desarrollo: Mini-Proyecto 3 - Ingesta FinTech (Finnhub)

## 📋 Comandos de Ejecución (Ecosistema `uv` + RTK)
- **Activar Entorno:** `source venv_DE_DS_E3/Scripts/activate`
- **Instalar RTK:** `uv pip install rtk-cli` (Proxy para Claude Code)
- **Ejecutar con RTK:** `rtk python src/pipeline.py` (Filtra la salida de la terminal)
- **Optimización Repomix:** `npx repomix --exclude "venv_DE_DS_E3/**,data/**,logs/**"`

## 🧩 Orquestación del Comité
1. **Senior Data Engineer (L5):** Resiliencia, Idempotencia y Parquet.
2. **Business Analyst:** Valor de negocio y métricas (RSI, Volatilidad).
3. **Trader Experto:** Precisión de timestamps e integridad financiera.
4. **Token Architect:** El estratega del contexto. Usa **Repomix** para el código y **RTK** para filtrar el ruido de la terminal.
5. **The Caveman:** Simplicidad radical. Exige que incluso con RTK, los errores críticos sean "ruidosos".
6. **SecOps Architect:** Guardián de la seguridad. Impone el manejo de secretos vía `.env`, valida el `.gitignore` y asegura que los logs sean seguros.

## 🚀 Flujo de Token Efficiency (The 2026 Standard)
- **Repomix (Estático):** Empaqueta el repo en XML para que Claude entienda la estructura total con bajo costo.
- **RTK (Dinámico):** Sitúa a **RTK** como interceptor de comandos Bash. Si el pipeline falla, RTK asegura que Claude solo reciba el Traceback relevante y no 1000 líneas de logs repetitivos.