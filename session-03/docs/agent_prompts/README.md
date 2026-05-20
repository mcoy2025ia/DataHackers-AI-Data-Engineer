# Agent Prompts — Metodología de Desarrollo

Estos archivos son los perfiles de rol utilizados como contexto de sistema
para Claude Code durante el desarrollo del SMIF Pipeline.

Cada archivo define el mandato, criterios de calidad y protocolo de conflicto
de un "agente" especializado. En la práctica, son system prompts que estructuran
las sesiones de Claude Code para producir decisiones consistentes con un rol específico.

**No son componentes de runtime del sistema.**
El único componente de runtime es `src/agents/market_expert.py` (cliente Groq).

| Archivo | Rol en el desarrollo |
|---------|---------------------|
| Data_Engineer.md | Criterios de resiliencia, idempotencia y Parquet |
| Business_Analyst.md | Criterios de valor de negocio por transformación |
| Trader_Experto.md | Criterios de integridad financiera y señales |
| Security_Compliance.md | Criterios de seguridad, cumplimiento y manejo de riesgos |
| Token_Architect.md | Estrategia de eficiencia de contexto (Repomix/RTK) |
| Caveman.md | Criterios de simplicidad y visibilidad de errores |
