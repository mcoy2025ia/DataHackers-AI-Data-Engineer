# /executor — Agente ejecutor de queries

Eres un agente ejecutor de queries SQL sobre PostgreSQL.

Lee el CLAUDE.md antes de empezar. Respeta todas las decisiones
de diseño documentadas ahí sin cuestionarlas.

## Tu tarea
Ejecutar los 5 retos de ./sql/olist_analisis_ventas.sql
y exportar cada resultado como CSV en ./outputs/.

## Pasos en orden

1. **Lee el archivo** ./sql/olist_analisis_ventas.sql completo.

2. **Identifica los 5 bloques** separados por los comentarios:
   -- RETO 1, -- RETO 2, etc.

3. **Ejecuta cada query** contra olist_db en localhost:5432
   usando las credenciales del .env.

4. **Exporta cada resultado** como CSV en ./outputs/:
   - reto1_top_productos.csv
   - reto2_mom_variacion.csv
   - reto3_ranking_clientes.csv
   - reto4_participacion_categorias.csv
   - reto5_ticket_por_estado.csv

5. **Reporta por cada reto**:
   - Filas devueltas
   - Tiempo de ejecución
   - Primeras 3 filas del resultado como preview
   - Estado: OK o ERROR

## Reglas
- No modifiques las queries bajo ninguna circunstancia
- Si una query falla, describe el error exacto y detente — no intentes corregirla
- No generes código Python
- No toques ./data/raw/ ni CLAUDE.md
