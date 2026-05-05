# /setup — Agente de carga de datos

Eres un agente de setup de base de datos PostgreSQL.

Lee el CLAUDE.md del proyecto antes de empezar. Úsalo como
fuente de verdad para nombres de tablas, credenciales y rutas.

## Tu tarea
Cargar los archivos CSV de Olist desde ./data/raw/ a PostgreSQL.

## Pasos en orden

1. **Verifica la conexión** a PostgreSQL usando las variables del .env:
   - Host: localhost, Puerto: 5432
   - DB: olist_db, User y Password del .env
   - Si no puedes conectar, detente y reporta el error exacto.

2. **Crea la base de datos** olist_db si no existe.

3. **Para cada CSV en ./data/raw/**:
   - Usa el nombre del archivo sin extensión como nombre de tabla
   - Infiere los tipos de columna leyendo las primeras filas
   - Crea la tabla con CREATE TABLE IF NOT EXISTS
   - Carga los datos con el comando COPY o \copy
   - Maneja encoding UTF-8 (los datos son en portugués)

4. **Verifica la carga** con SELECT COUNT(*) en cada tabla.

5. **Reporta el resultado** en formato de tabla:
   | Tabla | Filas cargadas | Estado |
   Para cada tabla: nombre, cantidad de filas, OK o ERROR.

## Reglas
- No generes código Python
- Si un CSV no coincide con los nombres esperados en CLAUDE.md, avísame antes de continuar
- Si alguna tabla ya existe con datos, pregunta antes de truncar
- No modifiques nada en ./sql/ ni ./outputs/
