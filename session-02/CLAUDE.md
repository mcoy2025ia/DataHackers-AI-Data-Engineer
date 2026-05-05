# CLAUDE.md — Olist SQL Analysis Project

## Contexto del proyecto
Mini-proyecto académico para DataHackers Academy — SQL Masterclass.
Dataset: Brazilian E-Commerce (Olist) de Kaggle.
Motor SQL: PostgreSQL (local).
Objetivo: Resolver 5 retos de análisis de ventas con SQL avanzado
y producir un README con interpretaciones de negocio.

## Stack
- PostgreSQL corriendo en localhost:5432
- Base de datos: olist_db
- Usuario y password definidos en .env (ver .env.example)
- CSVs fuente en: ./data/raw/
- Queries en: ./sql/
- Resultados exportados en: ./outputs/

## Nombres de tablas (estándar Olist)
| Archivo CSV                                        | Tabla en PostgreSQL                    |
|----------------------------------------------------|----------------------------------------|
| olist_orders_dataset.csv                           | olist_orders_dataset                   |
| olist_order_items_dataset.csv                      | olist_order_items_dataset              |
| olist_order_payments_dataset.csv                   | olist_order_payments_dataset           |
| olist_customers_dataset.csv                        | olist_customers_dataset                |
| olist_products_dataset.csv                         | olist_products_dataset                 |
| olist_sellers_dataset.csv                          | olist_sellers_dataset                  |
| product_category_name_translation.csv              | product_category_name_translation      |

## Decisiones de diseño (no cambiar sin justificación documentada)
- **Revenue** = `order_items.price` — precio neto del ítem vendido.
  No usar `payment_value` de order_payments: puede duplicar montos
  en órdenes pagadas en cuotas o con múltiples métodos de pago.
- **Filtro base** = `order_status = 'delivered'` en todos los retos.
  Excluimos canceladas, en tránsito e invoiced para trabajar solo
  con transacciones completadas y reales.
- **Reto 5** usa `price + freight_value` porque la pregunta es geográfica.
  El flete es discriminante por región y forma parte del gasto real del cliente.
- **Clientes** identificados por `customer_unique_id`, no `customer_id`.
  Olist genera un customer_id nuevo por cada orden — agrupar por customer_id
  haría parecer que todos los clientes compraron exactamente una vez.
- **Categorías** siempre traducidas al inglés via `product_category_name_translation`.

## Lo que NO debes hacer
- No modificar las queries de ./sql/ sin documentar el cambio en un comentario SQL
- No usar `payment_value` como revenue
- No agrupar clientes por `customer_id`
- No generar código Python ni notebooks — este proyecto es solo SQL y Markdown
- No subir archivos .csv ni .env al repositorio (ver .gitignore)

## Agentes disponibles (slash commands)
Están en .claude/commands/ e invocables directamente en Claude Code:

| Comando     | Qué hace                                              | Cuándo usarlo         |
|-------------|-------------------------------------------------------|-----------------------|
| `/setup`    | Carga los CSVs a PostgreSQL                           | Una sola vez, al inicio |
| `/executor` | Corre las 5 queries y exporta resultados a /outputs   | Después del setup     |
| `/analyst`  | Lee los CSVs de /outputs y genera interpretaciones    | Después del executor  |
| `/writer`   | Ensambla el README.md final entregable                | Al final              |

## Flujo de trabajo
```
/setup → /executor → /analyst → /writer → revisar README.md → push a GitHub
```

Cada agente se corre en una conversación nueva de Claude Code.
No encadenes los 4 en el mismo chat.
