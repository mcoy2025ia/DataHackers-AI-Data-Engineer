# Interpretaciones de Negocio — Olist E-Commerce
**Período de análisis:** septiembre 2016 – agosto 2018  
**Filtro aplicado:** solo órdenes entregadas (`order_status = 'delivered'`)  
**Revenue definido como:** precio neto del ítem (`order_items.price`)

---

### Reto 1: Top 10 Productos por Ingresos

**Hallazgos**

- **Health & Beauty domina el top con dos productos que suman $117.212 en ingresos.** El primer lugar genera $63.560 con un ticket promedio de $327 por unidad, y el segundo $53.652 con $350 promedio. Ambos combinan volumen razonable (186 y 148 órdenes) con precio unitario alto — perfil ideal de producto estrella.

- **Computers tiene el ticket promedio más alto del ranking: $1.392 por unidad**, con solo 33 órdenes y $45.949 en ingresos. Un solo quiebre de stock en este producto puede costar más de $1.300 por venta perdida; el margen de error logístico es mínimo.

- **La categoría Baby logra $38.907 con apenas 38 órdenes** (ticket promedio $1.024), lo que lo convierte en el producto de mayor valor relativo por transacción. Baja exposición de ventas pero altísimo impacto por orden — extremadamente sensible a la disponibilidad.

- **Bed & Bath Table y Furniture Decor ganan por volumen, no por precio.** El producto de bed_bath_table acumula 456 órdenes con un ticket de $88 y el de furniture_decor 425 órdenes a $71. Son motores de tráfico y frecuencia, no de margen por ítem.

- **El top 10 abarca cuatro categorías distintas** (health_beauty, computers, computers_accessories, bed_bath_table, baby, cool_stuff, watches_gifts, furniture_decor), lo que indica que el revenue no depende de un solo segmento — fortaleza estructural del catálogo.

**Recomendación**

El equipo **Comercial y Logística** debe establecer políticas de stock diferenciadas por perfil de producto: los dos productos de health_beauty y el de baby requieren disponibilidad garantizada de forma permanente dado su ticket alto combinado con volumen sostenido. Para computers (33 órdenes, $1.392 ticket), la prioridad es eliminar los tiempos de reposición — una semana de quiebre equivale a ~$10.000 en ventas no realizadas. Los productos de volumen bajo/ticket alto (baby, computers) son candidatos ideales para alertas de stock automáticas antes de llegar a cero unidades.

---

### Reto 2: Variación de Ventas Month-over-Month (MoM)

**Hallazgos**

- **Noviembre 2017 fue el mes cumbre del período: $987.765 en ingresos y 7.289 órdenes.** Esto corresponde al Black Friday brasileño (*Sexta-feira Negra*), que en 2017 cayó el 24 de noviembre. El crecimiento MoM fue de +52.4% respecto a octubre. Es el único mes que superó los $900K.

- **Diciembre 2017 cayó un 26.5% inmediatamente después**, retrocediendo a $726.033. La demanda se adelantó masivamente al Black Friday y dejó diciembre deprimido — patrón clásico de canibalización post-promoción en e-commerce.

- **El negocio creció de forma sostenida a lo largo de 2017:** de $111.798 en enero hasta $987.765 en noviembre, aproximadamente 8.8× en diez meses. El único mes negativo significativo en ese período fue abril (-5.2%), una caída menor que se corrigió en mayo (+43.6%).

- **2018 muestra estabilización entre $826.000 y $977.000**, sin recuperar el pico de noviembre. Junio 2018 registra la mayor caída del año: -12.4%. En Brasil, junio marca el inicio del invierno austral y coincide con reducción del consumo discrecional.

- **Diciembre 2016 y septiembre 2016 muestran datos atípicos** (1 orden cada uno, ingresos de $10 y $134 respectivamente). Son artefactos del dataset, no comportamiento real del negocio — deben excluirse de cualquier análisis de tendencia o proyección.

**Recomendación**

El equipo de **Growth y Comercial** debe anticipar el Black Friday con al menos 6 semanas de antelación: negociación de stock adicional con sellers, activación de campañas de awareness en octubre, y límites de descuento que no canibalicen diciembre. Dado que junio–agosto muestra caídas recurrentes (-12.4%, -3.4%), ese trimestre es el indicado para probar campañas de reactivación de demanda (cuotas sin interés, envío gratis) que amortigüen la estacionalidad invernal. La estabilización de 2018 sugiere que el negocio está madurando — las palancas de crecimiento ya no son orgánicas y requieren inversión activa.

---

### Reto 3: Ranking de Clientes por Ingresos

**Hallazgos**

- **El cliente #1 generó $13.440 en una sola orden**, compuesta por 8 ítems con un valor promedio de $1.680 cada uno. Es el cliente de mayor gasto absoluto del catálogo y compró una sola vez en septiembre de 2017 (Río de Janeiro).

- **9 de los 10 clientes top realizaron exactamente 1 orden.** Solo el cliente #2 tiene 2 compras. Esto confirma que los mayores gastadores del marketplace son compradores de alto ticket y baja frecuencia — no clientes recurrentes fidelizados.

- **Rio de Janeiro concentra 3 de los 10 primeros puestos** (ranks 1, 2 y 10), con tickets totales de $13.440, $7.388 y $4.400 respectivamente. Es el estado con mayor presencia entre los clientes VIP en términos de revenue individual.

- **El rango de gasto entre el rank 1 y el rank 10 es muy amplio:** de $13.440 a $4.400 — el primer cliente gasta 3× más que el décimo. La "crema" está extremadamente concentrada en la cima.

- **El cliente #4 (Campo Grande, MS) pagó $6.735 en una sola transacción**, con un ticket por ítem de $6.735 — probablemente un artículo de alto valor (electrónico o mueble de gama alta). Un cliente en una ciudad secundaria con gasto equivalente a los grandes centros urbanos.

**Recomendación**

El equipo de **CRM** debe crear un segmento VIP basado en gasto total acumulado, no en frecuencia de compra. El insight crítico es que estos clientes no repiten — pero no porque hayan tenido mala experiencia, sino porque compraron un ítem de alta inversión (computadora, electrodoméstico, joya) que tiene ciclo de recompra largo. La acción concreta: activar una secuencia de retención a los 90, 180 y 365 días post-compra con categorías complementarias a lo que adquirieron (accesorios, garantías extendidas, productos relacionados). Adicionalmente, dado el peso de RJ en este segmento, las campañas de upselling dirigidas a ese estado tendrían retorno desproporcionado.

---

### Reto 4: Participación por Categoría de Producto

**Hallazgos**

- **Health & Beauty es la categoría líder con el 9.33% del revenue total ($1.233M)**, seguida de Watches & Gifts (8.82%, $1.166M) y Bed, Bath & Table (7.74%, $1.023M). Solo estas tres categorías explican el 25.89% del negocio.

- **18 categorías concentran el 80% del revenue** — principio de Pareto confirmado en el dato de `cumulative_pct`. El restante 20% está distribuido entre 56 categorías adicionales, muchas de ellas con participaciones menores al 0.5%.

- **Computers tiene solo 177 órdenes pero genera $218.684 (1.65% del total)**: implica un ticket promedio por orden de ~$1.235. Es la categoría de mayor valor unitario en el top 20 — con potencial de revenue significativo si se aumenta el volumen de órdenes.

- **"Unknown" aparece como categoría con $170.727 en revenue** (1.29%), proveniente de 1.392 órdenes y 584 productos sin categoría asignada. Es revenue real que el negocio no puede atribuir ni optimizar por segmento.

- **Las últimas 10 categorías del ranking aportan menos del 0.1% del revenue cada una** (security_and_services: $283, fashion_childrens_clothes: $520, pc_gamer: $1.307). Mantener un catálogo activo para estas categorías tiene un costo operativo que probablemente supera el beneficio marginal.

**Recomendación**

El equipo **Comercial** debe enfocar el 80% del presupuesto de captación de sellers y campañas de marketing en las 18 categorías que explican el 80% del revenue. Concretamente: Health & Beauty, Watches & Gifts y Bed & Bath merecen managers de categoría dedicados y acuerdos de exclusividad con sellers top. Para Computers, el objetivo no es margen sino volumen — una estrategia de cuotas sin interés puede mover el ticket promedio ya existente hacia más órdenes. La categoría "Unknown" requiere una campaña de normalización de datos con los sellers: cada producto no categorizado es revenue invisible que no se puede escalar.

---

### Reto 5: Ticket Promedio por Estado (Región Geográfica)

**Hallazgos**

- **Paraíba (PB) lidera el ticket promedio con $266.61 por orden**, superando en 87% al estado con ticket más bajo, São Paulo ($142.46). PB tiene 517 órdenes — volumen modesto pero clientes de alto valor unitario.

- **Los 7 estados del Q1 (alto valor) son todos del Norte o Nordeste:** PB, AC, AP, AL, RO, PA, PI. El patrón es consistente: son estados remotos donde el flete promedio oscila entre $38 y $49, versus $17 en São Paulo. El cliente del interior compra menos veces pero gasta más por orden, posiblemente por consolidar múltiples necesidades en una sola compra.

- **São Paulo tiene 40.501 órdenes — 78× más que Amapá (67 órdenes)** — pero el ticket más bajo ($142.46) y el flete más barato ($17.33). Es el motor de volumen del negocio, no de ticket. Sus $5.769.703 en revenue total representan la mayor contribución absoluta con diferencia.

- **Acre (AC) y Amapá (AP) tienen tickets de $244 y $240 respectivamente, pero solo 80 y 67 órdenes.** Son mercados desatendidos con alta disposición de pago comprobada: si se reduce la fricción de entrega, el potencial de crecimiento es considerable.

- **El flete en estados del Norte/Nordeste representa entre el 20% y 22% del ticket total** (ej. RO: $46.43 flete sobre $234.43 ticket = 19.8%), versus apenas el 12% en SP ($17.33 sobre $142.46). El costo logístico es una barrera de conversión diferenciada por región.

**Recomendación**

El equipo de **Logística y Growth** debe diseñar una estrategia geográfica segmentada en dos velocidades: (1) para los estados Q1 del Norte/Nordeste (PB, AC, AP, AL, RO), implementar subsidio de envío o umbrales de envío gratis más bajos — dado que el ticket ya es alto, absorber parte del flete tiene ROI positivo en conversión; (2) para São Paulo y los estados Q4, la palanca no es el flete sino la frecuencia — programas de suscripción, cashback o "compra otra vez" dirigidos al segmento más activo. Acre y Amapá merecen un piloto de expansión logística: el cliente ya paga precios altos, solo falta reducir el tiempo de entrega para destrabar demanda latente.

---

*Archivo generado automáticamente por el agente /analyst.*  
*Datos fuente: ./outputs/ — Período 2016–2018 — Solo órdenes entregadas.*
