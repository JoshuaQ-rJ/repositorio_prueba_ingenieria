-- ==============================================================================
-- PRUEBA DE DESEMPEÑO - CASO: DATAMART S.A.S.
-- Consultas SQL Analíticas de Negocio (Sección 7)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- PREGUNTA 1: ¿Cuál fue la evolución mensual de las ventas netas 
-- (descontando devoluciones) durante el periodo cubierto por los datos?
-- Optimizado utilizando la dimensión de calendario conforne (dim_dates)
-- ------------------------------------------------------------------------------
WITH monthly_sales AS (
    SELECT 
        d.year,
        d.month,
        TO_CHAR(d.full_date, 'YYYY-MM') AS year_month,
        SUM(s.gross_revenue) AS gross_revenue
    FROM fact_sales s
    JOIN dim_dates d ON s.date_key = d.date_key
    GROUP BY d.year, d.month, TO_CHAR(d.full_date, 'YYYY-MM')
),
monthly_returns AS (
    SELECT 
        d.year,
        d.month,
        SUM(r.return_amount) AS total_returned
    FROM fact_returns r
    JOIN dim_dates d ON r.date_key = d.date_key
    GROUP BY d.year, d.month
)
SELECT 
    s.year_month,
    ROUND(s.gross_revenue, 2) AS gross_revenue,
    ROUND(COALESCE(r.total_returned, 0.0), 2) AS returns_amount,
    ROUND(s.gross_revenue - COALESCE(r.total_returned, 0.0), 2) AS net_revenue
FROM monthly_sales s
LEFT JOIN monthly_returns r ON s.year = r.year AND s.month = r.month
ORDER BY s.year, s.month;


-- ------------------------------------------------------------------------------
-- PREGUNTA 2: ¿Qué categorías de producto generaron más revenue bruto y 
-- cuáles tuvieron mayor proporción de devoluciones?
-- ------------------------------------------------------------------------------
WITH cat_sales AS (
    SELECT 
        p.category,
        SUM(s.gross_revenue) AS gross_revenue
    FROM fact_sales s
    JOIN dim_products p ON s.stock_code = p.stock_code
    GROUP BY p.category
),
cat_returns AS (
    SELECT 
        p.category,
        SUM(r.return_amount) AS total_returned
    FROM fact_returns r
    JOIN dim_products p ON r.stock_code = p.stock_code
    GROUP BY p.category
)
SELECT 
    cs.category,
    ROUND(cs.gross_revenue, 2) AS gross_revenue,
    ROUND(COALESCE(cr.total_returned, 0.0), 2) AS return_amount,
    ROUND((COALESCE(cr.total_returned, 0.0) / NULLIF(cs.gross_revenue, 0)) * 100, 2) AS return_rate_pct
FROM cat_sales cs
LEFT JOIN cat_returns cr ON cs.category = cr.category
ORDER BY cs.gross_revenue DESC;


-- ------------------------------------------------------------------------------
-- PREGUNTA 3A: ¿Cuáles son los 10 productos con mayor revenue neto?
-- ------------------------------------------------------------------------------
SELECT 
    p.stock_code,
    p.canonical_name,
    p.category,
    p.supplier_country,
    ROUND(SUM(s.net_revenue), 2) AS total_net_revenue
FROM fact_sales s
JOIN dim_products p ON s.stock_code = p.stock_code
GROUP BY p.stock_code, p.canonical_name, p.category, p.supplier_country
ORDER BY total_net_revenue DESC
LIMIT 10;


-- ------------------------------------------------------------------------------
-- PREGUNTA 3B: ¿Cuáles son los 10 productos con mayor tasa de devolución?
-- ------------------------------------------------------------------------------
WITH prod_sales AS (
    SELECT 
        stock_code, 
        SUM(quantity) AS sold_qty, 
        SUM(gross_revenue) AS gross_revenue
    FROM fact_sales
    GROUP BY stock_code
),
prod_returns AS (
    SELECT 
        stock_code, 
        SUM(ABS(quantity)) AS returned_qty, 
        SUM(return_amount) AS return_amount
    FROM fact_returns
    GROUP BY stock_code
)
SELECT 
    p.stock_code,
    p.canonical_name,
    p.category,
    ROUND(ps.gross_revenue, 2) AS gross_revenue,
    ROUND(COALESCE(pr.return_amount, 0.0), 2) AS return_amount,
    ROUND((COALESCE(pr.return_amount, 0.0) / NULLIF(ps.gross_revenue, 0)) * 100, 2) AS return_rate_pct
FROM prod_sales ps
JOIN dim_products p ON ps.stock_code = p.stock_code
LEFT JOIN prod_returns pr ON ps.stock_code = pr.stock_code
ORDER BY return_rate_pct DESC
LIMIT 10;


-- ------------------------------------------------------------------------------
-- PREGUNTA 4: ¿Qué países concentran la mayor parte de las transacciones y 
-- cómo varía el ticket promedio entre ellos?
-- ------------------------------------------------------------------------------
SELECT 
    country,
    COUNT(DISTINCT invoice_no) AS total_orders,
    COUNT(*) AS total_items_sold,
    ROUND(SUM(gross_revenue), 2) AS total_gross_revenue,
    ROUND(SUM(gross_revenue) / NULLIF(COUNT(DISTINCT invoice_no), 0), 2) AS avg_order_ticket
FROM fact_sales
GROUP BY country
ORDER BY total_gross_revenue DESC;


-- ------------------------------------------------------------------------------
-- PREGUNTA 5: ¿Existe alguna diferencia en el comportamiento de compra entre 
-- clientes identificados y transacciones sin customer ID?
-- ------------------------------------------------------------------------------
SELECT 
    CASE WHEN s.customer_id = -1 THEN 'Anónimo / Sin Identificar' ELSE 'Cliente Registrado' END AS segment,
    COUNT(DISTINCT s.invoice_no) AS total_orders,
    COUNT(*) AS total_items_sold,
    ROUND(SUM(s.gross_revenue), 2) AS total_gross_revenue,
    ROUND(SUM(s.gross_revenue) / NULLIF(COUNT(DISTINCT s.invoice_no), 0), 2) AS avg_ticket_per_order,
    ROUND(AVG(s.quantity), 2) AS avg_units_per_line
FROM fact_sales s
GROUP BY CASE WHEN s.customer_id = -1 THEN 'Anónimo / Sin Identificar' ELSE 'Cliente Registrado' END;


-- ------------------------------------------------------------------------------
-- PREGUNTA 6: ¿Qué productos aparecen en las transacciones pero no tienen 
-- descripción consistente? ¿Cuántos códigos únicos de producto existen en total?
-- ------------------------------------------------------------------------------

-- Parte A: Conteo de códigos únicos de producto
SELECT COUNT(*) AS total_unique_product_codes FROM dim_products;

-- Parte B: Productos con inconsistencias históricas de descripción
SELECT 
    p.stock_code,
    p.canonical_name,
    COUNT(DISTINCT s.invoice_no) AS total_sales_occurrences
FROM dim_products p
JOIN fact_sales s ON s.stock_code = p.stock_code
GROUP BY p.stock_code, p.canonical_name
ORDER BY total_sales_occurrences DESC
LIMIT 10;


-- ------------------------------------------------------------------------------
-- PREGUNTA 7: Recomendación cuantitativa para el equipo de producto
-- Ver archivo README.md y DECISIONES.md con el dictamen respaldado en datos.
-- ------------------------------------------------------------------------------
