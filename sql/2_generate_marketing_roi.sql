-- ==============================================================================
-- Paso 2: Modelo Semántico de Negocio (De Zona Staging a Zona Gold)
-- Objetivo: Cruzar transacciones limpias con rendimiento de campañas
-- para calcular el Retorno de Inversión Publicitaria (ROAS) diario.
-- ==============================================================================


CREATE OR REPLACE TABLE `enterprise-analytics-rgo.gold_ecommerce.marketing_roi_dashboard` AS

-- CTE 1: Agregamos las ventas limpias por día
WITH aggregated_sales AS (
    SELECT
        transaction_date as date,
        SUM(amount) as total_revenue,
        COUNT(DISTINCT order_id) as total_orders
    FROM `enterprise-analytics-rgo.staging_ecommerce.cleaned_transactions`
    GROUP BY transaction_date
),

-- CTE 2: Agregamos los esfuerzos de marketing diarios
marketing_efforts AS (
    SELECT
        PARSE_DATE('%Y-%m-%d', date) as date,
        SUM(ad_spend) as total_spend,
        SUM(clicks) as total_clicks,
        SUM(conversions) as total_conversions
    FROM `enterprise-analytics-rgo.raw_ecommerce.marketing_performance`
    GROUP BY PARSE_DATE('%Y-%m-%d', date)
)

-- Consulta Final: Unimos ambas fuentes y calculamos el ROAS
SELECT
    s.date,
    s.total_revenue,
    s.total_orders,
    m.total_spend,
    m.total_clicks,
    m.total_conversions,
    -- Usamos SAFE_DIVIDE para evitar errores de división por cero si un día no hubo gasto
    SAFE_DIVIDE(s.total_revenue, m.total_spend) as ROAS,
    -- KPI Adicional: Costo por Adquisición (CPA)
    SAFE_DIVIDE(m.total_spend, s.total_orders) as CPA
FROM aggregated_sales s
INNER JOIN marketing_efforts m ON s.date = m.date
ORDER BY s.date DESC;