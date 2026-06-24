-- ==============================================================================
-- Paso 1: Limpieza de Datos (De Zona Raw a Zona Staging)
-- Objetivo: Filtrar registros corruptos, estandarizar formatos y deduplicar.
-- ==============================================================================

CREATE OR REPLACE TABLE `enterprise-analytics-rgo.staging_ecommerce.cleaned_transactions` AS
WITH deduplicated_transactions AS (
    -- Usamos ROW_NUMBER para quedarnos solo con la primera ocurrencia de un order_id duplicado
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY transaction_date DESC) as row_num
    FROM `enterprise-analytics-rgo.raw_ecommerce.transactions`
)
SELECT
    order_id,
    -- Estandarización: texto a minúsculas
    LOWER(customer_email) as customer_email,
    -- Asegurar el tipo de dato correcto
    CAST(amount AS FLOAT64) as amount,
    -- Castear la fecha (asumiendo que venía como STRING desde el CSV)
    PARSE_DATE('%Y-%m-%d', transaction_date) as transaction_date
FROM deduplicated_transactions
WHERE
    row_num = 1 -- Filtrar duplicados
    AND amount > 0 -- Regla de negocio: Montos positivos
    AND customer_email LIKE '%@%'; -- Regla de negocio básica: Formato de email