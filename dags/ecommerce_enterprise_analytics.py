from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataplex import DataplexCreateTaskOperator

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False, # Lo apagamos para evitar configurar SMTP en desarrollo
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='enterprise_analytics_ecommerce_v1',
    default_args=default_args,
    description='Pipeline de orquestación avanzada con gobierno y calidad de datos en GCP',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ecommerce', 'marketing', 'dataplex', 'bigquery'],
) as dag:

    # 1. INGESTA: Transacciones
    ingest_transactions_raw = GCSToBigQueryOperator(
        task_id='ingest_transactions_to_raw',
        bucket='ecommerce-analytics-landing-zone-rgo', # Tu bucket único
        source_objects=['transactions/landing_transactions.csv'],
        destination_project_dataset_table='enterprise-analytics-rgo.raw_ecommerce.transactions',
        schema_fields=[
            {'name': 'order_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'customer_email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'transaction_date', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
    )

    # 1. INGESTA: Marketing
    ingest_marketing_raw = GCSToBigQueryOperator(
        task_id='ingest_marketing_to_raw',
        bucket='ecommerce-analytics-landing-zone-rgo', # Tu bucket único
        source_objects=['marketing/campaign_performance.csv'],
        destination_project_dataset_table='enterprise-analytics-rgo.raw_ecommerce.marketing_performance',
        schema_fields=[
            {'name': 'campaign_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'ad_spend', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'clicks', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'conversions', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'date', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
    )

    # 2. GOBIERNO Y CALIDAD: Dataplex
    run_dataplex_quality_check = DataplexCreateTaskOperator(
        task_id='run_dataplex_data_quality',
        project_id='enterprise-analytics-rgo',
        location='us-central1',
        lake_id='ecommerce-data-lake',
        task_id_to_create='dq-scan-raw-tables',
        body={
            "trigger_spec": {"type": "ON_DEMAND"},
            "data_quality_spec": {
                "rules": [
                    {"column": "order_id", "rule": {"non_null_expectation": {}}},
                    {"column": "amount", "rule": {"range_expectation": {"min_value": "0.01"}}},
                    {"column": "customer_email", "rule": {"regex_expectation": {"regex": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"}}}
                ]
            }
        }
    )

    # 3. TRANSFORMACIÓN: Plata (Staging) - Versión robusta con deduplicación por ventana
    transform_to_staging = BigQueryInsertJobOperator(
        task_id='transform_raw_to_staging',
        sql="""
                CREATE OR REPLACE TABLE `enterprise-analytics-rgo.staging_ecommerce.cleaned_transactions` AS
                WITH deduplicated_transactions AS (
                    SELECT 
                        *,
                        ROW_NUMBER() OVER(
                            PARTITION BY order_id 
                            ORDER BY transaction_date DESC
                        ) as row_num
                    FROM `enterprise-analytics-rgo.raw_ecommerce.transactions`
                )
                SELECT 
                    order_id,
                    LOWER(customer_email) as customer_email,
                    CAST(amount AS FLOAT64) as amount,
                    PARSE_DATE('%Y-%m-%d', transaction_date) as transaction_date
                FROM deduplicated_transactions
                WHERE 
                    row_num = 1 
                    AND amount > 0 
                    AND customer_email LIKE '%@%';
            """,
        use_legacy_sql=False,
    )

    # 4. AGREGACIÓN: Oro (Semantic)
    generate_semantic_gold = BigQueryInsertJobOperator(
        task_id='generate_marketing_roi_gold',
        sql="""
            CREATE OR REPLACE TABLE `enterprise-analytics-rgo.gold_ecommerce.marketing_roi_dashboard` AS
            WITH aggregated_sales AS (
                SELECT 
                    transaction_date as date,
                    SUM(amount) as total_revenue,
                    COUNT(order_id) as total_orders
                FROM `enterprise-analytics-rgo.staging_ecommerce.cleaned_transactions`
                GROUP BY transaction_date
            ),
            marketing_efforts AS (
                SELECT 
                    PARSE_DATE('%Y-%m-%d', date) as date,
                    SUM(ad_spend) as total_spend,
                    SUM(clicks) as total_clicks,
                    SUM(conversions) as total_conversions
                FROM `enterprise-analytics-rgo.raw_ecommerce.marketing_performance`
                GROUP BY PARSE_DATE('%Y-%m-%d', date)
            )
            SELECT 
                s.date,
                s.total_revenue,
                s.total_orders,
                m.total_spend,
                m.total_clicks,
                m.total_conversions,
                SAFE_DIVIDE(s.total_revenue, m.total_spend) as ROAS
            FROM aggregated_sales s
            INNER JOIN marketing_efforts m ON s.date = m.date;
        """,
        use_legacy_sql=False,
    )

    # Grafo de dependencias
    [ingest_transactions_raw, ingest_marketing_raw] >> run_dataplex_quality_check
    run_dataplex_quality_check >> transform_to_staging >> generate_semantic_gold