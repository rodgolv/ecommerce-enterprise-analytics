from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='enterprise_analytics_ecommerce_v1',
    default_args=default_args,
    description='Pipeline de orquestación avanzada en GCP',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ecommerce', 'marketing', 'bigquery'],
) as dag:

    # 1. INGESTA: Transacciones
    ingest_transactions_raw = GCSToBigQueryOperator(
        task_id='ingest_transactions_to_raw',
        bucket='ecommerce-analytics-landing-zone-rgo',
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
        bucket='ecommerce-analytics-landing-zone-rgo',
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

    # 2. TRANSFORMACIÓN: Plata (Staging)
    transform_to_staging = BigQueryInsertJobOperator(
        task_id='transform_raw_to_staging',
        configuration={
            "query": {
                "query": """
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
                "useLegacySql": False,
            }
        }
    )

    # 3. AGREGACIÓN: Oro (Semantic)
    generate_semantic_gold = BigQueryInsertJobOperator(
        task_id='generate_marketing_roi_gold',
        configuration={
            "query": {
                "query": """
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
                "useLegacySql": False,
            }
        }
    )

    # Grafo de dependencias limpio
    [ingest_transactions_raw, ingest_marketing_raw] >> transform_to_staging >> generate_semantic_gold