# Crea el bucket que servirá como la Landing Zone
gcloud storage buckets create gs://ecommerce-analytics-landing-zone-tu-nombre --location=us-central1

# Subir transacciones
gcloud storage cp data/landing_transactions.csv gs://ecommerce-analytics-landing-zone-rgo/transactions/

# Subir datos de marketing
gcloud storage cp data/campaign_performance.csv gs://ecommerce-analytics-landing-zone-rgo/marketing/

# Crea los datasets en BigQuery
# Capa Bronce (Datos crudos / Raw)
bq --location=us-central1 mk --dataset raw_ecommerce

# Capa Plata (Datos limpios / Staging)
bq --location=us-central1 mk --dataset staging_ecommerce

# Capa Oro (Datos de negocio / Semantic)
bq --location=us-central1 mk --dataset gold_ecommerce



