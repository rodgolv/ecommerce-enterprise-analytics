# Habilitar API de Dataplex
gcloud services enable dataplex.googleapis.com

# Crear un Datalake
gcloud dataplex lakes create ecommerce-data-lake \
    --location=us-central1 \
    --display-name="E-commerce Data Lake" \
    --description="Data Lake principal para analítica de e-commerce"

# Crear zona para los datos crudos Raw
gcloud dataplex zones create raw-data-zone \
    --lake=ecommerce-data-lake \
    --location=us-central1 \
    --type=RAW \
    --resource-location-type=SINGLE_REGION \
    --display-name="Raw Data Zone"

# Vinculamos el dataset de BigQuery con la zona de datos crudos.
# Aquí le decimos a Dataplex: "Vigila los datos que viven en este dataset de BigQuery"
gcloud dataplex assets create raw-transactions-asset \
    --location=us-central1 \
    --lake=ecommerce-data-lake \
    --zone=raw-data-zone \
    --resource-type=BIGQUERY_DATASET \
    --resource-name=projects/enterprise-analytics-rgo/datasets/raw_ecommerce \
    --display-name="Raw Ecommerce Dataset"