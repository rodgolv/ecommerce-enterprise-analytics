# E-Commerce Analytics & ML Pipeline on GCP

![Google Cloud](https://img.shields.io/badge/GoogleCloud-%234285F4.svg?style=for-the-badge&logo=google-cloud&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![SQL](https://img.shields.io/badge/sql-f29111?style=for-the-badge&logo=mysql&logoColor=white)

## Descripción del Proyecto
Este proyecto implementa un pipeline de datos empresarial (End-to-End) para un escenario de comercio electrónico. Extrae datos transaccionales y de campañas de marketing, orquesta las transformaciones necesarias para garantizar la calidad de los datos, y culmina en el entrenamiento de un modelo de Machine Learning predictivo para calcular el Retorno de Inversión Publicitaria (ROAS).

El pipeline está construido con una arquitectura Medallón (Raw, Staging, Gold) totalmente nativa en la nube, optimizada para analítica de negocios y escalabilidad.

## Arquitectura del Sistema

El siguiente diagrama ilustra el flujo de datos y la orquestación gestionada por **Cloud Composer (Airflow)**:

```mermaid
graph TD
    subgraph Orquestación
        Airflow[Apache Airflow / Composer]
    end

    subgraph Google Cloud Platform
        GCS[Cloud Storage Landing Zone] -->|GCSToBigQuery| Raw[(BigQuery: Raw Zone)]
        Raw -->|ROW_NUMBER Deduplication| Staging[(BigQuery: Staging Zone)]
        Staging -->|JOIN & Aggregation| Gold[(BigQuery: Gold Zone)]
        
        Gold -->|SQL Train| ML{BigQuery ML: Linear Regression}
        ML -->|ML.PREDICT| Pred[ROAS Predictions]
    end

    Airflow -.->|Trigger & Monitor| GCS
    Airflow -.->|Execute Job| Raw
    Airflow -.->|Execute Job| Staging
    Airflow -.->|Execute Job| Gold

    classDef gcp fill:#e8f0fe,stroke:#4285f4,stroke-width:2px,color:#1a73e8;
    classDef orchestrator fill:#fce8e6,stroke:#ea4335,stroke-width:2px,color:#d93025;
    
    class GCS,Raw,Staging,Gold,ML,Pred gcp;
    class Airflow orchestrator;

```
## Características Principales

1. **Ingesta Automatizada:** Lectura de archivos CSV (Transacciones y Desempeño de Marketing) desde Google Cloud Storage hacia la zona Raw de BigQuery.
2. **Limpieza y Transformación (ETL):** Deduplicación robusta utilizando `ROW_NUMBER() OVER()` (Window Functions) y estandarización de tipos de datos en la zona Staging.
3. **Capa Semántica de Negocio:** Construcción de un Dashboard en la zona Gold uniendo esfuerzos de marketing con conversiones de ventas mediante `SAFE_DIVIDE` para el cálculo del ROAS.
4. **Machine Learning Integrado (BigQuery ML):** Entrenamiento de un modelo de Regresión Lineal directamente en el Data Warehouse mediante SQL.
* **Métricas del Modelo Base:** $R^2 = 0.56$ (Capaz de explicar el 56% de la varianza del ROAS utilizando únicamente gasto, clics y conversiones).



## Tecnologías Utilizadas

* **Infraestructura:** Google Kubernetes Engine (GKE) subyacente.
* **Orquestación:** Google Cloud Composer 2 (Apache Airflow).
* **Almacenamiento y DWH:** Google Cloud Storage, Google BigQuery.
* **Machine Learning:** BigQuery ML.
* **Lenguajes:** Python (DAGs), Standard SQL (Transformaciones y ML).

## Estructura del Repositorio

```text
├── dags/
│   └── ecommerce_enterprise_analytics.py   # DAG principal de Airflow
├── sql/
│   └── bq_ml_roas_prediction.sql           # Query de entrenamiento y evaluación del modelo ML
├── data/
│   ├── landing_transactions.csv            # Datos simulados de ventas
│   └── campaign_performance.csv            # Datos simulados de marketing
└── README.md

```

## Próximos Pasos (Roadmap)

* Integrar un backend en Java o Python para exponer un endpoint REST que consuma las predicciones de BigQuery ML en tiempo real.
* Añadir dimensionalidad al modelo predictivo (estacionalidad, categorías de productos) para incrementar la precisión del R2.
* Conectar la tabla Gold a Looker Studio para visualización de métricas en vivo.

## Imagenes
<img width="1467" height="806" alt="Captura de pantalla 2026-06-24 a la(s) 12 11 17 p m" src="https://github.com/user-attachments/assets/373913f1-4bc3-43cc-9e29-2d66f73ae7bc" />

\
<img width="1467" height="806" alt="Captura de pantalla 2026-06-24 a la(s) 1 26 45 p m" src="https://github.com/user-attachments/assets/32cdc573-bf80-41e4-a8ec-205719fd2f48" />

\
<img width="1467" height="806" alt="Captura de pantalla 2026-06-24 a la(s) 1 31 49 p m" src="https://github.com/user-attachments/assets/bdd8821d-e86c-4c1a-abae-7f9606e5c419" />

\
<img width="1467" height="806" alt="Captura de pantalla 2026-06-24 a la(s) 1 41 48 p m" src="https://github.com/user-attachments/assets/aeb86815-4117-4489-83a8-e959cab630e6" />

\
<img width="1467" height="806" alt="Captura de pantalla 2026-06-24 a la(s) 1 44 58 p m" src="https://github.com/user-attachments/assets/e14667af-afca-4efd-bf6b-87b36a112904" />

\
<img width="1467" height="806" alt="Captura de pantalla 2026-06-24 a la(s) 1 47 49 p m" src="https://github.com/user-attachments/assets/ae506b79-bc0d-4d60-bf45-a78e75be0e92" />


----

**Desarrollado por:** Rodrigo García Olvera
