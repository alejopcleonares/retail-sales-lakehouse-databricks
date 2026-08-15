# Retail Sales Lakehouse - Azure Databricks

Proyecto final de Ingeniería de Datos desarrollado sobre Azure Databricks, orientado a implementar una arquitectura Lakehouse con capas Medallion, control de calidad, gobierno de datos, orquestación y CI/CD.

## Objetivo

Construir una plataforma analítica para procesar información de pedidos, productos y clientes utilizando Azure Data Lake Storage Gen2 y Azure Databricks.

El proyecto implementa un flujo de datos de extremo a extremo:

Raw → Bronze → Silver → Gold → Audit

e incorpora:

- PySpark
- Delta Lake
- Unity Catalog
- Managed Identity
- Databricks Workflows
- GitHub Actions
- CI/CD
- control de calidad
- Quarantine
- seguridad por grupos y permisos
- auditoría y reconciliación de registros

## Fuentes de datos

Se utilizaron cinco datasets:

- `orders.csv`
- `order_products__prior.csv`
- `products.csv`
- `aisles.csv`
- `departments.csv`

Los archivos originales se almacenan en Azure Data Lake Storage Gen2.

La fuente utilizada para el proyecto se encuentra documentada en:

`datasets/README.md`

## Arquitectura

```text
Kaggle / CSV
      ↓
Azure Data Lake Storage Gen2
      ↓
Managed Identity
      ↓
Storage Credential
      ↓
External Location - Unity Catalog
      ↓
Bronze
      ↓
Silver ─────────→ Quarantine
      ↓
Gold
      ↓
Audit

## Estructura del repositorio

```text
retail-sales-lakehouse-databricks/
│
├── .github/
│   └── workflows/
│       ├── databricks-ci.yml
│       └── databricks-cd.yml
│
├── PrepAmb/
│   └── README.md
│
├── certificaciones/
│   └── README.md
│
├── datasets/
│   └── README.md
│
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── evidence/
│       ├── 01_adls_raw_sources.png
│       ├── 02_managed_identity_access_connector.png
│       ├── 03_external_location.png
│       ├── 04_source_validation.png
│       ├── 05_bronze_tables.png
│       ├── 06_silver_tables.png
│       ├── 07_quarantine_tables.png
│       ├── 08_gold_tables.png
│       ├── 09_data_quality_audit.png
│       ├── 10_unity_catalog_permission.png
│       ├── 11_workflow_success.png
│       ├── 12_full_production_workflow_success.png
│       ├── 13_github_cd_success.png
│       └── 14_github_ci_success.png
│
├── notebooks/
│   ├── 01_ingesta_bronze.py
│   ├── 02_transformacion_silver.py
│   ├── 03_carga_gold.py
│   └── 04_auditoria_calidad.py
│
├── proceso/
│   ├── 00_preparacion_ambiente.py
│   ├── 01_ingesta_bronze.py
│   ├── 02_transformacion_silver.py
│   ├── 03_carga_gold.py
│   ├── 04_auditoria_calidad.py
│   └── 05_aplicar_grants.py
│
├── reversion/
│   └── README.md
│
├── seguridad/
│   └── grants.sql
│
└── README.md
