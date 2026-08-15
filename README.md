# Retail Sales Lakehouse - Azure Databricks

Proyecto final de Ingeniería de Datos desarrollado sobre Azure Databricks, orientado a implementar una arquitectura Lakehouse con capas Medallion, control de calidad, gobierno de datos, orquestación y CI/CD.

## Objetivo

Construir una plataforma analítica para procesar información de pedidos, productos y clientes utilizando Azure Data Lake Storage Gen2 y Azure Databricks.

El proyecto cubre el flujo completo:

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
- seguridad por grupos y permisos

## Fuentes de datos

Se utilizaron cinco datasets:

- `orders.csv`
- `order_products__prior.csv`
- `products.csv`
- `aisles.csv`
- `departments.csv`

Los archivos originales se almacenan en Azure Data Lake Storage Gen2.

## Arquitectura

```text
Kaggle / CSV
      ↓
Azure Data Lake Storage Gen2
      ↓
Managed Identity
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
