# Retail Sales Lakehouse - Databricks

Proyecto final de Ingeniería de Datos desarrollado en Azure Databricks.

## Tecnologías principales

- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark
- Delta Lake
- Unity Catalog
- Databricks Workflows
- GitHub Actions

## Arquitectura

El proyecto implementa una arquitectura Medallion:

Raw → Bronze → Silver → Gold

La ingesta desde la capa Raw se realiza mediante Managed Identity, sin claves, tokens ni secretos almacenados en el código.
