# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparación del ambiente
# MAGIC
# MAGIC Este notebook prepara de forma idempotente los esquemas requeridos por
# MAGIC la arquitectura Medallion antes de ejecutar el pipeline productivo.

# COMMAND ----------

catalog_name = "dbw_retail_lakehouse"

schemas = [
    "bronze",
    "silver",
    "gold",
    "quarantine",
    "audit",
]

for schema_name in schemas:
    spark.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS
        {catalog_name}.{schema_name}
        """
    )

    print(
        f"Esquema disponible: "
        f"{catalog_name}.{schema_name}"
    )
