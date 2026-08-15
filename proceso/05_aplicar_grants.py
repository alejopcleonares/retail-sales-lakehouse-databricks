# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicación de permisos en Unity Catalog
# MAGIC
# MAGIC Este notebook aplica de forma reproducible los permisos definidos
# MAGIC para los grupos de ingeniería y análisis sobre el catálogo del proyecto.

# COMMAND ----------

catalog_name = "dbw_retail_lakehouse"

engineer_group = "retail_data_engineers"
analyst_group = "retail_data_analysts"

# COMMAND ----------

# Permiso de uso del catálogo

spark.sql(
    f"""
    GRANT USE CATALOG
    ON CATALOG {catalog_name}
    TO `{engineer_group}`
    """
)

spark.sql(
    f"""
    GRANT USE CATALOG
    ON CATALOG {catalog_name}
    TO `{analyst_group}`
    """
)

# COMMAND ----------

# Permisos de Data Engineers

engineer_schemas = [
    "bronze",
    "silver",
    "gold",
    "quarantine",
    "audit",
]

for schema_name in engineer_schemas:
    spark.sql(
        f"""
        GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
        ON SCHEMA {catalog_name}.{schema_name}
        TO `{engineer_group}`
        """
    )

# COMMAND ----------

# Permisos de Data Analysts únicamente sobre Gold

spark.sql(
    f"""
    GRANT USE SCHEMA
    ON SCHEMA {catalog_name}.gold
    TO `{analyst_group}`
    """
)

spark.sql(
    f"""
    GRANT SELECT
    ON SCHEMA {catalog_name}.gold
    TO `{analyst_group}`
    """
)

# COMMAND ----------

print("Permisos de Unity Catalog aplicados correctamente.")
