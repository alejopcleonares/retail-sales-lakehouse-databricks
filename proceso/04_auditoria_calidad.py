# Databricks notebook source
# MAGIC %md
# MAGIC ## Auditoría y trazabilidad del procesamiento
# MAGIC
# MAGIC En este notebook se consolidan métricas de volumen y calidad de las principales fuentes procesadas. El objetivo es registrar cuántos registros ingresaron en Bronze, cuántos fueron aceptados en Silver y cuántos fueron enviados a cuarentena.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

catalog_name = "dbw_retail_lakehouse"

bronze_schema = "bronze"
silver_schema = "silver"
quarantine_schema = "quarantine"
audit_schema = "audit"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consolidación de métricas de calidad
# MAGIC
# MAGIC Se calculan los registros procesados en Bronze, Silver y Quarantine para comprobar la reconciliación de cada dataset y generar un indicador de porcentaje de aceptación.

# COMMAND ----------

audit_data = [
    (
        "orders",
        spark.table(
            f"{catalog_name}.bronze.orders"
        ).count(),
        spark.table(
            f"{catalog_name}.silver.orders"
        ).count(),
        0,
    ),
    (
        "order_products",
        spark.table(
            f"{catalog_name}.bronze.order_products"
        ).count(),
        spark.table(
            f"{catalog_name}.silver.order_products"
        ).count(),
        spark.table(
            f"{catalog_name}.quarantine.order_products_orphans"
        ).count(),
    ),
    (
        "products",
        spark.table(
            f"{catalog_name}.bronze.products"
        ).count(),
        spark.table(
            f"{catalog_name}.silver.products"
        ).count(),
        spark.table(
            f"{catalog_name}.quarantine.products_invalid"
        ).count(),
    ),
    (
        "aisles",
        spark.table(
            f"{catalog_name}.bronze.aisles"
        ).count(),
        spark.table(
            f"{catalog_name}.silver.aisles"
        ).count(),
        0,
    ),
    (
        "departments",
        spark.table(
            f"{catalog_name}.bronze.departments"
        ).count(),
        spark.table(
            f"{catalog_name}.silver.departments"
        ).count(),
        0,
    ),
]

df_audit = spark.createDataFrame(
    audit_data,
    [
        "dataset",
        "bronze_records",
        "silver_records",
        "quarantine_records",
    ],
)

df_audit = (
    df_audit
    .withColumn(
        "acceptance_rate_pct",
        F.round(
            F.col("silver_records")
            / F.col("bronze_records")
            * 100,
            2,
        ),
    )
    .withColumn(
        "is_reconciled",
        (
            F.col("bronze_records")
            == (
                F.col("silver_records")
                + F.col("quarantine_records")
            )
        ),
    )
    .withColumn(
        "audit_timestamp",
        F.current_timestamp(),
    )
)

display(df_audit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persistencia del reporte de auditoría
# MAGIC
# MAGIC Las métricas de calidad se almacenan como una tabla Delta dentro del esquema `audit`, permitiendo conservar evidencia del procesamiento y consultar posteriormente los niveles de aceptación y rechazo de cada fuente.

# COMMAND ----------

(
    df_audit.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalog_name}.{audit_schema}.data_quality_summary"
    )
)

print(
    "Tabla creada: "
    f"{catalog_name}.{audit_schema}.data_quality_summary"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación de la tabla de auditoría
# MAGIC
# MAGIC Se consulta la tabla persistida para comprobar que las métricas de calidad fueron almacenadas correctamente en Unity Catalog.

# COMMAND ----------

df_audit_check = spark.table(
    f"{catalog_name}.{audit_schema}.data_quality_summary"
)

display(df_audit_check)