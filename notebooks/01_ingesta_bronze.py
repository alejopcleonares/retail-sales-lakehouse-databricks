# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingesta de la capa Bronze
# MAGIC
# MAGIC En este notebook se leen las cinco fuentes oficiales desde la capa raw de Azure Data Lake Storage mediante Managed Identity. Los datos se almacenarán como tablas Delta dentro del esquema `bronze` de Unity Catalog, agregando metadatos técnicos para asegurar trazabilidad de la ingesta.

# COMMAND ----------

from pyspark.sql import functions as F

catalog_name = "dbw_retail_lakehouse"
bronze_schema = "bronze"

raw_base_path = (
    "abfss://raw@stretailalejo01.dfs.core.windows.net/"
    "retail_orders"
)

paths = {
    "orders": f"{raw_base_path}/orders/orders.csv",
    "order_products": (
        f"{raw_base_path}/order_products/"
        "order_products__prior.csv"
    ),
    "products": f"{raw_base_path}/products/products.csv",
    "aisles": f"{raw_base_path}/aisles/aisles.csv",
    "departments": (
        f"{raw_base_path}/departments/departments.csv"
    ),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lectura de las fuentes CSV
# MAGIC
# MAGIC Los archivos se leen utilizando punto y coma (`;`) como delimitador, según lo validado previamente. En Bronze se conserva la información de origen sin aplicar reglas de exclusión o limpieza de negocio.

# COMMAND ----------

def read_raw_csv(path):
    return (
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "true")
        .csv(path)
    )


df_orders_bronze = read_raw_csv(paths["orders"])
df_order_products_bronze = read_raw_csv(
    paths["order_products"]
)
df_products_bronze = read_raw_csv(paths["products"])
df_aisles_bronze = read_raw_csv(paths["aisles"])
df_departments_bronze = read_raw_csv(
    paths["departments"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incorporación de metadatos técnicos
# MAGIC
# MAGIC Se agregan columnas de auditoría para registrar la fecha de ingesta, el archivo de origen, el dataset de procedencia y la fecha de carga. Estos campos permitirán rastrear cada registro a lo largo de la arquitectura Medallion.

# COMMAND ----------

def add_bronze_metadata(dataframe, source_name):
    return (
        dataframe
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp(),
        )
        .withColumn(
            "_source_file",
            F.col("_metadata.file_path"),
        )
        .withColumn(
            "_source_dataset",
            F.lit(source_name),
        )
        .withColumn(
            "_load_date",
            F.current_date(),
        )
    )


df_orders_bronze = add_bronze_metadata(
    df_orders_bronze,
    "orders",
)

df_order_products_bronze = add_bronze_metadata(
    df_order_products_bronze,
    "order_products_prior",
)

df_products_bronze = add_bronze_metadata(
    df_products_bronze,
    "products",
)

df_aisles_bronze = add_bronze_metadata(
    df_aisles_bronze,
    "aisles",
)

df_departments_bronze = add_bronze_metadata(
    df_departments_bronze,
    "departments",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escritura de tablas Delta en Bronze
# MAGIC
# MAGIC Las cinco fuentes se almacenan como tablas Delta administradas dentro del esquema `bronze`. En esta etapa se conserva el contenido original y únicamente se añaden metadatos técnicos de trazabilidad.

# COMMAND ----------

bronze_tables = {
    "orders": df_orders_bronze,
    "order_products": df_order_products_bronze,
    "products": df_products_bronze,
    "aisles": df_aisles_bronze,
    "departments": df_departments_bronze,
}

for table_name, dataframe in bronze_tables.items():
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{catalog_name}.{bronze_schema}.{table_name}"
        )
    )

    print(
        f"Tabla creada: "
        f"{catalog_name}.{bronze_schema}.{table_name}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación de las tablas Bronze
# MAGIC
# MAGIC En este paso se verifica que las cinco tablas Delta hayan sido creadas correctamente en Unity Catalog. Se revisará la cantidad de registros y la presencia de las columnas técnicas agregadas durante la ingesta.

# COMMAND ----------

bronze_validation = []

for table_name in bronze_tables.keys():
    full_table_name = (
        f"{catalog_name}.{bronze_schema}.{table_name}"
    )

    df_table = spark.table(full_table_name)

    bronze_validation.append(
        (
            table_name,
            df_table.count(),
            len(df_table.columns),
            "_ingestion_timestamp" in df_table.columns,
            "_source_file" in df_table.columns,
            "_source_dataset" in df_table.columns,
            "_load_date" in df_table.columns,
        )
    )

df_bronze_validation = spark.createDataFrame(
    bronze_validation,
    [
        "table_name",
        "row_count",
        "column_count",
        "has_ingestion_timestamp",
        "has_source_file",
        "has_source_dataset",
        "has_load_date",
    ],
)

display(df_bronze_validation)