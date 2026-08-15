# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformación de la capa Silver
# MAGIC
# MAGIC En este notebook se aplican reglas de calidad, tipificación e integridad referencial sobre las tablas Bronze. Los registros válidos se almacenan en el esquema `silver`, mientras que los registros que incumplen reglas críticas se conservan en `quarantine` para mantener trazabilidad.

# COMMAND ----------

from pyspark.sql import functions as F

catalog_name = "dbw_retail_lakehouse"

bronze_schema = "bronze"
silver_schema = "silver"
quarantine_schema = "quarantine"

df_orders = spark.table(
    f"{catalog_name}.{bronze_schema}.orders"
)

df_order_products = spark.table(
    f"{catalog_name}.{bronze_schema}.order_products"
)

df_products = spark.table(
    f"{catalog_name}.{bronze_schema}.products"
)

df_aisles = spark.table(
    f"{catalog_name}.{bronze_schema}.aisles"
)

df_departments = spark.table(
    f"{catalog_name}.{bronze_schema}.departments"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpieza y tipificación de `orders`
# MAGIC
# MAGIC Se normalizan los tipos de datos de la tabla de pedidos y se mantienen únicamente los campos funcionales junto con los metadatos técnicos de trazabilidad. Los valores nulos de `days_since_prior_order` se conservan porque son válidos para el primer pedido de cada usuario.

# COMMAND ----------

df_orders_silver = (
    df_orders
    .select(
        F.col("order_id").cast("bigint").alias("order_id"),
        F.col("user_id").cast("bigint").alias("user_id"),
        F.col("eval_set").cast("string").alias("eval_set"),
        F.col("order_number").cast("int").alias("order_number"),
        F.col("order_dow").cast("tinyint").alias("order_dow"),
        F.col("order_hour_of_day")
        .cast("tinyint")
        .alias("order_hour_of_day"),
        F.col("days_since_prior_order")
        .cast("decimal(5,1)")
        .alias("days_since_prior_order"),
        "_ingestion_timestamp",
        "_source_file",
        "_source_dataset",
        "_load_date",
    )
)

display(df_orders_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Separación de registros válidos y huérfanos de `order_products`
# MAGIC
# MAGIC Se valida la relación entre `order_products.order_id` y `orders.order_id`. Los registros que encuentran correspondencia pasan a Silver; los registros sin pedido asociado se envían a cuarentena con un motivo de rechazo.

# COMMAND ----------

valid_order_ids = (
    df_orders_silver
    .select("order_id")
    .dropDuplicates()
)

df_order_products_silver = (
    df_order_products
    .join(
        valid_order_ids,
        on="order_id",
        how="inner",
    )
    .select(
        F.col("order_id").cast("bigint").alias("order_id"),
        F.col("product_id").cast("int").alias("product_id"),
        F.col("add_to_cart_order")
        .cast("int")
        .alias("add_to_cart_order"),
        F.col("reordered")
        .cast("tinyint")
        .alias("reordered"),
        "_ingestion_timestamp",
        "_source_file",
        "_source_dataset",
        "_load_date",
    )
)

df_order_products_quarantine = (
    df_order_products
    .join(
        valid_order_ids,
        on="order_id",
        how="left_anti",
    )
    .withColumn(
        "_rejection_reason",
        F.lit("ORDER_ID_NOT_FOUND_IN_ORDERS"),
    )
    .withColumn(
        "_quarantine_timestamp",
        F.current_timestamp(),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Separación de productos válidos e incompletos
# MAGIC
# MAGIC Los productos con `product_name`, `aisle_id` o `department_id` faltantes, o con contenido en la columna residual `_c4`, se consideran registros incompletos y se envían a cuarentena. Los demás registros pasan a Silver.

# COMMAND ----------

invalid_product_condition = (
    F.col("product_id").isNull()
    | F.col("product_name").isNull()
    | (F.trim(F.col("product_name")) == "")
    | F.col("aisle_id").isNull()
    | F.col("department_id").isNull()
    | F.col("_c4").isNotNull()
)

df_products_quarantine = (
    df_products
    .filter(invalid_product_condition)
    .withColumn(
        "_rejection_reason",
        F.lit("INCOMPLETE_OR_MALFORMED_PRODUCT"),
    )
    .withColumn(
        "_quarantine_timestamp",
        F.current_timestamp(),
    )
)

df_products_silver = (
    df_products
    .filter(~invalid_product_condition)
    .select(
        F.col("product_id").cast("int").alias("product_id"),
        F.trim(F.col("product_name")).alias("product_name"),
        F.col("aisle_id").cast("int").alias("aisle_id"),
        F.col("department_id")
        .cast("int")
        .alias("department_id"),
        "_ingestion_timestamp",
        "_source_file",
        "_source_dataset",
        "_load_date",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación de registros Silver y Quarantine
# MAGIC
# MAGIC Antes de persistir las tablas, se comparan los conteos de registros válidos y rechazados. Esta validación confirma que ningún registro se pierda durante la separación entre Silver y Quarantine.

# COMMAND ----------

silver_quarantine_validation = [
    (
        "orders",
        df_orders.count(),
        df_orders_silver.count(),
        0,
    ),
    (
        "order_products",
        df_order_products.count(),
        df_order_products_silver.count(),
        df_order_products_quarantine.count(),
    ),
    (
        "products",
        df_products.count(),
        df_products_silver.count(),
        df_products_quarantine.count(),
    ),
]

df_silver_quarantine_validation = spark.createDataFrame(
    silver_quarantine_validation,
    [
        "dataset",
        "bronze_records",
        "silver_records",
        "quarantine_records",
    ],
).withColumn(
    "reconciled_records",
    F.col("silver_records") + F.col("quarantine_records"),
).withColumn(
    "is_reconciled",
    F.col("bronze_records") == F.col("reconciled_records"),
)

display(df_silver_quarantine_validation)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparación de dimensiones maestras para Silver
# MAGIC
# MAGIC Las tablas `aisles` y `departments` no presentan incidencias relevantes de calidad. En este paso se normalizan sus tipos de datos y se conservan los metadatos técnicos antes de almacenarlas en la capa Silver.

# COMMAND ----------

df_aisles_silver = (
    df_aisles
    .select(
        F.col("aisle_id").cast("int").alias("aisle_id"),
        F.trim(F.col("aisle")).alias("aisle"),
        "_ingestion_timestamp",
        "_source_file",
        "_source_dataset",
        "_load_date",
    )
)

df_departments_silver = (
    df_departments
    .select(
        F.col("department_id").cast("int").alias("department_id"),
        F.trim(F.col("department")).alias("department"),
        "_ingestion_timestamp",
        "_source_file",
        "_source_dataset",
        "_load_date",
    )
)

display(df_aisles_silver.limit(5))
display(df_departments_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escritura de tablas Silver y Quarantine
# MAGIC
# MAGIC Los registros válidos se almacenan como tablas Delta en el esquema `silver`. Los registros rechazados por reglas críticas de calidad se conservan en `quarantine`, incluyendo el motivo y la fecha de rechazo para asegurar trazabilidad.

# COMMAND ----------

silver_tables = {
    "orders": df_orders_silver,
    "order_products": df_order_products_silver,
    "products": df_products_silver,
    "aisles": df_aisles_silver,
    "departments": df_departments_silver,
}

for table_name, dataframe in silver_tables.items():
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{catalog_name}.{silver_schema}.{table_name}"
        )
    )

    print(
        f"Tabla Silver creada: "
        f"{catalog_name}.{silver_schema}.{table_name}"
    )


quarantine_tables = {
    "order_products_orphans":
        df_order_products_quarantine,

    "products_invalid":
        df_products_quarantine,
}

for table_name, dataframe in quarantine_tables.items():
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{catalog_name}.{quarantine_schema}.{table_name}"
        )
    )

    print(
        f"Tabla Quarantine creada: "
        f"{catalog_name}.{quarantine_schema}.{table_name}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación final de la capa Silver
# MAGIC
# MAGIC Se verifican las tablas creadas en Silver y Quarantine, comparando sus cantidades de registros con los resultados de calidad previamente obtenidos.

# COMMAND ----------

tables_to_validate = [
    ("silver", "orders"),
    ("silver", "order_products"),
    ("silver", "products"),
    ("silver", "aisles"),
    ("silver", "departments"),
    ("quarantine", "order_products_orphans"),
    ("quarantine", "products_invalid"),
]

validation_results = []

for schema_name, table_name in tables_to_validate:
    full_name = (
        f"{catalog_name}.{schema_name}.{table_name}"
    )

    dataframe = spark.table(full_name)

    validation_results.append(
        (
            schema_name,
            table_name,
            dataframe.count(),
        )
    )

df_final_silver_validation = spark.createDataFrame(
    validation_results,
    [
        "schema_name",
        "table_name",
        "row_count",
    ],
)

display(df_final_silver_validation)