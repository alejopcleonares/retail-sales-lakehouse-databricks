# Databricks notebook source
# MAGIC %md
# MAGIC ## Construcción de la capa Gold
# MAGIC
# MAGIC En este notebook se generan las estructuras analíticas finales a partir de las tablas Silver. La capa Gold integra pedidos, productos, categorías y comportamiento de clientes para facilitar consultas, indicadores y visualizaciones orientadas al negocio.

# COMMAND ----------

from pyspark.sql import functions as F

catalog_name = "dbw_retail_lakehouse"
silver_schema = "silver"
gold_schema = "gold"

df_orders = spark.table(
    f"{catalog_name}.{silver_schema}.orders"
)

df_order_products = spark.table(
    f"{catalog_name}.{silver_schema}.order_products"
)

df_products = spark.table(
    f"{catalog_name}.{silver_schema}.products"
)

df_aisles = spark.table(
    f"{catalog_name}.{silver_schema}.aisles"
)

df_departments = spark.table(
    f"{catalog_name}.{silver_schema}.departments"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensión enriquecida de productos
# MAGIC
# MAGIC Se integra el maestro de productos con pasillos y departamentos para generar una dimensión analítica que permita interpretar los productos mediante nombres y categorías de negocio.

# COMMAND ----------

df_dim_products = (
    df_products.alias("p")
    .join(
        df_aisles.alias("a"),
        on="aisle_id",
        how="left",
    )
    .join(
        df_departments.alias("d"),
        on="department_id",
        how="left",
    )
    .select(
        F.col("p.product_id"),
        F.col("p.product_name"),
        F.col("p.aisle_id"),
        F.col("a.aisle"),
        F.col("p.department_id"),
        F.col("d.department"),
    )
)

display(df_dim_products.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla de hechos de pedidos
# MAGIC
# MAGIC Se construye una tabla de hechos a nivel de pedido con la información del cliente, secuencia de compra, día, hora y tiempo transcurrido desde el pedido anterior.

# COMMAND ----------

df_fact_orders = (
    df_orders
    .select(
        "order_id",
        "user_id",
        "eval_set",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "days_since_prior_order",
    )
)

display(df_fact_orders.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla de hechos de productos por pedido
# MAGIC
# MAGIC Se construye una tabla de detalle que relaciona cada producto con su pedido, incluyendo posición en el carrito e indicador de recompra.

# COMMAND ----------

df_fact_order_products = (
    df_order_products
    .select(
        "order_id",
        "product_id",
        "add_to_cart_order",
        "reordered",
    )
)

display(df_fact_order_products.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Indicadores de desempeño por producto
# MAGIC
# MAGIC Se agregan los registros de productos por pedido para calcular métricas de comportamiento comercial como cantidad de pedidos, frecuencia de aparición, tasa de recompra y posición promedio en el carrito. El resultado se enriquece con el nombre del producto, pasillo y departamento.

# COMMAND ----------

df_product_performance = (
    df_fact_order_products
    .groupBy("product_id")
    .agg(
        F.count("*").alias("total_product_records"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("reordered").alias("total_reordered"),
        F.round(
            F.avg("reordered") * 100,
            2,
        ).alias("reorder_rate_pct"),
        F.round(
            F.avg("add_to_cart_order"),
            2,
        ).alias("avg_add_to_cart_order"),
    )
    .join(
        df_dim_products,
        on="product_id",
        how="left",
    )
    .select(
        "product_id",
        "product_name",
        "aisle_id",
        "aisle",
        "department_id",
        "department",
        "total_product_records",
        "total_orders",
        "total_reordered",
        "reorder_rate_pct",
        "avg_add_to_cart_order",
    )
)

display(
    df_product_performance
    .orderBy(
        F.desc("total_product_records")
    )
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Indicadores de comportamiento por cliente
# MAGIC
# MAGIC Se consolidan los pedidos y productos asociados a cada usuario para generar métricas de comportamiento como cantidad de pedidos, frecuencia de compra, tamaño promedio del carrito y proporción de productos recomprados.

# COMMAND ----------

df_order_level_metrics = (
    df_fact_order_products
    .groupBy("order_id")
    .agg(
        F.count("*").alias("basket_size"),
        F.sum("reordered").alias("reordered_products"),
    )
)

df_customer_behavior = (
    df_fact_orders.alias("o")
    .join(
        df_order_level_metrics.alias("m"),
        on="order_id",
        how="left",
    )
    .groupBy("user_id")
    .agg(
        # Pedidos disponibles en orders
        F.countDistinct("order_id").alias("total_orders"),

        # Pedidos que sí tienen detalle de productos
        F.countDistinct(
            F.when(
                F.col("basket_size").isNotNull(),
                F.col("order_id"),
            )
        ).alias("orders_with_product_detail"),

        # Frecuencia promedio entre pedidos
        F.round(
            F.avg("days_since_prior_order"),
            2,
        ).alias("avg_days_between_orders"),

        # Promedio solo sobre pedidos con detalle
        F.round(
            F.avg("basket_size"),
            2,
        ).alias("avg_basket_size"),

        # Productos disponibles en el detalle
        F.sum(
            F.coalesce(
                F.col("basket_size"),
                F.lit(0),
            )
        ).alias("total_products"),

        F.sum(
            F.coalesce(
                F.col("reordered_products"),
                F.lit(0),
            )
        ).alias("total_reordered_products"),
    )
    .withColumn(
        "detail_coverage_pct",
        F.round(
            (
                F.col("orders_with_product_detail")
                / F.col("total_orders")
            ) * 100,
            2,
        ),
    )
    .withColumn(
        "reorder_rate_pct",
        F.when(
            F.col("total_products") > 0,
            F.round(
                (
                    F.col("total_reordered_products")
                    / F.col("total_products")
                ) * 100,
                2,
            ),
        ).otherwise(F.lit(0.0)),
    )
)

display(
    df_customer_behavior
    .orderBy(
        F.desc("total_orders")
    )
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escritura de tablas Gold
# MAGIC
# MAGIC Las estructuras analíticas construidas se almacenan como tablas Delta administradas dentro del esquema `gold` de Unity Catalog. Estas tablas representan la capa final de consumo para consultas, indicadores y visualizaciones del proyecto.

# COMMAND ----------

gold_tables = {
    "dim_products": df_dim_products,
    "fact_orders": df_fact_orders,
    "fact_order_products": df_fact_order_products,
    "product_performance": df_product_performance,
    "customer_behavior": df_customer_behavior,
}

for table_name, dataframe in gold_tables.items():
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            f"{catalog_name}.{gold_schema}.{table_name}"
        )
    )

    print(
        f"Tabla Gold creada: "
        f"{catalog_name}.{gold_schema}.{table_name}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación final de la capa Gold
# MAGIC
# MAGIC Se verifican las tablas Gold creadas en Unity Catalog mediante el conteo de registros. Esta validación confirma que las estructuras analíticas fueron persistidas correctamente antes de avanzar hacia auditoría, seguridad y automatización.

# COMMAND ----------

gold_validation = []

for table_name in gold_tables.keys():
    full_table_name = (
        f"{catalog_name}.{gold_schema}.{table_name}"
    )

    dataframe = spark.table(full_table_name)

    gold_validation.append(
        (
            table_name,
            dataframe.count(),
            len(dataframe.columns),
        )
    )

df_gold_validation = spark.createDataFrame(
    gold_validation,
    [
        "table_name",
        "row_count",
        "column_count",
    ],
)

display(df_gold_validation)