# Arquitectura del Retail Sales Lakehouse

## Objetivo

El proyecto implementa una plataforma analítica en Azure Databricks para procesar información de pedidos y productos mediante una arquitectura Medallion.

## Flujo general

```text
Kaggle CSV
   ↓
Azure Data Lake Storage Gen2 - Raw
   ↓
Managed Identity
   ↓
Azure Databricks
   ↓
Bronze
   ↓
Silver + Quarantine
   ↓
Gold
   ↓
Audit
```

## Capa Raw

Los archivos originales se almacenan en Azure Data Lake Storage Gen2 fuera de DBFS y Volumes.

Fuentes utilizadas:

* `orders.csv`
* `order_products__prior.csv`
* `products.csv`
* `aisles.csv`
* `departments.csv`

El acceso desde Databricks se realiza mediante Managed Identity, utilizando un Access Connector, Storage Credential y External Location administrados con Unity Catalog.

## Capa Bronze

La capa Bronze conserva los registros recibidos desde Raw y los almacena como tablas Delta.

Se incorporan metadatos técnicos de trazabilidad:

* `_ingestion_timestamp`
* `_source_file`
* `_source_dataset`
* `_load_date`

## Capa Silver

Silver contiene información tipificada y validada.

Principales controles aplicados:

* validación de claves;
* control de registros incompletos;
* integridad referencial;
* normalización de tipos;
* separación de registros inválidos.

Los registros rechazados son conservados en el esquema `quarantine`.

## Capa Gold

Gold contiene estructuras preparadas para consumo analítico:

* `dim_products`
* `fact_orders`
* `fact_order_products`
* `product_performance`
* `customer_behavior`

Estas tablas permiten analizar comportamiento de clientes, recompra, frecuencia de pedidos y desempeño de productos.

## Auditoría

El esquema `audit` contiene la tabla:

`data_quality_summary`

Esta tabla registra:

* registros recibidos en Bronze;
* registros aceptados en Silver;
* registros enviados a Quarantine;
* tasa de aceptación;
* reconciliación de registros;
* fecha de auditoría.

## Orquestación

El Workflow `wf_retail_lakehouse` ejecuta secuencialmente:

```text
bronze_ingestion
        ↓
silver_transformation
        ↓
gold_load
        ↓
quality_audit
```

Las cuatro tareas utilizan el Job Compute `compute-retail-prod`.

## Seguridad

Unity Catalog administra los permisos mediante grupos:

* `retail_data_engineers`: lectura y escritura para procesamiento.
* `retail_data_analysts`: lectura sobre la capa Gold.

No se almacenan claves, tokens ni secretos dentro de los notebooks.
