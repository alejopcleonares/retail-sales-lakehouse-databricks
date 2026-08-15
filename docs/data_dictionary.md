# Diccionario de datos

## orders

| Campo                    | Tipo Silver  | Descripción                              | Regla de calidad                                  |
| ------------------------ | ------------ | ---------------------------------------- | ------------------------------------------------- |
| `order_id`               | BIGINT       | Identificador único del pedido           | No nulo, único, mayor a 0                         |
| `user_id`                | BIGINT       | Identificador del cliente                | No nulo, mayor a 0                                |
| `eval_set`               | STRING       | Conjunto al que pertenece el pedido      | Solo `prior`, `train` o `test`                    |
| `order_number`           | INT          | Número secuencial del pedido por usuario | Mayor o igual a 1                                 |
| `order_dow`              | TINYINT      | Día de la semana del pedido              | Entre 0 y 6                                       |
| `order_hour_of_day`      | TINYINT      | Hora del día en que se realizó el pedido | Entre 0 y 23                                      |
| `days_since_prior_order` | DECIMAL(5,1) | Días desde el pedido anterior            | Nulo únicamente en el primer pedido; entre 0 y 30 |

## order_products

| Campo               | Tipo Silver | Descripción                                        | Regla de calidad                     |
| ------------------- | ----------- | -------------------------------------------------- | ------------------------------------ |
| `order_id`          | BIGINT      | Pedido asociado al producto                        | No nulo y debe existir en `orders`   |
| `product_id`        | INT         | Producto asociado al pedido                        | No nulo y debe existir en `products` |
| `add_to_cart_order` | INT         | Posición en que el producto fue añadido al carrito | Mayor o igual a 1                    |
| `reordered`         | TINYINT     | Indicador de recompra                              | Solo 0 o 1                           |

### Observación de calidad

La fuente presenta 728,034 registros cuyo `order_id` no existe en `orders`. Estos registros se conservan en:

`quarantine.order_products_orphans`

Los 320,541 registros con correspondencia válida son procesados en Silver.

## products

| Campo           | Tipo Silver | Descripción                      | Regla de calidad              |
| --------------- | ----------- | -------------------------------- | ----------------------------- |
| `product_id`    | INT         | Identificador único del producto | No nulo y único               |
| `product_name`  | STRING      | Nombre del producto              | No nulo y no vacío            |
| `aisle_id`      | INT         | Identificador del pasillo        | Debe existir en `aisles`      |
| `department_id` | INT         | Identificador del departamento   | Debe existir en `departments` |

### Columna residual

La fuente Raw contiene una columna adicional denominada `_c4`, producida por registros con estructura irregular.

Se identificaron 11 productos incompletos o malformados. Estos registros se conservan en:

`quarantine.products_invalid`

Los 49,677 productos válidos continúan hacia Silver.

## aisles

| Campo      | Tipo Silver | Descripción                     | Regla de calidad   |
| ---------- | ----------- | ------------------------------- | ------------------ |
| `aisle_id` | INT         | Identificador único del pasillo | No nulo y único    |
| `aisle`    | STRING      | Nombre del pasillo              | No nulo y no vacío |

La fuente contiene 134 registros y no presenta incidencias relevantes de calidad.

## departments

| Campo           | Tipo Silver | Descripción                          | Regla de calidad   |
| --------------- | ----------- | ------------------------------------ | ------------------ |
| `department_id` | INT         | Identificador único del departamento | No nulo y único    |
| `department`    | STRING      | Nombre del departamento              | No nulo y no vacío |

La fuente contiene 21 registros y no presenta incidencias relevantes de calidad.

## Metadatos técnicos de Bronze

Las tablas Bronze incorporan los siguientes campos de trazabilidad:

| Campo                  | Descripción                |
| ---------------------- | -------------------------- |
| `_ingestion_timestamp` | Fecha y hora de ingesta    |
| `_source_file`         | Ruta del archivo de origen |
| `_source_dataset`      | Dataset de procedencia     |
| `_load_date`           | Fecha de carga             |

## Reglas generales de calidad

Durante el procesamiento se validan:

* valores nulos;
* claves duplicadas;
* rangos válidos;
* integridad referencial;
* coherencia entre variables;
* registros incompletos;
* reconciliación entre Bronze, Silver y Quarantine.

Los registros rechazados no se eliminan silenciosamente, sino que se conservan en el esquema `quarantine` para mantener trazabilidad.
