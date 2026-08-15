# Preparación de ambiente

La preparación del ambiente productivo se ejecuta mediante el notebook:

`proceso/00_preparacion_ambiente.py`

Este proceso crea de forma idempotente los esquemas requeridos en Unity Catalog:

- bronze
- silver
- gold
- quarantine
- audit

El uso de `CREATE SCHEMA IF NOT EXISTS` permite ejecutar el proceso múltiples veces sin destruir objetos existentes.
