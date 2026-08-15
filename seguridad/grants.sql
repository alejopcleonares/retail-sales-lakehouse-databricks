-- Seguridad del proyecto Retail Sales Lakehouse
-- Permisos administrados mediante Unity Catalog.

GRANT USE CATALOG
ON CATALOG dbw_retail_lakehouse
TO `retail_data_engineers`;

GRANT USE CATALOG
ON CATALOG dbw_retail_lakehouse
TO `retail_data_analysts`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
ON SCHEMA dbw_retail_lakehouse.bronze
TO `retail_data_engineers`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
ON SCHEMA dbw_retail_lakehouse.silver
TO `retail_data_engineers`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
ON SCHEMA dbw_retail_lakehouse.gold
TO `retail_data_engineers`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
ON SCHEMA dbw_retail_lakehouse.quarantine
TO `retail_data_engineers`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
ON SCHEMA dbw_retail_lakehouse.audit
TO `retail_data_engineers`;

GRANT USE SCHEMA
ON SCHEMA dbw_retail_lakehouse.gold
TO `retail_data_analysts`;

GRANT SELECT
ON SCHEMA dbw_retail_lakehouse.gold
TO `retail_data_analysts`;
