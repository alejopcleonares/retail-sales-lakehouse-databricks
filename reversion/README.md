# Estrategia de reversión

El proyecto utiliza Git y GitHub como mecanismo principal de control de versiones y reversión.

Cada modificación del código queda registrada mediante commits sobre la rama `main`.

En caso de una implementación defectuosa se puede:

1. identificar el último commit estable;
2. revertir el commit problemático mediante Git;
3. ejecutar nuevamente el workflow de GitHub Actions;
4. volver a desplegar los notebooks de la carpeta `proceso/` hacia Databricks.

Las tablas del Lakehouse utilizan Delta Lake, lo que permite conservar historial de versiones de los datos y facilita estrategias de recuperación y auditoría.
