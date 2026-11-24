# Datos

## Cómo obtenerlos

1. Descargar `datos_proyecto_INF3862.zip` desde Canvas del curso.
2. Descomprimirlo en esta carpeta, de modo que quede
   `datos/datos_proyecto_INF3862/foodcom/` y
   `datos/datos_proyecto_INF3862/wearable/`.

## Contenido

- `foodcom/recipes.parquet` y `foodcom/reviews.parquet`: recetas y reseñas
  de Food.com - Recipes and Reviews, Kaggle, licencia CC0
  (https://www.kaggle.com/datasets/irkaal/foodcom-recipes-and-reviews).
- `wearable/`: datos sintéticos de relojes inteligentes creados por el equipo
  docente del curso (`dispositivos.csv`, `mediciones/`, `sesiones.jsonl`,
  `comidas.csv`). El diccionario de datos está en `wearable/LEEME.md`.

## Capas que genera el pipeline

- `datos_proyecto_INF3862/`: bronze, los archivos del curso sin cambios.
- `silver/`: mediciones limpias en Parquet, particionadas por fecha de
  medición, y sesiones y comidas que cumplen las reglas de calidad.
- `gold/`: hechos y dimensiones de la E1 en Parquet (los hechos, por mes),
  la copia en CSV de `hecho_usuario_dia` para la medición de la E2 y la tabla
  de modelamiento de la E3.

Se generan con `notebooks/proyecto.ipynb` (ver el README de la raíz).
