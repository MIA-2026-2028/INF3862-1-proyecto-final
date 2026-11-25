<p align="center">
  <img src="assets/logo_uc.png" alt="Pontificia Universidad Católica de Chile" width="320">
</p>

<h3 align="center">Magíster en Inteligencia Artificial - MIA</h3>
<p align="center">
  <b>Instituto de Ingeniería Matemática y Computacional - IMC</b><br>
  INF3862-1 Plataformas Big Data para Data Science
</p>
<p align="center">
  <i>Proyecto del curso</i>
</p>
<h1 align="center">Hábitos de actividad y alimentación asociados a mejores indicadores de salud</h1>

---

**Alumnos:** Miguel Angel Cervantes Buleje y Roger Ruben Reyes Miranda.

## Caso de uso

Una aplicación de recetas basada en Food.com recibe la actividad física de los
relojes inteligentes de sus usuarios y registra qué recetas preparan. Este
proyecto construye un dashboard para el equipo de nutrición de la aplicación,
que decide qué recetas y actividades promover. El dashboard arma un perfil de
hábitos por usuario que combina:

- **Actividad:** pasos y sesiones de actividad registrados por el reloj.
- **Alimentación:** la información nutricional (sodio, grasa saturada, azúcar,
  fibra, proteína, calorías) de las recetas que el usuario prepara.

Con ese perfil, el proyecto compara qué patrones de actividad y alimentación
tienen los usuarios con frecuencia cardíaca en reposo más baja. Los resultados
describen asociaciones en datos de Food.com y en datos sintéticos de relojes,
no causas ni recomendaciones médicas.

## Entregas

| Entrega | Fecha | Contenido |
|---|---|---|
| [E1](entregas/E1/informe.pdf) Diseño de datos | 16 de noviembre de 2026 | Objetivo, fuentes, tablas OLTP y OLAP, consultas y arquitectura inicial |
| [E2](entregas/E2/informe.pdf) Ingesta y procesamiento | 30 de noviembre de 2026 | Pipeline con DuckDB, capas bronze, silver y gold, reglas de calidad, medición CSV frente a Parquet y análisis de escala. Notebook: [entregas/E2/src/notebook_e2.ipynb](entregas/E2/src/notebook_e2.ipynb) |
| [E3](entregas/E3/) Datos para ML y arquitectura final | 14 de diciembre de 2026 | Dataset de modelamiento, modelo base, arquitectura y operación |

## Preparar el entorno

Las dependencias están en `pyproject.toml` y sus versiones exactas en
`uv.lock`. Primero, ubicar los datos según [datos/README.md](datos/README.md).

Con Docker, sin instalar Python en el equipo:

```bash
docker compose up -d --build
```

Jupyter Lab queda en http://localhost:8900/lab?token=inf3862-proyecto, con el
repositorio completo en la carpeta de trabajo. Se detiene con
`docker compose down`.

Con uv instalado en el equipo:

```bash
uv sync
uv run jupyter lab
```

## Verificar las cifras del informe E1

Con el entorno levantado y los datos ubicados, desde la raíz del repositorio:

```bash
docker compose exec jupyter uv run python entregas/E1/anexos/verificacion_datos.py
```

Sin Docker: `uv run python entregas/E1/anexos/verificacion_datos.py`. Imprime
el volumen de cada fuente y las cifras del universo del dashboard.

## Ejecutar el pipeline (E2 y E3)

El notebook principal es `notebooks/proyecto.ipynb`. Corre de principio a fin
el pipeline de `src/pipeline.py` (bronze, silver y gold), las consultas 4, 5
y 6 de la E1 (`src/consultas.py`), la medición de CSV frente a Parquet, el
análisis de escala y el modelo base de la E3 (`src/modelamiento.py`). Escribe
las capas en `datos/silver/` y `datos/gold/`, que git ignora. Con el entorno
levantado y los datos ubicados:

```bash
docker compose exec -w /proyecto/notebooks jupyter uv run jupyter nbconvert --to notebook --execute --inplace proyecto.ipynb
```

También se abre en Jupyter Lab y se ejecuta con "Run All". Al cerrar cada
entrega, `entregas/E<n>/src/` guarda una copia fija del notebook con solo
las secciones de esa entrega, ejecutada desde esa carpeta, y los módulos que
usa: esa copia importa los módulos de su propia carpeta, de modo que sigue
corriendo aunque `src/` cambie en la entrega siguiente. La copia no se edita a
mano; un cambio se hace en `src/` o `notebooks/` y se vuelve a copiar.

## Generar los informes PDF

Los informes están escritos en LaTeX y se compilan con `latexmk`, que viene
incluido en las distribuciones TeX Live y MacTeX. En macOS se instala con:

```bash
brew install --cask mactex-no-gui
```

Desde la raíz del repositorio, para generar el informe de E1:

```bash
latexmk -pdf -cd -interaction=nonstopmode entregas/E1/informe.tex
latexmk -c -cd entregas/E1/informe.tex
```

El primer comando deja `entregas/E1/informe.pdf`; el segundo borra los
archivos auxiliares de la compilación. Para E2 y E3 se cambia `E1` por la
entrega correspondiente. La portada, los colores y el formato comunes están en
`latex/preambulo.tex`.

## Estructura

```
decisiones.md      registro de decisiones del proyecto
pyproject.toml     dependencias de Python; uv.lock fija sus versiones
Dockerfile         imagen con uv y Jupyter Lab
docker-compose.yml servicio de Jupyter Lab en el puerto 8900
latex/             preámbulo común de los informes
assets/            logo de la portada
datos/README.md    cómo obtener y ubicar los datos
entregas/E1/       informe.tex, informe.pdf y anexos/verificacion_datos.py
entregas/E2/       informe.tex, informe.pdf y src/ (copia fija del notebook y los módulos)
entregas/E3/       informe.tex e informe.pdf
notebooks/         proyecto.ipynb, el notebook principal de E2 y E3
src/               pipeline.py, consultas.py y modelamiento.py
datos/             datos del curso (bronze) y capas silver y gold; solo README.md en git
```
