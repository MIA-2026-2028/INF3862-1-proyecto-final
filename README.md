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
| [E2](entregas/E2/) Ingesta y procesamiento | 30 de noviembre de 2026 | Pipeline en notebook, capas, calidad y análisis de escala |
| [E3](entregas/E3/) Datos para ML y arquitectura final | 14 de diciembre de 2026 | Dataset de modelamiento, modelo base, arquitectura y operación |

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
latex/             preámbulo común de los informes
assets/            logo de la portada
datos/README.md    cómo obtener y ubicar los datos
entregas/E1/       informe.tex, informe.pdf y anexos
entregas/E2/       informe.tex, informe.pdf, src/ y anexos
entregas/E3/       informe.tex, informe.pdf, src/ y anexos
src/               código reutilizable de los notebooks
```
