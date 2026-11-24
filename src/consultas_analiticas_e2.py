"""Ejecuta las tres consultas OLAP descritas en E1 sobre los productos E2."""
from __future__ import annotations

import csv
import os
import shutil
import subprocess
from pathlib import Path


def run_analytical_queries(repo_root: Path, output_dir: Path, data_root: Path) -> dict:
    cli = os.environ.get("DUCKDB_CLI") or shutil.which("duckdb")
    candidate = repo_root / ".local-tools" / "duckdb" / "duckdb.exe"
    if not cli and candidate.is_file():
        cli = str(candidate)
    if not cli:
        raise FileNotFoundError("DuckDB CLI es necesario para ejecutar las consultas OLAP")

    def qpath(path: Path) -> str:
        return path.resolve().as_posix().replace("'", "''")

    fact = qpath(output_dir / "fact_usuario_dia.csv")
    by_device = qpath(output_dir / "fact_usuario_reloj_dia.csv")
    meals = qpath(data_root / "wearable" / "comidas.csv")
    recipes = qpath(data_root / "foodcom" / "recipes.parquet")
    sqls = {
        "consulta_4_perfil_cuartil": f"""
          WITH perfil AS (
            SELECT usuario_id, avg(try_cast(fc_reposo AS DOUBLE)) fc,
              avg(try_cast(pasos AS DOUBLE)) pasos,
              avg(try_cast(minutos_sesion AS DOUBLE)) minutos,
              avg(try_cast(sodio AS DOUBLE)) sodio,
              avg(try_cast(grasa_saturada AS DOUBLE)) grasa_sat,
              avg(try_cast(fibra AS DOUBLE)) fibra
            FROM read_csv_auto('{fact}')
            GROUP BY usuario_id HAVING count(fc_reposo) > 0
          ), cuartiles AS (
            SELECT *, ntile(4) OVER (ORDER BY fc) cuartil FROM perfil
          )
          SELECT cuartil, count(*) usuarios, round(avg(pasos),2) pasos_promedio,
            round(avg(minutos),2) minutos_promedio, round(avg(sodio),2) sodio_promedio,
            round(avg(grasa_sat),2) grasa_saturada_promedio, round(avg(fibra),2) fibra_promedio
          FROM cuartiles GROUP BY cuartil ORDER BY cuartil
        """,
        "consulta_5_pasos_semana_modelo": f"""
          SELECT strftime(try_cast(fecha_local AS DATE), '%G-W%V') semana, modelo,
            round(avg(try_cast(pasos AS DOUBLE)),2) pasos_promedio, count(*) filas
          FROM read_csv_auto('{by_device}') GROUP BY semana, modelo ORDER BY semana, modelo
        """,
        "consulta_6_recetas_por_cuartil": f"""
          WITH perfil AS (
            SELECT usuario_id, avg(try_cast(fc_reposo AS DOUBLE)) fc
            FROM read_csv_auto('{fact}') GROUP BY usuario_id HAVING count(fc_reposo)>0
          ), cuartiles AS (
            SELECT usuario_id, ntile(4) OVER (ORDER BY fc) cuartil FROM perfil
          ), conteo AS (
            SELECT c.cuartil, r.RecipeId, r.Name, count(*) veces
            FROM read_csv_auto('{meals}') m JOIN cuartiles c ON m.AuthorId=c.usuario_id
            JOIN read_parquet('{recipes}') r ON m.RecipeId=r.RecipeId
            GROUP BY c.cuartil, r.RecipeId, r.Name
          ), ranking AS (
            SELECT *, row_number() OVER(PARTITION BY cuartil ORDER BY veces DESC, Name) rn
            FROM conteo
          ) SELECT cuartil, RecipeId, Name, veces FROM ranking WHERE rn<=10
          ORDER BY cuartil, veces DESC
        """,
    }
    results = {}
    for name, sql in sqls.items():
        proc = subprocess.run([str(cli), "-no-agent", "-csv", "-c", sql], check=True,
                              capture_output=True, text=True, encoding="utf-8")
        path = output_dir / f"{name}.csv"
        path.write_text(proc.stdout, encoding="utf-8-sig")
        rows = list(csv.DictReader(path.open(encoding="utf-8-sig", newline="")))
        results[name] = {"filas_resultado": len(rows), "archivo": path.name,
                         "muestra": rows[:8]}
    return results
