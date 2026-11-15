"""Cifras del informe E1, calculadas desde los datos del curso.

Se ejecuta desde la raíz del repositorio, con los datos ubicados según
datos/README.md:

    docker compose exec jupyter uv run python entregas/E1/anexos/verificacion_datos.py
"""

import glob
import os

try:
    import duckdb
except ImportError:
    duckdb = None

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DATOS = os.environ.get("INF3862_DATA_DIR") or str(ROOT / "datos" / "datos_proyecto_INF3862")
if not os.path.isdir(DATOS):
    sibling = ROOT.parent / "datos_proyecto_INF3862"
    if sibling.is_dir():
        DATOS = str(sibling)
HORAS_NOCHE = (2, 4)                         # 2:00 a 4:59, hora local
HORAS_TARDE = (12, 18)                       # 12:00 a 18:59, hora local
MINUTOS_MIN_HORA = {"Pulso S": 30, "FitBand 2": 6}  # media hora de mediciones por modelo

RECETAS = f"{DATOS}/foodcom/recipes.parquet"
COMIDAS = f"{DATOS}/wearable/comidas.csv"
DISPOSITIVOS = f"{DATOS}/wearable/dispositivos.csv"
SESIONES = f"{DATOS}/wearable/sesiones.jsonl"
MEDICIONES = f"{DATOS}/wearable/mediciones/*/*.jsonl.gz"

if not os.path.exists(RECETAS):
    raise SystemExit("Faltan los datos: ver datos/README.md")

if duckdb is None:
    report = ROOT / "datos" / "processed" / "e2" / "quality_report.json"
    print("DuckDB no está instalado; no se recalculan las consultas de E1.")
    if report.exists():
        import json
        quality = json.loads(report.read_text(encoding="utf-8"))
        print("Conteos verificados por el pipeline E2:")
        print(json.dumps(quality["counts"], ensure_ascii=False, indent=2))
        print("Las cifras de Food.com y las métricas nocturnas de E1 requieren DuckDB.")
    else:
        print("Ejecuta primero el pipeline E2 para obtener conteos verificables.")
    raise SystemExit(0)


def megabytes(patron):
    return sum(os.path.getsize(archivo) for archivo in glob.glob(patron)) / 1e6


def valor(consulta):
    return con.sql(consulta).fetchall()[0]


con = duckdb.connect()
con.sql("set preserve_insertion_order = false; set enable_progress_bar = false")

# 4.1 y 4.2: volumen de cada fuente
dias = len(glob.glob(f"{DATOS}/wearable/mediciones/*"))
print("Días de recepción:", dias)

recetas = valor(f"select count(*) from '{RECETAS}'")[0]
recetas_2007 = valor(f"select count(*) from '{RECETAS}' where year(DatePublished) = 2007")[0]
print("Recetas:", recetas, "|", round(megabytes(RECETAS), 1), "MB | publicadas en 2007:", recetas_2007,
      "| por mes:", round(recetas_2007 / 12))

comidas = valor(f"select count(*) from read_csv_auto('{COMIDAS}')")[0]
comidas_sin_receta = valor(f"""
    select count(*) from read_csv_auto('{COMIDAS}') c
    anti join '{RECETAS}' r on c.RecipeId = r.RecipeId""")[0]
print("Comidas:", comidas, "|", round(megabytes(COMIDAS), 1), "MB | por día:", round(comidas / dias),
      "| con receta fuera del catálogo:", comidas_sin_receta)

relojes, usuarios, desvinculados = valor(f"""
    select count(*), count(distinct AuthorId), count(fecha_desvinculacion)
    from read_csv_auto('{DISPOSITIVOS}')""")
# vinculaciones dentro del período de las mediciones
vinculaciones = valor(f"""
    select count(*) from read_csv_auto('{DISPOSITIVOS}')
    where fecha_vinculacion >= (select min(fecha_hora) from read_csv_auto('{COMIDAS}'))""")[0]
print("Relojes:", relojes, "| usuarios:", usuarios, "| desvinculados:", desvinculados,
      "| vinculaciones en el período:", vinculaciones, "| por mes:", round(vinculaciones / dias * 30))

sesiones = valor(f"select count(*) from read_json_auto('{SESIONES}')")[0]
print("Sesiones:", sesiones, "|", round(megabytes(SESIONES), 1), "MB | por día:", round(sesiones / dias))

# los dos modelos nombran distinto los mismos campos: se unifican al leer
con.sql(f"""
    create table mediciones_crudas as
    select coalesce(device_id, dev) as reloj,
           coalesce(try_cast(ts_medicion as timestamptz), to_timestamp(timestamp / 1000)) as ts,
           coalesce(frecuencia_cardiaca, fc) as fc
    from read_json('{MEDICIONES}', columns = {{
        'device_id': 'VARCHAR', 'dev': 'VARCHAR', 'ts_medicion': 'VARCHAR',
        'timestamp': 'BIGINT', 'frecuencia_cardiaca': 'DOUBLE', 'fc': 'DOUBLE'}})""")
mediciones, fc_nula = valor("select count(*), count(*) - count(fc) from mediciones_crudas")
print("Mediciones:", mediciones, "|", len(glob.glob(MEDICIONES)), "archivos |",
      round(megabytes(MEDICIONES), 1), "MB | por día:", round(mediciones / dias),
      f"({megabytes(MEDICIONES) / dias:.1f} MB) | frecuencia cardíaca nula: {100 * fc_nula / mediciones:.1f}%")

total_mb = sum(megabytes(p) for p in (RECETAS, COMIDAS, DISPOSITIVOS, SESIONES, MEDICIONES))
print("Datos que usa el proyecto:", round(total_mb), "MB")

# 4.1: frecuencia cardíaca sin repetidos, fuera de sesiones, por hora local
con.sql(f"create table dispositivos as select * from read_csv_auto('{DISPOSITIVOS}')")
con.sql(f"""create table sesiones as select device_id as reloj, inicio::timestamptz as inicio,
            fin::timestamptz as fin from read_json_auto('{SESIONES}')""")
con.sql("""
    create table por_hora as
    select d.AuthorId as usuario, d.modelo,
           cast(timezone(d.zona_horaria, m.ts) as date) as dia,
           hour(timezone(d.zona_horaria, m.ts)) as hora,
           avg(m.fc) as fc, count(*) as n
    from (select distinct * from mediciones_crudas where fc is not null) m
    join dispositivos d on d.device_id = m.reloj
    where not exists (select 1 from sesiones s
                      where s.reloj = m.reloj and m.ts between s.inicio and s.fin)
    group by all""")

noche = valor(f"""select sum(fc * n) / sum(n) from por_hora
                  where hora between {HORAS_NOCHE[0]} and {HORAS_NOCHE[1]}""")[0]
tarde = valor(f"""select sum(fc * n) / sum(n) from por_hora
                  where hora between {HORAS_TARDE[0]} and {HORAS_TARDE[1]}""")[0]
print(f"Frecuencia cardíaca fuera de sesiones: madrugada {noche:.1f} | tarde {tarde:.1f}")

# por usuario y día: promedio de madrugada y hora con el promedio más bajo
minimos = " ".join(f"when '{modelo}' then {n}" for modelo, n in MINUTOS_MIN_HORA.items())
con.sql(f"""
    create table por_dia as
    select usuario, dia,
           min(fc) as fc_hora_mas_baja,
           sum(fc * n) filter (where hora between {HORAS_NOCHE[0]} and {HORAS_NOCHE[1]})
             / sum(n) filter (where hora between {HORAS_NOCHE[0]} and {HORAS_NOCHE[1]}) as fc_noche
    from por_hora
    where n >= case modelo {minimos} end
    group by usuario, dia""")
con.sql("""
    create table por_usuario as
    select usuario, avg(fc_noche) as fc_noche, avg(fc_hora_mas_baja) as fc_hora_mas_baja,
           count(fc_noche) as noches
    from por_dia group by usuario""")

universo, fuera, noches_min = valor("""
    select count(*) filter (where noches > 0), count(*) filter (where noches = 0),
           min(noches) filter (where noches > 0) from por_usuario""")
print("Universo:", universo, "usuarios | sin noche:", fuera, "| noches mínimas en el universo:", noches_min)

hora_min, hora_max = valor("""
    select min(hora), max(hora) from por_hora
    where usuario in (select usuario from por_usuario where noches = 0)""")
print(f"Horas locales con mediciones de los usuarios sin noche: {hora_min}:00 a {hora_max}:59")

parecido = valor("""select corr(fc_hora_mas_baja, fc_noche), avg(fc_hora_mas_baja - fc_noche)
                    from por_dia where fc_noche is not null""")
print(f"Universo, hora más baja frente a madrugada: correlación {parecido[0]:.3f} | diferencia {parecido[1]:.1f}")

hora_baja_universo, hora_baja_fuera = valor("""
    select avg(fc_hora_mas_baja) filter (where noches > 0),
           avg(fc_hora_mas_baja) filter (where noches = 0) from por_usuario""")
limite_cuartil = valor("select quantile_cont(fc_noche, 0.75) from por_usuario where noches > 0")[0]
sobre_limite = valor(f"""select count(*) from por_usuario
                         where noches = 0 and fc_hora_mas_baja > {limite_cuartil}""")[0]
print(f"Hora más baja: universo {hora_baja_universo:.1f} | sin noche {hora_baja_fuera:.1f}",
      f"| límite del cuartil más alto: {limite_cuartil:.1f} | sin noche sobre el límite: {sobre_limite}")
