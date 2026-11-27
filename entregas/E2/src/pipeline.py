"""Pipeline del proyecto: capas bronze, silver y gold con DuckDB.

Cada función pública es una tarea del DAG de la E2 y se puede volver a
ejecutar sin duplicar registros: reescribe completa la carpeta que produce.
"""

from __future__ import annotations

import os
import resource
import shutil
import statistics
import time
from pathlib import Path

import duckdb

HORAS_NOCHE = (2, 4)                                 # 2:00 a 4:59, hora local
MINUTOS_MIN_HORA = {"Pulso S": 30, "FitBand 2": 6}   # media hora de mediciones por modelo
FC_VALIDA = (20, 250)                                # latidos por minuto posibles en una persona
USUARIOS_FOODCOM = "foodcom/reviews.parquet"         # usuarios de Food.com: autores de reseñas


def raiz_proyecto(desde: Path | None = None) -> Path:
    """Sube desde la carpeta actual hasta la que tiene pyproject.toml."""
    inicio = (desde or Path.cwd()).resolve()
    for carpeta in (inicio, *inicio.parents):
        if (carpeta / "pyproject.toml").exists():
            return carpeta
    raise FileNotFoundError("No encuentro la raíz del proyecto (pyproject.toml)")


class Pipeline:
    def __init__(self, raiz: Path):
        self.raiz = raiz
        self.bronze = raiz / "datos" / "datos_proyecto_INF3862"
        self.silver = raiz / "datos" / "silver"
        self.gold = raiz / "datos" / "gold"
        if not (self.bronze / "wearable" / "mediciones").is_dir():
            raise FileNotFoundError("Faltan los datos: ver datos/README.md")
        self.con = duckdb.connect()
        self.con.sql("set preserve_insertion_order = false")
        self.calidad = {}     # regla de calidad: registros que afecta
        self.tiempos = {}     # tarea del DAG: segundos

    def sql(self, consulta):
        return self.con.sql(consulta)

    def valor(self, consulta):
        return self.con.sql(consulta).fetchall()[0]

    def tarea(self, nombre, funcion):
        inicio = time.perf_counter()
        funcion()
        self.tiempos[nombre] = round(time.perf_counter() - inicio, 1)

    def _escribir(self, consulta, destino: Path, particion: str | None = None):
        """Reescribe la carpeta completa: repetir la tarea no duplica filas."""
        if destino.exists():
            shutil.rmtree(destino)
        destino.parent.mkdir(parents=True, exist_ok=True)
        opciones = "FORMAT PARQUET, COMPRESSION ZSTD"
        if particion:
            opciones += f", PARTITION_BY ({particion})"
        else:
            destino.mkdir()
            destino = destino / "datos.parquet"
        self.con.sql(f"COPY ({consulta}) TO '{destino}' ({opciones})")

    def tabla(self, capa: Path, nombre: str) -> str:
        return f"read_parquet('{capa / nombre}/**/*.parquet', hive_partitioning = true)"

    # Tarea 1: bronze. Los archivos originales se leen tal como llegaron.
    def cargar_bronze(self):
        w = self.bronze / "wearable"
        # los dos modelos nombran distinto los mismos campos: se unifican al leer
        self.sql(f"""
            create or replace table mediciones_crudas as
            select coalesce(device_id, dev) as device_id,
                   coalesce(try_cast(ts_medicion as timestamptz),
                            to_timestamp(timestamp / 1000)) as ts_medicion,
                   coalesce(try_cast(ts_recepcion as timestamptz),
                            try_cast(received_at as timestamptz)) as ts_recepcion,
                   coalesce(frecuencia_cardiaca, fc) as frecuencia_cardiaca,
                   coalesce(pasos, steps) as pasos,
                   coalesce(calorias, kcal) as calorias
            from read_json('{w}/mediciones/*/*.jsonl.gz', columns = {{
                'device_id': 'VARCHAR', 'dev': 'VARCHAR', 'ts_medicion': 'VARCHAR',
                'timestamp': 'BIGINT', 'ts_recepcion': 'VARCHAR', 'received_at': 'VARCHAR',
                'frecuencia_cardiaca': 'DOUBLE', 'fc': 'DOUBLE', 'pasos': 'DOUBLE',
                'steps': 'DOUBLE', 'calorias': 'DOUBLE', 'kcal': 'DOUBLE'}})""")
        self.sql(f"create or replace table dispositivos as select * from read_csv_auto('{w}/dispositivos.csv')")
        self.sql(f"""
            create or replace table sesiones_crudas as
            select session_id, device_id, tipo, inicio::timestamptz as inicio, fin::timestamptz as fin,
                   distancia_km, calorias
            from read_json_auto('{w}/sesiones.jsonl')""")
        self.sql(f"create or replace table comidas_crudas as select * from read_csv_auto('{w}/comidas.csv')")
        self.sql(f"create or replace view recetas as select * from read_parquet('{self.bronze}/foodcom/recipes.parquet')")
        self.archivos_mediciones = len(list((w / "mediciones").glob("*/*.jsonl.gz")))
        self.dias_recepcion = len(list((w / "mediciones").glob("*")))

    def resumen_bronze(self):
        """Archivos, filas y tamaño de cada fuente tal como llegó."""
        w = self.bronze / "wearable"
        return [
            ("mediciones", "JSONL.GZ", self.archivos_mediciones,
             self.valor("select count(*) from mediciones_crudas")[0], megabytes(w / "mediciones")),
            ("dispositivos", "CSV", 1, self.valor("select count(*) from dispositivos")[0],
             megabytes(w, "dispositivos.csv")),
            ("sesiones", "JSONL", 1, self.valor("select count(*) from sesiones_crudas")[0],
             megabytes(w, "sesiones.jsonl")),
            ("comidas", "CSV", 1, self.valor("select count(*) from comidas_crudas")[0],
             megabytes(w, "comidas.csv")),
            ("recetas", "Parquet", 1, self.valor("select count(*) from recetas")[0],
             megabytes(self.bronze / "foodcom", "recipes.parquet")),
        ]

    def memoria_maxima_gb(self):
        # ru_maxrss viene en kilobytes en Linux
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6

    # Tarea 2: silver. Reglas de calidad y mediciones limpias por fecha de medición.
    def construir_silver(self):
        crudas = self.valor("select count(*) from mediciones_crudas")[0]
        self.calidad["mediciones leídas"] = crudas
        self.calidad["tipos: sin marca de tiempo o reloj"] = self.valor(
            "select count(*) from mediciones_crudas where ts_medicion is null or device_id is null")[0]
        self.calidad["claves: reloj fuera de dispositivos.csv"] = self.valor(
            "select count(*) from mediciones_crudas where device_id not in (select device_id from dispositivos)")[0]
        # un lote reenviado repite la medición con los mismos valores: se conserva la primera recepción
        self.sql("""
            create or replace table mediciones_unicas as
            select * exclude (orden) from (
                select *, row_number() over (partition by device_id, ts_medicion
                                             order by ts_recepcion) as orden
                from mediciones_crudas
                where ts_medicion is not null
                  and device_id in (select device_id from dispositivos))
            where orden = 1""")
        unicas = self.valor("select count(*) from mediciones_unicas")[0]
        self.calidad["duplicados: mismo reloj y marca de tiempo"] = crudas - unicas
        self.calidad["duplicados con valores distintos"] = self.valor("""
            select count(*) from (select device_id, ts_medicion from mediciones_crudas
                                  group by all having count(distinct (frecuencia_cardiaca, pasos, calorias)) > 1)""")[0]
        # una medición vale solo mientras el reloj está vinculado a su usuario
        self.sql("""
            create or replace table mediciones_limpias as
            select m.*, d.AuthorId as usuario_key, d.modelo, d.zona_horaria,
                   cast(m.ts_medicion as date) as fecha_medicion,
                   m.frecuencia_cardiaca is not null
                   and m.frecuencia_cardiaca not between {0} and {1} as fc_fuera_de_rango
            from mediciones_unicas m join dispositivos d using (device_id)
            where m.ts_medicion >= d.fecha_vinculacion
              and (d.fecha_desvinculacion is null or m.ts_medicion < d.fecha_desvinculacion)
            """.format(*FC_VALIDA))
        limpias = self.valor("select count(*) from mediciones_limpias")[0]
        self.calidad["claves: medición fuera del vínculo del reloj"] = unicas - limpias
        self.calidad["nulos: frecuencia cardíaca nula"] = self.valor(
            "select count(*) from mediciones_limpias where frecuencia_cardiaca is null")[0]
        self.calidad["rangos: frecuencia cardíaca fuera de 20-250"] = self.valor(
            "select count(*) from mediciones_limpias where fc_fuera_de_rango")[0]
        self.calidad["rangos: pasos o calorías negativos"] = self.valor(
            "select count(*) from mediciones_limpias where pasos < 0 or calorias < 0")[0]
        self.calidad["atrasos: recibidas más de 1 hora después"] = self.valor(
            "select count(*) from mediciones_limpias where ts_recepcion - ts_medicion > interval 1 hour")[0]
        self.calidad["atrasos: recibidas otro día (UTC)"] = self.valor(
            "select count(*) from mediciones_limpias where cast(ts_recepcion as date) > fecha_medicion")[0]
        self.atraso_maximo_dias = self.valor(
            "select max(cast(ts_recepcion as date) - fecha_medicion) from mediciones_limpias")[0]
        self.calidad["mediciones en silver"] = limpias

        # sesiones: una por session_id
        sesiones = self.valor("select count(*) from sesiones_crudas")[0]
        self.sql("""
            create or replace table sesiones as
            select * exclude (orden) from (
                select s.*, row_number() over (partition by session_id order by inicio) as orden
                from sesiones_crudas s
                where device_id in (select device_id from dispositivos) and fin > inicio)
            where orden = 1""")
        self.calidad["sesiones leídas"] = sesiones
        self.calidad["duplicados: sesión repetida"] = sesiones - self.valor("select count(*) from sesiones")[0]

        # comidas: la receta tiene que existir en el catálogo
        comidas = self.valor("select count(*) from comidas_crudas")[0]
        self.sql("""
            create or replace table comidas as
            select c.* from comidas_crudas c
            where c.RecipeId in (select RecipeId from recetas)
              and c.AuthorId in (select AuthorId from dispositivos)""")
        self.calidad["comidas leídas"] = comidas
        self.calidad["claves: comida con receta fuera del catálogo"] = self.valor(
            "select count(*) from comidas_crudas where RecipeId not in (select RecipeId from recetas)")[0]
        self.calidad["duplicados: comida repetida"] = comidas - self.valor(
            "select count(distinct comida_id) from comidas_crudas")[0]

        self._escribir("select * exclude (zona_horaria) from mediciones_limpias",
                       self.silver / "mediciones", particion="fecha_medicion")
        self._escribir("select * from sesiones", self.silver / "sesiones")
        self._escribir("select * from comidas", self.silver / "comidas")

    # Tarea 3: gold. Hechos y dimensiones del modelo de la E1, particionados por mes.
    def construir_gold(self):
        noche = f"between {HORAS_NOCHE[0]} and {HORAS_NOCHE[1]}"
        minimo = " ".join(f"when '{modelo}' then {n}" for modelo, n in MINUTOS_MIN_HORA.items())
        self.sql(f"create or replace view m as select * from {self.tabla(self.silver, 'mediciones')}")
        self.sql(f"create or replace view s as select * from {self.tabla(self.silver, 'sesiones')}")
        self.sql(f"create or replace view c as select * from {self.tabla(self.silver, 'comidas')}")
        self.sql("""
            create or replace table zona as
            select AuthorId as usuario_key, any_value(zona_horaria) as zona_horaria
            from dispositivos group by 1""")

        # frecuencia en reposo: madrugada local, fuera de sesiones, horas con cobertura mínima
        self.sql(f"""
            create or replace table fc_hora as
            select m.usuario_key, m.modelo,
                   cast(timezone(z.zona_horaria, m.ts_medicion) as date) as fecha_key,
                   hour(timezone(z.zona_horaria, m.ts_medicion)) as hora,
                   sum(m.frecuencia_cardiaca) as suma, count(*) as n
            from m join zona z using (usuario_key)
            where m.frecuencia_cardiaca is not null and not m.fc_fuera_de_rango
              and not exists (select 1 from s where s.device_id = m.device_id
                              and m.ts_medicion between s.inicio and s.fin)
            group by all""")
        self.sql(f"""
            create or replace table fc_reposo as
            select usuario_key, fecha_key, sum(suma) / sum(n) as fc_reposo, sum(n) as n_mediciones_fc_reposo
            from fc_hora
            where hora {noche} and n >= case modelo {minimo} end
            group by all""")

        self.sql("""
            create or replace table reloj_dia as
            select m.usuario_key, cast(timezone(z.zona_horaria, m.ts_medicion) as date) as fecha_key,
                   m.device_id as reloj_key, sum(m.pasos) as pasos,
                   sum(m.calorias) as calorias_gastadas, count(*) as mediciones
            from m join zona z using (usuario_key) group by all""")
        self.sql("""
            create or replace table sesion_dia as
            select d.AuthorId as usuario_key,
                   cast(timezone(d.zona_horaria, s.inicio) as date) as fecha_key,
                   sum(epoch(s.fin - s.inicio) / 60) as minutos_sesion,
                   sum(s.calorias) as calorias_sesion
            from s join dispositivos d using (device_id) group by all""")
        # lo consumido es la nutrición por porción de la receta por las porciones de la comida
        self.sql("""
            create or replace table comidas_gold as
            select c.comida_id, c.AuthorId as usuario_key, c.RecipeId as receta_key,
                   cast(timezone(z.zona_horaria, c.fecha_hora) as date) as fecha_key,
                   c.porciones,
                   r.Calories * c.porciones as calorias_consumidas,
                   r.SodiumContent * c.porciones as sodio,
                   r.SaturatedFatContent * c.porciones as grasa_saturada,
                   r.SugarContent * c.porciones as azucar,
                   r.FiberContent * c.porciones as fibra,
                   r.ProteinContent * c.porciones as proteina
            from c join recetas r using (RecipeId) join zona z on z.usuario_key = c.AuthorId""")
        self.sql("""
            create or replace table comida_dia as
            select usuario_key, fecha_key, count(*) as numero_comidas, sum(porciones) as porciones,
                   sum(calorias_consumidas) as calorias_consumidas, sum(sodio) as sodio,
                   sum(grasa_saturada) as grasa_saturada, sum(azucar) as azucar,
                   sum(fibra) as fibra, sum(proteina) as proteina
            from comidas_gold group by all""")

        # un día del usuario existe si hay mediciones o comidas; sin mediciones, pasos queda nulo
        self.sql("""
            create or replace table usuario_dia as
            with dias as (select usuario_key, fecha_key from reloj_dia
                          union select usuario_key, fecha_key from comida_dia),
            actividad as (select usuario_key, fecha_key, sum(pasos) as pasos,
                                 sum(calorias_gastadas) as calorias_gastadas, sum(mediciones) as mediciones
                          from reloj_dia group by all)
            select d.usuario_key, d.fecha_key, strftime(d.fecha_key, '%Y-%m') as mes,
                   a.pasos, a.calorias_gastadas, coalesce(a.mediciones, 0) as mediciones,
                   case when a.mediciones > 0 then coalesce(sd.minutos_sesion, 0) end as minutos_sesion,
                   case when a.mediciones > 0 then coalesce(sd.calorias_sesion, 0) end as calorias_sesion,
                   f.fc_reposo, coalesce(f.n_mediciones_fc_reposo, 0) as n_mediciones_fc_reposo,
                   coalesce(cd.numero_comidas, 0) as numero_comidas, coalesce(cd.porciones, 0) as porciones,
                   cd.calorias_consumidas, cd.sodio, cd.grasa_saturada, cd.azucar, cd.fibra, cd.proteina
            from dias d
            left join actividad a using (usuario_key, fecha_key)
            left join sesion_dia sd using (usuario_key, fecha_key)
            left join fc_reposo f using (usuario_key, fecha_key)
            left join comida_dia cd using (usuario_key, fecha_key)""")

        self._escribir("select * from usuario_dia", self.gold / "hecho_usuario_dia", particion="mes")
        self._escribir("select *, strftime(fecha_key, '%Y-%m') as mes from reloj_dia",
                       self.gold / "hecho_usuario_reloj_dia", particion="mes")
        self._escribir("select *, strftime(fecha_key, '%Y-%m') as mes from comidas_gold",
                       self.gold / "hecho_comida", particion="mes")
        self._escribir("""
            select distinct fecha_key, isodow(fecha_key) as dia_semana,
                   strftime(fecha_key, '%G-W%V') as semana, strftime(fecha_key, '%Y-%m') as mes
            from usuario_dia""", self.gold / "dim_fecha")
        self._escribir("select usuario_key, zona_horaria from zona", self.gold / "dim_usuario")
        self._escribir("""
            select device_id as reloj_key, modelo, AuthorId as usuario_key,
                   fecha_vinculacion as desde, fecha_desvinculacion as hasta
            from dispositivos""", self.gold / "dim_reloj")
        self._escribir("""
            select RecipeId as receta_key, Name as nombre, RecipeCategory as categoria,
                   Calories as calorias, SodiumContent as sodio, SaturatedFatContent as grasa_saturada,
                   SugarContent as azucar, FiberContent as fibra, ProteinContent as proteina
            from recetas where RecipeId in (select RecipeId from c)""", self.gold / "dim_receta")

    def registrar_vistas_gold(self):
        for nombre in ("hecho_usuario_dia", "hecho_usuario_reloj_dia", "hecho_comida",
                       "dim_fecha", "dim_usuario", "dim_reloj", "dim_receta"):
            self.sql(f"create or replace view {nombre} as select * from {self.tabla(self.gold, nombre)}")

    def ejecutar(self):
        """Corre el DAG completo y devuelve el tiempo de cada tarea."""
        self.tarea("cargar_bronze", self.cargar_bronze)
        self.tarea("construir_silver", self.construir_silver)
        self.tarea("construir_gold", self.construir_gold)
        self.registrar_vistas_gold()
        return self.tiempos


def megabytes(carpeta: Path, patron: str = "**/*") -> float:
    return sum(p.stat().st_size for p in carpeta.glob(patron) if p.is_file()) / 1e6


def medir(funcion, repeticiones: int = 7) -> float:
    """Mediana de varias ejecuciones, en milisegundos: la primera calienta la caché del sistema."""
    tiempos = []
    for _ in range(repeticiones):
        inicio = time.perf_counter()
        funcion()
        tiempos.append((time.perf_counter() - inicio) * 1000)
    return statistics.median(tiempos)


def cpus_visibles() -> int:
    return len(os.sched_getaffinity(0))
