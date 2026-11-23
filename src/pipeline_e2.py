"""Pipeline local E2 para el caso wearable + recetas.

Procesa CSV/JSONL/GZIP con la biblioteca estándar de Python. Si DuckDB Python o el CLI están
disponibles, también incorpora nutrientes desde recipes.parquet. Los originales
no se modifican; los resultados van a datos/processed/e2 (ignorado por Git).
"""

from __future__ import annotations

import bisect
import csv
import gzip
import io
import json
import math
import os
import shutil
import subprocess
import time
from collections import defaultdict
from datetime import date, datetime, time as daytime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


UTC = timezone.utc
NIGHT_HOURS = range(2, 5)  # intervalo local [02:00, 05:00)
MIN_SAMPLES_PER_HOUR = {"Pulso S": 30, "FitBand 2": 6}
NUTRIENTS = ("calorias", "sodio", "grasa_saturada", "azucar", "fibra", "proteina")


def locate_data(repo_root: Path) -> Path:
    """Encuentra datos tanto dentro del repo como en la carpeta hermana descargada."""
    candidates = [
        Path(os.environ["INF3862_DATA_DIR"]) if os.environ.get("INF3862_DATA_DIR") else None,
        repo_root / "datos" / "datos_proyecto_INF3862",
        repo_root.parent / "datos_proyecto_INF3862",
    ]
    for candidate in candidates:
        if candidate and (candidate / "wearable" / "mediciones").is_dir():
            return candidate.resolve()
    raise FileNotFoundError(
        "No encuentro datos_proyecto_INF3862. Colócalos en repo/datos/ o define INF3862_DATA_DIR."
    )


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def parse_measurement_ts(row: dict) -> datetime:
    if row.get("ts_medicion"):
        parsed = parse_ts(row["ts_medicion"])
        if parsed is None:
            raise ValueError("ts_medicion vacío")
        return parsed
    epoch_ms = row.get("timestamp")
    if epoch_ms is None:
        raise ValueError("medición sin ts_medicion ni timestamp")
    return datetime.fromtimestamp(int(epoch_ms) / 1000, tz=UTC)


def safe_zone(name: str | None, counters: dict) -> ZoneInfo:
    try:
        return ZoneInfo(name or "UTC")
    except ZoneInfoNotFoundError:
        counters["unknown_timezone"] += 1
        return ZoneInfo("UTC")


def read_csv_rows(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        yield from csv.DictReader(stream)


def as_float(value, default=0.0):
    try:
        if value in (None, ""):
            return default
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError):
        return default


def local_day(ts: datetime, zone: ZoneInfo) -> tuple[date, int]:
    local = ts.astimezone(zone)
    return local.date(), local.hour


def is_bound(device: dict, ts: datetime) -> bool:
    return device["linked"] <= ts and (device["unlinked"] is None or ts < device["unlinked"])


def session_index(sessions):
    result = {}
    for device_id, items in sessions.items():
        items.sort(key=lambda item: item[0])
        result[device_id] = ([item[0] for item in items], items)
    return result


def in_session(index, device_id: str, ts: datetime) -> bool:
    pair = index.get(device_id)
    if pair is None:
        return False
    starts, items = pair
    i = bisect.bisect_right(starts, ts) - 1
    # Sessions are sparse; a few preceding intervals cover overlap/retry cases.
    for j in range(i, max(-1, i - 5), -1):
        if items[j][0] <= ts < items[j][1]:
            return True
    return False


def summarize_resting_hr(hourly_hr: dict) -> dict:
    """Recalculate the E1 coverage/universe figures from cleaned hourly data."""
    global_groups = {"night": [0.0, 0], "day": [0.0, 0]}
    eligible_by_user_day = defaultdict(list)
    for (user_id, day, hour, model), (total, n) in hourly_hr.items():
        if hour in NIGHT_HOURS:
            global_groups["night"][0] += total
            global_groups["night"][1] += n
        if 12 <= hour <= 18:
            global_groups["day"][0] += total
            global_groups["day"][1] += n
        if n >= MIN_SAMPLES_PER_HOUR.get(model, 1):
            eligible_by_user_day[(user_id, day)].append((hour, total / n, total, n))

    by_user = defaultdict(lambda: {"night": [], "low": []})
    for (user_id, _day), hours in eligible_by_user_day.items():
        by_user[user_id]["low"].append(min(item[1] for item in hours))
        night = [item for item in hours if item[0] in NIGHT_HOURS]
        if night:
            by_user[user_id]["night"].append(
                sum(item[2] for item in night) / sum(item[3] for item in night)
            )
    night_users = [u for u, values in by_user.items() if values["night"]]
    no_night_users = [u for u, values in by_user.items() if not values["night"]]
    # Keep day-level coverage visible as well as the user-level universe.
    eligible_users = set(night_users)
    universe_days = [
        hours for (user_id, _day), hours in eligible_by_user_day.items()
        if user_id in eligible_users
    ]
    days_without_night = sum(
        not any(hour in NIGHT_HOURS for hour, *_ in hours)
        for hours in universe_days
    )
    avg = lambda xs: round(sum(xs) / len(xs), 1) if xs else None
    mean_user_night = {u: sum(by_user[u]["night"]) / len(by_user[u]["night"])
                       for u in night_users}
    sorted_values = sorted(mean_user_night.values())
    if sorted_values:
        pos = 0.75 * (len(sorted_values) - 1)
        lo, hi = math.floor(pos), math.ceil(pos)
        q75 = sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (pos - lo)
    else:
        q75 = None
    return {
        "mean_measurement_hr_02_04": avg([global_groups["night"][0] / global_groups["night"][1]] if global_groups["night"][1] else []),
        "mean_measurement_hr_12_18": avg([global_groups["day"][0] / global_groups["day"][1]] if global_groups["day"][1] else []),
        "users_with_eligible_night": len(night_users),
        "users_without_eligible_night": len(no_night_users),
        "eligible_days_in_night_universe": len(universe_days),
        "eligible_days_without_night_readings": days_without_night,
        "mean_user_night_hr": avg(list(mean_user_night.values())),
        "mean_user_lowest_hour_hr_with_night": avg([avg(by_user[u]["low"]) for u in night_users if by_user[u]["low"]]),
        "mean_user_lowest_hour_hr_without_night": avg([avg(by_user[u]["low"]) for u in no_night_users if by_user[u]["low"]]),
        "q75_user_night_hr": round(q75, 1) if q75 is not None else None,
        "users_without_night_above_q75": sum(
            1 for u in no_night_users if by_user[u]["low"] and avg(by_user[u]["low"]) > q75
        ) if q75 is not None else None,
        "method": "same deduplicated, in-binding, out-of-session hourly observations used by E2; per-hour minimum coverage follows device model",
    }


def load_recipe_nutrition(data_root: Path, meals_path: Path, devices_path: Path):
    """Join meals to recipe nutrients using DuckDB Python or the portable CLI."""
    try:
        import duckdb  # type: ignore
        if not callable(getattr(duckdb, "connect", None)):
            duckdb = None
    except ImportError:
        duckdb = None

    cli = os.environ.get("DUCKDB_CLI") or shutil.which("duckdb")
    if not cli:
        candidate = Path(__file__).resolve().parents[1] / ".local-tools" / "duckdb" / "duckdb.exe"
        if candidate.is_file():
            cli = str(candidate)
    if duckdb is None and not cli:
        return None, "DuckDB Python package or CLI not found; nutrient join skipped.", None, None

    recipes = (data_root / "foodcom" / "recipes.parquet").as_posix().replace("'", "''")
    meals = meals_path.as_posix().replace("'", "''")
    devices = devices_path.as_posix().replace("'", "''")

    def run_sql(sql):
        if duckdb is not None:
            cur = con.execute(sql)
            return [col[0] for col in cur.description], cur.fetchall()
        proc = subprocess.run(
            [str(cli), "-no-agent", "-csv", "-c", sql],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        parsed = list(csv.reader(io.StringIO(proc.stdout)))
        if not parsed:
            return [], []
        return parsed[0], parsed[1:]

    con = duckdb.connect() if duckdb is not None else None
    try:
        _, schema_rows = run_sql(f"DESCRIBE SELECT * FROM read_parquet('{recipes}')")
        columns = {row[0] for row in schema_rows}
        map_cols = {
            "calorias": next((c for c in ("Calories", "calories") if c in columns), None),
            "sodio": next((c for c in ("SodiumContent", "Sodium", "sodium") if c in columns), None),
            "grasa_saturada": next((c for c in ("SaturatedFatContent", "SaturatedFat", "saturated_fat") if c in columns), None),
            "azucar": next((c for c in ("SugarContent", "Sugar", "sugar") if c in columns), None),
            "fibra": next((c for c in ("FiberContent", "Fiber", "fiber") if c in columns), None),
            "proteina": next((c for c in ("ProteinContent", "Protein", "protein") if c in columns), None),
        }
        if "RecipeId" not in columns or not any(map_cols.values()):
            return None, "Could not recognize the nutrition columns in recipes.parquet.", None, None
        expressions = []
        for out_name, col in map_cols.items():
            expr = f"try_cast(r.\"{col}\" AS DOUBLE)" if col else "NULL::DOUBLE"
            expressions.append(f"sum({expr} * try_cast(c.porciones AS DOUBLE)) AS {out_name}")
        query = f"""
          SELECT c.AuthorId AS usuario_id,
                 CAST(timezone(coalesce(d.zona_horaria, 'UTC'), c.fecha_hora::TIMESTAMPTZ) AS DATE) AS dia,
                 {', '.join(expressions)}
          FROM read_csv_auto('{meals}') c
          JOIN read_parquet('{recipes}') r ON c.RecipeId = r.RecipeId
          LEFT JOIN (SELECT AuthorId, any_value(zona_horaria) AS zona_horaria
                     FROM read_csv_auto('{devices}') GROUP BY AuthorId) d
            ON c.AuthorId = d.AuthorId
          GROUP BY c.AuthorId, dia
        """
        names, rows = run_sql(query)
        result = {}
        for row in rows:
            result[(str(row[0]), str(row[1]))] = dict(zip(names[2:], row[2:]))
        recipe_count_names, recipe_count_rows = run_sql(
            f"SELECT count(*) AS recipe_rows FROM read_parquet('{recipes}')"
        )
        _, unmatched_rows = run_sql(f"""
          SELECT count(*) AS unmatched_meals
          FROM read_csv_auto('{meals}') c
          LEFT JOIN read_parquet('{recipes}') r ON c.RecipeId = r.RecipeId
          WHERE r.RecipeId IS NULL
        """)
        recipe_count = int(recipe_count_rows[0][0]) if recipe_count_rows else 0
        unmatched_count = int(unmatched_rows[0][0]) if unmatched_rows else 0
        engine = "DuckDB Python" if duckdb is not None else "DuckDB CLI"
        note = (f"Nutrition joined from recipes.parquet by RecipeId and scaled by servings "
                f"using {engine}; source-null nutrient values remain blank.")
        return result, note, recipe_count, unmatched_count
    finally:
        if con is not None:
            con.close()


def run_pipeline(repo_root: str | Path | None = None, output_dir: str | Path | None = None) -> dict:
    started = time.time()
    root = Path(repo_root or Path(__file__).resolve().parents[1]).resolve()
    data_root = locate_data(root)
    wearable = data_root / "wearable"
    recipes_path = data_root / "foodcom" / "recipes.parquet"
    measures_root = wearable / "mediciones"
    output = Path(output_dir) if output_dir else root / "datos" / "processed" / "e2"
    output.mkdir(parents=True, exist_ok=True)

    counters = defaultdict(int)
    devices = {}
    user_zones = {}
    users = set()
    for row in read_csv_rows(wearable / "dispositivos.csv"):
        device_id = row["device_id"]
        user_id = str(row["AuthorId"])
        users.add(user_id)
        zone = row.get("zona_horaria") or "UTC"
        user_zones.setdefault(user_id, zone)
        devices[device_id] = {
            "user": user_id,
            "model": row.get("modelo") or "desconocido",
            "zone_name": zone,
            "zone": safe_zone(zone, counters),
            "linked": parse_ts(row.get("fecha_vinculacion")) or datetime.min.replace(tzinfo=UTC),
            "unlinked": parse_ts(row.get("fecha_desvinculacion")),
        }

    sessions = defaultdict(list)
    session_minutes = defaultdict(float)
    session_calories = defaultdict(float)
    sessions_path = wearable / "sesiones.jsonl"
    with sessions_path.open("r", encoding="utf-8") as stream:
        for line in stream:
            try:
                row = json.loads(line)
                device = devices.get(row.get("device_id"))
                start, end = parse_ts(row.get("inicio")), parse_ts(row.get("fin"))
                if not device or not start or not end or end <= start:
                    counters["invalid_sessions"] += 1
                    continue
                if not is_bound(device, start):
                    counters["sessions_outside_binding"] += 1
                    continue
                sessions[row["device_id"]].append((start, end, row))
                local_date = start.astimezone(device["zone"]).date().isoformat()
                key = (device["user"], local_date)
                session_minutes[key] += (end - start).total_seconds() / 60
                session_calories[key] += as_float(row.get("calorias"))
            except (ValueError, TypeError, json.JSONDecodeError):
                counters["invalid_sessions"] += 1
    sessions_lookup = session_index({k: [(x[0], x[1]) for x in v] for k, v in sessions.items()})

    daily = defaultdict(lambda: {"pasos": 0.0, "calorias_gastadas": 0.0, "lecturas": 0})
    per_device_day = defaultdict(lambda: {"pasos": 0.0, "calorias_gastadas": 0.0, "lecturas": 0})
    hourly_hr = defaultdict(lambda: [0.0, 0])
    seen = set()
    measurement_files = sorted(measures_root.rglob("*.jsonl.gz"))
    day_names = sorted(p.name for p in measures_root.iterdir() if p.is_dir())
    if not measurement_files:
        raise FileNotFoundError(f"No encuentro JSONL gzip en {measures_root}")
    device_ids = {device_id: i for i, device_id in enumerate(sorted(devices))}

    for file_number, path in enumerate(measurement_files, 1):
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            for line in stream:
                counters["measurement_rows_raw"] += 1
                try:
                    row = json.loads(line)
                    device_id = row.get("device_id") or row.get("dev")
                    device = devices.get(device_id)
                    if device is None:
                        counters["unknown_device"] += 1
                        continue
                    ts = parse_measurement_ts(row)
                    epoch_us = int(ts.timestamp() * 1_000_000)
                    unique_key = (device_ids[device_id] << 51) | epoch_us
                    if unique_key in seen:
                        counters["duplicate_device_timestamp"] += 1
                        continue
                    seen.add(unique_key)
                    counters["measurement_rows_unique"] += 1
                    if not is_bound(device, ts):
                        counters["measurements_outside_binding"] += 1
                        continue
                    day, hour = local_day(ts, device["zone"])
                    key = (device["user"], day.isoformat())
                    dkey = (device["user"], day.isoformat(), device_id)
                    steps = as_float(row.get("pasos", row.get("steps")))
                    kcal = as_float(row.get("calorias", row.get("kcal")))
                    daily[key]["pasos"] += steps
                    daily[key]["calorias_gastadas"] += kcal
                    daily[key]["lecturas"] += 1
                    per_device_day[dkey]["pasos"] += steps
                    per_device_day[dkey]["calorias_gastadas"] += kcal
                    per_device_day[dkey]["lecturas"] += 1
                    fc_raw = row.get("frecuencia_cardiaca", row.get("fc"))
                    if fc_raw in (None, ""):
                        counters["heart_rate_null"] += 1
                    elif not in_session(sessions_lookup, device_id, ts):
                        fc = as_float(fc_raw, float("nan"))
                        if math.isfinite(fc):
                            hkey = (device["user"], day.isoformat(), hour, device["model"])
                            hourly_hr[hkey][0] += fc
                            hourly_hr[hkey][1] += 1
                    received = row.get("ts_recepcion") or row.get("received_at")
                    recv = parse_ts(received) if received else None
                    if recv and (recv - ts).total_seconds() > 3600:
                        counters["received_over_1h_late"] += 1
                except (ValueError, TypeError, KeyError, json.JSONDecodeError, OverflowError):
                    counters["invalid_measurements"] += 1
        if file_number % 120 == 0:
            print(f"Procesados {file_number}/{len(measurement_files)} archivos de mediciones; filas={counters['measurement_rows_raw']:,}")

    # Incluye solo horas con cobertura suficiente para no comparar frecuencia de muestreo desigual.
    rhr = defaultdict(lambda: [0.0, 0])
    for (user_id, day, hour, model), (hr_sum, n) in hourly_hr.items():
        if hour in NIGHT_HOURS and n >= MIN_SAMPLES_PER_HOUR.get(model, 1):
            rhr[(user_id, day)][0] += hr_sum
            rhr[(user_id, day)][1] += n

    meals_path = wearable / "comidas.csv"
    meal_daily = defaultdict(lambda: {"comidas": 0, "porciones": 0.0})
    meal_ids = set()
    meal_recipe_ids = set()
    for row in read_csv_rows(meals_path):
        meal_id = row.get("comida_id")
        if meal_id in meal_ids:
            counters["duplicate_meal_id"] += 1
        meal_ids.add(meal_id)
        user_id = str(row.get("AuthorId", ""))
        recipe_id = row.get("RecipeId")
        meal_recipe_ids.add(recipe_id)
        user_zone = safe_zone(user_zones.get(user_id, "UTC"), counters)
        ts = parse_ts(row.get("fecha_hora"))
        if not ts:
            counters["invalid_meals"] += 1
            continue
        day = ts.astimezone(user_zone).date().isoformat()
        meal_daily[(user_id, day)]["comidas"] += 1
        meal_daily[(user_id, day)]["porciones"] += as_float(row.get("porciones"))

    nutrients_daily, nutrients_note, recipe_rows, unmatched_meals = load_recipe_nutrition(
        data_root, meals_path, wearable / "dispositivos.csv"
    )
    if nutrients_daily is None:
        counters["nutrient_join_available"] = 0
        counters["recipe_rows_parquet_read"] = "not read"
        counters["meals_without_recipe_match"] = "not measured"
    else:
        counters["nutrient_join_available"] = 1
        counters["recipe_rows_parquet_read"] = recipe_rows
        counters["meals_without_recipe_match"] = unmatched_meals
    recipe_ids_available = None
    if nutrients_daily is not None:
        recipe_ids_available = True  # unmatched references are counted in the join query below when enabled

    # Dense user-day spine over the measurement acquisition window, shifted into each user's local dates.
    utc_start = date.fromisoformat(day_names[0])
    utc_end = date.fromisoformat(day_names[-1])
    user_days = {}
    for user_id in sorted(users):
        zone = safe_zone(user_zones.get(user_id, "UTC"), counters)
        start_local = datetime.combine(utc_start, daytime.min, UTC).astimezone(zone).date()
        end_local = datetime.combine(utc_end + timedelta(days=1), daytime.min, UTC).astimezone(zone).date()
        day = start_local
        while day <= end_local:
            key = (user_id, day.isoformat())
            base = daily.get(key, {})
            meal = meal_daily.get(key, {})
            rhr_sum, rhr_n = rhr.get(key, (0.0, 0))
            nutrient = (nutrients_daily or {}).get(key, {})
            user_days[key] = {
                "usuario_id": user_id,
                "fecha_local": day.isoformat(),
                "pasos": round(base.get("pasos", 0.0), 3),
                # Measurement calories and session-reported calories are kept
                # separate to avoid double counting the same activity.
                "calorias_gastadas": round(base.get("calorias_gastadas", 0.0), 3),
                "calorias_sesion": round(session_calories.get(key, 0.0), 3),
                "minutos_sesion": round(session_minutes.get(key, 0.0), 3),
                "numero_comidas": meal.get("comidas", 0),
                "porciones": round(meal.get("porciones", 0.0), 3),
                "fc_reposo": round(rhr_sum / rhr_n, 3) if rhr_n else "",
                "n_mediciones_fc_reposo": rhr_n,
                "calorias_consumidas": nutrient.get("calorias", ""),
                "sodio": nutrient.get("sodio", ""),
                "grasa_saturada": nutrient.get("grasa_saturada", ""),
                "azucar": nutrient.get("azucar", ""),
                "fibra": nutrient.get("fibra", ""),
                "proteina": nutrient.get("proteina", ""),
            }
            day += timedelta(days=1)

    # Per-device-day table supports model comparison without assigning a switched watch's data to another model.
    device_by_day_rows = []
    for (user_id, day, device_id), values in sorted(per_device_day.items()):
        device = devices[device_id]
        device_by_day_rows.append({
            "usuario_id": user_id,
            "fecha_local": day,
            "device_id": device_id,
            "modelo": device["model"],
            "pasos": round(values["pasos"], 3),
            "calorias_gastadas": round(values["calorias_gastadas"], 3),
            "mediciones": values["lecturas"],
        })

    daily_rows = list(user_days.values())
    write_csv(output / "fact_usuario_dia.csv", daily_rows)
    write_csv(output / "fact_usuario_reloj_dia.csv", device_by_day_rows)
    analytical_products = materialize_analytical_products(output, data_root)
    from consultas_analiticas_e2 import run_analytical_queries
    analytical_queries = run_analytical_queries(root, output, data_root)

    inventory = []
    for path in sorted(data_root.rglob("*")):
        if path.is_file():
            inventory.append({"ruta_relativa": path.relative_to(data_root).as_posix(), "bytes": path.stat().st_size})
    write_csv(output / "manifest_fuentes.csv", inventory)

    valid_rhr_users = {user_id for (user_id, _), val in rhr.items() if val[1] > 0}
    resting_hr_audit = summarize_resting_hr(hourly_hr)
    counters["users_in_device_registry"] = len(users)
    counters["users_with_resting_hr"] = len(valid_rhr_users)
    counters["days_in_utc_window"] = len(day_names)
    counters["measurement_files"] = len(measurement_files)
    counters["meal_rows"] = len(meal_ids)
    counters["meal_recipe_ids_distinct"] = len(meal_recipe_ids)
    counters["recipe_parquet_present"] = int(recipes_path.exists())
    if not recipes_path.exists():
        counters["recipe_rows_parquet_read"] = 0
    counters["session_rows_raw"] = sum(len(v) for v in sessions.values()) + counters["invalid_sessions"] + counters["sessions_outside_binding"]
    counters["fact_usuario_dia_rows"] = len(daily_rows)
    counters["fact_usuario_reloj_dia_rows"] = len(device_by_day_rows)
    counters["rows_with_resting_hr"] = sum(1 for row in daily_rows if row["fc_reposo"] != "")
    counters["rows_with_nutrient_join"] = sum(1 for row in daily_rows if row["calorias_consumidas"] != "")
    counters["elapsed_seconds"] = round(time.time() - started, 2)
    quality = {
        "pipeline": "E2 local, Python estándar + DuckDB",
        "data_root": str(data_root),
        "output_dir": str(output.resolve()),
        "nutrient_join": nutrients_note,
        "analytical_products": analytical_products,
        "analytical_queries": analytical_queries,
        "measurements_definition": "deduplicated by (device_id, measurement timestamp); UTC timestamps converted using device timezone",
        "resting_hr_definition": "mean of valid measurements in local hours 02:00-04:59, outside sessions; hourly coverage threshold 30 (Pulso S), 6 (FitBand 2)",
        "e1_resting_hr_audit": resting_hr_audit,
        "missing_value_policy": "no meal or activity event becomes zero in the dense user-day table; resting heart rate stays null without eligible nighttime readings; absent nutrient source values remain blank",
        "counts": dict(counters),
    }
    (output / "quality_report.json").write_text(json.dumps(quality, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(quality, ensure_ascii=False, indent=2))
    return quality


def write_csv(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def materialize_analytical_products(output: Path, data_root: Path) -> dict:
    """Create compact dimensional outputs and date-partitioned Parquet with DuckDB CLI.

    We materialize the already validated daily fact rather than loading all 15M
    raw measurements into one in-memory relation. Raw gzip files remain the
    immutable Bronze source; the user-day fact is the Silver/Gold boundary for
    the descriptive workload in this project.
    """
    cli = os.environ.get("DUCKDB_CLI") or shutil.which("duckdb")
    if not cli:
        candidate = Path(__file__).resolve().parents[1] / ".local-tools" / "duckdb" / "duckdb.exe"
        cli = str(candidate) if candidate.is_file() else None
    if not cli:
        return {"parquet_available": False, "reason": "DuckDB CLI unavailable"}

    fact_csv = (output / "fact_usuario_dia.csv").resolve().as_posix().replace("'", "''")
    parquet_dir = output / "fact_usuario_dia_parquet"
    if parquet_dir.exists():
        shutil.rmtree(parquet_dir)
    parquet_sql_path = parquet_dir.resolve().as_posix().replace("'", "''")
    sql = f"""
      SET threads=2;
      SET memory_limit='2GB';
      SET preserve_insertion_order=false;
      COPY (SELECT * FROM read_csv_auto('{fact_csv}', header=true))
      TO '{parquet_sql_path}' (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (fecha_local));
    """
    subprocess.run([str(cli), "-no-agent", "-c", sql], check=True,
                   capture_output=True, text=True, encoding="utf-8")

    # Small star-schema dimensions derived from the source registry and date spine.
    devices = list(read_csv_rows(data_root / "wearable" / "dispositivos.csv"))
    user_zones = {}
    for device in devices:
        user_zones.setdefault(str(device["AuthorId"]), device.get("zona_horaria") or "UTC")
    write_csv(output / "dim_usuario.csv", [
        {"usuario_id": uid, "zona_horaria": zone}
        for uid, zone in sorted(user_zones.items())
    ])
    write_csv(output / "dim_dispositivo.csv", [{
        "device_id": d.get("device_id"), "usuario_id": d.get("AuthorId"),
        "modelo": d.get("modelo"), "fecha_vinculacion": d.get("fecha_vinculacion"),
        "fecha_desvinculacion": d.get("fecha_desvinculacion"), "zona_horaria": d.get("zona_horaria")
    } for d in devices])
    unique_days = sorted({r["fecha_local"] for r in read_csv_rows(output / "fact_usuario_dia.csv")})
    write_csv(output / "dim_fecha.csv", [{"fecha_local": d, "anio": d[:4], "mes": d[5:7],
        "dia": d[8:10], "dia_semana": date.fromisoformat(d).isoweekday()} for d in unique_days])

    # Benchmark an analytical daily aggregation against CSV and partition-pruned Parquet.
    selected_day = unique_days[len(unique_days) // 2]
    parquet_glob = (parquet_dir / "**" / "*.parquet").resolve().as_posix().replace("'", "''")
    query_csv = (f"SELECT sum(try_cast(pasos AS DOUBLE)) FROM read_csv_auto('{fact_csv}') "
                 f"WHERE fecha_local='{selected_day}'")
    query_parquet = (f"SELECT sum(try_cast(pasos AS DOUBLE)) FROM read_parquet('{parquet_glob}', "
                     f"hive_partitioning=true) WHERE fecha_local='{selected_day}'")

    def benchmark(query):
        start = time.perf_counter()
        result = subprocess.run([str(cli), "-no-agent", "-csv", "-c", query], check=True,
                                capture_output=True, text=True, encoding="utf-8")
        elapsed = round(time.perf_counter() - start, 4)
        return elapsed, result.stdout.strip().splitlines()[-1]

    csv_seconds, csv_result = benchmark(query_csv)
    parquet_seconds, parquet_result = benchmark(query_parquet)
    if csv_result != parquet_result:
        raise ValueError(f"CSV/Parquet benchmark mismatch: {csv_result} != {parquet_result}")
    parquet_bytes = sum(p.stat().st_size for p in parquet_dir.rglob("*.parquet"))
    observed_rows = 15_540_132
    observed_bytes = 115_500_000
    three_year_factor = (365.25 * 3) / 61
    scale_estimate = {
        "usuarios_con_reloj_observados": 300,
        "ventana_observada_dias": 61,
        "tres_anios_usuarios_observados": {
            "filas_aprox": round(observed_rows * three_year_factor),
            "bytes_comprimidos_aprox": round(observed_bytes * three_year_factor),
        },
        "escenario_cien_veces": {
            "filas_aprox": round(observed_rows * three_year_factor * 100),
            "bytes_comprimidos_aprox": round(observed_bytes * three_year_factor * 100),
        },
        "supuesto": "extrapolación lineal del ritmo de mediciones; no incluye réplicas ni crecimiento de recetas",
    }
    return {
        "parquet_available": True,
        "silver_gold_fact": "fact_usuario_dia_parquet/ (Parquet ZSTD, particion fecha_local)",
        "dimensions": ["dim_usuario.csv", "dim_dispositivo.csv", "dim_fecha.csv"],
        "scale_estimate": scale_estimate,
        "benchmark": {"consulta": "suma de pasos de todos los usuarios en un día",
            "fecha": selected_day, "resultado_igual": True,
            "csv_seconds": csv_seconds, "parquet_seconds": parquet_seconds,
            "csv_bytes": (output / "fact_usuario_dia.csv").stat().st_size,
            "parquet_bytes": parquet_bytes,
            "nota": "Tiempos incluyen inicio del proceso CLI; útiles solo como comparación local aproximada."}
    }


if __name__ == "__main__":
    run_pipeline()
