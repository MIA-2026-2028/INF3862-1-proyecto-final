"""Dataset y modelo base E3, con partición por usuario y sin fuga temporal."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import statistics
from datetime import date, timedelta
from pathlib import Path


BASE_FEATURES = ["pasos_7d_promedio", "minutos_sesion_7d_promedio", "comidas_7d", "porciones_7d"]
NUTRIENT_SOURCE = {
    "calorias_consumidas": "calorias_7d",
    "sodio": "sodio_7d",
    "grasa_saturada": "grasa_saturada_7d",
    "azucar": "azucar_7d",
    "fibra": "fibra_7d",
    "proteina": "proteina_7d",
}


def read_csv(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def num(value):
    try:
        if value in (None, ""):
            return None
        x = float(value)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def write_csv(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def solve_linear_system(matrix, vector):
    """Gaussian elimination with pivoting; matrix is tiny (<= 11 x 11)."""
    a = [list(map(float, row)) + [float(vector[i])] for i, row in enumerate(matrix)]
    n = len(a)
    for col in range(n):
        pivot = max(range(col, n), key=lambda r: abs(a[r][col]))
        if abs(a[pivot][col]) < 1e-12:
            a[pivot][col] += 1e-8
        a[col], a[pivot] = a[pivot], a[col]
        denom = a[col][col]
        if abs(denom) < 1e-12:
            raise ValueError("Diseño singular: no es posible ajustar la regresión base")
        for j in range(col, n + 1):
            a[col][j] /= denom
        for r in range(n):
            if r == col:
                continue
            factor = a[r][col]
            for j in range(col, n + 1):
                a[r][j] -= factor * a[col][j]
    return [a[i][n] for i in range(n)]


def fit_ols(train_rows, feature_names):
    means, scales = {}, {}
    for feature in feature_names:
        vals = [r[feature] for r in train_rows]
        means[feature] = statistics.fmean(vals)
        sd = statistics.pstdev(vals)
        scales[feature] = sd if sd > 1e-10 else 1.0
    width = len(feature_names) + 1
    xtx = [[0.0] * width for _ in range(width)]
    xty = [0.0] * width
    for row in train_rows:
        x = [1.0] + [(row[f] - means[f]) / scales[f] for f in feature_names]
        y = row["target_fc_reposo"]
        for i in range(width):
            xty[i] += x[i] * y
            for j in range(width):
                xtx[i][j] += x[i] * x[j]
    # Small numerical regularization, excluding the intercept.
    for i in range(1, width):
        xtx[i][i] += 1e-6
    return solve_linear_system(xtx, xty), means, scales


def predict_ols(row, beta, means, scales, feature_names):
    return beta[0] + sum(beta[i + 1] * ((row[f] - means[f]) / scales[f]) for i, f in enumerate(feature_names))


def run_model(repo_root: str | Path | None = None, input_path: str | Path | None = None, output_dir: str | Path | None = None):
    root = Path(repo_root or Path(__file__).resolve().parents[1]).resolve()
    source = Path(input_path) if input_path else root / "datos" / "processed" / "e2" / "fact_usuario_dia.csv"
    output = Path(output_dir) if output_dir else root / "datos" / "processed" / "e3"
    if not source.exists():
        raise FileNotFoundError(f"No encuentro {source}; ejecuta primero E2.")
    raw_rows = read_csv(source)
    by_user = {}
    for row in raw_rows:
        by_user.setdefault(row["usuario_id"], {})[date.fromisoformat(row["fecha_local"])] = row

    features = list(BASE_FEATURES)
    # A blank nutrient on a day without meal events means zero nutrients were
    # recorded in the app that day; it does not mean zero total dietary intake.
    # Require nutrient coverage on at least 95% of days with meal events.
    meal_days = [r for r in raw_rows if (num(r.get("numero_comidas")) or 0.0) > 0]
    for source_name, target_name in NUTRIENT_SOURCE.items():
        nonblank = sum(1 for r in meal_days if num(r.get(source_name)) is not None)
        if meal_days and nonblank >= int(0.95 * len(meal_days)):
            features.append(target_name)

    dataset = []
    for user_id, user_days in by_user.items():
        days = sorted(user_days)
        for idx in range(6, len(days) - 1):
            window_dates = [days[j] for j in range(idx - 6, idx + 1)]
            if any(window_dates[j + 1] != window_dates[j] + timedelta(days=1) for j in range(6)):
                continue
            target_row = user_days[days[idx + 1]]
            target = num(target_row.get("fc_reposo"))
            if target is None:
                continue
            window = [user_days[d] for d in window_dates]
            feat = {
                "pasos_7d_promedio": statistics.fmean(num(r.get("pasos")) or 0.0 for r in window),
                "minutos_sesion_7d_promedio": statistics.fmean(num(r.get("minutos_sesion")) or 0.0 for r in window),
                "comidas_7d": sum(num(r.get("numero_comidas")) or 0.0 for r in window),
                "porciones_7d": sum(num(r.get("porciones")) or 0.0 for r in window),
            }
            nutrient_window_complete = True
            for source_name, target_name in NUTRIENT_SOURCE.items():
                if target_name in features:
                    total = 0.0
                    for day_row in window:
                        meal_count = num(day_row.get("numero_comidas")) or 0.0
                        nutrient_value = num(day_row.get(source_name))
                        if meal_count == 0:
                            continue  # zero logged meal events, not zero total diet
                        if nutrient_value is None:
                            nutrient_window_complete = False
                            break
                        total += nutrient_value
                    feat[target_name] = total
            if not nutrient_window_complete:
                continue
            dataset.append({
                "usuario_id": user_id,
                "fecha_features_hasta": days[idx].isoformat(),
                "fecha_objetivo": days[idx + 1].isoformat(),
                **feat,
                "target_fc_reposo": target,
                "particion": "pendiente",
            })

    # Hold out the last 20% of distinct target dates. Feature windows end
    # strictly before the target day, so no future observation enters a row.
    target_dates = sorted({r["fecha_objetivo"] for r in dataset})
    if len(target_dates) < 5:
        raise ValueError("Se requieren al menos cinco fechas objetivo para un corte temporal")
    cutoff_index = max(0, min(len(target_dates) - 2, int(len(target_dates) * 0.8) - 1))
    cutoff_date = target_dates[cutoff_index]
    for row in dataset:
        row["particion"] = "train" if row["fecha_objetivo"] <= cutoff_date else "test"

    train_rows = [r for r in dataset if r["particion"] == "train"]
    test_rows = [r for r in dataset if r["particion"] == "test"]
    train_users = sorted({r["usuario_id"] for r in train_rows})
    test_users = sorted({r["usuario_id"] for r in test_rows})
    if not train_rows or not test_rows:
        raise ValueError(f"Partición insuficiente: train={len(train_rows)}, test={len(test_rows)}")
    beta, means, scales = fit_ols(train_rows, features)
    baseline = statistics.median(r["target_fc_reposo"] for r in train_rows)
    predictions = []
    for row in test_rows:
        pred = predict_ols(row, beta, means, scales, features)
        predictions.append({**row, "prediccion_mediana": baseline, "prediccion_ols": pred,
                            "error_abs_ols": abs(row["target_fc_reposo"] - pred)})

    y = [r["target_fc_reposo"] for r in test_rows]
    p_base = [baseline] * len(y)
    p_ols = [r["prediccion_ols"] for r in predictions]
    def metrics(preds):
        mae = statistics.fmean(abs(a - b) for a, b in zip(y, preds))
        rmse = math.sqrt(statistics.fmean((a - b) ** 2 for a, b in zip(y, preds)))
        mean_y = statistics.fmean(y)
        total = sum((a - mean_y) ** 2 for a in y)
        residual = sum((a - b) ** 2 for a, b in zip(y, preds))
        return {"mae_bpm": round(mae, 4), "rmse_bpm": round(rmse, 4),
                "r2": round(1 - residual / total, 4) if total else None}

    output.mkdir(parents=True, exist_ok=True)
    write_csv(output / "dataset_modelamiento.csv", dataset)
    write_csv(output / "predicciones_test.csv", predictions)
    metrics_json = {
        "objetivo": "frecuencia cardíaca en reposo del día siguiente (bpm)",
        "ventana_features": "7 días anteriores; no incluye frecuencia cardíaca como predictor",
        "particion": f"temporal global: train hasta {cutoff_date}; test posterior. Los usuarios pueden aparecer en ambos periodos; ninguna variable posterior a fecha_features_hasta se usa como predictor",
        "fecha_corte_temporal": cutoff_date,
        "features": features,
        "nutrition_feature_policy": "7-day sums over matched, logged meals; no-meal days contribute zero recorded intake; windows with meal events missing nutrient joins are excluded",
        "n_rows_total": len(dataset),
        "n_rows_train": len(train_rows),
        "n_rows_test": len(test_rows),
        "n_users_train": len(train_users),
        "n_users_test": len(test_users),
        "usuarios_compartidos_train_test": len(set(train_users) & set(test_users)),
        "baseline_mediana_train": round(baseline, 4),
        "baseline": metrics(p_base),
        "ols": metrics(p_ols),
        "coeficientes_ols_estandarizados": {"intercept": beta[0], **dict(zip(features, beta[1:]))},
        "nota": "Modelo exploratorio sobre registros de wearables y comidas; los nutrientes representan comidas registradas con receta coincidente, no la dieta completa. No identifica causalidad ni es una recomendación médica.",
    }
    (output / "metricas_modelo.json").write_text(json.dumps(metrics_json, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(metrics_json, ensure_ascii=False, indent=2))
    return metrics_json


if __name__ == "__main__":
    run_model()
