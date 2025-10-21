import os
import numpy as np
import pandas as pd
from pathlib import Path

PROC = Path("data/processed")
F_FEATS = PROC / "features.parquet"
F_FC_CAL = PROC / "forecast_reconciled_calibrated.parquet"
F_FC_RAW = PROC / "forecast_reconciled.parquet"
DEST = PROC / "reassort_plan_from_latest.csv"

# --- 1) Charger forecast (priorité au calibré) ---
if F_FC_CAL.exists():
    fc = pd.read_parquet(F_FC_CAL)
else:
    if not F_FC_RAW.exists():
        raise FileNotFoundError("Aucun forecast trouvé. Lance d'abord `python -m src.train_pipeline`.")
    fc = pd.read_parquet(F_FC_RAW)

# choisir la meilleure colonne disponible
forecast_col = None
for c in ["doses_per_100k_forecast", "yhat_reconciled", "yhat"]:
    if c in fc.columns:
        forecast_col = c
        break
if forecast_col is None:
    raise KeyError("Pas de colonne forecast (doses_per_100k_forecast / yhat_reconciled / yhat).")

fc = fc[["date","region","age_band", forecast_col]].copy()
fc["date"] = pd.to_datetime(fc["date"])
fc.rename(columns={forecast_col: "doses_per_100k_forecast"}, inplace=True)

# --- 2) Historique pour moyenne 12 mois glissants ---
f = pd.read_parquet(F_FEATS)[["date","region","age_band","doses_per_100k"]].copy()
f["date"] = pd.to_datetime(f["date"])

def trailing_mean_12m(hist_df: pd.DataFrame, when: pd.Timestamp) -> float:
    hist = hist_df[hist_df["date"] < when].sort_values("date").tail(12)
    return float(hist["doses_per_100k"].mean()) if len(hist) else np.nan

rows = []
for (r,a), g in fc.groupby(["region","age_band"], sort=False):
    hist = f[(f["region"]==r) & (f["age_band"]==a)]
    gg = g.copy()
    gg["mean_hist"] = gg["date"].apply(lambda d: trailing_mean_12m(hist, d))
    rows.append(gg)
out = pd.concat(rows, ignore_index=True)

# --- 3) Filtre temporel : à partir du mois prochain (Europe/Paris), + option H mois ---
next_month = (
    pd.Timestamp.now(tz="Europe/Paris").to_period("M").to_timestamp()
    + pd.offsets.MonthBegin(1)
).tz_localize(None)

H = int(os.environ.get("CSV_HORIZON_MONTHS", 12))  # horizon visible dans le CSV
max_month = next_month + pd.offsets.MonthBegin(H-1)

out = out[(out["date"] >= next_month) & (out["date"] <= max_month)]

# --- 4) Qty & ratio ---
# buffer +10%, arrondi par 100, MOQ=100
qty = np.ceil((out["doses_per_100k_forecast"] * 1.10) / 100) * 100
out["qty"] = np.maximum(qty, 100).astype(int)

# ratio %, avec protection division par 0
den = out["mean_hist"].replace(0, np.nan)
out["forecast_vs_hist_%"] = 100.0 * out["doses_per_100k_forecast"] / den
out["forecast_vs_hist_%"] = out["forecast_vs_hist_%"].fillna(np.inf)  # si mean_hist=0 et forecast>0

# --- 5) Ordonner, arrondir, écrire ---
out = out[["date","region","age_band","doses_per_100k_forecast","qty","mean_hist","forecast_vs_hist_%"]]
out = out.sort_values(["date","region","age_band"]).reset_index(drop=True)
out["doses_per_100k_forecast"] = out["doses_per_100k_forecast"].round(2)
out["mean_hist"] = out["mean_hist"].round(2)
out["forecast_vs_hist_%"] = out["forecast_vs_hist_%"].round(1)

out.to_csv(DEST, index=False)
print(f"OK -> {DEST} | lignes: {len(out)}")
print(out.head(10))
