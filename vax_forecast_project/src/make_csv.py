# scripts/make_csv.py
import numpy as np
import pandas as pd
from pathlib import Path

PROC = Path("data/processed")
F_FEAT = PROC / "features.parquet"
F_FC1  = PROC / "forecast_reconciled_calibrated.parquet"
F_FC2  = PROC / "forecast_reconciled.parquet"  # fallback si pas calibré
DEST   = PROC / "reassort_plan_from_latest.csv"

# 1) Charger les prévisions (calibrées si dispo)
if F_FC1.exists():
    fc = pd.read_parquet(F_FC1).copy()
    # essayer d'unifier le nom de la colonne de prévision
    pred_col = None
    for c in ["doses_per_100k_forecast","yhat_reconciled","yhat","yhat_ens"]:
        if c in fc.columns:
            pred_col = c; break
    if pred_col is None:
        raise ValueError("Aucune colonne de prévision trouvée dans le fichier calibré.")
    fc["doses_per_100k_forecast"] = fc[pred_col].astype(float)
elif F_FC2.exists():
    fc = pd.read_parquet(F_FC2).copy()
    pred_col = None
    for c in ["yhat_reconciled","yhat","yhat_ens"]:
        if c in fc.columns:
            pred_col = c; break
    if pred_col is None:
        raise ValueError("Aucune colonne de prévision trouvée (yhat/yhat_reconciled) dans le parquet.")
    fc["doses_per_100k_forecast"] = fc[pred_col].astype(float)
else:
    raise FileNotFoundError("Aucun fichier de prévision trouvé dans data/processed/.")

fc = fc[["date","region","age_band","doses_per_100k_forecast"]].copy()
fc["date"] = pd.to_datetime(fc["date"])

# 2) Charger l'historique pour calculer la moyenne des 12 mois précédents
f = pd.read_parquet(F_FEAT)[["date","region","age_band","doses_per_100k"]].copy()
f["date"] = pd.to_datetime(f["date"])

def trailing_mean_12m(hist_df: pd.DataFrame, when: pd.Timestamp) -> float:
    # 12 mois strictement avant la date "when"
    hist = hist_df[hist_df["date"] < when].sort_values("date").tail(12)
    if hist.empty:
        return np.nan
    return float(hist["doses_per_100k"].mean())

rows = []
for (r,a), g in fc.groupby(["region","age_band"], sort=False):
    hist = f[(f["region"]==r) & (f["age_band"]==a)].copy()
    gg = g.copy()
    gg["mean_hist"] = gg["date"].apply(lambda d: trailing_mean_12m(hist, d))
    rows.append(gg)

out = pd.concat(rows, ignore_index=True)

# 3) Qty = +10% puis arrondi par 100
out["qty"] = (np.ceil((out["doses_per_100k_forecast"] * 1.10) / 100) * 100).astype(int)

# 4) Ratio (%) – évite les divisions par 0 / NaN
out["forecast_vs_hist_%"] = np.where(
    np.isfinite(out["mean_hist"]) & (out["mean_hist"] > 0),
    100.0 * out["doses_per_100k_forecast"] / out["mean_hist"],
    np.nan
)

# 5) Filtre : à partir du MOIS SUIVANT (Europe/Paris)
next_month = (pd.Timestamp.now(tz="Europe/Paris").to_period("M").to_timestamp()
              + pd.offsets.MonthBegin(1)).tz_localize(None)
out = out[out["date"] >= next_month]

# 6) Ordonner et sauver
out = out[["date","region","age_band","doses_per_100k_forecast","qty","mean_hist","forecast_vs_hist_%"]]
out = out.sort_values(["date","region","age_band"]).reset_index(drop=True)

DEST.parent.mkdir(parents=True, exist_ok=True)
out.to_csv(DEST, index=False)
print(f"OK -> {DEST} | lignes: {len(out)}")
print(out.head(10).to_string(index=False))
