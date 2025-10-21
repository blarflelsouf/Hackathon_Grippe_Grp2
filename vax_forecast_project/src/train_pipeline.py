"""
Pipeline simple: features -> GBDT -> OOF metrics -> HTS (démo top-down) -> Export.
"""
import os
from pathlib import Path
import pandas as pd
import numpy as np
from .feature_engineering import build_feature_table
from .models.gbdt_demand import rolling_cv_fit_predict
from .models.ensemble import fit_predict_ensemble
from .hts import topdown_proportions, reconcile_topdown
from .config import PROCESSED_DIR, MODELS_DIR
from .mlflow_utils import setup_mlflow

def run_pipeline():
    mlflow = setup_mlflow()
    with mlflow.start_run(run_name="GBDT_demand_weekly"):
        X = build_feature_table(save=True)
        oof, future_fc, models, metrics = rolling_cv_fit_predict(X)

        # Log des métriques globales
        if not metrics.empty:
            mlflow.log_metric("SMAPE_mean", metrics["SMAPE"].mean())
            mlflow.log_metric("MAE_mean", metrics["MAE"].mean())

        # Exemple HTS: on crée un total national yhat et on le redistribue
        if not future_fc.empty:
            nat = (future_fc.groupby("date", as_index=False)["yhat"].sum()
                   .rename(columns={"yhat":"national_total"}))
            props = topdown_proportions(future_fc.rename(columns={"yhat":"yhat"}),
                                        on=("region","age_band"), target="yhat")
            reconciled = reconcile_topdown(nat, props, on=("region","age_band"),
                                           total_col="national_total", out_col="yhat_reconciled")

            reconciled.to_parquet(PROCESSED_DIR / "forecast_reconciled.parquet", index=False)

        metrics.to_csv(PROCESSED_DIR / "metrics_by_series.csv", index=False)
        return {"metrics": metrics.head(10).to_dict(orient="records")}


def run_pipeline_ensemble():
    mlflow = setup_mlflow()
    with mlflow.start_run(run_name="GBDT_demand_monthly"):
        X = build_feature_table(save=True)
        feature_cols = X.attrs.get("FEATURE_COLS")  # past-only lags/MA + month/year
        # 1) Entraînement ensemble
        metrics_ens, future_fc = fit_predict_ensemble(
            features_df=X, feature_cols=feature_cols,
            min_train_months=8, horizon_months=int(os.environ.get("FORECAST_HORIZON_MONTHS", 6)), w_lgbm=0.7, w_base=0.3
        )
        # 2) Sauvegardes
        outm = PROCESSED_DIR / "metrics_by_series.csv"
        metrics_ens.to_csv(outm, index=False)

        # -> forecast_reconciled.parquet (champ yhat_ens) si on a du futur
        if not future_fc.empty:
            future_fc = future_fc.rename(columns={"yhat_ens":"yhat"})
            (PROCESSED_DIR / "forecast_reconciled.parquet").unlink(missing_ok=True)
            future_fc.to_parquet(PROCESSED_DIR / "forecast_reconciled.parquet", index=False)

        # === Recalibration d’échelle + CSV opérationnel ===
        fc_in  = PROCESSED_DIR / "forecast_reconciled.parquet"
        fc_out = PROCESSED_DIR / "forecast_reconciled_calibrated.parquet"
        feats  = PROCESSED_DIR / "features.parquet"
        csv_plan = PROCESSED_DIR / "reassort_plan_from_latest.csv"

        if fc_in.exists():
            fc_cal = _calibrate_scale_after_model(fc_in, feats, fc_out)
            _write_reassort_csv_from_latest(fc_cal, feats, csv_plan)
            print("OK: fichiers écrits dans data/processed/ :")
            print("- features.parquet")
            print("- metrics_by_series.csv")
            print("- forecast_reconciled.parquet")
            print("- forecast_reconciled_calibrated.parquet")
            print("- reassort_plan_from_latest.csv")

        return {"metrics": metrics_ens.to_dict(orient="records")}



def _calibrate_scale_after_model(parquet_in: Path, features_path: Path, parquet_out: Path) -> pd.DataFrame:
    """
    Recalibre l'échelle des prévisions en 'par 100k' en s'alignant sur le même mois de l'année précédente.
    - Calibrage robuste par tranche d'âge (médiane des ratios régionaux).
    - Si pas dispo, fallback sur médiane globale, sinon 1.0.
    Écrit un parquet *_calibrated.parquet et retourne le DataFrame calibré.
    """
    fc = pd.read_parquet(parquet_in).copy()
    # Détecte la colonne de prévision
    pred_col = "yhat"
    if "yhat_reconciled" in fc.columns:
        pred_col = "yhat_reconciled"
    if pred_col not in fc.columns:
        raise ValueError("Aucune colonne de prévision trouvée (yhat / yhat_reconciled).")
    fc["doses_per_100k_forecast"] = fc[pred_col].astype(float)

    # Historique (réel/proxy appris)
    feat = pd.read_parquet(features_path)[["date","region","age_band","doses_per_100k"]].copy()
    feat["date"] = pd.to_datetime(feat["date"])
    fc["date"] = pd.to_datetime(fc["date"])

    # Associer à chaque date prédites le même mois N-1 (mois calendaire)
    fc["_hist_date"] = (fc["date"] - pd.DateOffset(years=1)).dt.to_period("M").dt.to_timestamp()

    cal = fc.merge(
        feat.rename(columns={"date":"_hist_date"}),
        on=["_hist_date","region","age_band"],
        how="left",
        suffixes=("", "_hist")
    )

    # Ratio = hist / forecast (uniquement là où c’est dispo et >0)
    cal["ratio"] = cal["doses_per_100k"] / cal["doses_per_100k_forecast"]
    # Garde les ratios valides
    cal_valid = cal[np.isfinite(cal["ratio"]) & (cal["ratio"] > 0)].copy()

    # Médiane par tranche d'âge (robuste), puis fallback globale
    age_medians = cal_valid.groupby("age_band")["ratio"].median()
    global_median = cal_valid["ratio"].median() if not cal_valid.empty else 1.0
    # map et fallback
    fc["scale_age"] = fc["age_band"].map(age_medians).fillna(global_median if np.isfinite(global_median) and global_median>0 else 1.0)
    # bornes de sécurité pour éviter des explosions (laisser large, cas réel ~x7 chez toi)
    fc["scale_age"] = fc["scale_age"].clip(lower=0.2, upper=10.0)

    # Appliquer
    fc["doses_per_100k_forecast"] = fc["doses_per_100k_forecast"] * fc["scale_age"]
    # Nettoyage colonnes techniques
    fc.drop(columns=[c for c in ["_hist_date","scale_age"] if c in fc.columns], inplace=True)

    parquet_out = Path(parquet_out)
    parquet_out.parent.mkdir(parents=True, exist_ok=True)
    fc.to_parquet(parquet_out, index=False)
    return fc


def _write_reassort_csv_from_latest(fc_calibrated: pd.DataFrame, features_path: Path, csv_out: Path):
    """
    Produit le CSV opérationnel : date, region, age_band, doses_per_100k_forecast, qty, mean_hist, forecast_vs_hist_%
    - mean_hist: moyenne des 12 derniers mois strictement avant la date prédit (par série).
    - qty: +10% buffer puis arrondi par tranches de 100.
    - filtre: à partir du MOIS SUIVANT (Europe/Paris).
    """
    f = pd.read_parquet(features_path)[["date","region","age_band","doses_per_100k"]].copy()
    f["date"] = pd.to_datetime(f["date"])

    def trailing_mean_12m(hist_df: pd.DataFrame, when: pd.Timestamp) -> float:
        hist = hist_df[hist_df["date"] < when].sort_values("date").tail(12)
        return float(hist["doses_per_100k"].mean()) if len(hist) else np.nan

    out_rows = []
    for (r,a), g in fc_calibrated.groupby(["region","age_band"], sort=False):
        hist = f[(f["region"]==r) & (f["age_band"]==a)]
        gg = g.copy()
        gg["mean_hist"] = gg["date"].apply(lambda d: trailing_mean_12m(hist, d))
        out_rows.append(gg[["date","region","age_band","doses_per_100k_forecast","mean_hist"]])

    out = pd.concat(out_rows, ignore_index=True)
    # qty = +10% puis arrondi par 100
    out["qty"] = (np.ceil((out["doses_per_100k_forecast"]*1.10)/100)*100).astype(int)
    out["forecast_vs_hist_%"] = 100.0 * out["doses_per_100k_forecast"] / out["mean_hist"]

    # Filtrer à partir du mois prochain (Europe/Paris)
    next_month = (pd.Timestamp.now(tz="Europe/Paris").to_period("M").to_timestamp() + pd.offsets.MonthBegin(1)).tz_localize(None)
    out = out[out["date"] >= next_month].sort_values(["date","region","age_band"]).reset_index(drop=True)

    csv_out = Path(csv_out)
    csv_out.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(csv_out, index=False)
    print(f"OK -> {csv_out} | lignes: {len(out)}")


if __name__ == "__main__":
    # Exécution directe du pipeline : features -> GBDT -> HTS -> métriques
    res = run_pipeline_ensemble()
    print(res)
    print("OK: fichiers écrits dans data/processed/ :")
    print("- features.parquet")
    print("- metrics_by_series.csv")
    print("- forecast_reconciled.parquet (si future_fc non vide)")
