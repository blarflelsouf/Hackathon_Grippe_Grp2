"""
Quickstart exécutable sur données synthétiques pour valider la pipeline end-to-end.
1) Génère des CSV synthétiques dans data/raw/
2) Construit features
3) Entraîne GBDT + réconciliation
4) Produit un plan de réassort
"""
import numpy as np, pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from src.config import RAW_DIR, PROCESSED_DIR, REGIONS, AGE_BANDS, FREQ
from src.train_pipeline import run_pipeline
from src.opt.plan_reassort import make_plan

def gen_synthetic():
    np.random.seed(42)
    start = pd.Timestamp("2022-07-04")  # lundi
    dates = pd.date_range(start, periods=130, freq=FREQ)  # ~2.5 saisons
    # Région mapping
    regions = REGIONS
    pd.DataFrame({"region":regions, "region_name":[f"REG-{r}" for r in regions]}).to_csv(RAW_DIR/"synthetic_regions.csv", index=False)

    # Population par âge
    pop_rows = []
    for r in regions:
        base = 1_000_000 + 300_000*np.random.rand()
        for a in AGE_BANDS:
            w = {"0-17":0.2,"18-64":0.6,"65+":0.2}[a]
            pop_rows.append({"region":r, "age_band":a, "population": int(base*w)})
    pd.DataFrame(pop_rows).to_csv(RAW_DIR/"synthetic_insee_population.csv", index=False)

    # Incidence Sentinelles
    inc_rows = []
    for r in regions:
        seasonal = np.sin(np.linspace(0,6*np.pi,len(dates))) * 50 + 80
        noise = np.random.randn(len(dates))*10
        vals = np.clip(seasonal+noise, 0, None)
        for d,v in zip(dates, vals):
            inc_rows.append({"date":d, "region":r, "incidence_per_100k": v})
    pd.DataFrame(inc_rows).to_csv(RAW_DIR/"synthetic_sentinelles.csv", index=False)

    # Météo
    met_rows = []
    for r in regions:
        base = 12 + 8*np.sin(np.linspace(0,2*np.pi,len(dates)))  # saisonnalité
        noise = np.random.randn(len(dates))*2
        t = base + noise
        for d,v in zip(dates, t):
            met_rows.append({"date":d, "region":r, "tmean": v})
    pd.DataFrame(met_rows).to_csv(RAW_DIR/"synthetic_meteo.csv", index=False)

    # Urgences OSCOUR
    urg_rows = []
    for r in regions:
        for a in AGE_BANDS:
            mu = {"0-17":5,"18-64":8,"65+":15}[a]
            vals = np.maximum(0, (mu + np.random.randn(len(dates))*2)).astype(int)
            for d,v in zip(dates, vals):
                urg_rows.append({"date":d,"region":r,"age_band":a,"er_visits":v,"admissions":int(v*0.2)})
    pd.DataFrame(urg_rows).to_csv(RAW_DIR/"synthetic_oscour.csv", index=False)

    # Vaccination (doses): dépend de l'incidence lissée (proxy comportement)
    vax_rows = []
    inc = pd.read_csv(RAW_DIR/"synthetic_sentinelles.csv", parse_dates=["date"])
    for r in regions:
        inc_r = inc[inc["region"]==r].sort_values("date")
        inc_ma = inc_r["incidence_per_100k"].rolling(2).mean().fillna(method="bfill").values
        for a in AGE_BANDS:
            mult = {"0-17":0.5,"18-64":1.0,"65+":1.8}[a]
            base = 50*mult + (inc_ma*0.3*mult)
            noise = np.random.randn(len(dates))*5
            y = np.maximum(0, base + noise)
            for d,yy in zip(inc_r["date"].values, y):
                vax_rows.append({"date": d, "region": r, "age_band": a, "doses": float(yy)})
    pd.DataFrame(vax_rows).to_csv(RAW_DIR/"synthetic_vaccination.csv", index=False)

def main():
    gen_synthetic()
    res = run_pipeline()
    plan = make_plan(capacity=120000)
    print("Aperçu métriques par série:", res["metrics"])
    print("Plan de réassort (dernière semaine):")
    print(plan.head())

if __name__ == "__main__":
    main()
