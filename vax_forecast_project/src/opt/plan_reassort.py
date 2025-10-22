"""
Exemple d'utilisation de l'optimiseur: convertir prévisions en plan de réassort.
"""
import pandas as pd
import numpy as np
from .optimize_inventory import lp_replenishment


def make_plan(capacity=50000):
    fc = pd.read_parquet('data/processed/forecast_reconciled_calibrated.parquet')
    # Agréger par région (toutes tranches d'âge confondues)
    agg = (fc.groupby(["date","region"], as_index=False)["yhat_reconciled"].sum())

    # Prendre la dernière semaine pour illustrer
    last = agg[agg["date"] == agg["date"].max()].copy()
    regions = last["region"].tolist()
    demand_mean = dict(zip(regions, last["yhat_reconciled"]))
    # p90 simple = mean * 1.2 (placeholder, remplacer par intervalle prédictif réel)
    demand_p90 = {r: demand_mean[r] * 1.2 for r in regions}

    plan = lp_replenishment(regions, demand_mean, demand_p90, capacity)
    out = pd.DataFrame({"region": regions, "allocation": [plan[r] for r in regions]})
    out.to_csv('data/processed/reassort_plan_from_latest.csv', index=False)
    return out
