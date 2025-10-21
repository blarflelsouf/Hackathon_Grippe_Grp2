"""
Réconciliation hiérarchique simple (Top-Down par proportions historiques).
Hiérarchie: National -> Région -> (Région x âge)
Pour une implémentation avancée: MinT (nécessite covariance des erreurs).
"""
import pandas as pd
import numpy as np

def topdown_proportions(df_hist, on=("region","age_band"), target="yhat"):
    """
    Calcule proportions moyennes historiques par nœud fin, utilisées pour distribuer un total.
    df_hist: historique avec colonnes on + target
    """
    base = df_hist.groupby(list(on), as_index=False)[target].mean()
    total = base[target].sum()
    if total == 0:
        base["prop"] = 1.0 / len(base)
    else:
        base["prop"] = base[target] / total
    return base[list(on)+["prop"]]

def reconcile_topdown(national_fc, proportions, on=("region","age_band"), total_col="national_total", out_col="yhat_reconciled"):
    """
    Distribue le total national par proportions vers chaque nœud fin.
    national_fc: DF avec [date, national_total]
    proportions: DF avec [on..., prop]
    """
    all_rows = []
    for d, row in national_fc.set_index("date").iterrows():
        tmp = proportions.copy()
        tmp["date"] = d
        tmp[out_col] = row[total_col] * tmp["prop"]
        all_rows.append(tmp[["date"]+list(on)+[out_col]])
    return pd.concat(all_rows, ignore_index=True)
