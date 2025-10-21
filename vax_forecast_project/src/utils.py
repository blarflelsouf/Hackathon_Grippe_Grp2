import numpy as np
import pandas as pd


def smape(y_true, y_pred, eps=1e-3):
    """Symmetric MAPE."""
    yt = np.asarray(y_true, dtype=float)
    yp = np.asarray(y_pred, dtype=float)
    denom = np.maximum(np.abs(yt) + np.abs(yp), eps)
    return 100.0 * np.mean(np.abs(yp - yt) / denom)

def week_start(d):
    """Force date au lundi (alignement hebdomadaire)."""
    d = pd.Timestamp(d)
    return (d - pd.offsets.Week(weekday=0)).normalize()

def safe_merge(left, right, on, how="left"):
    """Merge avec colonnes triées et index reset pour lisibilité."""
    out = left.merge(right, on=on, how=how)
    return out.sort_values(on).reset_index(drop=True)
