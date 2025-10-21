"""
Baselines de prévision: Prophet et ARIMA.
Chaque série (region x age_band) est entraînée séparément.
"""
import pandas as pd
import numpy as np
from prophet import Prophet
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

def forecast_prophet(df_series, horizon_weeks=4):
    """
    df_series: DataFrame avec colonnes [date, y]
    Retourne DataFrame avec colonnes [date, yhat]
    """
    tmp = df_series.rename(columns={"date":"ds","y":"y"}).copy()
    m = Prophet(weekly_seasonality=False, yearly_seasonality=True, daily_seasonality=False)
    m.fit(tmp)
    future = m.make_future_dataframe(periods=horizon_weeks, freq="W-MON")
    fcst = m.predict(future)[["ds","yhat"]].rename(columns={"ds":"date"})
    return fcst

def per_series_prophet(df, group_cols=("region","age_band"), target="doses_per_100k", horizon_weeks=4):
    """
    Applique Prophet série par série et concatène les résultats.
    """
    out = []
    for keys, part in df.groupby(list(group_cols)):
        ser = part[["date", target]].rename(columns={target:"y"}).dropna()
        if len(ser) < 10:
            continue
        fc = forecast_prophet(ser, horizon_weeks=horizon_weeks)
        fc[group_cols[0]] = keys[0]
        fc[group_cols[1]] = keys[1]
        out.append(fc)
    if not out:
        return pd.DataFrame(columns=["date"]+list(group_cols)+["yhat"])
    res = pd.concat(out, ignore_index=True)
    return res

import pandas as pd

def seasonal_naive_future(df: pd.DataFrame,
                          group_cols=("region","age_band"),
                          target="doses_per_100k",
                          date_col="date"):
    """
    Prévoit le FUTUR (où target est NaN) par 'saisonnière naïve':
      yhat = valeur à t-12 mois (si dispo), sinon moyenne des 3 derniers mois disponibles.
    Le DF doit être mensuel, trié, et contenir l'historique + les lignes futures (y NaN).
    Retourne un DataFrame [date, region, age_band, yhat_baseline].
    """
    out = []
    for keys, g in df.groupby(list(group_cols)):
        g = g.sort_values(date_col).copy()
        g["_lag12"] = g[target].shift(12)
        g["_ma3"]   = g[target].rolling(3, min_periods=1).mean()
        fut = g[g[target].isna()].copy()
        if fut.empty:
            continue
        # yhat = lag12 si dispo, sinon ma3 historique
        fut["yhat_baseline"] = fut["_lag12"]
        if fut["yhat_baseline"].isna().any():
            # backfill avec ma3 calculée sur l'historique uniquement
            hist_ma3 = g.loc[g[target].notna(), "_ma3"].iloc[-1] if (g[target].notna().any()) else 0.0
            fut["yhat_baseline"] = fut["yhat_baseline"].fillna(hist_ma3)
        fut[group_cols[0]] = keys[0]
        fut[group_cols[1]] = keys[1]
        out.append(fut[[date_col, group_cols[0], group_cols[1], "yhat_baseline"]])
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=[date_col, *group_cols, "yhat_baseline"])
