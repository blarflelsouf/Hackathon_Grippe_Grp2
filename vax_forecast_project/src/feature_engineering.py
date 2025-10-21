"""
Assemblage & features: jointure des sources, lags, moyennes mobiles, calendrier.
"""
import os
import pandas as pd
import numpy as np
from .data_ingestion import (load_insee_population, load_region_mapping,
                             load_sentinelles_incidence, load_oscour_urgences,
                             load_meteo_temperature, load_vaccination_doses)
from .config import PROCESSED_DIR, INTERIM_DIR, FREQ, AGE_BANDS
from .utils import week_start, safe_merge

def to_month_start(s: pd.Series) -> pd.Series:
    """
    Convertit des dates en premier jour du mois (00:00).
    Exemple: 2024-07-15 -> 2024-07-01
    """
    dt = pd.to_datetime(s, errors="coerce")
    return dt.dt.to_period("M").dt.to_timestamp()  # début du mois par défaut



def _weekly_resample(df, date_col="date", on_cols=("region",), how="sum"):
    """
    Regroupe en hebdo (fin lundi) selon 'how' sur les mesures.
    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    # Semaine alignée au lundi
    df["week"] = df[date_col].dt.to_period("W-MON").dt.start_time
    group_cols = list(on_cols) + ["week"]
    num_cols = [c for c in df.columns if c not in group_cols and c != date_col]
    agg = {c: how for c in num_cols}
    return df.groupby(group_cols, as_index=False).agg(agg).rename(columns={"week":"date"})

def build_feature_table(save=True):
    """
    Construit la table d'apprentissage MENSUELLE :
      - fréquence: 1er jour du mois (MS)
      - cible y = doses_per_100k (mensuel)
      - features: uniquement *_lag* / *_ma* (past-only) + calendrier (month/year)
      - filets de sécurité : proxy de doses mensuel sans fuite si séries plates
    """
    # ========= 1) Chargement =========
    pop = load_insee_population()
    inc = load_sentinelles_incidence(with_future=True, future_until="2025-12-31")      # [date, region, incidence_per_100k]
    urg = load_oscour_urgences()            # [date, region, age_band, er_visits, admissions]
    met = load_meteo_temperature(with_future=True, future_until="2025-12-31")        # [date, region, tmean]
    vac = load_vaccination_doses()          # [date, region, age_band, doses]

    # ========= 2) Mensualisation des sources =========
    # Vaccination : somme par mois
    vac_m = vac.copy()
    vac_m["date"] = to_month_start(vac_m["date"])
    vac_m = (vac_m.groupby(["region","age_band","date"], as_index=False)["doses"]
                  .sum().sort_values(["region","age_band","date"]))

    # Sentinelles : moyenne mensuelle
    inc_m = inc.copy()
    inc_m["date"] = to_month_start(inc_m["date"])
    inc_m = (inc_m.groupby(["region","date"], as_index=False)["incidence_per_100k"]
                  .mean().sort_values(["region","date"]))

    # Météo : moyenne mensuelle
    met_m = met.copy()
    met_m["date"] = to_month_start(met_m["date"])
    met_m = (met_m.groupby(["region","date"], as_index=False)["tmean"]
                  .mean().sort_values(["region","date"]))

    # OSCOUR : somme mensuelle (si tu préfères moyenne, remplace .sum() par .mean())
    urg_m = urg.copy()
    urg_m["date"] = to_month_start(urg_m["date"])
    urg_m = (urg_m.groupby(["region","age_band","date"], as_index=False)[["er_visits","admissions"]]
                  .sum().sort_values(["region","age_band","date"]))

    # ========= 3) Proxy doses mensuel SANS FUITE si séries plates =========
    is_flat = (vac_m.groupby(["region","age_band"])["doses"].std().fillna(0) == 0)
    flat_keys = is_flat[is_flat].index.tolist()
    if flat_keys:
        tmp = vac_m.merge(inc_m, on=["region","date"], how="left").sort_values(["region","age_band","date"])
        # lissage + décalage d'1 mois (no leakage)
        tmp["inc_ma2"] = tmp.groupby("region")["incidence_per_100k"] \
                            .transform(lambda s: s.rolling(2, min_periods=1).mean())
        tmp["inc_ma2_lag1m"] = tmp.groupby("region")["inc_ma2"].shift(1)

        # saison mensuelle + poids âge
        age_w = {"0-17":0.5, "18-64":1.0, "65+":1.6}
        tmp["month_int"] = tmp["date"].dt.month
        season = 1.0 + 0.20 * np.sin(2*np.pi*(tmp["month_int"]-2)/12.0)  # pic hiver
        alpha = 5.0
        rng = np.random.default_rng(123)

        mask = tmp.set_index(["region","age_band"]).index.isin(flat_keys)
        synth = alpha * tmp["inc_ma2_lag1m"].fillna(0) * tmp["age_band"].map(lambda a: age_w.get(a,1.0)) * season
        synth = (synth * (1 + rng.normal(0, 0.05, len(synth)))).clip(lower=0)
        tmp.loc[mask, "doses"] = synth[mask]
        vac_m = tmp[["region","age_band","date","doses"]]

    # ========= 4) Grille mensuelle complète + merges =========
    # bornes de date (prends la vaccination par défaut; sinon prends inc_m)
    dmin = vac_m["date"].min() if not vac_m.empty else inc_m["date"].min()
    dmax = pd.Timestamp("2025-12-01")

    all_dates = pd.date_range(dmin, dmax, freq="MS")

    regions = sorted(vac_m["region"].unique())
    ages = sorted(vac_m["age_band"].unique())
    grid = (pd.MultiIndex.from_product([all_dates, regions, ages],
            names=["date","region","age_band"]).to_frame(index=False))

    X = (grid
         .merge(vac_m, on=["date","region","age_band"], how="left")
         .merge(inc_m, on=["date","region"], how="left")
         .merge(met_m, on=["date","region"], how="left")
         .merge(urg_m, on=["date","region","age_band"], how="left"))

    # ========= 5) Remplissages exogènes & normalisation =========
    X[["er_visits","admissions"]] = X[["er_visits","admissions"]].fillna(0)
    X["incidence_per_100k"] = X["incidence_per_100k"].fillna(0)

    # Météo : ffill/bfill par région (mensuel)
    X["tmean"] = (X.sort_values("date")
                   .groupby("region", group_keys=False)["tmean"]
                   .apply(lambda s: s.ffill().bfill()))

    # Population & per 100k
    X = X.merge(pop, on=["region","age_band"], how="left")
    X["population"] = X["population"].fillna(1_000_000)
    X["pop_100k"]   = X["population"] / 100_000.0

    # doses manquantes à 0 (après proxy)
    X["doses"] = X["doses"].fillna(0.0)
    X["doses_per_100k"] = X["doses"] / X["pop_100k"].replace(0, np.nan)

    # Calendrier
    X["month"] = X["date"].dt.month.astype(int)
    X["year"]  = X["date"].dt.year.astype(int)

    # Campagne grippe: sept(9) -> jan(1) inclus
    X["is_campaign"] = X["month"].isin([9,10,11,12,1]).astype(int)
    # Hiver
    X["is_winter"] = X["month"].isin([11,12,1,2]).astype(int)


    # ========= 6) Lags & moyennes mobiles (mensuel, past-only) =========
    def add_lags(df, cols, lags=(1,2,3,6,12)):
        df = df.sort_values(["region","age_band","date"]).copy()
        g = df.groupby(["region","age_band"], sort=False)
        for col in cols:
            for L in lags:
                df[f"{col}_lag{L}"] = g[col].transform(lambda s: s.shift(L))
        return df

    def add_rollings(df, cols, windows=(2,3,6,12)):
        df = df.sort_values(["region","age_band","date"]).copy()
        g = df.groupby(["region","age_band"], sort=False)
        for col in cols:
            for W in windows:
                df[f"{col}_ma{W}"] = g[col].transform(lambda s: s.rolling(window=W, min_periods=1).mean())
        return df

    X = add_lags(X, ["doses_per_100k","incidence_per_100k","tmean","er_visits","admissions"])
    X = add_rollings(X, ["doses_per_100k","incidence_per_100k","tmean","er_visits","admissions"])

    # Remplissage de secours sur lags/MA (médiane par série) pour éviter drop total
    lagma_cols = [c for c in X.columns if any(s in c for s in ["_lag","_ma"])]
    for col in lagma_cols:
        X[col] = X.groupby(["region","age_band"])[col].transform(lambda s: s.fillna(s.median()))

    # ========= 7) Sélection des features (past-only) + cible =========
    past_feats = []
    for c in ["doses_per_100k","incidence_per_100k","tmean","er_visits","admissions"]:
        past_feats += [col for col in X.columns if col.startswith(c+"_lag") or col.startswith(c+"_ma")]
    past_feats += ["month","year"]  # calendaires OK

    X["y"] = X["doses_per_100k"]
    X.attrs["FEATURE_COLS"] = sorted(set(past_feats))

    start_future = (pd.Timestamp.now(tz="Europe/Paris")
                .to_period("M").to_timestamp() + pd.offsets.MonthBegin(1)
               ).tz_localize(None)

    # -- horizon futur paramétrable --
    H = int(os.environ.get("FORECAST_HORIZON_MONTHS", 6))  # par défaut 6 mois
    start_future = (
        pd.Timestamp.now(tz="Europe/Paris").to_period("M").to_timestamp()
        + pd.offsets.MonthBegin(1)
    ).tz_localize(None)

    # dmax = fin d'horizon (inclus) = start_future + (H-1) mois
    dmax = (start_future + pd.offsets.MonthBegin(H-1))


    # marquer le VRAI futur
    X.loc[X["date"] >= start_future, "y"] = np.nan


    # ========= 8) Sauvegarde =========
    X = X.sort_values(["region","age_band","date"]).reset_index(drop=True)
    if save:
        out = PROCESSED_DIR / "features.parquet"
        X.to_parquet(out, index=False)
    return X
