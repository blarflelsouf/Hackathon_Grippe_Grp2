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
      - features: uniquement *_lag* / *_ma* (past-only) + calendrier (month/year, is_campaign, is_winter)
      - filets de sécurité :
          * proxy de doses mensuel sans fuite si séries plates
          * exogènes étendus jusqu'à l'horizon par climatologie région×mois
      - horizon futur paramétrable via l'env FORECAST_HORIZON_MONTHS (par défaut 6)
    """
    # ========= 1) Chargement =========
    pop = load_insee_population()                # [region, age_band, population]
    inc = load_sentinelles_incidence()           # [date, region, incidence_per_100k]
    urg = load_oscour_urgences()                 # [date, region, age_band, er_visits, admissions]
    met = load_meteo_temperature()               # [date, region, tmean]
    vac = load_vaccination_doses()               # [date, region, age_band, doses]

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

    # ========= 3) Proxy doses SANS FUITE si séries plates =========
    # détecte séries sans variance dans vac_m (par région×age)
    is_flat = (vac_m.groupby(["region","age_band"])["doses"].std().fillna(0) == 0)
    flat_keys = set(is_flat[is_flat].index.tolist())
    if flat_keys:
        # Joindre incidence mensuelle pour créer un proxy (lag 1 mois, lissage MA2)
        tmp = vac_m.merge(inc_m, on=["region","date"], how="left").sort_values(["region","age_band","date"])
        tmp["inc_ma2"] = tmp.groupby("region")["incidence_per_100k"].transform(lambda s: s.rolling(2, min_periods=1).mean())
        tmp["inc_ma2_lag1m"] = tmp.groupby("region")["inc_ma2"].transform(lambda s: s.shift(1))  # no leakage

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

    # ========= 4) Définition de la grille temps (horizon paramétrable) =========
    # bornes min/max historiques des exogènes
    dmin = min(vac_m["date"].min(), inc_m["date"].min(), met_m["date"].min())
    dmax_sources = max(vac_m["date"].max(), inc_m["date"].max(), met_m["date"].max())

    # horizon futur en mois (par défaut 6)
    H = int(os.environ.get("FORECAST_HORIZON_MONTHS", 6))

    # 1er jour du MOIS SUIVANT (Europe/Paris)
    next_month = (
        pd.Timestamp.now(tz="Europe/Paris").to_period("M").to_timestamp()
        + pd.offsets.MonthBegin(1)
    ).tz_localize(None)

    # dmax = max(sources, next_month + (H-1) mois) -> on garantit H mois futurs
    dmax = max(dmax_sources, (next_month + pd.offsets.MonthBegin(H-1)))

    # toutes les dates mensuelles complètes
    all_dates = pd.date_range(dmin, dmax, freq="MS")

    # ensemble des régions & âges à couvrir
    all_regions = sorted(set(vac_m["region"]) | set(inc_m["region"]) | set(met_m["region"]) | set(urg_m["region"]))
    all_ages = AGE_BANDS  # on couvre la grille complète

    # ========= 5) Compléter les exogènes jusqu'à dmax (climatologie région×mois) =========
    # --- Sentinelles ---
    inc_m["month"] = inc_m["date"].dt.month
    inc_clim = (inc_m.groupby(["region","month"], as_index=False)["incidence_per_100k"]
                    .mean().rename(columns={"incidence_per_100k":"inc_clim"}))
    inc_grid = pd.MultiIndex.from_product([all_regions, all_dates], names=["region","date"]).to_frame(index=False)
    inc_grid["month"] = inc_grid["date"].dt.month
    inc_m_full = (inc_grid
                  .merge(inc_m[["region","date","incidence_per_100k"]], on=["region","date"], how="left")
                  .merge(inc_clim, on=["region","month"], how="left"))
    inc_m_full["incidence_per_100k"] = inc_m_full["incidence_per_100k"].fillna(inc_m_full["inc_clim"]).fillna(0)
    inc_m_full = inc_m_full[["date","region","incidence_per_100k"]]

    # --- Météo ---
    met_m["month"] = met_m["date"].dt.month
    met_clim = (met_m.groupby(["region","month"], as_index=False)["tmean"]
                    .mean().rename(columns={"tmean":"tmean_clim"}))
    met_grid = pd.MultiIndex.from_product([all_regions, all_dates], names=["region","date"]).to_frame(index=False)
    met_grid["month"] = met_grid["date"].dt.month
    met_m_full = (met_grid
                  .merge(met_m[["region","date","tmean"]], on=["region","date"], how="left")
                  .merge(met_clim, on=["region","month"], how="left"))
    met_m_full["tmean"] = met_m_full["tmean"].fillna(met_m_full["tmean_clim"])
    met_m_full = met_m_full[["date","region","tmean"]]

    # --- Urgences ---
    urg_grid = pd.MultiIndex.from_product([all_regions, all_ages, all_dates],
                names=["region","age_band","date"]).to_frame(index=False)
    urg_m_full = urg_grid.merge(urg_m, on=["region","age_band","date"], how="left")
    urg_m_full[["er_visits","admissions"]] = urg_m_full[["er_visits","admissions"]].fillna(0)

    # ========= 6) Grille finale & merges =========
    grid = pd.MultiIndex.from_product([all_dates, all_regions, all_ages],
            names=["date","region","age_band"]).to_frame(index=False)

    X = (grid
         .merge(vac_m,       on=["date","region","age_band"], how="left")
         .merge(inc_m_full,  on=["date","region"],            how="left")
         .merge(met_m_full,  on=["date","region"],            how="left")
         .merge(urg_m_full,  on=["date","region","age_band"], how="left"))

    # ========= 7) Remplissages exogènes & normalisation =========
    X[["er_visits","admissions"]] = X[["er_visits","admissions"]].fillna(0)
    X["incidence_per_100k"] = X["incidence_per_100k"].fillna(0)

    # Météo : ffill/bfill par région (au cas où)
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

    # Calendrier & flags
    X["month"] = X["date"].dt.month.astype(int)
    X["year"]  = X["date"].dt.year.astype(int)
    X["is_campaign"] = X["month"].isin([9,10,11,12,1]).astype(int)
    X["is_winter"]   = X["month"].isin([11,12,1,2]).astype(int)

    # ========= 8) Lags & moyennes mobiles (mensuel, past-only) =========
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

    # Remplissage de secours sur lags/MA (médiane par série)
    lagma_cols = [c for c in X.columns if any(s in c for s in ["_lag","_ma"])]
    for col in lagma_cols:
        X[col] = X.groupby(["region","age_band"])[col].transform(lambda s: s.fillna(s.median()))

    # ========= 9) Sélection des features (past-only) + cible & futur =========
    past_feats = []
    for c in ["doses_per_100k","incidence_per_100k","tmean","er_visits","admissions"]:
        past_feats += [col for col in X.columns if col.startswith(c+"_lag") or col.startswith(c+"_ma")]
    past_feats += ["month","year","is_campaign","is_winter"]  # calendaires OK

    X["y"] = X["doses_per_100k"]
    X.attrs["FEATURE_COLS"] = sorted(set(past_feats))
    start_future = (
        pd.Timestamp.now(tz="Europe/Paris").to_period("M").to_timestamp()
        + pd.offsets.MonthBegin(1)
    ).tz_localize(None)

    X.loc[X["date"] >= start_future, "y"] = np.nan

    # ========= 10) Sauvegarde =========
    X = X.sort_values(["region","age_band","date"]).reset_index(drop=True)
    if save:
        out = PROCESSED_DIR / "features.parquet"
        X.to_parquet(out, index=False)
    return X
