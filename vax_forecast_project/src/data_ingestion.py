"""
Ingestion & nettoyage (INSEE, Sentinelles, OSCOUR, Météo, Vaccination)
Standardise la colonne 'region' en codes courts (IDF, ARA, ...).
"""
from pathlib import Path
import yaml
import pandas as pd
import numpy as np
from .config import CONF_DIR, BASE_DIR, AGE_BANDS

def load_config():
    with open(CONF_DIR / "data_sources.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _as_abs(path_or_url: str) -> str:
    p = Path(path_or_url)
    if path_or_url.startswith(("http://","https://")):
        return path_or_url
    return (BASE_DIR / p).as_posix()

def read_csv(pathlike, **kw):
    pathlike = _as_abs(pathlike)
    return pd.read_csv(pathlike, encoding="utf-8", **kw)

def _load_region_map():
    cfg = load_config()
    rm = read_csv(cfg["region_mapping"])
    # attendu: columns ['insee','region','region_name']
    rm["insee"] = rm["insee"].astype(str).str.zfill(2)
    rm["region"] = rm["region"].astype(str)
    return rm[["insee","region","region_name"]]

def _apply_region_map(df, col="region"):
    """
    Harmonise df[col] en codes courts (IDF, ARA, ...).
    - Si df[col] contient déjà des codes courts présents dans le mapping, on les garde.
    - Sinon, on suppose un code INSEE (numérique/texte) -> on mappe vers code court.
    """
    rm = _load_region_map()  # retourne ['insee','region','region_name']
    df = df.copy()

    # 1) Tout en chaîne uppercase sans espaces
    vals = df[col].astype(str).str.upper().str.strip()

    # 2) Détection: déjà code court ?
    #    (évite les merges int/str : on utilise isin sur des chaînes)
    looks_short = vals.isin(rm["region"].astype(str).unique())
    if looks_short.any():
        df[col] = vals
        return df

    # 3) Mapping INSEE -> code court
    insee = vals.str.extract(r"(\d{2})", expand=False)
    insee = insee.str.zfill(2)
    tmp = pd.DataFrame({col: vals, "_insee": insee})
    tmp = tmp.merge(rm[["insee","region"]].rename(columns={"region": "_short"}),
                    left_on="_insee", right_on="insee", how="left")
    # si non mappable, on garde la valeur d'origine
    df[col] = tmp["_short"].fillna(vals)
    return df


def load_insee_population():
    """DF: [region, age_band, population] (region = code court)"""
    cfg = load_config()
    df = read_csv(cfg["insee_population"])
    # accepte ['region'] numérique INSEE ou déjà court
    if "region" not in df.columns and "reg" in df.columns:
        df = df.rename(columns={"reg":"region"})
    df = _apply_region_map(df, col="region")
    df["age_band"] = pd.Categorical(df["age_band"], categories=AGE_BANDS, ordered=True)
    return df[["region","age_band","population"]]

def load_region_mapping():
    """Retourne mapping final pour info."""
    rm = _load_region_map()
    return rm.rename(columns={"insee":"insee_code"})

def load_sentinelles_incidence(with_future: bool = False, future_until: str = "2025-12-31"):
    """
    DF: [date, region, incidence_per_100k] (region = code court)
    - Lit le CSV normalisé, aligne en hebdo (lundi).
    - 'Anti-zéro' si historique vide.
    - Si with_future=True : génère des semaines futures jusqu'à 'future_until' (inclus),
      par climatologie (moyenne par semaine ISO & région) + léger ajustement de tendance récent.
    """
    cfg = load_config()
    df = read_csv(cfg["sentinelles_incidence"])
    df.columns = [c.lower() for c in df.columns]

    # date
    dcol = "date" if "date" in df.columns else ("week" if "week" in df.columns else None)
    if dcol is None:
        raise ValueError("date/week manquant dans sentinelles_norm")
    if dcol == "date":
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    else:
        # "2024-S02" -> lundi de la semaine ISO
        w = df["week"].astype(str).str.replace("S","W", regex=False)
        yr = w.str.extract(r"(\d{4})", expand=False).fillna(method="ffill")
        wk = w.str.extract(r"[W\-](\d{1,2})", expand=False).fillna("1")
        iso = yr + "-W" + wk.str.zfill(2) + "-1"
        df["date"] = pd.to_datetime(iso, format="%G-W%V-%u", errors="coerce")

    # region
    rcol = "region" if "region" in df.columns else ("region_code" if "region_code" in df.columns else "geo_insee")
    df = df.rename(columns={rcol:"region"})
    df = _apply_region_map(df, col="region")

    # incidence
    for cand in ["incidence_per_100k","inc100","ili_per_100k","incidence","value","valeur"]:
        if cand in df.columns:
            df["incidence_per_100k"] = pd.to_numeric(df[cand], errors="coerce")
            break
    if "incidence_per_100k" not in df.columns:
        raise ValueError("Colonne d'incidence introuvable dans sentinelles_norm")

    # hebdo W-MON
    out = df[["date","region","incidence_per_100k"]].dropna(subset=["date"]).copy()
    out["date"] = out["date"].dt.to_period("W-MON").dt.start_time
    out = (out.groupby(["date","region"], as_index=False)["incidence_per_100k"]
             .mean().sort_values(["region","date"]))

    # --- ANTI-ZÉRO historique ---
    if (out["incidence_per_100k"].fillna(0).sum() == 0):
        rng = np.random.default_rng(42)
        out["woy"] = out["date"].dt.isocalendar().week.astype(int)
        base, amp = 8.0, 6.0
        phase_per_region = {r:i*5.0 for i,r in enumerate(out["region"].unique())}
        out["incidence_per_100k"] = base + amp*np.sin(
            2*np.pi*(out["woy"] - out["region"].map(phase_per_region))/52.0
        ) + rng.normal(0, 0.8, len(out))
        out["incidence_per_100k"] = out["incidence_per_100k"].clip(lower=0.1)
        out = out.drop(columns=["woy"])

    # --- FUTUR 2025 par climatologie + tendance récente ---
    if with_future:
        future_until = pd.to_datetime(future_until)  # borne inclusive
        # semaines futures (lundi) jusqu'à fin 2025
        last_hist = out["date"].max()
        first_future = (last_hist + pd.Timedelta(days=7)).to_period("W-MON").to_timestamp()
        fut_dates = pd.date_range(first_future, future_until, freq="W-MON")
        if len(fut_dates):
            # Climatologie région × semaine ISO
            hist = out.copy()
            hist["woy"] = hist["date"].dt.isocalendar().week.astype(int)
            clim = (hist.groupby(["region","woy"], as_index=False)["incidence_per_100k"]
                        .mean().rename(columns={"incidence_per_100k":"inc_clim"}))

            # Tendance récente: ratio (dernières 8 semaines) / (climatologie correspondante)
            tr = []
            for r, grp in hist.groupby("region"):
                g = grp.sort_values("date").tail(8).copy()
                if g.empty:
                    tr.append({"region": r, "trend": 1.0})
                    continue
                g["woy"] = g["date"].dt.isocalendar().week.astype(int)
                g = g.merge(clim[clim["region"]==r][["woy","inc_clim"]], on="woy", how="left")
                den = g["inc_clim"].replace(0, np.nan).mean()
                num = g["incidence_per_100k"].mean()
                ratio = (num/den) if pd.notnull(den) and den>0 else 1.0
                tr.append({"region": r, "trend": float(np.clip(ratio, 0.7, 1.3))})
            trend = pd.DataFrame(tr)

            fut = pd.DataFrame({"date": fut_dates})
            fut["woy"] = fut["date"].dt.isocalendar().week.astype(int)
            regions = out["region"].unique()
            fut = fut.assign(key=1).merge(pd.DataFrame({"region":regions,"key":[1]*len(regions)}), on="key").drop(columns=["key"])
            fut = fut.merge(clim, on=["region","woy"], how="left").merge(trend, on="region", how="left")
            fut["incidence_per_100k"] = (fut["inc_clim"].fillna(fut.groupby("region")["inc_clim"].transform("mean"))
                                         * fut["trend"].fillna(1.0))
            fut = fut[["date","region","incidence_per_100k"]]
            out = pd.concat([out, fut], ignore_index=True).sort_values(["region","date"])

    return out[["date","region","incidence_per_100k"]]



def load_oscour_urgences():
    """DF: [date, region, age_band, er_visits, admissions] (region = code court)"""
    cfg = load_config()
    df = read_csv(cfg["oscour_urgences"])
    df["date"] = pd.to_datetime(df["date"])
    # region peut être 'IDF' déjà -> laisse tel quel ; sinon map depuis INSEE
    df = _apply_region_map(df, col="region")
    for col in ["er_visits","admissions"]:
        if col not in df.columns:
            df[col] = 0
    if "age_band" not in df.columns:
        df["age_band"] = "18-64"
    return df[["date","region","age_band","er_visits","admissions"]]

def load_meteo_temperature(with_future: bool = False, future_until: str = "2025-12-31"):
    """
    DF: [date, region, tmean] (region = code court)
    - Lit la température moyenne historique.
    - Si with_future=True : génère 2025 par climatologie région × mois (tmean_clim).
    Remarque: on génère des points au 1er du mois pour le futur; le build mensuel
    les agrègera de toute façon.
    """
    cfg = load_config()
    df = read_csv(cfg["meteo_temperature"])
    df["date"] = pd.to_datetime(df["date"])
    if "tmean" not in df.columns and "tmoy" in df.columns:
        df = df.rename(columns={"tmoy":"tmean"})
    df = _apply_region_map(df, col="region")
    out = df[["date","region","tmean"]].copy()

    if with_future:
        future_until = pd.to_datetime(future_until)
        # Climatologie région × mois
        hist = out.copy()
        hist["month"] = hist["date"].dt.month
        clim = (hist.groupby(["region","month"], as_index=False)["tmean"]
                    .mean().rename(columns={"tmean":"tmean_clim"}))

        # Mois 2025
        months_2025 = pd.date_range(pd.Timestamp("2025-01-01"), future_until, freq="MS")
        regions = out["region"].unique()
        fut = (pd.MultiIndex.from_product([months_2025, regions], names=["date","region"])
                            .to_frame(index=False))
        fut["month"] = fut["date"].dt.month
        fut = fut.merge(clim, on=["region","month"], how="left")
        # backup au cas où
        fut["tmean"] = fut["tmean_clim"].fillna(fut.groupby("region")["tmean_clim"].transform("mean"))
        fut = fut[["date","region","tmean"]]

        out = pd.concat([out, fut], ignore_index=True).sort_values(["region","date"])

    return out[["date","region","tmean"]]


def load_vaccination_doses():
    """
    DF: [date, region, age_band, doses]
    - Si le fichier référencé par conf['vaccination_doses'] n'existe pas OU
      n'a pas de colonne 'doses' OU somme(doses)==0, on construit un PROXY
      à partir de l'incidence Sentinelles lissée (MA2) et d'un profil par âge.
    - Pas d'import de fonctions du même module pour éviter tout cycle.
    """
    import os
    cfg = load_config()
    vac_path = _as_abs(cfg["vaccination_doses"])
    use_proxy = False

    if not os.path.exists(vac_path):
        use_proxy = True
    else:
        df = pd.read_csv(vac_path, encoding="utf-8")
        if "date" not in df.columns or "region" not in df.columns:
            use_proxy = True
        else:
            # normalisation minimale
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            if "age_band" not in df.columns:
                df["age_band"] = "18-64"
            doses_col = None
            for c in df.columns:
                if c.lower() in ("doses","nb","valeur","value"):
                    doses_col = c; break
            if doses_col is None:
                df["doses"] = 0.0
            else:
                df = df.rename(columns={doses_col:"doses"})
                df["doses"] = pd.to_numeric(df["doses"], errors="coerce").fillna(0.0)
            # map régions
            df = _apply_region_map(df, col="region")
            # si tout est à 0 -> proxy
            if float(df["doses"].sum()) == 0.0:
                use_proxy = True

    if not use_proxy:
        return df[["date","region","age_band","doses"]]

        # ---------- PROXY VACCINS DE SECOURS (sans fuite) ----------
    # On repart de l'incidence (déjà ANTI-ZÉRO ci-dessus, via le CSV… ou sa synthèse)
    inc = load_sentinelles_incidence(with_future=True, future_until="2025-12-31")

    # hebdo + lissage + décalage t-2 (no leakage)
    inc["date"] = inc["date"].dt.to_period("W-MON").dt.start_time
    inc_w = (inc.groupby(["region","date"], as_index=False)["incidence_per_100k"]
                .mean().sort_values(["region","date"]))
    inc_w["ili_ma2"] = inc_w.groupby("region")["incidence_per_100k"].transform(
        lambda s: s.rolling(2, min_periods=1).mean()
    )
    inc_w["ili_ma2_lag2"] = inc_w.groupby("region")["ili_ma2"].shift(2)

    # grille région×âge×date
    regions = inc_w["region"].unique()
    ages = AGE_BANDS
    all_dates = pd.date_range(inc_w["date"].min(), inc_w["date"].max(), freq="W-MON")
    grid = (pd.MultiIndex.from_product([all_dates, regions, ages],
            names=["date","region","age_band"]).to_frame(index=False))

    proxy = grid.merge(inc_w[["date","region","ili_ma2_lag2"]],
                       on=["date","region"], how="left")
    proxy["ili_ma2_lag2"] = proxy["ili_ma2_lag2"].fillna(method="ffill").fillna(0)

    # profil âge + saison + échelle + bruit léger
    weight = {"0-17":0.5, "18-64":1.0, "65+":1.6}
    proxy["weekofyear"] = proxy["date"].dt.isocalendar().week.astype(int)
    season = 1.0 + 0.15*np.sin(2*np.pi*(proxy["weekofyear"]-6)/52.0)
    alpha = 4.0
    rng = np.random.default_rng(123)
    proxy["doses"] = alpha * proxy["ili_ma2_lag2"] * proxy["age_band"].map(lambda a: weight.get(a,1.0)) * season
    proxy["doses"] = (proxy["doses"] * (1 + rng.normal(0, 0.03, len(proxy)))).clip(lower=0)

    out = proxy[["date","region","age_band","doses"]].copy()
    out["date"] = pd.to_datetime(out["date"])
    return out
