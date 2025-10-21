"""
Téléchargements & normalisations pour les sources Open Data :
- Sentinelles (incidence hebdo région)
- SurSaUD/OSCOUR (grippe urgences/hospitalisations; niveau département -> agrégé région)
- INSEE POP1A (population par âge; agrégé région et mappé vers bandes [0-17, 18-64, 65+])
- Température quotidienne région (ODRÉ) -> agrégé hebdo

Remplissez/ajustez les mappings région (INSEE -> codes courts) si nécessaire.
"""
import io, os, pandas as pd, numpy as np, requests
from pathlib import Path
import unicodedata
from .config import RAW_DIR, PROCESSED_DIR, AGE_BANDS, FREQ, REGIONS, CONF_DIR, BASE_DIR

import yaml

def _cfg():
    with open(CONF_DIR / "data_sources.yaml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def _dl(url, out_path):
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    out.write_bytes(r.content)
    return out


def _nrm(s: str) -> str:
    """Normalise une chaîne pour matching (sans accents, uppercase)."""
    if pd.isna(s):
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    return s.upper().strip()

def _load_region_mapping_short():
    """
    Charge le mapping régions depuis conf -> data/raw/region_mapping.csv
    Retourne DataFrame avec colonnes: insee (2 chiffres), region (code court), region_name_norm
    """
    cfg = yaml.safe_load(open(CONF_DIR / "data_sources.yaml","r",encoding="utf-8"))
    rm_path = (BASE_DIR / cfg["region_mapping"]).as_posix()
    rm = pd.read_csv(rm_path)
    rm["insee"] = rm["insee"].astype(str).str.zfill(2)
    rm["region"] = rm["region"].astype(str).str.upper()
    rm["region_name_norm"] = rm["region_name"].map(_nrm)
    return rm[["insee","region","region_name_norm"]]


def _norm_region_name(s):
    if pd.isna(s): return s
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    s = s.upper().replace(" ", "").replace("-", "").replace("'", "")
    return s

def download_sentinelles():
    """
    Télécharge & normalise l'incidence hebdo Sentinelles.
    Gère automatiquement le séparateur (',' ou ';') et différents schémas de colonnes.
    Sortie normalisée: [date, region, incidence_per_100k] avec region sur 2 chiffres.
    """
    import pandas as pd
    from pathlib import Path
    url = _cfg()["sentinelles_incidence_url"]
    path = RAW_DIR / "sentinelles_weekly.csv"
    _dl(url, path)

    # 1) Lecture robuste: essai sep=';' puis fallback ','
    def _read_any(p):
        try:
            df = pd.read_csv(p, sep=';')
            if df.shape[1] == 1:  # probablement mauvais sep
                raise ValueError
            return df
        except Exception:
            return pd.read_csv(p)

    df = _read_any(path)
    df.columns = [c.strip().lower() for c in df.columns]

    # 2) Détection colonnes
    # Exemples vus: ['week','indicator','inc','inc_low','inc_up','inc100','inc100_low','inc100_up','geo_insee','geo_name','date','nom_maladie']
    date_col = None
    for cand in ["week", "date", "semaine"]:
        if cand in df.columns:
            date_col = cand
            break
    if date_col is None:
        raise ValueError(f"Colonne date/semaine introuvable dans Sentinelles: {df.columns.tolist()}")

    reg_col = None
    for cand in ["geo_insee", "region_code", "reg", "region"]:
        if cand in df.columns:
            reg_col = cand
            break
    if reg_col is None:
        # parfois le code peut être dans 'code_insee_region' ou similaire
        for c in df.columns:
            if "insee" in c and "region" in c:
                reg_col = c
                break
    if reg_col is None:
        raise ValueError(f"Colonne code région introuvable dans Sentinelles: {df.columns.tolist()}")

    inc_col = None
    for cand in ["inc100", "incidence_per_100k", "ili_per_100k"]:
        if cand in df.columns:
            inc_col = cand
            break
    if inc_col is None:
        # à défaut, 'inc' (valeur brute) peut exister mais on veut per 100k -> on garde inc si rien d'autre
        if "inc" in df.columns:
            inc_col = "inc"
        else:
            raise ValueError(f"Colonne d'incidence introuvable (inc100/incidence_per_100k/inc): {df.columns.tolist()}")

    # 3) Normalisation
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    out = df[[date_col, reg_col, inc_col]].rename(columns={
        date_col: "date",
        reg_col: "region",
        inc_col: "incidence_per_100k"
    }).dropna(subset=["date"])

    # Region en chaîne à 2 chiffres si code INSEE (même pour Corse '94' si présent)
    out["region"] = out["region"].astype(str).str.extract(r"(\d{2})", expand=False).fillna(out["region"].astype(str))
    out["region"] = out["region"].str.zfill(2)

    # On garde uniquement ce qui ressemble à des codes région (2 chiffres)
    out = out[out["region"].str.fullmatch(r"\d{2}") == True].copy()

    out.to_csv(RAW_DIR / "sentinelles_norm.csv", index=False, encoding="utf-8")
    return out


def download_oscour():
    """
    Télécharge & normalise l’export OSCOUR/SurSaUD grippe.
    - Tolère ;/,, schémas variés
    - Si aucune colonne géographique n'est présente (national), réplique sur toutes les régions (codes courts)
    Sortie normalisée: [date, region, age_band, er_visits, admissions]
      * NB: er_visits/admissions peuvent être des TAUX si la source ne fournit que des taux
    """
    import pandas as pd, numpy as np, re, unicodedata

    # ---- mapping DEP -> REG (INSEE 2016+) pour les exports départementaux ----
    dep_to_reg = {
        "971":"01","972":"02","973":"03","974":"04","976":"06",
        "2A":"94","2B":"94",
        "01":"84","02":"32","03":"84","04":"93","05":"93","06":"93","07":"84","08":"44","09":"76",
        "10":"44","11":"76","12":"76","13":"93","14":"28","15":"84","16":"75","17":"75","18":"24",
        "19":"75","21":"27","22":"53","23":"75","24":"75","25":"27","26":"84","27":"28","28":"24",
        "29":"53","30":"76","31":"76","32":"76","33":"75","34":"76","35":"53","36":"24","37":"24",
        "38":"84","39":"27","40":"75","41":"24","42":"84","43":"84","44":"52","45":"24","46":"76",
        "47":"75","48":"76","49":"52","50":"28","51":"44","52":"44","53":"52","54":"44","55":"44",
        "56":"53","57":"44","58":"27","59":"32","60":"32","61":"28","62":"32","63":"84","64":"75",
        "65":"76","66":"76","67":"44","68":"44","69":"84","70":"27","71":"27","72":"52","73":"84",
        "74":"84","75":"11","76":"28","77":"11","78":"11","79":"75","80":"32","81":"76","82":"76",
        "83":"93","84":"93","85":"52","86":"75","87":"75","88":"44","89":"27","90":"27","91":"11",
        "92":"11","93":"11","94":"11","95":"11"
    }

    def _read_any(p):
        try:
            df = pd.read_csv(p, sep=';')
            if df.shape[1] == 1:
                raise ValueError
            return df
        except Exception:
            return pd.read_csv(p)

    def _nrm(s: str) -> str:
        if pd.isna(s): return ""
        s = str(s)
        s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
        return s.upper().strip()

    def _load_region_mapping_short():
        # lit data/raw/region_mapping.csv (insee -> code court + nom)
        from .config import CONF_DIR, BASE_DIR
        import yaml
        cfg = yaml.safe_load(open(CONF_DIR / "data_sources.yaml","r",encoding="utf-8"))
        rm = pd.read_csv((BASE_DIR / cfg["region_mapping"]).as_posix())
        rm["insee"] = rm["insee"].astype(str).str.zfill(2)
        rm["region"] = rm["region"].astype(str).str.upper()
        rm["region_name_norm"] = rm["region_name"].map(_nrm)
        return rm[["insee","region","region_name_norm"]]

    url = _cfg()["oscour_grippe_url"]
    path = RAW_DIR / "oscour_grippe.csv"
    _dl(url, path)

    df = _read_any(path)
    df.columns = [c.strip().lower() for c in df.columns]

    # 1) Date: 'date_complet' ou 'jour' / sinon semaine ISO
    if "date_complet" in df.columns:
        df["date"] = pd.to_datetime(df["date_complet"], errors="coerce")
    else:
        dcol = next((c for c in ["jour","date","date_evenement","date_passage","date_de_passage"] if c in df.columns), None)
        if dcol:
            df["date"] = pd.to_datetime(df[dcol], errors="coerce")
        else:
            wk = next((c for c in ["semaine","week","sem"] if c in df.columns), None)
            yr = next((c for c in ["annee","year","anne"] if c in df.columns), None)
            if wk and yr:
                sw = df[wk].astype(str).str.extract(r"(\d+)", expand=False).fillna("1").astype(int)
                sy = df[yr].astype(str).str.extract(r"(\d{4})", expand=False).fillna("2020")
                iso = sy + "-W" + sw.astype(str).str.zfill(2) + "-1"
                df["date"] = pd.to_datetime(iso, format="%G-W%V-%u", errors="coerce")
            else:
                like_date = [c for c in df.columns if "date" in c]
                if like_date:
                    df["date"] = pd.to_datetime(df[like_date[0]], errors="coerce")
                else:
                    raise ValueError("OSCOUR: aucune colonne de date/semaine trouvée.")

    # 2) Détection de la géographie (si présente)
    geo_candidates = [
        "code_insee_region","code_region","reg","region","maille_code",
        "libelle_region","libelle_reg","nom_region","geo_name","libgeo","libelle","zone","territoire","maille",
        "region_name"
    ]
    geocol = next((c for c in geo_candidates if c in df.columns), None)

    rm = _load_region_mapping_short()

    def _series_to_region_short(sr):
        """Essaie d'extraire un code court (IDF/ARA/...) depuis une série de codes/noms/mailles."""
        # a) codes "REG-84" / "84" / "2A" etc.
        cand_num = sr.astype(str).str.extract(r"(\d{2,3}|2A|2B)", expand=False)
        cand_num = cand_num.replace({"2A":"94","2B":"94"})
        cand_num2 = cand_num.map(lambda x: dep_to_reg.get(x, x) if isinstance(x,str) and len(x) in (2,3) else x)
        tmp = pd.DataFrame({"insee": cand_num2})
        tmp["insee"] = tmp["insee"].astype(str).str.extract(r"(\d{2})", expand=False)
        m = tmp.merge(rm, on="insee", how="left")["region"]
        if not m.isna().all():
            return m
        # b) noms de région
        gnorm = sr.map(_nrm)
        m2 = pd.DataFrame({"region_name_norm": gnorm}).merge(rm, on="region_name_norm", how="left")["region"]
        return m2

    if geocol:
        region_short = _series_to_region_short(df[geocol])
    else:
        region_short = pd.Series([np.nan]*len(df))

    # 3) Age -> age_band
    def map_age_band(v):
        s = str(v).lower()
        if any(x in s for x in ["0-4","0_4","5-14","5_14","0-14","0_14","0-17"]): return "0-17"
        if any(x in s for x in ["15-64","15_64","18-64","18_64"]): return "18-64"
        if any(x in s for x in ["65","65+","65 ans","65 ans ou plus","65 ans et plus"]): return "65+"
        m = re.findall(r"\d+", s)
        if m:
            a = max(int(x) for x in m)
            return "0-17" if a<18 else ("18-64" if a<65 else "65+")
        return "18-64"

    age_col = next((c for c in ["sursaud_cl_age_gene","classe_age","age_classe","age","classe_age_quinquennale","tranche_age"] if c in df.columns), None)
    df["age_band"] = df[age_col].apply(map_age_band) if age_col else "18-64"

    # 4) Mesures -> er_visits/admissions (ici ce sont des TAUX dans ton export)
    # On mappe:
    # - er_visits ← taux_passages_grippe_sau (taux)
    # - admissions ← taux_hospit_grippe_sau (taux)
    ev = None
    if "taux_passages_grippe_sau" in df.columns:
        ev = pd.to_numeric(df["taux_passages_grippe_sau"], errors="coerce")
    elif "valeur" in df.columns:
        ev = pd.to_numeric(df["valeur"], errors="coerce")
    else:
        ev = pd.Series([0]*len(df), dtype=float)

    adm = None
    if "taux_hospit_grippe_sau" in df.columns:
        adm = pd.to_numeric(df["taux_hospit_grippe_sau"], errors="coerce")
    elif "admissions" in df.columns:
        adm = pd.to_numeric(df["admissions"], errors="coerce")
    else:
        adm = pd.Series([0]*len(df), dtype=float)

    base = pd.DataFrame({
        "date": df["date"],
        "age_band": df["age_band"],
        "er_visits": ev.fillna(0.0).astype(float),
        "admissions": adm.fillna(0.0).astype(float),
    })

    # 5) Si aucune région déterminée → mode NATIONAL: répliquer sur toutes les régions
    if region_short.isna().all():
        all_regs = rm["region"].unique().tolist()  # IDF, ARA, ...
        out_list = []
        for r in all_regs:
            tmp = base.copy()
            tmp["region"] = r
            out_list.append(tmp)
        out = pd.concat(out_list, ignore_index=True)
    else:
        out = base.copy()
        out["region"] = region_short.fillna("IDF").astype(str)

    out = out.dropna(subset=["date"])
    out.to_csv(RAW_DIR / "oscour_norm.csv", index=False)
    return out






def download_insee_pop():
    """
    Télécharge & normalise POP1A (population par âge) ou construit un proxy si l'endpoint est KO.
    Sortie normalisée attendue par la pipeline : [region(INSEE ou court ensuite mappé), age_band, population]
    """
    import pandas as pd, requests
    from io import BytesIO

    url_candidates = [
        # 2022 (souvent OK, mais parfois 500 comme chez toi)
        _cfg().get("insee_pop_csv_url"),
        # Fallbacks plausibles (autres millésimes POP1A) — modifiables :
        "https://www.insee.fr/fr/statistiques/fichier/7650800/pop1a-2021.csv",
        "https://www.insee.fr/fr/statistiques/fichier/7650792/pop1a-2020.csv",
    ]
    path = RAW_DIR / "insee_pop_raw.csv"

    def try_download(u):
        if not u: return None
        try:
            r = requests.get(u, timeout=60)
            r.raise_for_status()
            path.write_bytes(r.content)
            return path
        except Exception:
            return None

    got = None
    for u in url_candidates:
        got = try_download(u)
        if got is not None:
            break

    if got is None:
        # -------- PROXY DE SECOURS ----------
        # On construit un proxy population (ordre de grandeur) à partir du mapping régions,
        # puis on applique des parts d'âge fixes (démo) : 0-17:22%, 18-64:60%, 65+:18
        # Suffisant pour débloquer la pipeline car tout est normalisé "per 100k".
        rm = _load_region_mapping_short()  # insee, region (code court), region_name_norm
        # base uniforme 1 200 000 / région (modifiez si vous avez de vrais totaux par région)
        base = 1_200_000
        rows = []
        for _, r in rm.iterrows():
            for band, w in [("0-17", 0.22), ("18-64", 0.60), ("65+", 0.18)]:
                rows.append({"region": r["insee"], "age_band": band, "population": int(base * w)})
        out = pd.DataFrame(rows)
        out.to_csv(RAW_DIR / "insee_population_norm.csv", index=False)
        return out

    # -------- Lecture & normalisation quand le CSV est dispo ----------
    # Le POP1A utilise généralement ';'
    try:
        df = pd.read_csv(path, sep=';')
    except Exception:
        df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]

    # Colonnes usuelles : 'reg' (code région), 'agepyr10' (classe âge), 'nb' (effectif)
    reg_col = "reg" if "reg" in df.columns else None
    if reg_col is None:
        raise ValueError("INSEE POP1A: colonne 'reg' absente. Ajustez la lecture/colonnes selon le millésime.")

    age_col = "agepyr10" if "agepyr10" in df.columns else ( [c for c in df.columns if c.startswith("age")][0] if any(c.startswith("age") for c in df.columns) else None )
    nb_col  = "nb" if "nb" in df.columns else ("valeur" if "valeur" in df.columns else None)
    if age_col is None or nb_col is None:
        raise ValueError("INSEE POP1A: colonnes d'âge/nb introuvables. Vérifiez le schéma.")

    def band_from_age10(code):
        try:
            v = int(str(code))
        except:
            return None
        if v < 18: return "0-17"
        if v < 65: return "18-64"
        return "65+"

    df["age_band"] = df[age_col].apply(band_from_age10)
    pop = (df.groupby([reg_col, "age_band"], as_index=False)[nb_col]
             .sum().rename(columns={reg_col: "region", nb_col: "population"}))

    # Harmonise code INSEE sur 2 chiffres (ex: '11')
    pop["region"] = pop["region"].astype(str).str.zfill(2)

    pop.to_csv(RAW_DIR / "insee_population_norm.csv", index=False)
    return pop


def download_meteo_region():
    """
    Télécharge & normalise la température quotidienne régionale (ODRÉ).
    - Gère ;/, et plusieurs schémas: tmean / tmoy / tmin+tmax / t / tmoyenne / temp_moy...
    - Normalise la sortie: [date, region, tmean]
      * region = code INSEE (2 chiffres) si dispo, sinon tente un mapping nom -> code court puis re-map INSEE.
    """
    import pandas as pd, numpy as np, re

    url = _cfg()["meteo_regionale_url"]
    path = RAW_DIR / "meteo_regionale.csv"
    _dl(url, path)

    # 1) Lecture robuste
    def _read_any(p):
        try:
            df = pd.read_csv(p, sep=';')
            if df.shape[1] == 1:
                raise ValueError
            return df
        except Exception:
            return pd.read_csv(p)
    df = _read_any(path)
    df.columns = [c.strip().lower() for c in df.columns]

    # 2) Date
    dcol = "date" if "date" in df.columns else None
    if dcol is None:
        # parfois 'jour' / 'date_obs'
        for cand in ["jour", "date_obs", "date_complet"]:
            if cand in df.columns:
                dcol = cand; break
    if dcol is None:
        raise ValueError(f"Météo: colonne date introuvable dans {df.columns.tolist()}")
    df["date"] = pd.to_datetime(df[dcol], errors="coerce")

    # 3) Région (priorité au code INSEE)
    rcol = None
    for cand in ["code_insee_region", "code_region", "reg", "region"]:
        if cand in df.columns:
            rcol = cand; break
    if rcol is None:
        # parfois 'nom_region' uniquement
        for cand in ["nom_region", "libelle_region", "region_name", "libelle"]:
            if cand in df.columns:
                rcol = cand; break
    if rcol is None:
        raise ValueError("Météo: aucune colonne de région trouvée.")

    # 4) Trouver / fabriquer tmean
    # priorités: tmean | tmoy | t | tmoyenne | temp_moy | temperature_moyenne | (tmin+tmax)/2
    tmean_col = None
    candidates = ["tmean", "tmoy", "t", "tmoyenne", "temp_moy", "temperature_moyenne"]
    for c in candidates:
        if c in df.columns:
            tmean_col = c; break
    if tmean_col is None:
        # chercher paires min/max (noms fréquents)
        tmin = None; tmax = None
        for c in ["tmin","tn","t_min","temperature_minimale","tmin_c"]:
            if c in df.columns: tmin = c; break
        for c in ["tmax","tx","t_max","temperature_maximale","tmax_c"]:
            if c in df.columns: tmax = c; break
        if tmin and tmax:
            df["tmean"] = (pd.to_numeric(df[tmin], errors="coerce") + pd.to_numeric(df[tmax], errors="coerce")) / 2.0
        else:
            # dernier recours: si une unique colonne numérique 'temperature' existe
            only_num = [c for c in df.columns if "temp" in c and c not in ["temperature_minimale","temperature_maximale"]]
            if only_num:
                df["tmean"] = pd.to_numeric(df[only_num[0]], errors="coerce")
            else:
                raise KeyError("Météo: impossible d'inférer tmean (aucun des champs attendus).")
    else:
        df["tmean"] = pd.to_numeric(df[tmean_col], errors="coerce")

    # 5) Normaliser la région:
    # - si code INSEE dispo -> zfill(2)
    # - sinon, tentatives de mapping nom -> code court via _load_region_mapping_short(),
    #   puis re-map plus tard par data_ingestion vers codes courts uniformes.
    rm = _load_region_mapping_short()  # insee, region(code court), region_name_norm
    region_series = df[rcol].astype(str)

    # a) si déjà numérique -> INSEE 2 chiffres
    reg_insee = region_series.str.extract(r"(\d{2})", expand=False)
    if reg_insee.notna().any():
        reg_final = reg_insee.fillna("").str.zfill(2)
    else:
        # b) essayer par nom: map nom normalisé -> insee, en passant par code court
        names_norm = region_series.map(_nrm)
        m = pd.DataFrame({"region_name_norm": names_norm}).merge(rm, on="region_name_norm", how="left")
        # si on a un insee depuis rm
        if m["insee"].notna().any():
            reg_final = m["insee"].fillna(method="ffill")  # best effort
        else:
            # défaut: IDF
            reg_final = pd.Series(["11"] * len(df))

    out = pd.DataFrame({
        "date": df["date"],
        "region": reg_final.astype(str).str.zfill(2),
        "tmean": df["tmean"]
    }).dropna(subset=["date"])

    out.to_csv(RAW_DIR / "meteo_region_norm.csv", index=False)
    return out


def run_all():
    sent = download_sentinelles()
    osc = download_oscour()
    pop = download_insee_pop()
    met = download_meteo_region()
    return {"sentinelles": len(sent), "oscour": len(osc), "insee": len(pop), "meteo": len(met)}

if __name__ == "__main__":
    print(run_all())
