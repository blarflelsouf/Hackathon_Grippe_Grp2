import pandas as pd
import re
import unicodedata

# ==============================
# Helpers
# ==============================
def strip_accents(s: str) -> str:
    if pd.isna(s):
        return s
    s = unicodedata.normalize("NFD", str(s))
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s

def norm_city(s: str) -> str:
    if pd.isna(s):
        return s
    s = strip_accents(s).upper().strip()
    # Nettoyages légers usuels
    s = re.sub(r"\s+", " ", s)
    return s

def zfill_cp(x) -> str:
    s = str(x)
    # Parfois des CP à 4 chiffres dans tes CSV (ex: 8240 -> 08240)
    if s.isdigit():
        return s.zfill(5)
    # Sinon, garde uniquement les chiffres, puis zfill
    digits = re.sub(r"\D", "", s)
    return digits.zfill(5) if digits else None

# Mapping département (2 digits CP) -> région (INSEE, code3, nom)
DEPT2_TO_REGION = {
    "75": ("11","IDF","Île-de-France"), "77": ("11","IDF","Île-de-France"),
    "78": ("11","IDF","Île-de-France"), "91": ("11","IDF","Île-de-France"),
    "92": ("11","IDF","Île-de-France"), "93": ("11","IDF","Île-de-France"),
    "94": ("11","IDF","Île-de-France"), "95": ("11","IDF","Île-de-France"),
    "18": ("24","CVL","Centre-Val de Loire"), "28": ("24","CVL","Centre-Val de Loire"),
    "36": ("24","CVL","Centre-Val de Loire"), "37": ("24","CVL","Centre-Val de Loire"),
    "41": ("24","CVL","Centre-Val de Loire"), "45": ("24","CVL","Centre-Val de Loire"),
    "21": ("27","BFC","Bourgogne-Franche-Comté"), "25": ("27","BFC","Bourgogne-Franche-Comté"),
    "39": ("27","BFC","Bourgogne-Franche-Comté"), "58": ("27","BFC","Bourgogne-Franche-Comté"),
    "70": ("27","BFC","Bourgogne-Franche-Comté"), "71": ("27","BFC","Bourgogne-Franche-Comté"),
    "89": ("27","BFC","Bourgogne-Franche-Comté"), "90": ("27","BFC","Bourgogne-Franche-Comté"),
    "14": ("28","NOR","Normandie"), "27": ("28","NOR","Normandie"),
    "50": ("28","NOR","Normandie"), "61": ("28","NOR","Normandie"),
    "76": ("28","NOR","Normandie"),
    "02": ("32","HDF","Hauts-de-France"), "59": ("32","HDF","Hauts-de-France"),
    "60": ("32","HDF","Hauts-de-France"), "62": ("32","HDF","Hauts-de-France"),
    "80": ("32","HDF","Hauts-de-France"),
    "08": ("44","GES","Grand Est"), "10": ("44","GES","Grand Est"),
    "51": ("44","GES","Grand Est"), "52": ("44","GES","Grand Est"),
    "54": ("44","GES","Grand Est"), "55": ("44","GES","Grand Est"),
    "57": ("44","GES","Grand Est"), "67": ("44","GES","Grand Est"),
    "68": ("44","GES","Grand Est"), "88": ("44","GES","Grand Est"),
    "44": ("52","PDL","Pays de la Loire"), "49": ("52","PDL","Pays de la Loire"),
    "53": ("52","PDL","Pays de la Loire"), "72": ("52","PDL","Pays de la Loire"),
    "85": ("52","PDL","Pays de la Loire"),
    "22": ("53","BRE","Bretagne"), "29": ("53","BRE","Bretagne"),
    "35": ("53","BRE","Bretagne"), "56": ("53","BRE","Bretagne"),
    "16": ("75","NAQ","Nouvelle-Aquitaine"), "17": ("75","NAQ","Nouvelle-Aquitaine"),
    "19": ("75","NAQ","Nouvelle-Aquitaine"), "23": ("75","NAQ","Nouvelle-Aquitaine"),
    "24": ("75","NAQ","Nouvelle-Aquitaine"), "33": ("75","NAQ","Nouvelle-Aquitaine"),
    "40": ("75","NAQ","Nouvelle-Aquitaine"), "47": ("75","NAQ","Nouvelle-Aquitaine"),
    "64": ("75","NAQ","Nouvelle-Aquitaine"), "79": ("75","NAQ","Nouvelle-Aquitaine"),
    "86": ("75","NAQ","Nouvelle-Aquitaine"), "87": ("75","NAQ","Nouvelle-Aquitaine"),
    "09": ("76","OCC","Occitanie"), "11": ("76","OCC","Occitanie"),
    "12": ("76","OCC","Occitanie"), "30": ("76","OCC","Occitanie"),
    "31": ("76","OCC","Occitanie"), "32": ("76","OCC","Occitanie"),
    "34": ("76","OCC","Occitanie"), "46": ("76","OCC","Occitanie"),
    "48": ("76","OCC","Occitanie"), "65": ("76","OCC","Occitanie"),
    "66": ("76","OCC","Occitanie"), "81": ("76","OCC","Occitanie"),
    "82": ("76","OCC","Occitanie"),
    "01": ("84","ARA","Auvergne-Rhône-Alpes"), "07": ("84","ARA","Auvergne-Rhône-Alpes"),
    "26": ("84","ARA","Auvergne-Rhône-Alpes"), "38": ("84","ARA","Auvergne-Rhône-Alpes"),
    "42": ("84","ARA","Auvergne-Rhône-Alpes"), "43": ("84","ARA","Auvergne-Rhône-Alpes"),
    "63": ("84","ARA","Auvergne-Rhône-Alpes"), "69": ("84","ARA","Auvergne-Rhône-Alpes"),
    "73": ("84","ARA","Auvergne-Rhône-Alpes"), "74": ("84","ARA","Auvergne-Rhône-Alpes"),
    "04": ("93","PAC","Provence-Alpes-Côte d'Azur"), "05": ("93","PAC","Provence-Alpes-Côte d'Azur"),
    "06": ("93","PAC","Provence-Alpes-Côte d'Azur"), "13": ("93","PAC","Provence-Alpes-Côte d'Azur"),
    "83": ("93","PAC","Provence-Alpes-Côte d'Azur"), "84": ("93","PAC","Provence-Alpes-Côte d'Azur"),
    "20": ("94","COR","Corse"),
}

def map_cp_to_region_tuple(cp5: str):
    if not cp5 or pd.isna(cp5):
        return (None, None, None)
    return DEPT2_TO_REGION.get(cp5[:2], (None, None, None))

# ==============================
# 1) Préparer pharmacies
# ==============================
def prepare_pharmacies_commune(df_pharma: pd.DataFrame) -> pd.DataFrame:
    # Colonnes attendues (adapte si besoin)
    col_nom = "Titre" if "Titre" in df_pharma.columns else "nom_pharmacie"
    col_cp  = "Adresse_codepostal" if "Adresse_codepostal" in df_pharma.columns else "code_postal"
    col_vil = "Adresse_ville" if "Adresse_ville" in df_pharma.columns else "ville"

    out = df_pharma.copy()
    out = out.rename(columns={col_nom: "pharmacie"})
    out["cp5"] = out[col_cp].apply(zfill_cp)
    out["ville_norm"] = out[col_vil].apply(norm_city)

    # Région (pour info/colonnes demandées)
    out[["region_insee","region_code3","region_name"]] = out["cp5"].apply(map_cp_to_region_tuple).apply(pd.Series)
    return out

# ==============================
# 2) Préparer communes INSEE (exploser les codes postaux)
# ==============================
def explode_codes_postaux(df_villes: pd.DataFrame) -> pd.DataFrame:
    """
    Crée une table (ville_norm, cp5) -> infos commune (population, codes INSEE, etc.).
    'codes_postaux' peut contenir plusieurs CP dans un champ: on extrait toutes les séquences de 5 chiffres.
    """
    base = df_villes.copy()
    # Normaliser la ville pour la jointure
    # On privilégie nom_standard s'il existe, sinon nom_standard_majuscule ou nom_sans_accent
    if "nom_standard" in base.columns:
        base["ville_norm"] = base["nom_standard"].apply(norm_city)
    elif "nom_standard_majuscule" in base.columns:
        base["ville_norm"] = base["nom_standard_majuscule"].apply(norm_city)
    else:
        base["ville_norm"] = base["nom_sans_accent"].apply(norm_city)

    # Préparer les CP (colonne 'codes_postaux' ou 'code_postal')
    if "codes_postaux" in base.columns and base["codes_postaux"].notna().any():
        # Extraire toutes les séquences de 5 chiffres (robuste aux séparateurs différents)
        base["_all_cp"] = base["codes_postaux"].astype(str).apply(lambda s: re.findall(r"\b\d{5}\b", s))
    elif "code_postal" in base.columns:
        base["_all_cp"] = base["code_postal"].astype(str).apply(lambda s: re.findall(r"\b\d{5}\b", s))
    else:
        # Pas de CP -> pas de jointure possible par CP (très rare sur un bon fichier INSEE)
        base["_all_cp"] = [[] for _ in range(len(base))]

    exploded = base.explode("_all_cp", ignore_index=True)
    exploded = exploded.rename(columns={"_all_cp": "cp5"})
    exploded["cp5"] = exploded["cp5"].apply(zfill_cp)
    # Garder les colonnes utiles
    keep_cols = [
        "ville_norm", "cp5", "code_insee", "population", "reg_code", "reg_nom",
        "dep_code", "dep_nom", "latitude_centre", "longitude_centre"
    ]
    keep_cols = [c for c in keep_cols if c in exploded.columns]
    return exploded[keep_cols].dropna(subset=["cp5","ville_norm"]).drop_duplicates()

# ==============================
# 3) Jointure pharmacie -> commune
# ==============================
def match_pharmacies_to_communes(df_pharma: pd.DataFrame, df_communes_cp: pd.DataFrame) -> pd.DataFrame:
    # 3.a Jointure stricte (cp5 + ville_norm)
    merged = df_pharma.merge(
        df_communes_cp,
        on=["cp5","ville_norm"],
        how="left",
        suffixes=("","_commune")
    )

    # 3.b Fallback: si pas de match, tenter par cp5 seul -> prendre la commune la plus peuplée sur ce CP
    missing = merged["population"].isna()
    if missing.any():
        # Meilleure commune par CP (max population)
        best_by_cp = (
            df_communes_cp
            .sort_values("population", ascending=False)
            .drop_duplicates(subset=["cp5"])
            .rename(columns={
                "ville_norm": "ville_norm_best",
                "code_insee": "code_insee_best",
                "population": "population_best"
            })
        )
        merged = merged.merge(best_by_cp[["cp5","ville_norm_best","code_insee_best","population_best"]],
                              on="cp5", how="left")

        # Remplir les trous
        for col_src, col_dst in [("population_best","population"),
                                 ("code_insee_best","code_insee")]:
            merged.loc[missing & merged[col_dst].isna(), col_dst] = merged.loc[missing & merged[col_dst].isna(), col_src]

        # Si ville_norm manquante côté communes, garder la ville pharma
        merged["ville_norm"] = merged["ville_norm"]

        # Nettoyage colonnes auxiliaires
        merged = merged.drop(columns=["ville_norm_best","code_insee_best","population_best"], errors="ignore")

    return merged

# ==============================
# 4) Calcul stock au niveau COMMUNE
# ==============================
def compute_stock_par_commune(df_matched: pd.DataFrame,
                              target_coverage=0.50,
                              doses_per_person=1.0,
                              buffer_factor=1.10) -> pd.DataFrame:
    # Pharmacies par commune (par code_insee)
    n_pharma_commune = (
        df_matched.groupby("code_insee", as_index=False).size()
        .rename(columns={"size":"n_pharmacies_commune"})
    )
    out = df_matched.merge(n_pharma_commune, on="code_insee", how="left")

    # Stock cible par commune
    out["stock_total_commune_cible"] = (
        out["population"].fillna(0)
        * target_coverage
        * doses_per_person
        * buffer_factor
    )

    # Part égale par pharmacie de la commune
    out["stock_potentiel_vaccins"] = (
        out["stock_total_commune_cible"] / out["n_pharmacies_commune"].replace({0: pd.NA})
    ).round().fillna(0).astype(int)

    # Colonnes finales
    final_cols = [
        "pharmacie",
        "cp5",
        "region_code3", "region_insee", "region_name",
        "code_insee",           # commune
        "ville_norm",           # nom commune normalisé
        "population",           # population de la commune
        "n_pharmacies_commune",
        "stock_potentiel_vaccins"
    ]
    return out[final_cols].sort_values(["code_insee","pharmacie"])


# -- Pharmacies (extrait fourni) :
df_pharma = pd.read_csv("data/raw/Classeur1.csv", sep=";", encoding="cp1252", dtype={"Adresse_codepostal":"string"})

# -- df_villes : doit contenir (au moins) code_insee, population, reg_code, reg_nom, codes_postaux/code_postal, nom_standard*
df_villes = pd.read_csv("data/raw/communes-france-2025.csv", dtype={"reg_code":"string","code_postal":"string"})

# 1) Prépare pharmacies + région
dfP = prepare_pharmacies_commune(df_pharma)

# 2) Explose INSEE villes par codes postaux
communes_cp = explode_codes_postaux(df_villes)

# 3) Jointure pharmacie -> commune
matched = match_pharmacies_to_communes(dfP, communes_cp)

# 4) Calcul du stock à l'échelle de la commune (équitable entre pharmacies de la même commune)
result = compute_stock_par_commune(
    matched,
    target_coverage=0.50,   # 50% de couverture
    doses_per_person=1.0,   # 1 dose/grippe
    buffer_factor=1.08      # +8% de marge
)
result.drop(columns=['cp5', 'region_insee','region_name', 'code_insee', 'ville_norm'], inplace=True)
f = result.loc[:, ~result.columns.str.contains("^Unnamed")]

# 2️⃣ Convertir toutes les colonnes numériques en int (sans erreur si NaN)
for col in result.select_dtypes(include="number").columns:
    result[col] = result[col].fillna(0).astype(int)


result.to_csv('data/processed/pharma_clean.csv')

print(result)
