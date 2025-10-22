# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import csv
from pathlib import Path

# =========================
# Paramètres fichiers
# =========================
FORECAST_PATH = "data/raw/reassort_plan_from_latest.csv"     # contient: date, region, age_band, doses_per_100k_forecast, ...
PHARMA_PATH   = "data/processed/pharma_clean.csv"                  # contient: pharmacie, region_code3, population, (optionnel) stock_initial_oct | stock_potentiel_vaccins
COMMUNES_PATH = "data/raw/communes-france-2025.csv"          # contient: reg_code (INSEE), population des communes (qu'on somme par région)

# =========================
# Aides I/O robustes
# =========================
def read_csv_robust(path, dtypes=None):
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
    seps = [",",";","\t","|"]
    last_err = None
    for enc in encodings:
        for sep in seps:
            try:
                return pd.read_csv(path, encoding=enc, sep=sep, engine="python", quoting=csv.QUOTE_MINIMAL, dtype=dtypes)
            except Exception as e:
                last_err = e
                continue
    raise last_err

# =========================
# Mapping INSEE -> code région 3 lettres
# =========================
REG_INSEE_TO_CODE3 = {
    "11":"IDF", "24":"CVL", "27":"BFC", "28":"NOR", "32":"HDF",
    "44":"GES", "52":"PDL", "53":"BRE", "75":"NAQ", "76":"OCC",
    "84":"ARA", "93":"PAC", "94":"COR",
    "01":"GUA", "02":"MAR", "03":"GUY", "04":"REU", "06":"MAY"
}

# =========================
# Charger les trois datasets
# =========================
df_forecast = read_csv_robust(FORECAST_PATH)
df_pharma   = read_csv_robust(PHARMA_PATH)
df_comm     = read_csv_robust(COMMUNES_PATH)

# =========================
# Normalisations de base
# =========================
# forecast
df_forecast.columns = df_forecast.columns.str.strip()
df_forecast["region"] = df_forecast["region"].astype(str).str.upper().str.strip()
df_forecast["date"]   = pd.to_datetime(df_forecast["date"], errors="coerce").dt.to_period("M").dt.to_timestamp()

# convertir numeric même si virgule française
def to_num_fr(x):
    return pd.to_numeric(str(x).replace(",", "."), errors="coerce")

if "doses_per_100k_forecast" in df_forecast.columns:
    df_forecast["doses_per_100k_forecast"] = df_forecast["doses_per_100k_forecast"].map(to_num_fr)
if "mean_hist" in df_forecast.columns:
    df_forecast["mean_hist"] = df_forecast["mean_hist"].map(to_num_fr)
if "forecast_vs_hist_%" in df_forecast.columns:
    df_forecast["forecast_vs_hist_%"] = df_forecast["forecast_vs_hist_%"].map(to_num_fr)

# pharma
df_pharma["region_code3"] = df_pharma["region_code3"].astype(str).str.upper().str.strip()
df_pharma["population"]   = pd.to_numeric(df_pharma.get("population", 0), errors="coerce").fillna(0)

# communes (trouver colonnes reg_code + population)
comm_cols_l = {c.lower(): c for c in df_comm.columns}
reg_code_col = next((comm_cols_l[k] for k in ["reg_code","code_region","insee_region","region_insee","region_code_insee"] if k in comm_cols_l), None)
pop_comm_col = next((comm_cols_l[k] for k in ["population","pop","pop_totale"] if k in comm_cols_l), None)
if reg_code_col is None or pop_comm_col is None:
    raise ValueError("Dans communes-france-2025.csv, il faut une colonne code région INSEE (ex: reg_code) et une colonne population.")

df_comm[reg_code_col] = df_comm[reg_code_col].astype(str).str.zfill(2)
df_comm[pop_comm_col] = pd.to_numeric(df_comm[pop_comm_col], errors="coerce").fillna(0)

# =========================
# Population régionale depuis les communes (INSEE -> code3)
# =========================
pop_insee = (
    df_comm.groupby(reg_code_col, as_index=False)[pop_comm_col]
           .sum()
           .rename(columns={reg_code_col: "reg_insee", pop_comm_col: "population_region"})
)
pop_insee["region"] = pop_insee["reg_insee"].map(REG_INSEE_TO_CODE3)
pop_region = pop_insee.dropna(subset=["region"]).groupby("region", as_index=False)["population_region"].sum()

# =========================
# Garantir stock initial (octobre) pour la simulation
# =========================
def ensure_stock_initial(df_pharma: pd.DataFrame,
                         df_initial: pd.DataFrame | None = None,
                         key: str = "pharmacie",
                         default_if_missing: int = 100,
                         min_initial: int | None = 100) -> pd.DataFrame:
    df = df_pharma.copy()

    if "stock_initial_oct" in df.columns:
        df["stock_initial_oct"] = pd.to_numeric(df["stock_initial_oct"], errors="coerce").fillna(0).astype(int)
    elif "stock_potentiel_vaccins" in df.columns:
        df["stock_initial_oct"] = pd.to_numeric(df["stock_potentiel_vaccins"], errors="coerce").fillna(0).astype(int)
    elif df_initial is not None and key in df.columns and key in df_initial.columns and "stock_initial_oct" in df_initial.columns:
        df = df.merge(df_initial[[key, "stock_initial_oct"]], on=key, how="left")
        df["stock_initial_oct"] = pd.to_numeric(df["stock_initial_oct"], errors="coerce").fillna(0).astype(int)
    else:
        df["stock_initial_oct"] = default_if_missing  # <- 100 par défaut

    if min_initial is not None:
        df["stock_initial_oct"] = df["stock_initial_oct"].clip(lower=min_initial)

    return df


df_pharma = ensure_stock_initial(df_pharma)

# =========================
# AGRÉGATION (somme sur âges) -> (date, region)
# =========================
if ("doses_per_100k_forecast" not in df_forecast.columns) or df_forecast["doses_per_100k_forecast"].isna().all():
    # fallback: si besoin de reconstruire depuis mean_hist * (forecast_vs_hist_%/100)
    if {"mean_hist","forecast_vs_hist_%"} <= set(df_forecast.columns):
        df_forecast["doses_per_100k_forecast"] = df_forecast["mean_hist"] * (df_forecast["forecast_vs_hist_%"] / 100.0)
    else:
        raise ValueError("La colonne 'doses_per_100k_forecast' manque et ne peut pas être reconstruite.")

df_region_month = (
    df_forecast.groupby(["date","region"], as_index=False)["doses_per_100k_forecast"]
               .sum()
               .rename(columns={"doses_per_100k_forecast":"sum_doses_per_100k_forecast"})
)

# Merge population régionale (depuis communes)
df_region_month = df_region_month.merge(pop_region, on="region", how="left")

missing = df_region_month["population_region"].isna()
if missing.any():
    print("[WARN] Régions sans population (mismatch codes ?):",
          df_region_month.loc[missing,"region"].unique().tolist())

# Stock total prévu (sans distinction d'âge)
df_region_month["stock_prev_total"] = (
    df_region_month["sum_doses_per_100k_forecast"] * df_region_month["population_region"] / 100000.0
)

# =========================
# Répartition pro-rata population (par pharmacie, sans distinction d'âge)
# =========================
def repartition_par_pharmacie(df_pharma, df_region_month):
    out = []
    pharma = df_pharma.copy()
    pharma["region_code3"] = pharma["region_code3"].astype(str).str.upper().str.strip()
    pharma["population"]   = pd.to_numeric(pharma["population"], errors="coerce").fillna(0.0)

    for (dt, reg), g in df_region_month.groupby(["date","region"]):
        total = float(g["stock_prev_total"].iloc[0]) if pd.notna(g["stock_prev_total"].iloc[0]) else 0.0
        sub = pharma[pharma["region_code3"] == reg].copy()
        if sub.empty:
            continue

        tot_pop = float(sub["population"].sum())
        if tot_pop <= 0 or total <= 0:
            sub["date"] = dt; sub["region"] = reg; sub["consommation_prevue"] = 0
            out.append(sub[["date","region","pharmacie","region_code3","population","consommation_prevue"]])
            continue

        # parts proportionnelles + méthode des plus grands restes pour coller à l'arrondi global
        parts = sub["population"] / tot_pop * total
        base  = np.floor(parts).astype(int)
        reste = parts - base
        manque = int(round(total)) - int(base.sum())
        if manque > 0:
            idx = np.argsort(reste.values)[::-1][:manque]
            base.iloc[idx] += 1

        sub["date"] = dt
        sub["region"] = reg
        sub["consommation_prevue"] = base.values.astype(int)
        out.append(sub[["date","region","pharmacie","region_code3","population","consommation_prevue"]])

    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(
        columns=["date","region","pharmacie","region_code3","population","consommation_prevue"]
    )

df_forecast_pharma = repartition_par_pharmacie(df_pharma, df_region_month)

# =========================
# Simulation stock mensuelle (snapshot ouverture + déroulé)
# =========================
def simulate_stock(df_forecast_pharma: pd.DataFrame, df_pharma: pd.DataFrame) -> pd.DataFrame:
    df_forecast_pharma = df_forecast_pharma.sort_values(["region_code3", "pharmacie", "date"]).reset_index(drop=True)
    init_lookup = (
        df_pharma.assign(stock_initial_oct=pd.to_numeric(df_pharma["stock_initial_oct"], errors="coerce").fillna(0).astype(int))
                 .set_index(["region_code3","pharmacie"])["stock_initial_oct"].to_dict()
    )

    rows = []
    current_stock = {}  # (region, pharmacie) -> stock courant

    for (reg, ph), g in df_forecast_pharma.groupby(["region_code3","pharmacie"], sort=False):
        stock_actuel = current_stock.get((reg, ph), init_lookup.get((reg, ph), 0))
        for _, r in g.iterrows():
            conso = int(r["consommation_prevue"]) if pd.notna(r["consommation_prevue"]) else 0
            rows.append({
                "pharmacie": ph,
                "region": reg,
                "date": r["date"],
                "stock_initial": stock_actuel,
                "consommation_prevue": conso,
                "stock_final": max(stock_actuel - conso, 0),
            })
            stock_actuel = max(stock_actuel - conso, 0)
        current_stock[(reg, ph)] = stock_actuel

    return pd.DataFrame(rows)

# Snapshot d'ouverture (mois précédent le 1er mois de prévision)
first_month = df_region_month["date"].min()
opening_month = (first_month.to_period("M") - 1).to_timestamp()

snapshot_open = df_pharma[["pharmacie","region_code3","stock_initial_oct"]].copy()
snapshot_open["region"] = snapshot_open["region_code3"]
snapshot_open["date"] = opening_month
snapshot_open["stock_initial"] = snapshot_open["stock_initial_oct"]
snapshot_open["consommation_prevue"] = 0
snapshot_open["stock_final"] = snapshot_open["stock_initial_oct"]
snapshot_open = snapshot_open.drop(columns=["stock_initial_oct","region_code3"])

# Simulation
df_stock = simulate_stock(df_forecast_pharma, df_pharma)

# Concat : Octobre (snapshot) + Nov, Dec, ...
df_stock = pd.concat([snapshot_open, df_stock], ignore_index=True).sort_values(
    ["date","region","pharmacie"]
).reset_index(drop=True)

# =========================
# Exports
# =========================
df_stock.to_csv("pharma_2mois_prev.csv", index=False)
df_forecast_pharma.to_csv("pharma_conso_prevue_mensuelle.csv", index=False)

print("\n[OK] Exportés :")
print(" - pharma_2mois_prev.csv  (snapshot + simulation mensuelle par pharmacie)")
print(" - pharma_conso_prevue_mensuelle.csv  (consommation prévue allouée aux pharmacies)")
print("\nAperçu simulation :")
print(df_stock.head(12))
