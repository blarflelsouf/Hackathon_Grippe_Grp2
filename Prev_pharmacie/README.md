# 💉 Prévision & Simulation des stocks vaccinaux – Pharmacies (FR)

Ce repo calcule la **consommation prévisionnelle mensuelle** de vaccins par région, la **répartit** sur les **pharmacies** (pro-rata population communale), puis **simule** l’évolution **mois par mois** du stock (snapshot d’ouverture en octobre ➜ novembre, décembre, …).

---

## 🚀 Quickstart

1) **Python 3.10+ recommandé**
2) Installer les dépendances :
   ```bash
   pip install -r requirements.txt
3) Vérifier / déposer les fichiers d’entrée dans data/raw/ :
- reassort_plan_from_latest.csv → prévisions (par région & tranche d’âge)
- communes-france-2025.csv → population des communes (avec le code région INSEE)
- pharma_clean.csv → pharmacies, région (code 3 lettres), population communale, et stock initial/potentiel
4) Lancer :
python run_quickstart.py

Les résultats seront écrits dans data/processed/ :

pharma_conso_prevue_mensuelle.csv → consommation prévue par pharmacie et par mois

pharma_2mois_prev.csv → simulation de stock par pharmacie (snapshot d’octobre + mois suivants)


📦 Arborescence

Prev_pharmacie/
├─ data/
│  ├─ processed/
│  │  ├─ pharma_2mois_prev.csv
│  │  └─ pharma_conso_prevue_mensuelle.csv
│  └─ raw/
│     ├─ reassort_plan_from_latest.csv
│     └─ communes-france-2025.csv
├─ fusion_previs.py   # logique principale
├─ trans.py           # fonctions utilitaires (nettoyage, mapping, etc.)
├─ requirements.txt
└─ README.md


🧠 Logique de calcul (rappel)

Agrégation régionale (sans distinction d’âge)
On somme doses_per_100k_forecast par (date, région) 	​

doses_per_100k_forecast

Conversion en stock total régional
La population régionale provient de communes-france-2025.csv (somme des communes, via code INSEE mappé en code à 3 lettres)

Arrondi via méthode des plus grands restes (on conserve la somme globale).

Simulation mensuelle du stock
Snapshot d’ouverture en octobre

🗂️ Détails des entrées
reassort_plan_from_latest.csv

date (YYYY-MM-01), region (code 3 lettres : ARA, IDF, …), age_band

doses_per_100k_forecast (prévision), mean_hist, forecast_vs_hist_%

Si doses_per_100k_forecast est absent, on peut la reconstruire : mean_hist * forecast_vs_hist_% / 100.

communes-france-2025.csv

Colonnes attendues : reg_code (INSEE région : 11, 24, 84, …), population

Agrégation par reg_code, puis mapping INSEE → code 3 lettres (84 → ARA, 11 → IDF, …)

pharma_clean.csv

pharmacie, region_code3 (ARA, IDF, …), population (commune)

Stock initial octobre :

Priorité à stock_initial_oct si présent

Sinon stock_potentiel_vaccins

Sinon valeur par défaut = 100 (et on clip à min 100 si besoin)

📤 Sorties

data/processed/pharma_conso_prevue_mensuelle.csv
date, region, pharmacie, region_code3, population, consommation_prevue

data/processed/pharma_2mois_prev.csv
pharmacie, region, date, stock_initial, consommation_prevue, stock_final
(contient octobre = snapshot d’ouverture, puis novembre/décembre…)

🔧 Paramètres usuels

Stock initial minimal (par défaut 100) : dans ensure_stock_initial(...)

Chemins d’E/S : constants au début de run_quickstart.py

Méthode de répartition : pro-rata population communale (modifiable si tu veux pondérer par +65, historique, etc.)
