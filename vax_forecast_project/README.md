
# Vaccination Demand & Hospital Load – Predictive Pipeline (FR)

Ce repo propose un **pipeline complet et prêt à brancher** (ingestion ➜ features ➜ modèles ➜ réconciliation hiérarchique ➜ plan de réassort) pour **optimiser la couverture vaccinale** et **réduire la charge hospitalière** par région et tranche d’âge.

---

## 🎯 Objectifs

- **Prédire la demande vaccinale** (par mois, région, tranche d’âge), à partir de **données ouvertes** : INSEE, Sentinelles, OSCOUR/SurSaUD, Météo.
- **Réconcilier** les prévisions (HTS) pour garantir la cohérence national ↔ régions ↔ âges.
- **Générer un plan de réassort** (quantités, marge de sécurité, comparatif à l’historique) pour les opérations.

---

## 🏗️ Architecture & principaux fichiers

```
vax_forecast_project/
├── conf/
│   └── data_sources.yaml          # URLs/fichiers sources (modifiable)
├── data/
│   ├── raw/                       # dumps bruts (téléchargés)
│   └── processed/                 # sorties pipeline (parquet/csv)
├── src/
│   ├── config.py                  # chemins, constantes globales (AGE_BANDS, etc.)
│   ├── utils.py                   # helpers (SMAPE, safe_merge, etc.)
│   ├── data_ingestion.py          # import & normalisation des sources
│   ├── feature_engineering.py     # assemblage MENSUEL + lags/MA + calendaires
│   ├── models/
│   │   ├── baselines.py           # baseline simple (ex : moyenne mobile)
│   │   ├── gbdt_demand.py         # LightGBM (GBDT) rolling-origin
│   │   └── ensemble.py            # ensemblage LGBM + baseline
│   ├── hts.py                     # top-down proportions (démo)
│   ├── opt/
│   │   └── optimize_inventory.py  # Newsvendor / PL (optionnel)
│   ├── mlflow_utils.py            # trace simple d’un run
│   ├── download_open_data.py      # télécharge + normalise open data
│   └── train_pipeline.py          # pipeline: features ➜ modèles ➜ calibration ➜ exports
├── dashboards/
│   ├── superset/                  # docker compose (exemple)
│   └── metabase-docker-compose.yaml
└── README.md
```

### Flux de traitement (vue d’ensemble)

1) **Ingestion** `src/data_ingestion.py`
2) **Features (mensuelles)** `src/feature_engineering.py`
3) **Modélisation** `src/models/ensemble.py`
4) **HTS** `src/hts.py`
5) **Calibration d’échelle** `train_pipeline.py`
6) **Plan de réassort** : CSV prêt à charger dans Superset/Metabase/ERP.

---

## 🔎 Données (explications & schémas attendus)

- **INSEE (population)** : `region, age_band, population`
- **Sentinelles (incidence ILI/grippe)** : `date, region, incidence_per_100k`
- **SurSaUD / OSCOUR** : `date, region, age_band, er_visits, admissions`
- **Météo** : `date, region, tmean`
- **Vaccination** : `date, region, age_band, doses`

Les **exogènes** (incidence, météo, urgences) sont extrapolés sur l’horizon via **climatologie région×mois**, garantissant un jeu complet jusqu’à la période future demandée.

---

## ⚙️ Modèles & choix

- **LightGBM (GBDT)** : non-linéarités & interactions.
- **Baseline** : moyenne mobile / drift.
- **Ensemble (LGBM + baseline)** : pondération ajustable.
- **Rolling-origin validation** : simulation réaliste.
- **HTS top-down** : cohérence entre niveaux.

---

## 📦 Sorties

- `features.parquet`
- `metrics_by_series.csv`
- `forecast_reconciled_calibrated.parquet`
- `reassort_plan_from_latest.csv`

---

## 🚀 Démarrage rapide

```bash
pip install -r requirements.txt
FORECAST_HORIZON_MONTHS=12 python -m src.train_pipeline
```

---

## 🔗 Sources Open Data

- Sentinelles – incidence hebdomadaire  
- SurSaUD / OSCOUR – urgences grippe  
- INSEE – POP1A 2022  
- ODRÉ – Température quotidienne régionale  

---

_Fait pour servir de starter industrialisable._
