# Vaccination Demand & Hospital Load – Predictive Pipeline (FR)

Ce projet fournit un **squelette complet** (ingestion ➜ features ➜ modèles ➜ HTS ➜ optimisation stock)
pour **optimiser la couverture vaccinale** et **réduire la charge hospitalière**.

## 🧱 Contenu
- `src/data_ingestion.py` : import & nettoyage (INSEE, Sentinelles, OSCOUR, Météo, Vaccination).
- `src/feature_engineering.py` : assemblage hebdo, lags, moyennes mobiles, calendrier, normalisation per 100k.
- `src/models/baselines.py` : Prophet baseline par (région × âge).
- `src/models/gbdt_demand.py` : LightGBM avec validation rolling-origin.
- `src/hts.py` : réconciliation hiérarchique Top-Down (proportions historiques).
- `src/opt/optimize_inventory.py` : Newsvendor & PL de réassort avec `pulp`.
- `src/train_pipeline.py` : run MLflow + export métriques & forecast réconcilié.
- `src/opt/plan_reassort.py` : plan de réassort hebdo à partir des prévisions.
- `run_quickstart.py` : **démo synthétique** end-to-end.

## 🚀 Démarrage (démo synthétique)
```bash
pip install -r requirements.txt
python run_quickstart.py
```

Les fichiers de sortie sont dans `data/processed/` :
- `features.parquet`
- `metrics_by_series.csv`
- `forecast_reconciled.parquet`
- `reassort_plan.csv`

## 🔌 Brancher vos vraies données
Éditez `conf/data_sources.yaml` et pointez vers vos CSV/API réels (SPF/Sentinelles/OSCOUR/INSEE/Météo-France/IQVIA).
Le schéma attendu :
- **INSEE**: `region, age_band, population`
- **Sentinelles**: `date, region, incidence_per_100k`
- **OSCOUR**: `date, region, age_band, er_visits, admissions`
- **Météo**: `date, region, tmean`
- **Vaccination**: `date, region, age_band, doses`
- **Régions**: `region, region_name`

> Astuce : gardez des **codes courts** (`IDF`, `ARA`, …) harmonisés partout.

## 🧪 Validation & métriques
- `SMAPE` et `MAE` par (région × âge).
- Validation **rolling-origin** (simule les prévisions semaine après semaine).

## 🧩 HTS (Hiérarchique)
Démonstration **Top-Down**. Pour MinT, utilisez des lib spécialisées ou implémentez la covariance des erreurs.

## 🏭 Optimisation stock
- Newsvendor (fractile) pour dimensionner les seuils.
- **PL** via `pulp` pour allouer la capacité totale par région en contrôlant le risque de rupture.

## 📝 Notes
- Prophet: baseline robuste sur saisonnalités.
- LightGBM: capture non-linéarités (incidence/météo/retards).
- Les intervalles prédictifs et les scénarios météo/incidence sont à intégrer pour la prod.

## 🔒 Conformité
Travailler sur **agrégats hebdo** (région × âge) ; pas d’IPD. Journalisez sources et fraicheur des données.

---

_Fait pour servir de point de départ rapide ; ajoutez vos dashboards (Metabase/Superset/PowerBI) et votre infra (dbt/Airflow/MLflow Registry)._

## 🔗 Sources Open Data (branchées)

- **Sentinelles – incidence hebdo** (Opendatasoft): `https://public.opendatasoft.com/api/records/1.0/download/?dataset=healthref-france-sentinelles-weekly&format=csv`
- **SurSaUD / OSCOUR – urgences grippe** (ODiSSe): `https://odisse.santepubliquefrance.fr/api/records/1.0/download/?dataset=grippe-passages-aux-urgences-et-actes-sos-medecins-france&format=csv`
- **INSEE – POP1A 2022** (CSV direct): `https://www.insee.fr/fr/statistiques/fichier/8581810/pop1a-2022.csv`
- **ODRÉ – Températures quotidiennes régionales**: `https://odre.opendatasoft.com/api/records/1.0/download/?dataset=temperature-quotidienne-regionale&format=csv`

### ▶️ Importer les vraies données
```bash
# 1) Installer deps si pas fait
pip install -r requirements.txt  requests

# 2) Télécharger et normaliser
python -c "from src.download_open_data import run_all; print(run_all())"

# 3) (Re)construire les features et entraîner
python -c "from src.train_pipeline import run_pipeline; print(run_pipeline())"
```

## 📊 Superset & Metabase (exemples)
### Superset
```bash
cd dashboards/superset
docker compose up -d
# Ouvre http://localhost:8088 (admin/admin), ajoute une DB (DuckDB/SQLite/Postgres),
# puis 'Datasets' -> ajoutez data/processed/features.parquet (via un connecteur ou après import en SQL).
```

### Metabase
```bash
cd dashboards
docker compose -f metabase-docker-compose.yaml up -d
# Ouvre http://localhost:3000, créez l'admin, connectez votre base (DuckDB/Postgres),
# et créez des cartes à partir de 'features.parquet' (si exposé via un connecteur) ou tables importées.
```
