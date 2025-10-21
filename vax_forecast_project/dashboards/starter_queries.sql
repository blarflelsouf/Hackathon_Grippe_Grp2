-- Créez une vue hebdo pour Superset/Metabase à partir des features
-- Exemple DuckDB (remplacez par votre SGBD)
-- duckdb -c "CREATE VIEW v_demand_weekly AS SELECT date, region, age_band, doses_per_100k, incidence_per_100k, tmean FROM 'data/processed/features.parquet';"

-- Exemple de KPIs
-- 1) Demande vs Prévision (ajoutez vos colonnes yhat si chargées)
-- SELECT date, region, age_band, doses_per_100k AS actual, yhat AS forecast FROM v_demand_weekly;

-- 2) Incidence ➜ Demande (corrélation glissante)
-- SELECT region, age_band, corr(doses_per_100k, incidence_per_100k) AS rho FROM v_demand_weekly GROUP BY region, age_band;

-- 3) Température moyenne par région (hebdo)
-- SELECT date, region, avg(tmean) AS tmean FROM v_demand_weekly GROUP BY date, region;
