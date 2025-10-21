"""
Configuration centrale du projet.
Toutes les routes et constantes sont définies ici.
"""
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]  # dossier racine (un cran au-dessus de src)

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"
MODELS_DIR = BASE_DIR / "models"
REPORTS_DIR = BASE_DIR / "reports"
CONF_DIR = BASE_DIR / "conf"

# Paramètres généraux
SEED = 42
REGIONS = ["IDF","CVL","BFC","NAQ","OCC","PAC","ARA","HDF","NOR","BRE","PDL","COR","GES"]
AGE_BANDS = ["0-17","18-64","65+"]  # exemples
FREQ = "W-MON"  # hebdo (semaine finissant le lundi)

# Pour MLflow (modifiable)
MLFLOW_TRACKING_URI = (BASE_DIR / "mlruns").as_posix()
EXPERIMENT_NAME = "vax_demand_forecast"
