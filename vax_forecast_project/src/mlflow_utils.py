import mlflow
from .config import MLFLOW_TRACKING_URI, EXPERIMENT_NAME

def setup_mlflow():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    return mlflow
