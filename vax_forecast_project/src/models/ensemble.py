import pandas as pd
import inspect
from .gbdt_demand import rolling_cv_fit_predict
from .baselines import seasonal_naive_future
from sklearn.metrics import mean_absolute_error
from ..utils import smape

def _infer_feature_cols(df: pd.DataFrame):
    past_feats = []
    for base in ["doses_per_100k","incidence_per_100k","tmean","er_visits","admissions"]:
        past_feats += [c for c in df.columns if c.startswith(base+"_lag") or c.startswith(base+"_ma")]
    past_feats += ["month","year"]  # calendaires
    return sorted(set([c for c in past_feats if c in df.columns]))

def _call_rolling_cv_compat(fn, *, df, group_cols, target, features,
                            min_train, horizon):
    """
    Appelle rolling_cv_fit_predict quel que soit le nom attendu par la signature.
    Essaie différents noms de paramètres (weeks / months / generic).
    """
    sig = inspect.signature(fn)
    params = set(sig.parameters.keys())
    kwargs = {
        "df": df,
        "group_cols": group_cols,
        "target": target,
        "features": features,
    }
    if "min_train_weeks" in params:
        kwargs["min_train_weeks"] = min_train
    elif "min_train_months" in params:
        kwargs["min_train_months"] = min_train
    elif "min_train" in params:
        kwargs["min_train"] = min_train
    if "horizon_weeks" in params:
        kwargs["horizon_weeks"] = horizon
    elif "horizon_months" in params:
        kwargs["horizon_months"] = horizon
    elif "horizon" in params:
        kwargs["horizon"] = horizon
    return fn(**kwargs)

def fit_predict_ensemble(features_df: pd.DataFrame,
                         feature_cols=None,
                         target="doses_per_100k",
                         group_cols=("region","age_band"),
                         min_train_months=8,
                         horizon_months=2,
                         w_lgbm=0.7, w_base=0.3):
    """
    Entraîne LGBM (past-only features) + baseline saisonnière (lag12),
    puis produit un ensemble pour le FUTUR (lignes où target est NaN).
    Retourne (oof_metrics, future_fc_ensemble)
    """
    feats = (feature_cols
         or features_df.attrs.get("FEATURE_COLS")
         or _infer_feature_cols(features_df))

    # 1) LGBM
    oof, future_lgbm, models, metrics_lgbm = _call_rolling_cv_compat(
        rolling_cv_fit_predict,
        df=features_df,
        group_cols=group_cols,
        target=target,
        features=feats,
        min_train=min_train_months,
        horizon=horizon_months
    )



    # 2) Baseline saisonnière sur les mêmes lignes FUTURES
    base = seasonal_naive_future(features_df, group_cols=group_cols, target=target, date_col="date")

    # 3) Ensemble sur l'intersection des futures
    if not future_lgbm.empty and not base.empty:
        fut = future_lgbm.merge(base, on=["date", *group_cols], how="inner")
        fut["yhat_ens"] = w_lgbm * fut["yhat"] + w_base * fut["yhat_baseline"]
    elif not future_lgbm.empty:
        fut = future_lgbm.copy()
        fut["yhat_ens"] = fut["yhat"]
    elif not base.empty:
        fut = base.copy()
        fut.rename(columns={"yhat_baseline":"yhat_ens"}, inplace=True)
    else:
        fut = pd.DataFrame(columns=["date", *group_cols, "yhat_ens"])

    # 4) Métriques OOF pour l'ensemble (où on peut)
    if not oof.empty:
        # baseline OOF (facultatif): approximée via lag12 sur y (pas parfait mais informatif)
        df = features_df.copy()
        df = df.sort_values(["region","age_band","date"])
        df["y_lag12"] = df.groupby(list(group_cols))["doses_per_100k"].shift(12)
        oof2 = oof.merge(df[["date", *group_cols, "y_lag12"]], on=["date", *group_cols], how="left")
        oof2["yhat_ens"] = w_lgbm*oof2["yhat"] + w_base*oof2["y_lag12"].fillna(oof2["yhat"])
        metrics_ens = (oof2.groupby(list(group_cols)).apply(
            lambda g: pd.Series({
                "SMAPE": smape(g["doses_per_100k"], g["yhat_ens"]),
                "MAE": mean_absolute_error(g["doses_per_100k"], g["yhat_ens"])
            })
        ).reset_index())
    else:
        metrics_ens = pd.DataFrame(columns=[*group_cols,"SMAPE","MAE"])

    return metrics_ens, fut[["date", *group_cols, "yhat_ens"]]
