"""
Modèle GBDT (LightGBM) pour la demande vaccinale hebdomadaire (par 100k).
- Entraînement sur features tabulaires
- Validation rolling-origin
- Anti-fuite: on n'utilise JAMAIS de features contemporaines (seulement *_lag* / *_ma*)
"""
import pandas as pd
import numpy as np
from lightgbm import LGBMRegressor
from sklearn.metrics import mean_absolute_error
from ..utils import smape


FEATURES_CALENDAR = ["weekofyear", "month", "year"]

def _past_only_feature_list(df_cols, fallback=None):
    """Construit la liste des features sans fuite: uniquement *_lag* et *_ma* + calendaires."""
    lags = [c for c in df_cols if c.endswith(tuple([f"_lag{i}" for i in range(1, 13)]))]
    mas  = [c for c in df_cols if c.endswith(tuple([f"_ma{i}"  for i in (2,4,8,12)]))]
    feats = sorted(set(lags + mas + [c for c in FEATURES_CALENDAR if c in df_cols]))
    if not feats and fallback:
        feats = fallback
    return feats

def rolling_cv_fit_predict(
    df,
    group_cols=("region","age_band"),
    target="doses_per_100k",
    features=None,
    min_train_months=3,
    horizon_weeks=2
):
    """
    Validation rolling-origin par série, avec prévisions horizon fixes.
    Retourne : oof (prévisions historiques), future_fc (horizon futur si possible), modèles par clé, métriques.
    """
    # ——— Sélection robuste des features (anti-fuite) ———
    # 1) Si la table porte la liste des features (attrs) on la prend, sinon on construit past-only.
    features_from_attrs = getattr(df, "attrs", {}).get("FEATURE_COLS")
    if features is None:
        features = features_from_attrs or _past_only_feature_list(df.columns)

    # 2) Retire toute contemporaine par précaution
    ban_now = {"doses_per_100k","incidence_per_100k","tmean","er_visits","admissions"}
    features = [f for f in features if f not in ban_now]

    # 3) S'assure qu'on a au moins quelques features
    if not features:
        features = _past_only_feature_list(df.columns)

    oof_all, models, future_all = [], {}, []

    for keys, part in df.groupby(list(group_cols)):
        part = part.sort_values("date").reset_index(drop=True).copy()

        # Remplace NaN résiduels dans les features par la médiane de la série
        for col in features:
            part[col] = part.groupby(["region","age_band"])[col].transform(
                lambda s: s.fillna(s.median())
            )
        # Drop si target manquante uniquement
        part = part.dropna(subset=[target])
        # Variance minimale sur la cible
        if part[target].fillna(0).std() < 1e-6:
            continue


        if len(part) < (min_train_months + horizon_weeks):
            continue

        preds = []
        for split in range(min_train_months, len(part) - horizon_weeks + 1):
            train = part.iloc[:split]
            test  = part.iloc[split: split + horizon_weeks]

            Xtr, ytr = train[features], train[target]
            Xte, yte = test[features], test[target]

            model = LGBMRegressor(
                random_state=123,
                n_estimators=500,
                learning_rate=0.05,
                max_depth=-1,
                num_leaves=31,
                subsample=0.9,
                colsample_bytree=0.9
            )
            model.fit(Xtr, ytr)
            phat = model.predict(Xte)

            preds.append(pd.DataFrame({
                "date": test["date"].values,
                target: yte.values,
                "yhat": phat,
            }))

        if not preds:
            continue

        oof = pd.concat(preds, ignore_index=True)
        oof[group_cols[0]] = keys[0]
        oof[group_cols[1]] = keys[1]
        oof_all.append(oof)

        # Entraînement final
        hist = part[part[target].notna()].copy()

        model_final = LGBMRegressor(
            random_state=123,
            n_estimators=700,
            learning_rate=0.05,
            max_depth=-1,
            num_leaves=31,
            subsample=0.9,
            colsample_bytree=0.9
        )
        model_final.fit(hist[features], hist[target])
        models[keys] = model_final

        fut = part[part[target].isna()].copy()
        if not fut.empty:
            # Remplacer NaN résiduels des features par médiane série avant predict
            for col in features:
                fut[col] = fut[col].fillna(hist[col].median() if col in hist else fut[col].median())

            fut["yhat"] = model_final.predict(fut[features])
            fut[group_cols[0]] = keys[0]
            fut[group_cols[1]] = keys[1]
            future_all.append(fut[["date", group_cols[0], group_cols[1], "yhat"]])

    oof_all = pd.concat(oof_all, ignore_index=True) if oof_all else pd.DataFrame()
    future_all = pd.concat(future_all, ignore_index=True) if future_all else pd.DataFrame()

    # Métriques
    if not oof_all.empty:
        m = (oof_all
             .groupby(list(group_cols))
             .apply(lambda g: pd.Series({
                 "SMAPE": smape(g[target], g["yhat"]),
                 "MAE": mean_absolute_error(g[target], g["yhat"])
             }))
             .reset_index())
    else:
        m = pd.DataFrame(columns=list(group_cols)+["SMAPE","MAE"])

    return oof_all, future_all, models, m
