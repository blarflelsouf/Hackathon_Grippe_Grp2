"""
Optimisation de stock: Newsvendor & PL robuste (via pulp).
- On illustre une politique hebdo par région (toutes tranches confondues) à partir des prévisions.
"""
import pandas as pd
import numpy as np
import pulp

def newsvendor(q_hat, sigma, understock_cost, overstock_cost):
    """
    Renvoie le quantile optimal (fractile) et la quantité recommandée.
    q_hat: prévision moyenne
    sigma: écart-type de la demande (approx)
    """
    crit = understock_cost / (understock_cost + overstock_cost)
    from scipy.stats import norm
    z = norm.ppf(crit)
    return max(0.0, q_hat + z * sigma)

def lp_replenishment(regions, demand_mean, demand_p90, capacity, cost_transport=1.0):
    """
    Petite PL: minimiser le coût transport + pénalités de rupture en respectant une capacité globale.
    - regions: liste des régions
    - demand_mean: dict region->moyenne prévue
    - demand_p90: dict region->p90 (sécurité)
    - capacity: capacité totale (doses) disponible
    """
    prob = pulp.LpProblem("replenishment", pulp.LpMinimize)
    x = {r: pulp.LpVariable(f"x_{r}", lowBound=0) for r in regions}

    # Objectif: coût transport + pénalité keynesienne si on livre < p90
    penalty = {r: 5.0 for r in regions}  # à ajuster
    prob += pulp.lpSum([cost_transport * x[r] + penalty[r] * pulp.lpSum([max(0, demand_p90[r])]) for r in regions])

    # Capacité totale
    prob += pulp.lpSum([x[r] for r in regions]) <= capacity

    # Bornes sup facultatives (ex: pas plus que p90)
    for r in regions:
        prob += x[r] <= demand_p90[r]

    prob.solve(pulp.PULP_CBC_CMD(msg=False))

    return {r: x[r].value() for r in regions}
