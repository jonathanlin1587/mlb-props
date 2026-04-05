from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

BF_PROJECTED = 21

PropSide = Literal["over", "under"]

EDGE_MIN_STRICT_PCT = 5.0
EDGE_MIN_RELAXED_PCT = 2.0


@dataclass(frozen=True)
class ValueVerdict:
    """Value-based read vs American odds on the chosen side (Over or Under)."""

    side: PropSide
    our_prob_pct: float
    implied_prob_pct: float
    edge_pct: float
    min_edge_required_pct: float
    recommend: bool
    headline: str
    detail: str


def compute_projection(
    pitcher_k_pct: float,
    team_k_pct: float,
    batters_faced: float = BF_PROJECTED,
) -> float:
    return ((pitcher_k_pct + team_k_pct) / 2) * batters_faced / 100


def american_odds_to_implied_probability_pct(american_odds: float) -> float:
    """Convert American odds to implied win probability in percent (no vig adjustment)."""
    if american_odds == 0:
        raise ValueError("American odds cannot be zero")
    if american_odds < 0:
        a = abs(american_odds)
        return (a / (a + 100.0)) * 100.0
    return (100.0 / (american_odds + 100.0)) * 100.0


def calculate_kelly_bet(
    probability: float,
    decimal_odds: float,
    bankroll: float,
    fraction: float,
) -> float:
    """
    Calculate fractional Kelly stake amount in dollars.

    probability: win probability as a decimal in [0, 1]
    decimal_odds: decimal odds (e.g. 1.91 for -110)
    bankroll: total bankroll in dollars
    fraction: Kelly fraction multiplier in [0, 1]
    """
    if decimal_odds <= 1.0:
        return 0.0
    if bankroll <= 0.0 or fraction <= 0.0:
        return 0.0
    p = max(0.0, min(1.0, float(probability)))
    b = float(decimal_odds) - 1.0
    f = (b * p - (1.0 - p)) / b
    return f * float(bankroll) * float(fraction)


def _poisson_pmf(k: int, mu: float) -> float:
    if k < 0:
        return 0.0
    if mu <= 0.0:
        return 1.0 if k == 0 else 0.0
    return math.exp(k * math.log(mu) - mu - math.lgamma(k + 1))


def poisson_over_probability_pct(projected_k: float, line: float) -> float:
    """P(strikeouts strictly over the book line) under Poisson(projected_k)."""
    mu = max(float(projected_k), 1e-6)
    k_min = int(math.floor(float(line))) + 1
    cdf_below = 0.0
    for i in range(k_min):
        cdf_below += _poisson_pmf(i, mu)
    p_over = max(0.0, min(1.0, 1.0 - cdf_below))
    return p_over * 100.0


def poisson_under_probability_pct(projected_k: float, line: float) -> float:
    """P(strikeouts at or under the book line) — complement of P(Over) for this line rule."""
    return max(0.0, min(100.0, 100.0 - poisson_over_probability_pct(projected_k, line)))


def compute_value_verdict(
    projection: float,
    line: float,
    american_odds: float,
    *,
    side: PropSide,
    relax_criteria: bool,
) -> ValueVerdict:
    """Compare model P(side) to implied prob from American odds for that side."""
    implied = american_odds_to_implied_probability_pct(american_odds)
    if side == "over":
        our_p = poisson_over_probability_pct(projection, line)
        side_label = "Over"
    else:
        our_p = poisson_under_probability_pct(projection, line)
        side_label = "Under"
    edge = our_p - implied
    min_need = (
        EDGE_MIN_RELAXED_PCT if relax_criteria else EDGE_MIN_STRICT_PCT
    )
    recommend = edge >= min_need
    edge_s = f"{edge:+.1f}%"
    if recommend:
        headline = f"Edge: {edge_s} - RECOMMEND BET ({side_label.upper()})"
    elif edge >= 0:
        headline = f"Edge: {edge_s} - NO BET (need ≥{min_need:.0f}% edge)"
    else:
        headline = f"Edge: {edge_s} - NO BET"
    detail = (
        f"Model P({side_label}) **{our_p:.1f}%** vs implied **{implied:.1f}%** "
        f"(Poisson λ = {projection:.2f} K vs line **{line:.1f}**)."
    )
    return ValueVerdict(
        side=side,
        our_prob_pct=our_p,
        implied_prob_pct=implied,
        edge_pct=edge,
        min_edge_required_pct=min_need,
        recommend=recommend,
        headline=headline,
        detail=detail,
    )
