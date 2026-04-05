# Pitcher strikeout “algorithm” — plain English

This doc explains how the MLB strikeout prop helper turns a few simple stats into a **projected strikeout count** and a **OVER / UNDER / NO BET** suggestion. No math background required.

---

## What are we even predicting?

Sportsbooks set a **line**: “Will this pitcher get more or fewer than *X* strikeouts?”

The app does **not** simulate every at-bat. It uses two percentages and a rough guess for how many hitters the pitcher will face, then compares the result to your line.

---

## The two ideas you need: K%

**Strikeout rate (K%)** means: *out of every 100 plate appearances, how many ended in a strikeout?*

- A pitcher with **30% K** strikes out about 30 batters per 100 batters faced.
- A **team** K% (in this project) is the **offense’s** rate: how often **that lineup** strikes out against pitchers in general.

So you have:

1. **Pitcher K%** — how good *this* pitcher is at punching guys out.
2. **Opponent team K%** — how whiff-prone *that* lineup tends to be.

The model blends both: neither the pitcher nor the lineup acts in a vacuum.

---

## The projection formula (one line)

The code averages the two K% numbers, then scales to a **fixed number of batters faced**:

\[
\text{projected K} = \frac{\text{pitcher K\%} + \text{opponent team K\%}}{2} \times \frac{\text{batters faced}}{100}
\]

In the app, **batters faced is always 21** (a typical-ish number for a starter who goes ~5–6 innings; you can think of it as “about three times through the order, minus a little”).

### Tiny worked example

- Pitcher K% = **28**
- Opponent team K% = **22**
- Average = **(28 + 22) / 2 = 25**
- That means **25 strikeouts per 100 batters faced** on this blended view.
- For **21** batters: **0.25 × 21 = 5.25** projected strikeouts.

So if the book’s line were **5.5**, you’d be *slightly* under that raw projection — but the app doesn’t stop there; it uses a **buffer** before it tells you to bet (see below).

---

## Where the numbers come from (in this repo)

| Input | What it is | In the project |
|--------|------------|----------------|
| Pitcher K% | You type it (or it prefills) | Streamlit sidebar; a few known pitchers prefilled from `data/pitcher_k_stats.py` |
| Opponent | Who they’re facing | Dropdown; often prefilled from schedule API when possible |
| Team K% | Lineup strikeout tendency | `data/team_k_2025.py` — one number per MLB team for 2025 |

If a team has no K% row, the app won’t compute a verdict until you add it.

---

## OVER, UNDER, or NO BET?

The app compares **projection** to the **betting line** with a **0.75 strikeout cushion** so it doesn’t scream “bet!” when the number is basically tied.

- **OVER** — projection is **more than** `line + 0.75`
- **UNDER** — projection is **less than** `line - 0.75`
- **NO BET** — anything in the middle (too close to call)

Example: line **5.5**

- Need projection **> 6.25** for OVER
- Need projection **< 4.75** for UNDER
- Between **4.75** and **6.25** → **NO BET**

Those thresholds live in `logic/projection.py` as `EDGE_THRESHOLD` (0.75) and `BF_PROJECTED` (21).

---

## What this model is *not*

- **Not** a full game simulator (no platoon splits, park factors, bullpen length, weather, umpire, etc.).
- **Not** personalized to tonight’s exact batting order unless you manually adjust inputs.
- **Not** a guarantee — it’s a quick blend of two rates and a fixed workload guess.

Use it as a **structured sanity check** next to the line, not as financial advice.

---

## Code map (if you want to read the source)

- **Math + verdict:** `logic/projection.py` — `compute_projection`, `verdict`
- **UI wiring:** `mlb_prop_app.py`
- **Team K% table:** `data/team_k_2025.py`
- **Optional pitcher prefills:** `data/pitcher_k_stats.py`
