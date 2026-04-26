"""
layer1_descriptive_stats.py
===========================
CCIN — Layer 1: Descriptive Statistics
BC Heat Pump Deployment · 2018–2025

Computes all six Layer 1 outputs from raw CSV data:
  1. Total heat pump stock       (cumulative stock + penetration rate)
  2. Annual installation flow    (flow metric + YoY delta)
  3. Fuel switching breakdown    (composition by fuel displaced)
  4. Heat pump type mix          (composition by equipment type)
  5. Regional distribution       (penetration by BC region)
  6. Income tier distribution    (equity metric — LMI vs market)

Usage:
    python layer1_descriptive_stats.py

Outputs (written to outputs/layer1/):
    stock.json          — L1.1: cumulative stock, monthly
    flow.json           — L1.2: annual + monthly installation flow
    fuel_mix.json       — L1.3: fuel switching by year
    hp_type_mix.json    — L1.4: equipment type mix by year
    regional.json       — L1.5: regional distribution by year
    income_tier.json    — L1.6: income tier split by year
    layer1_summary.json — all outputs combined + key stats
"""

import csv
import json
import os
from collections import defaultdict
from pathlib import Path
from statistics import mean, median, stdev

# ── Paths ──────────────────────────────────────────────────────────────────────
# All paths are relative to THIS script's location so the project is portable.
# Expected folder layout (put the CSVs in the same folder as this script,
# or one level up in a "data" subfolder — see DATA_DIR below):
#
#   ccin_algorthim/
#   ├── layer1_descriptive_stats.py   ← this file
#   ├── hp_installations_monthly.csv
#   ├── hp_program_rebates.csv
#   ├── hp_installer_network.csv
#   └── outputs/
#       └── layer1/                   ← JSON outputs written here (auto-created)

SCRIPT_DIR = Path(__file__).resolve().parent

# Data files — same folder as the script
DATA_DIR = SCRIPT_DIR
OUT_DIR  = SCRIPT_DIR / "outputs" / "layer1"
OUT_DIR.mkdir(parents=True, exist_ok=True)

INSTALLS_FILE  = DATA_DIR / "hp_installations_monthly_v2.csv"
REBATES_FILE   = DATA_DIR / "hp_program_rebates_v2.csv"
INSTALLER_FILE = DATA_DIR / "hp_installer_network_v2.csv"

BC_HOUSEHOLDS = 1_850_000
TARGET_2030   = 785_000
YEARS         = list(range(2018, 2026))

GHG_PER_INSTALL = {
    "natural_gas":       2.1,
    "oil":               4.6,
    "propane":           3.2,
    "electric_baseboard": 0.4,
}
ENERGY_SAVINGS_PER_INSTALL = {
    "natural_gas":       980,
    "oil":               1420,
    "propane":           1180,
    "electric_baseboard": 310,
}


# ══════════════════════════════════════════════════════════════════════════════
# LOAD RAW DATA
# ══════════════════════════════════════════════════════════════════════════════

def load_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def cast_installs(rows: list[dict]) -> list[dict]:
    """Cast numeric fields in the installations CSV."""
    int_fields = [
        "year", "month", "total_installs", "cleanbc_rebate_installs",
        "utility_rebate_installs", "self_funded_installs",
        "market_rate_installs", "low_mid_income_installs",
        "est_annual_energy_savings_cad", "cumulative_stock_province",
        "bc_total_households",
    ]
    float_fields = ["est_ghg_avoided_tco2e_yr", "penetration_pct"]
    for r in rows:
        for f in int_fields:
            r[f] = int(r.get(f, 0) or 0)
        for f in float_fields:
            r[f] = float(r.get(f, 0) or 0)
    return rows


def cast_rebates(rows: list[dict]) -> list[dict]:
    int_fields = [
        "year", "month", "total_rebates_issued", "market_rate_rebates",
        "low_mid_income_rebates", "avg_rebate_market_cad",
        "avg_rebate_lmi_cad", "total_programme_spend_cad",
    ]
    for r in rows:
        for f in int_fields:
            r[f] = int(r.get(f, 0) or 0)
    return rows


def cast_installers(rows: list[dict]) -> list[dict]:
    int_fields = [
        "year", "quarter", "registered_contractors",
        "new_certified_this_quarter", "lapsed_this_quarter",
        "net_change", "training_programme_enrolments",
        "implied_capacity_ceiling_annual",
    ]
    float_fields = ["avg_installs_per_contractor_qtly"]
    for r in rows:
        for f in int_fields:
            r[f] = int(r.get(f, 0) or 0)
        for f in float_fields:
            r[f] = float(r.get(f, 0) or 0)
    return rows


# ══════════════════════════════════════════════════════════════════════════════
# OUTPUT 1 — TOTAL HEAT PUMP STOCK
# Cumulative households with a heat pump, monthly + annual snapshots
# ══════════════════════════════════════════════════════════════════════════════

def compute_stock(rows: list[dict]) -> dict:
    """
    L1.1 — Total heat pump stock.

    Monthly cumulative stock and penetration rate.
    Annual end-of-year snapshots.
    Derived: years to target at current stock growth rate.
    """
    # Aggregate monthly totals across all dimensions
    monthly = defaultdict(lambda: {"total": 0, "stock": 0, "pen": 0.0})
    for r in rows:
        k = r["period"]
        monthly[k]["total"] += r["total_installs"]

    # Pick stock/penetration from last row of each period (all rows same value)
    period_stock = {}
    for r in rows:
        period_stock[r["period"]] = (
            r["cumulative_stock_province"],
            r["penetration_pct"],
        )

    monthly_out = []
    for period in sorted(monthly.keys()):
        stock, pen = period_stock[period]
        yr = int(period[:4])
        mo = int(period[5:])
        monthly_out.append({
            "period":            period,
            "year":              yr,
            "month":             mo,
            "monthly_installs":  monthly[period]["total"],
            "cumulative_stock":  stock,
            "penetration_pct":   round(pen, 2),
            "gap_to_target":     TARGET_2030 - stock,
        })

    # Annual end-of-year snapshots (December of each year)
    annual_snap = []
    for yr in YEARS:
        dec = f"{yr}-12"
        if dec in period_stock:
            stock, pen = period_stock[dec]
            annual_snap.append({
                "year":           yr,
                "eoy_stock":      stock,
                "penetration_pct": round(pen, 2),
                "gap_to_target":  TARGET_2030 - stock,
                "pct_of_target":  round(stock / TARGET_2030 * 100, 1),
            })

    # Stock growth rate (2018→2025)
    first_stock = monthly_out[0]["cumulative_stock"]
    last_stock  = monthly_out[-1]["cumulative_stock"]
    n_years     = (YEARS[-1] - YEARS[0])
    cagr        = ((last_stock / first_stock) ** (1 / n_years) - 1) * 100

    # At current CAGR, years until target
    import math
    if cagr > 0:
        years_to_target = math.log(TARGET_2030 / last_stock) / math.log(1 + cagr / 100)
    else:
        years_to_target = None

    return {
        "output": "L1.1 — Total heat pump stock",
        "description": "Cumulative BC households with a heat pump. Monthly series plus annual end-of-year snapshots.",
        "bc_total_households": BC_HOUSEHOLDS,
        "target_2030":         TARGET_2030,
        "current_stock_dec_2025": last_stock,
        "current_penetration_pct": round(last_stock / BC_HOUSEHOLDS * 100, 2),
        "gap_to_target":       TARGET_2030 - last_stock,
        "stock_cagr_2018_2025_pct": round(cagr, 2),
        "years_to_target_at_current_cagr": round(years_to_target, 1) if years_to_target else None,
        "monthly": monthly_out,
        "annual_snapshots": annual_snap,
    }


# ══════════════════════════════════════════════════════════════════════════════
# OUTPUT 2 — ANNUAL INSTALLATION FLOW
# Net-new installations per period — the key rate metric
# ══════════════════════════════════════════════════════════════════════════════

def compute_flow(rows: list[dict]) -> dict:
    """
    L1.2 — Annual installation flow.

    Annual totals broken into rebate channels.
    YoY growth rate.
    Monthly flow with trailing-12-month rolling average.
    Descriptive stats: mean, median, peak month, trough month.
    """
    # Annual totals
    annual = defaultdict(lambda: {
        "total": 0, "cleanbc_rebate": 0,
        "utility_rebate": 0, "self_funded": 0,
        "ghg_avoided": 0.0, "energy_savings": 0,
    })
    for r in rows:
        yr = r["year"]
        annual[yr]["total"]          += r["total_installs"]
        annual[yr]["cleanbc_rebate"] += r["cleanbc_rebate_installs"]
        annual[yr]["utility_rebate"] += r["utility_rebate_installs"]
        annual[yr]["self_funded"]    += r["self_funded_installs"]
        annual[yr]["ghg_avoided"]    += r["est_ghg_avoided_tco2e_yr"]
        annual[yr]["energy_savings"] += r["est_annual_energy_savings_cad"]

    annual_out = []
    prev_total = None
    for yr in YEARS:
        d = annual[yr]
        total = d["total"]
        yoy = round((total - prev_total) / prev_total * 100, 1) if prev_total else None
        cleanbc_share = round(d["cleanbc_rebate"] / total * 100, 1) if total else 0
        util_share    = round(d["utility_rebate"] / total * 100, 1) if total else 0
        self_share    = round(d["self_funded"] / total * 100, 1) if total else 0
        annual_out.append({
            "year":                    yr,
            "total_installs":          total,
            "cleanbc_rebate_installs": d["cleanbc_rebate"],
            "utility_rebate_installs": d["utility_rebate"],
            "self_funded_installs":    d["self_funded"],
            "cleanbc_share_pct":       cleanbc_share,
            "utility_share_pct":       util_share,
            "self_funded_share_pct":   self_share,
            "yoy_growth_pct":          yoy,
            "ghg_avoided_tco2e_yr":    round(d["ghg_avoided"], 0),
            "energy_savings_cad":      d["energy_savings"],
        })
        prev_total = total

    # Monthly flow with trailing 12-month average
    monthly_totals = defaultdict(int)
    for r in rows:
        monthly_totals[r["period"]] += r["total_installs"]

    periods = sorted(monthly_totals.keys())
    monthly_out = []
    for i, period in enumerate(periods):
        total = monthly_totals[period]
        # Trailing 12-month rolling average (where enough history exists)
        if i >= 11:
            trailing_12 = [monthly_totals[periods[j]] for j in range(i-11, i+1)]
            rolling_avg = round(mean(trailing_12), 0)
            ann_run_rate = round(sum(trailing_12), 0)  # annualised from trailing 12
        else:
            rolling_avg = None
            ann_run_rate = None

        monthly_out.append({
            "period":          period,
            "year":            int(period[:4]),
            "month":           int(period[5:]),
            "monthly_installs": total,
            "rolling_12mo_avg": rolling_avg,
            "ann_run_rate":     ann_run_rate,
        })

    # Descriptive stats across all months
    all_monthly = [monthly_totals[p] for p in periods]
    peak_period   = max(periods, key=lambda p: monthly_totals[p])
    trough_period = min(periods, key=lambda p: monthly_totals[p])

    return {
        "output": "L1.2 — Annual installation flow",
        "description": "Net-new heat pump installations per period. The key rate metric for detecting acceleration or deceleration.",
        "annual": annual_out,
        "monthly": monthly_out,
        "descriptive_stats": {
            "mean_monthly_installs":   round(mean(all_monthly), 0),
            "median_monthly_installs": round(median(all_monthly), 0),
            "stdev_monthly_installs":  round(stdev(all_monthly), 0),
            "peak_month":              peak_period,
            "peak_month_installs":     monthly_totals[peak_period],
            "trough_month":            trough_period,
            "trough_month_installs":   monthly_totals[trough_period],
            "total_installs_2018_2025": sum(all_monthly),
            "required_annual_rate":    round((TARGET_2030 - 278251) / (2030 - 2026), 0),
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# OUTPUT 3 — FUEL SWITCHING BREAKDOWN
# What fuel is being displaced — and what GHG impact does each switch deliver?
# ══════════════════════════════════════════════════════════════════════════════

def compute_fuel_mix(rows: list[dict]) -> dict:
    """
    L1.3 — Fuel switching breakdown.

    Annual installs by fuel displaced.
    Annual share (%) by fuel type.
    Trend: is the mix shifting over time?
    GHG avoided by fuel type — weighted by install volume.
    """
    FUELS = ["natural_gas", "oil", "propane", "electric_baseboard"]

    annual_fuel = defaultdict(lambda: defaultdict(int))
    for r in rows:
        annual_fuel[r["year"]][r["fuel_switched_from"]] += r["total_installs"]

    annual_out = []
    for yr in YEARS:
        d = annual_fuel[yr]
        total = sum(d.values())
        fuel_rows = {}
        total_ghg = 0.0
        for fuel in FUELS:
            count = d.get(fuel, 0)
            share = round(count / total * 100, 1) if total else 0
            ghg   = round(count * GHG_PER_INSTALL.get(fuel, 2.0), 0)
            total_ghg += ghg
            fuel_rows[fuel] = {
                "installs": count,
                "share_pct": share,
                "ghg_avoided_tco2e_yr": ghg,
                "avg_ghg_per_install": GHG_PER_INSTALL.get(fuel, 2.0),
            }
        annual_out.append({
            "year":  yr,
            "total": total,
            "fuels": fuel_rows,
            "total_ghg_avoided_tco2e_yr": round(total_ghg, 0),
            "weighted_avg_ghg_per_install": round(total_ghg / total, 2) if total else 0,
        })

    # Trend: share of each fuel over time (for trend analysis handoff)
    trend = {fuel: [] for fuel in FUELS}
    for yr_row in annual_out:
        for fuel in FUELS:
            trend[fuel].append({
                "year": yr_row["year"],
                "share_pct": yr_row["fuels"][fuel]["share_pct"],
                "installs":  yr_row["fuels"][fuel]["installs"],
            })

    # 2025 snapshot (most recent full year)
    latest = annual_out[-1]

    return {
        "output": "L1.3 — Fuel switching breakdown",
        "description": "Share of installations by fuel displaced. Each fuel switch has a different GHG impact. Tracks which fuel types are being displaced fastest.",
        "fuels_tracked": FUELS,
        "ghg_rates_tco2e_per_install": GHG_PER_INSTALL,
        "annual": annual_out,
        "trend_by_fuel": trend,
        "latest_year_snapshot": {
            "year": latest["year"],
            "dominant_fuel": max(latest["fuels"], key=lambda f: latest["fuels"][f]["installs"]),
            "fuels": {f: latest["fuels"][f]["share_pct"] for f in FUELS},
            "total_ghg_avoided_tco2e_yr": latest["total_ghg_avoided_tco2e_yr"],
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# OUTPUT 4 — HEAT PUMP TYPE MIX
# Equipment type composition — signals market maturity and grid implications
# ══════════════════════════════════════════════════════════════════════════════

def compute_type_mix(rows: list[dict]) -> dict:
    """
    L1.4 — Heat pump type mix.

    Annual installs by equipment type.
    Share trend over time.
    Notable signals: ground-source share (market maturity indicator).
    """
    TYPES = ["mini_split", "central_ducted", "ground_source", "water_heater"]

    annual_type = defaultdict(lambda: defaultdict(int))
    for r in rows:
        annual_type[r["year"]][r["hp_type"]] += r["total_installs"]

    annual_out = []
    for yr in YEARS:
        d = annual_type[yr]
        total = sum(d.values())
        type_rows = {}
        for t in TYPES:
            count = d.get(t, 0)
            share = round(count / total * 100, 1) if total else 0
            type_rows[t] = {"installs": count, "share_pct": share}
        annual_out.append({
            "year":              yr,
            "total":             total,
            "types":             type_rows,
            "dominant_type":     max(type_rows, key=lambda t: type_rows[t]["installs"]),
            "ground_source_share_pct": type_rows["ground_source"]["share_pct"],
        })

    # Ground source trend — market maturity signal
    gs_trend = [
        {"year": r["year"], "share_pct": r["ground_source_share_pct"],
         "installs": r["types"]["ground_source"]["installs"]}
        for r in annual_out
    ]
    gs_growth = round(
        gs_trend[-1]["share_pct"] - gs_trend[0]["share_pct"], 1
    )

    return {
        "output": "L1.4 — Heat pump type mix",
        "description": "Annual installs by equipment type. Ground-source share is a market maturity signal.",
        "types_tracked": TYPES,
        "annual": annual_out,
        "ground_source_trend": gs_trend,
        "ground_source_share_change_pp_2018_2025": gs_growth,
        "latest_year_snapshot": {
            "year": annual_out[-1]["year"],
            "dominant_type": annual_out[-1]["dominant_type"],
            "types": {t: annual_out[-1]["types"][t]["share_pct"] for t in TYPES},
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# OUTPUT 5 — REGIONAL DISTRIBUTION
# Geographic breakdown — surfaces laggards for program targeting
# ══════════════════════════════════════════════════════════════════════════════

# Approximate housing stock by region (derived from 2021 Census proportions)
REGIONAL_HOUSING_STOCK = {
    "Lower Mainland":    870_000,
    "Vancouver Island":  333_000,
    "Thompson-Okanagan": 241_000,
    "Fraser Valley":     185_000,
    "Northern BC":       111_000,
    "Kootenay-Boundary":  74_000,
    "Cariboo":            37_000,
}

def compute_regional(rows: list[dict]) -> dict:
    """
    L1.5 — Regional distribution.

    Annual installs by BC region.
    Regional share of provincial total.
    Regional penetration rate (vs regional housing stock).
    Gap from provincial average — identifies laggards.
    Cumulative regional stock (approximated from 2018 install share).
    """
    REGIONS = list(REGIONAL_HOUSING_STOCK.keys())

    # Accumulate regional installs by year
    annual_region = defaultdict(lambda: defaultdict(int))
    for r in rows:
        annual_region[r["year"]][r["region"]] += r["total_installs"]

    # Build cumulative regional stock
    # Approximate starting stock 2017 proportional to housing share
    STARTING_STOCK_PROVINCE = 121_000
    regional_stock = {
        reg: int(STARTING_STOCK_PROVINCE * REGIONAL_HOUSING_STOCK[reg] / BC_HOUSEHOLDS)
        for reg in REGIONS
    }

    annual_out = []
    for yr in YEARS:
        d = annual_region[yr]
        provincial_total = sum(d.get(reg, 0) for reg in REGIONS)
        provincial_pen = 0.0

        region_rows = {}
        for reg in REGIONS:
            installs = d.get(reg, 0)
            regional_stock[reg] += installs
            stock       = regional_stock[reg]
            hh_stock    = REGIONAL_HOUSING_STOCK[reg]
            share_pct   = round(installs / provincial_total * 100, 1) if provincial_total else 0
            pen_pct     = round(stock / hh_stock * 100, 2)
            region_rows[reg] = {
                "installs":         installs,
                "share_of_province_pct": share_pct,
                "cumulative_stock": stock,
                "regional_housing_stock": hh_stock,
                "penetration_pct":  pen_pct,
            }

        # Provincial penetration for this year's end state
        prov_stock = sum(region_rows[r]["cumulative_stock"] for r in REGIONS)
        prov_pen   = round(prov_stock / BC_HOUSEHOLDS * 100, 2)

        # Delta from provincial average per region
        for reg in REGIONS:
            delta = round(region_rows[reg]["penetration_pct"] - prov_pen, 2)
            region_rows[reg]["delta_from_provincial_avg_pp"] = delta

        # Rank regions by penetration (ascending — lowest first = most behind)
        ranked = sorted(REGIONS, key=lambda r: region_rows[r]["penetration_pct"])

        annual_out.append({
            "year":               yr,
            "provincial_total":   provincial_total,
            "provincial_pen_pct": prov_pen,
            "regions":            region_rows,
            "ranked_by_penetration_asc": ranked,  # first = most behind
            "laggard_region":     ranked[0],
            "leader_region":      ranked[-1],
        })

    # 2025 snapshot
    latest = annual_out[-1]

    return {
        "output": "L1.5 — Regional distribution",
        "description": "Heat pump penetration by BC region. Surfaces geographic laggards for targeted program intervention.",
        "regions_tracked":       REGIONS,
        "regional_housing_stock": REGIONAL_HOUSING_STOCK,
        "annual":                annual_out,
        "latest_year_snapshot": {
            "year":              latest["year"],
            "provincial_pen_pct": latest["provincial_pen_pct"],
            "laggard_region":    latest["laggard_region"],
            "laggard_pen_pct":   latest["regions"][latest["laggard_region"]]["penetration_pct"],
            "leader_region":     latest["leader_region"],
            "leader_pen_pct":    latest["regions"][latest["leader_region"]]["penetration_pct"],
            "spread_pp":         round(
                latest["regions"][latest["leader_region"]]["penetration_pct"] -
                latest["regions"][latest["laggard_region"]]["penetration_pct"], 2
            ),
            "ranked": [
                {
                    "rank": i + 1,
                    "region": reg,
                    "penetration_pct": latest["regions"][reg]["penetration_pct"],
                    "delta_pp": latest["regions"][reg]["delta_from_provincial_avg_pp"],
                }
                for i, reg in enumerate(latest["ranked_by_penetration_asc"])
            ],
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# OUTPUT 6 — INCOME TIER DISTRIBUTION
# Equity metric — is adoption reaching the households that need it most?
# ══════════════════════════════════════════════════════════════════════════════

def compute_income_tier(rows: list[dict]) -> dict:
    """
    L1.6 — Income tier distribution.

    Annual installs split by market-rate vs low/middle-income households.
    LMI share trend — tracks equity goal progress.
    Programme-driven shift: Energy Savings Programme launch Jun 2024.
    Energy savings flowing to LMI households vs market-rate.
    """
    annual = defaultdict(lambda: {
        "total": 0, "lmi": 0, "market": 0,
        "lmi_savings": 0, "mkt_savings": 0,
    })
    for r in rows:
        yr   = r["year"]
        fuel = r["fuel_switched_from"]
        sav  = ENERGY_SAVINGS_PER_INSTALL.get(fuel, 980)
        annual[yr]["total"]      += r["total_installs"]
        annual[yr]["lmi"]        += r["low_mid_income_installs"]
        annual[yr]["market"]     += r["market_rate_installs"]
        annual[yr]["lmi_savings"] += r["low_mid_income_installs"] * sav
        annual[yr]["mkt_savings"] += r["market_rate_installs"] * sav

    LMI_TARGET_PCT = 40.0  # CleanBC equity goal: >40% of installs to LMI households

    annual_out = []
    for yr in YEARS:
        d = annual[yr]
        total = d["total"]
        lmi   = d["lmi"]
        mkt   = d["market"]
        lmi_pct = round(lmi / total * 100, 1) if total else 0
        mkt_pct = round(mkt / total * 100, 1) if total else 0
        meets_target = lmi_pct >= LMI_TARGET_PCT

        annual_out.append({
            "year":                  yr,
            "total_installs":        total,
            "lmi_installs":          lmi,
            "market_rate_installs":  mkt,
            "lmi_share_pct":         lmi_pct,
            "market_share_pct":      mkt_pct,
            "lmi_target_pct":        LMI_TARGET_PCT,
            "meets_lmi_target":      meets_target,
            "gap_to_lmi_target_pp":  round(LMI_TARGET_PCT - lmi_pct, 1) if not meets_target else 0.0,
            "lmi_energy_savings_cad": d["lmi_savings"],
            "mkt_energy_savings_cad": d["mkt_savings"],
            "lmi_savings_share_pct": round(d["lmi_savings"] / (d["lmi_savings"] + d["mkt_savings"]) * 100, 1)
                                     if (d["lmi_savings"] + d["mkt_savings"]) > 0 else 0,
        })

    # Programme effect: LMI share before vs after Energy Savings Programme (Jun 2024)
    pre_esp  = [r for r in annual_out if r["year"] < 2024]
    post_esp = [r for r in annual_out if r["year"] >= 2024]
    avg_lmi_pre  = round(mean([r["lmi_share_pct"] for r in pre_esp]), 1)
    avg_lmi_post = round(mean([r["lmi_share_pct"] for r in post_esp]), 1)
    esp_uplift   = round(avg_lmi_post - avg_lmi_pre, 1)

    return {
        "output": "L1.6 — Income tier distribution",
        "description": "Share of installations reaching low/middle-income vs market-rate households. Equity signal for CleanBC programme evaluation.",
        "lmi_target_pct": LMI_TARGET_PCT,
        "annual": annual_out,
        "programme_effect_energy_savings_programme": {
            "launch_date":          "June 2024",
            "avg_lmi_share_pre_esp_pct":  avg_lmi_pre,
            "avg_lmi_share_post_esp_pct": avg_lmi_post,
            "lmi_share_uplift_pp":        esp_uplift,
            "note": "Interrupted time series analysis of programme effect is a Layer 2 output.",
        },
        "years_meeting_lmi_target": [r["year"] for r in annual_out if r["meets_lmi_target"]],
        "latest_year_snapshot": {
            "year":             annual_out[-1]["year"],
            "lmi_share_pct":    annual_out[-1]["lmi_share_pct"],
            "meets_target":     annual_out[-1]["meets_lmi_target"],
            "gap_pp":           annual_out[-1]["gap_to_lmi_target_pp"],
            "lmi_savings_cad":  annual_out[-1]["lmi_energy_savings_cad"],
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY — combine all outputs + print key stats
# ══════════════════════════════════════════════════════════════════════════════

def build_summary(outputs: dict) -> dict:
    stock  = outputs["stock"]
    flow   = outputs["flow"]
    fuel   = outputs["fuel_mix"]
    hptype = outputs["hp_type_mix"]
    region = outputs["regional"]
    equity = outputs["income_tier"]

    return {
        "layer": "Layer 1 — Descriptive Statistics",
        "dataset": "CCIN v2 synthetic dataset · 2018–2025",
        "generated_at": __import__("datetime").datetime.now().isoformat(),
        "key_stats": {
            # Stock
            "current_stock":           stock["current_stock_dec_2025"],
            "current_penetration_pct": stock["current_penetration_pct"],
            "gap_to_target":           stock["gap_to_target"],
            "target_2030":             stock["target_2030"],
            "pct_of_target_achieved":  round(stock["current_stock_dec_2025"] / stock["target_2030"] * 100, 1),
            # Flow
            "total_installs_2018_2025":  flow["descriptive_stats"]["total_installs_2018_2025"],
            "required_annual_rate":      flow["descriptive_stats"]["required_annual_rate"],
            "peak_month":                flow["descriptive_stats"]["peak_month"],
            "mean_monthly_installs":     flow["descriptive_stats"]["mean_monthly_installs"],
            # Fuel
            "dominant_fuel_2025":        fuel["latest_year_snapshot"]["dominant_fuel"],
            "total_ghg_avoided_2025":    fuel["latest_year_snapshot"]["total_ghg_avoided_tco2e_yr"],
            # Type
            "dominant_type_2025":        hptype["latest_year_snapshot"]["dominant_type"],
            "ground_source_growth_pp":   hptype["ground_source_share_change_pp_2018_2025"],
            # Regional
            "laggard_region_2025":       region["latest_year_snapshot"]["laggard_region"],
            "laggard_pen_pct":           region["latest_year_snapshot"]["laggard_pen_pct"],
            "regional_spread_pp":        region["latest_year_snapshot"]["spread_pp"],
            # Equity
            "lmi_share_2025_pct":        equity["latest_year_snapshot"]["lmi_share_pct"],
            "meets_lmi_target":          equity["latest_year_snapshot"]["meets_target"],
            "esp_lmi_uplift_pp":         equity["programme_effect_energy_savings_programme"]["lmi_share_uplift_pp"],
        },
        "outputs_computed": list(outputs.keys()),
    }


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def write_json(data: dict, path: Path):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    size = os.path.getsize(path) / 1024
    print(f"  wrote {path.name} ({size:.1f} KB, {len(json.dumps(data)):,} chars)")


def main():
    print("CCIN — Layer 1: Descriptive Statistics")
    print("=" * 42)

    # Load
    print("\nLoading raw data...")
    installs  = cast_installs(load_csv(INSTALLS_FILE))
    rebates   = cast_rebates(load_csv(REBATES_FILE))
    installers = cast_installers(load_csv(INSTALLER_FILE))
    print(f"  installs:   {len(installs):,} rows")
    print(f"  rebates:    {len(rebates):,} rows")
    print(f"  installers: {len(installers):,} rows")

    # Compute
    print("\nComputing outputs...")
    outputs = {
        "stock":        compute_stock(installs),
        "flow":         compute_flow(installs),
        "fuel_mix":     compute_fuel_mix(installs),
        "hp_type_mix":  compute_type_mix(installs),
        "regional":     compute_regional(installs),
        "income_tier":  compute_income_tier(installs),
    }

    # Write individual output files
    print("\nWriting output files...")
    for key, data in outputs.items():
        write_json(data, OUT_DIR / f"{key}.json")

    # Summary
    summary = build_summary(outputs)
    write_json(summary, OUT_DIR / "layer1_summary.json")

    # Print key stats to console
    s = summary["key_stats"]
    print("\n── Key stats ──────────────────────────────────")
    print(f"  Stock (Dec 2025):         {s['current_stock']:>10,}  ({s['current_penetration_pct']}% penetration)")
    print(f"  Target (2030):            {s['target_2030']:>10,}")
    print(f"  Gap to target:            {s['gap_to_target']:>10,}  ({100 - s['pct_of_target_achieved']:.1f}% remaining)")
    print(f"  Required annual rate:     {s['required_annual_rate']:>10,.0f}  installs/year from 2026")
    print(f"  Total installs 2018-25:   {s['total_installs_2018_2025']:>10,}")
    print(f"  Peak install month:       {s['peak_month']:>10}")
    print(f"  Dominant fuel 2025:       {s['dominant_fuel_2025']:>10}")
    print(f"  GHG avoided 2025:         {s['total_ghg_avoided_2025']:>10,.0f}  tCO2e/yr")
    print(f"  Dominant HP type 2025:    {s['dominant_type_2025']:>10}")
    print(f"  Ground source growth:     {s['ground_source_growth_pp']:>+10.1f}pp (2018-2025)")
    print(f"  Laggard region 2025:      {s['laggard_region_2025']:>10}  ({s['laggard_pen_pct']}% pen)")
    print(f"  Regional spread:          {s['regional_spread_pp']:>+10.2f}pp leader vs laggard")
    print(f"  LMI share 2025:           {s['lmi_share_2025_pct']:>10.1f}%  (target: >40%)")
    print(f"  Meets LMI target:         {str(s['meets_lmi_target']):>10}")
    print(f"  ESP LMI uplift:           {s['esp_lmi_uplift_pp']:>+10.1f}pp post Energy Savings Programme")
    print(f"\nAll outputs written to: {OUT_DIR}")


if __name__ == "__main__":
    main()
