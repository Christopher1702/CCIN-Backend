"""
main.py
=======
CCIN Backend — FastAPI application
Serves Layer 1 descriptive statistics as a REST API.

Endpoints:
    GET /                           health check + available routes
    GET /api/layer1/summary         key stats across all L1 outputs
    GET /api/layer1/stock           cumulative stock + penetration (monthly + annual)
    GET /api/layer1/flow            installation flow (annual + monthly + descriptive stats)
    GET /api/layer1/fuel-mix        fuel switching breakdown by year
    GET /api/layer1/hp-type         heat pump type mix by year
    GET /api/layer1/regional        regional distribution + penetration ranking
    GET /api/layer1/income-tier     income tier split + equity signal

All endpoints accept an optional ?year= query param to filter to a single year.

Start locally:
    uvicorn main:app --reload

Deploy on Render:
    Start command: uvicorn main:app --host 0.0.0.0 --port $PORT
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pathlib import Path
from typing import Optional
import json
import sys

# ── Import Layer 1 analytics ──────────────────────────────────────────────────
# layer1_descriptive_stats.py must be in the same directory as main.py
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from layer1_descriptive_stats import (
    load_csv, cast_installs, cast_rebates, cast_installers,
    compute_stock, compute_flow, compute_fuel_mix,
    compute_type_mix, compute_regional, compute_income_tier,
    build_summary,
    INSTALLS_FILE, REBATES_FILE, INSTALLER_FILE,
)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="CCIN API",
    description="Climate Change Intelligence Network — BC Heat Pump Deployment Analytics",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── Load and compute all Layer 1 outputs at startup ──────────────────────────
# Data is loaded once when the server starts — not on every request.
print("CCIN API — loading data...")

try:
    _installs   = cast_installs(load_csv(INSTALLS_FILE))
    _rebates    = cast_rebates(load_csv(REBATES_FILE))
    _installers = cast_installers(load_csv(INSTALLER_FILE))
    print(f"  installs:   {len(_installs):,} rows")
    print(f"  rebates:    {len(_rebates):,} rows")
    print(f"  installers: {len(_installers):,} rows")
except FileNotFoundError as e:
    print(f"ERROR: Could not load data files — {e}")
    print("Make sure the CSV files are in the same directory as main.py")
    raise

print("Computing Layer 1 outputs...")
_outputs = {
    "stock":       compute_stock(_installs),
    "flow":        compute_flow(_installs),
    "fuel_mix":    compute_fuel_mix(_installs),
    "hp_type_mix": compute_type_mix(_installs),
    "regional":    compute_regional(_installs),
    "income_tier": compute_income_tier(_installs),
}
_summary = build_summary(_outputs)
print("Ready.")


# ── Helpers ───────────────────────────────────────────────────────────────────

def filter_by_year(data: dict, year: Optional[int]) -> dict:
    """
    If year is provided, filter any 'annual' or 'monthly' lists in the
    response to only include rows matching that year.
    """
    if year is None:
        return data

    result = dict(data)

    if "annual" in result:
        filtered = [r for r in result["annual"] if r.get("year") == year]
        if not filtered:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for year {year}. Available: 2018–2025."
            )
        result["annual"] = filtered

    if "monthly" in result:
        result["monthly"] = [r for r in result["monthly"] if r.get("year") == year]

    if "annual_snapshots" in result:
        result["annual_snapshots"] = [
            r for r in result["annual_snapshots"] if r.get("year") == year
        ]

    return result


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def root():
    """Health check — returns service status and available endpoints."""
    return {
        "service": "CCIN API",
        "status":  "ok",
        "version": "1.0.0",
        "dataset": "BC Heat Pump Deployment · Synthetic v2 · 2018–2025",
        "layer":   "Layer 1 — Descriptive Statistics",
        "endpoints": {
            "summary":     "/api/layer1/summary",
            "stock":       "/api/layer1/stock",
            "flow":        "/api/layer1/flow",
            "fuel_mix":    "/api/layer1/fuel-mix",
            "hp_type":     "/api/layer1/hp-type",
            "regional":    "/api/layer1/regional",
            "income_tier": "/api/layer1/income-tier",
        },
        "docs": "/docs",
    }


@app.get("/api/layer1/summary", tags=["Layer 1"])
def get_summary():
    """
    Key statistics summary across all Layer 1 outputs.
    Returns the headline numbers: stock, penetration, gap to target,
    required annual rate, dominant fuel, laggard region, LMI share.
    """
    return JSONResponse(content=_summary)


@app.get("/api/layer1/stock", tags=["Layer 1"])
def get_stock(year: Optional[int] = Query(default=None, ge=2018, le=2025)):
    """
    L1.1 — Cumulative heat pump stock and penetration rate.
    Monthly series + annual end-of-year snapshots.
    Optional: ?year=2024 to filter to a single year.
    """
    return JSONResponse(content=filter_by_year(_outputs["stock"], year))


@app.get("/api/layer1/flow", tags=["Layer 1"])
def get_flow(year: Optional[int] = Query(default=None, ge=2018, le=2025)):
    """
    L1.2 — Installation flow metric.
    Annual totals by rebate channel + YoY growth rate.
    Monthly flow with trailing 12-month rolling average.
    Descriptive stats: mean, median, peak month, required annual rate.
    Optional: ?year=2024 to filter annual/monthly rows to a single year.
    """
    return JSONResponse(content=filter_by_year(_outputs["flow"], year))


@app.get("/api/layer1/fuel-mix", tags=["Layer 1"])
def get_fuel_mix(year: Optional[int] = Query(default=None, ge=2018, le=2025)):
    """
    L1.3 — Fuel switching breakdown.
    Annual installs by fuel displaced (natural gas, oil, propane, electric baseboard).
    Includes GHG avoided per fuel type and weighted average.
    Optional: ?year=2024 to filter to a single year.
    """
    return JSONResponse(content=filter_by_year(_outputs["fuel_mix"], year))


@app.get("/api/layer1/hp-type", tags=["Layer 1"])
def get_hp_type(year: Optional[int] = Query(default=None, ge=2018, le=2025)):
    """
    L1.4 — Heat pump type mix.
    Annual installs by equipment type (mini-split, central ducted, ground-source, water heater).
    Ground-source share trend included as a market maturity signal.
    Optional: ?year=2024 to filter to a single year.
    """
    return JSONResponse(content=filter_by_year(_outputs["hp_type_mix"], year))


@app.get("/api/layer1/regional", tags=["Layer 1"])
def get_regional(year: Optional[int] = Query(default=None, ge=2018, le=2025)):
    """
    L1.5 — Regional distribution.
    Penetration rate by BC region with delta from provincial average.
    Regions ranked by penetration (ascending — most behind first).
    Optional: ?year=2024 to filter to a single year.
    """
    return JSONResponse(content=filter_by_year(_outputs["regional"], year))


@app.get("/api/layer1/income-tier", tags=["Layer 1"])
def get_income_tier(year: Optional[int] = Query(default=None, ge=2018, le=2025)):
    """
    L1.6 — Income tier distribution.
    LMI vs market-rate share by year.
    Tracks CleanBC equity goal (>40% of installs to LMI households).
    Energy Savings Programme effect quantified.
    Optional: ?year=2024 to filter to a single year.
    """
    return JSONResponse(content=filter_by_year(_outputs["income_tier"], year))
