#!/usr/bin/env python3
"""
Analyze periodicity and predictability of metrics in the weekly_sales table.
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from statsmodels.tsa.stattools import acf
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sklearn.metrics import mean_absolute_percentage_error

from core import db_postgres


def load_data(db_url):
    """
    Load the weekly_sales table and create a datetime index from year and week.
    """
    engine = create_engine(db_url)
    df = pd.read_sql_table("weekly_sales", schema="staging_lincoln_liquors", con=engine)

    # Drop rows where sales_year or sales_week is null
    df = df.dropna(subset=["sales_year", "sales_week"])
    # Ensure integer types for conversion
    df["sales_year"] = df["sales_year"].astype(int)
    df["sales_week"] = df["sales_week"].astype(int)

    # Construct year-week string and convert to datetime, assuming week starts on Monday
    df["year_week"] = df["sales_year"].astype(str) + df["sales_week"].astype(
        int
    ).astype(str).str.zfill(2)
    df["date"] = pd.to_datetime(df["year_week"] + "1", format="%Y%W%w")
    df = df.set_index("date").sort_index()
    return df


def detect_periodicity(series, max_lag=52):
    """
    Detect dominant period (in weeks) using autocorrelation.
    Returns (period, strength).
    """
    # Compute autocorrelation up to max_lag weeks
    acf_vals = acf(series, nlags=max_lag, fft=False, missing="drop")
    # Exclude lag 0
    lags = np.arange(len(acf_vals))
    # Find lag with maximum absolute correlation
    dominant_lag = lags[1:][np.argmax(np.abs(acf_vals[1:]))]
    strength = acf_vals[dominant_lag]
    return int(dominant_lag), float(strength)


def assess_predictability(series, period=None, train_frac=0.8):
    """
    Fit a simple Exponential Smoothing model and compute MAPE on a holdout test set.
    Uses seasonal component if a valid period is provided.
    """
    n = len(series)
    train_end = int(n * train_frac)
    train, test = series.iloc[:train_end], series.iloc[train_end:]

    try:
        # Choose seasonal model if period is reasonable
        if period and period > 1 and period < len(train):
            model = ExponentialSmoothing(
                train, trend="add", seasonal="add", seasonal_periods=period
            )
        else:
            model = ExponentialSmoothing(train, trend="add", seasonal=None)
        fit = model.fit(optimized=True)
        pred = fit.forecast(len(test))
        mape = mean_absolute_percentage_error(test, pred)
    except Exception:
        mape = np.nan
    return float(mape)


def analyze_metrics(df, metrics):
    """
    Run periodicity detection and predictability assessment for each metric.
    Returns a DataFrame with metrics ranked by overall score.
    """
    results = []
    for metric in metrics:
        series = df[metric].dropna()
        if len(series) < 10:
            # Skip series that are too short for meaningful analysis
            continue
        period, strength = detect_periodicity(series)
        mape = assess_predictability(series, period=period)
        results.append(
            {
                "metric": metric,
                "period_weeks": period,
                "periodicity_strength": strength,
                "mape": mape,
            }
        )

    res_df = pd.DataFrame(results)
    # Rank by periodicity strength (higher is better) and predictability (lower MAPE is better)
    res_df["periodicity_rank"] = res_df["periodicity_strength"].rank(
        ascending=False, method="dense"
    )
    res_df["predictability_rank"] = res_df["mape"].rank(ascending=True, method="dense")
    # Combine ranks
    res_df["overall_rank"] = res_df["periodicity_rank"] + res_df["predictability_rank"]
    return res_df.sort_values(by=["overall_rank", "periodicity_rank"])


def main():
    df = load_data(db_postgres.get_db_url())
    metrics = [
        "current_sales_quantity",
        "last_week_quantity",
        "last_week_diff",
        "last_13_week_quantity",
        "last_13_week_diff",
        "last_year_quantity",
        "last_year_diff",
    ]
    results = analyze_metrics(df, metrics)
    print("Metric Analysis Results:")
    print(results.to_string(index=False))


if __name__ == "__main__":
    main()
