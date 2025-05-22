from typing import Dict, List, Tuple
import pandas as pd
import os
from sqlalchemy.engine import Engine, create_engine
from statsmodels.tsa.statespace.sarimax import SARIMAX


def generate_seasonal_forecasts(
    engine: Engine,
    schema: str = "staging_lincoln_liquors",
    table: str = "weekly_sales",
    date_cols: Tuple[str, str] = ("sales_year", "sales_week"),
    quantity_col: str = "current_sales_quantity",
    order: tuple = (1, 1, 1),
    trend: str = "n",
    lags: List[int] = [1, 2, 3, 4, 13, 26, 52],
    forecast_horizon: int = 12,
) -> Dict[int, pd.DataFrame]:
    """
    Load weekly sales from the given SQLAlchemy engine and fit
    SARIMAX seasonal models for each period in `lags`.

    Returns a dict mapping each lag to its forecast DataFrame.
    """
    year_col, week_col = date_cols
    # Load sales data
    query = f"""

WITH max_week AS (

    SELECT ({year_col}::text || {week_col}::text)::int max_week_year,
           MAX({year_col}) as max_year,
           MAX({week_col}) AS max_week,
           sum({quantity_col}) AS total_sales
    FROM {schema}.{table}
    WHERE {quantity_col} is not null
    and {quantity_col} > 0
    group by {year_col}, {week_col}
    order by 1 desc
    limit 1
)
SELECT sales.{year_col}, sales.{week_col}, sales.{quantity_col} AS sales_quantity
FROM {schema}.{table} sales
CROSS JOIN max_week mw
WHERE ({year_col}::text || {week_col}::text)::int < mw.max_week_year
  AND {quantity_col} IS NOT NULL
  AND {quantity_col} > 0
  
    """

    print(query)

    actual = pd.read_sql(query, con=engine)

    # Drop rows missing year or week before casting
    actual = actual.dropna(subset=[year_col, week_col])

    # Ensure year and week are integers (prevent floats like '2012.0')
    actual[year_col] = actual[year_col].astype(int)
    actual[week_col] = actual[week_col].astype(int)

    # Build a weekly DateTimeIndex based on ISO year-week (Monday as day 1)
    year_str = actual[year_col].astype(str)
    week_str = actual[week_col].astype(str).str.zfill(2)
    actual["date"] = pd.to_datetime(
        year_str + week_str + "1", format="%Y%W%w", errors="coerce"
    )
    # Drop bad parses and duplicates: sum quantities for the same date
    actual = actual.dropna(subset=["date"])
    actual = actual.groupby("date", as_index=True)["sales_quantity"].sum().sort_index()

    # Ensure a regular weekly frequency
    ts = actual.asfreq("W-MON")

    seasonal_forecasts: Dict[int, pd.DataFrame] = {}

    predicted_mean: Dict[int, pd.Series] = {}

    for lag in lags:
        # Fit SARIMAX(1,1,1) with seasonal (1,1,1,lag)
        # Use seasonal_order only if lag > 1
        if lag > 1:
            seasonal_order = (1, 1, 1, lag)
        else:
            seasonal_order = (0, 0, 0, 0)

        print(f"seasonal_order: {seasonal_order}")

        model = SARIMAX(
            ts,
            order=order,
            trend=trend,
            seasonal_order=seasonal_order,
            enforce_stationarity=False,
            enforce_invertibility=False,
        )
        res = model.fit(disp=False, maxiter=200)

        # Build forecast index (next forecast_horizon weeks)
        last_date = ts.index[-1]
        forecast_index = pd.date_range(
            start=last_date + pd.offsets.Week(weekday=0),
            periods=forecast_horizon,
            freq="W-MON",
        )

        # Generate and store forecasts
        pred = res.get_forecast(steps=forecast_horizon)
        forecast_df = pred.predicted_mean.to_frame(name=f"forecast_{lag}w")
        forecast_df.index = forecast_index
        seasonal_forecasts[lag] = forecast_df
        predicted_mean[lag] = pred.predicted_mean

    return actual, seasonal_forecasts, predicted_mean


def find_critical_points(predicted_mean: dict) -> dict:
    """
    Identify critical points (slope direction changes) in forecast series.

    Returns a dictionary with lag keys and list of timestamps where slope changes occur.
    """
    critical_points = {}

    for lag, series in predicted_mean.items():
        series = series.dropna()
        diffs = series.diff().fillna(0)
        signs = diffs.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        change_points = signs.diff().fillna(0).ne(0)
        turning_points = series.index[change_points].tolist()
        critical_points[lag] = turning_points

    return critical_points


def find_annotated_critical_points(
    predicted_mean: dict, value_range: tuple = None, keep: int = 2
) -> list:
    """
    Identify and annotate critical points in forecast series.
    Returns a dictionary mapping each lag to a list of dicts with:
    - timestamp
    - value
    - type: 'min' or 'max'
    - is_global: True if it's a global min/max within range
    """

    direction = None

    annotations = []
    prev_val = None
    prev_idx = None

    min_value = None
    max_value = None

    for idx, value in predicted_mean.items():
        if value_range == None:
            min_value = value
            max_value = value

        if pd.isna(value) or prev_val is None:
            prev_val = value
            continue

        last_direction = direction

        if value > prev_val:
            direction = "up"
        elif value < prev_val:
            direction = "down"
        else:
            direction = "flat"

        if direction != last_direction and direction == "down":
            point_type = "max"
        elif direction != last_direction and direction == "up":
            point_type = "min"

        if value < min_value:
            min_value = value
        elif value > max_value:
            max_value = value

        if (point_type == "max" or point_type == "min") and prev_idx is not None:
            annotations.append(
                {
                    "timestamp": prev_idx,
                    "value": prev_val,
                    "label": prev_idx.strftime("%b %d") + f" ({point_type})",
                    "type": point_type,
                    "is_global": (value == point_type),
                }
            )

        prev_idx = idx
        prev_val = value

    mins = sorted(
        [pt for pt in annotations if pt["type"] == "min"], key=lambda x: x["value"]
    )[:keep]
    maxs = sorted(
        [pt for pt in annotations if pt["type"] == "max"],
        key=lambda x: x["value"],
        reverse=True,
    )[:keep]
    return mins + maxs


def main():
    """
    Entry point: create engine, generate forecasts, and display them.
    """
    DB_USER = os.getenv("JUPYTER_POSTGRES_USER")
    DB_PASSWORD = os.getenv("JUPYTER_POSTGRES_PASSWORD")
    DB_HOST = os.getenv("JUPYTER_POSTGRES_HOST")
    DB_PORT = os.getenv("JUPYTER_POSTGRES_PORT", "5432")
    DB_NAME = os.getenv("JUPYTER_POSTGRES_DB")

    # Database connection parameters
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)

    actual, forecasts, predicted_mean = generate_seasonal_forecasts(engine, lags=[1, 2])
    for lag, df in forecasts.items():
        print(f"\nForecasts for lag={lag} weeks (first {len(df)} rows):")
        print(df.head())

    critical_points = find_critical_points(predicted_mean)
    print(critical_points)

    annotated_points = find_annotated_critical_points(predicted_mean)
    print(annotated_points)


if __name__ == "__main__":
    main()
