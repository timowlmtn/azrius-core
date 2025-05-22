# forecast.py

import pandas as pd

from sqlalchemy import create_engine
from predict_parameters import PredictParameters


def load_inventory_history(db_url: str, schema: str, item_id: int) -> pd.Series:
    """
    Pull weekly snapshots of quantity_on_hand from inventory_history.
    Returns a Series indexed by date.
    """
    query = f"""
      SELECT 
        history_date::date AS date,
        quantity_on_hand
      FROM {schema}.inventory_history
      WHERE item_id = '{item_id}'
      ORDER BY history_date
    """
    engine = create_engine(db_url)
    df = pd.read_sql_query(query, con=engine)
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date")["quantity_on_hand"]


def forecast_no_reorder(
    params: PredictParameters,
    conn_str: str,
    schema: str,
    forecast_horizon: int,
) -> pd.DataFrame:
    """
    - Pulls historical inventory via load_inventory_history()
    - Computes average daily usage from weekly diffs
    - Simulates day-by-day inventory levels with no reorders
    - Returns DataFrame with ['date','inventory_level']
    """
    # 1) load history
    hist = load_inventory_history(conn_str, schema, params.item_id)
    hist = hist.sort_index()

    # 2) derive average daily usage
    #    weekly_diff is positive consumption
    weekly_diff = -hist.diff().dropna()
    avg_weekly_consumption = weekly_diff.mean()
    avg_daily_usage = avg_weekly_consumption / 7

    # 3) roll forward from last known level
    last_date = hist.index.max()
    inv = hist.iloc[-1]

    records = []
    for d in pd.date_range(
        start=last_date + pd.Timedelta(days=1), periods=forecast_horizon, freq="D"
    ):
        inv = max(inv - avg_daily_usage, 0)
        records.append({"date": d, "inventory_level": inv})

    return pd.DataFrame(records)


if __name__ == "__main__":
    # quick demo runner
    from predict_parameters import build_conn_str, load_api_parameters, load_inventory

    schema, item_id, reorder_quantity = load_api_parameters()
    conn_str = build_conn_str()

    # 2. Query inventory
    print("\n=== Querying Current Inventory ===")
    inv = load_inventory(conn_str, schema, item_id)
    for k, v in inv.items():
        print(f"  {k}: {v}")

    # 3. Build PredictParameters and show full payload
    params = PredictParameters(
        item_id=item_id,
        lead_time_days=inv["lead_time_days"],
        reorder_threshold=inv["reorder_threshold"],
        reorder_quantity=reorder_quantity,
    )

    df_forecast = forecast_no_reorder(params, conn_str, schema, forecast_horizon=30)
    print(df_forecast)
