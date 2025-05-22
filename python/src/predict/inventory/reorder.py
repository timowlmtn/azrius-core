# reorder.py

import pandas as pd
from forecast import forecast_no_reorder
from predict_parameters import (
    PredictParameters,
    build_conn_str,
    load_api_parameters,
    load_inventory,
)


def apply_first_reorder(
    df: pd.DataFrame, params: PredictParameters
) -> (pd.DataFrame, dict):
    """
    - Scan df until inventory_level <= params.reorder_threshold
    - On that day, place an order of params.reorder_quantity
    - Add to inventory after params.lead_time_days
    - Return updated df and an event dict for this reorder
    """
    events = {}
    for _, row in df.iterrows():
        if row.inventory_level <= params.reorder_threshold:
            order_day = row.date
            arrival_day = order_day + pd.Timedelta(days=params.lead_time_days)
            events["first"] = {
                "order_day": order_day,
                "arrival_day": arrival_day,
                "quantity": params.reorder_quantity,
            }
            df.loc[df.date >= arrival_day, "inventory_level"] += params.reorder_quantity
            break
    return df, events


def apply_second_reorder(
    df: pd.DataFrame, params: PredictParameters, events: dict
) -> (pd.DataFrame, dict):
    """
    - After first arrival, scan remainder of df for second reorder
    - On reorder day, order and apply arrival after lead_time
    - Return updated df and updated events dict
    """
    for _, row in df[df.date > events["first"]["arrival_day"]].iterrows():
        if row.inventory_level <= params.reorder_threshold:
            order_day = row.date
            arrival_day = order_day + pd.Timedelta(days=params.lead_time_days)
            events["second"] = {
                "order_day": order_day,
                "arrival_day": arrival_day,
                "quantity": params.reorder_quantity,
            }
            df.loc[df.date >= arrival_day, "inventory_level"] += params.reorder_quantity
            break
    return df, events


if __name__ == "__main__":
    # 1. Load API parameters and current inventory
    schema, item_id, reorder_quantity = load_api_parameters()
    conn_str = build_conn_str()
    inv = load_inventory(conn_str, schema, item_id)

    params = PredictParameters(
        item_id=item_id,
        lead_time_days=inv["lead_time_days"],
        reorder_threshold=inv["reorder_threshold"],
        reorder_quantity=reorder_quantity,
    )

    # 2. Generate no-reorder forecast
    df_forecast = forecast_no_reorder(params, conn_str, schema, forecast_horizon=30)
    print("\n=== No-Reorder Forecast ===")
    print(df_forecast)

    # 3. Apply first and second reorders
    df_first, events = apply_first_reorder(df_forecast.copy(), params)
    df_second, events = apply_second_reorder(df_first, params, events)

    # 4. Output events and updated forecast
    print("\n=== Reorder Events ===")
    print(events)
    print("\n=== Forecast with Reorders ===")
    print(df_second)
