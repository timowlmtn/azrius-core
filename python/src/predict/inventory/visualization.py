# visualization.py

import matplotlib.pyplot as plt
import io
import base64
import pandas as pd

from reorder import apply_first_reorder, apply_second_reorder
from forecast import forecast_no_reorder, load_inventory_history
from predict_parameters import (
    build_conn_str,
    load_api_parameters,
    load_inventory,
    PredictParameters,
)


def plot_forecast_with_reorders(
    df: pd.DataFrame,
    events: dict,
    date_col: str = "date",
    level_col: str = "inventory_level",
    title: str = "Forecast with Reorders",
) -> str:
    """
    - Plot a forecast DataFrame with reorder annotations.
    - Returns a base64-encoded PNG string for embedding in JSON APIs.
    """
    data = df.copy()
    data[date_col] = pd.to_datetime(data[date_col])

    fig, ax = plt.subplots()
    ax.plot(data[date_col], data[level_col], marker="o", label="Inventory Level")

    for key, ev in events.items():
        arrival = pd.to_datetime(ev["arrival_day"])
        qty = ev.get("quantity", 0)
        ax.axvline(arrival, linestyle="--", linewidth=1, label=f"{key.title()} Arrival")
        level_at = data.loc[data[date_col] == arrival, level_col]
        if not level_at.empty:
            ax.scatter(
                arrival,
                level_at.values[0],
                s=50,
                zorder=5,
                label=f"{key.title()} +{qty}",
            )
        else:
            y_min, y_max = ax.get_ylim()
            ax.text(
                arrival, y_max * 0.95, f"+{qty}", rotation=90, verticalalignment="top"
            )

    ax.set_title(title)
    ax.set_xlabel("Date")
    ax.set_ylabel("Inventory Level")
    ax.legend()
    fig.autofmt_xdate()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def plot_inventory_history(
    schema: str,
    item_id: int,
    conn_str: str,
    date_col: str = "date",
    level_col: str = "quantity_on_hand",
    title: str = "Historical Inventory",
) -> str:
    """
    - Loads full history via `load_inventory_history`
    - Plots quantity_on_hand over time
    - Returns a base64-encoded PNG string
    """
    # load history series
    series = load_inventory_history(conn_str, schema, item_id)
    df = series.reset_index()
    df.rename(columns={series.name: level_col}, inplace=True)
    df[date_col] = pd.to_datetime(df[date_col])

    fig, ax = plt.subplots()
    ax.plot(df[date_col], df[level_col], marker=".", linestyle="-", label="On-Hand")
    ax.set_title(title)
    ax.set_xlabel("Date")
    ax.set_ylabel("Quantity on Hand")
    ax.legend()
    fig.autofmt_xdate()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


if __name__ == "__main__":
    # Load parameters and connection
    schema, item_id, reorder_quantity = load_api_parameters()
    conn_str = build_conn_str()

    # Plot historical inventory
    print("Generating historical inventory plot...")
    hist_img = plot_inventory_history(schema, item_id, conn_str)
    with open(f"data/inventory/history_{item_id}.png", "wb") as f:
        f.write(base64.b64decode(hist_img))
    print(f"Saved history plot to data/inventory/history_{item_id}.png")

    # Also generate forecast with reorders
    inv = load_inventory(conn_str, schema, item_id)
    params = PredictParameters(
        item_id=item_id,
        lead_time_days=inv["lead_time_days"],
        reorder_threshold=inv["reorder_threshold"],
        reorder_quantity=reorder_quantity,
    )
    df_fc = forecast_no_reorder(params, conn_str, schema, forecast_horizon=30)
    df1, events = apply_first_reorder(df_fc.copy(), params)
    df2, events = apply_second_reorder(df1, params, events)
    print("Generating forecast with reorders plot...")
    fc_img = plot_forecast_with_reorders(df2, events)
    with open(f"data/inventory/forecast_{item_id}.png", "wb") as f:
        f.write(base64.b64decode(fc_img))
    print(f"Saved forecast plot to data/inventory/forecast_{item_id}.png")
