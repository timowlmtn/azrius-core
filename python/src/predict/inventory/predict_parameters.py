# predict_parameters.py
from dataclasses import dataclass
import argparse
import psycopg2
import os
import json


@dataclass
class PredictParameters:
    item_id: int
    lead_time_days: int
    reorder_threshold: int
    reorder_quantity: int


def build_conn_str() -> str:
    return (
        f'postgresql://{os.environ.get("POSTGRES_USER")}:{os.environ.get("POSTGRES_PASSWORD")}'
        f'@{os.environ.get("POSTGRES_HOST")}:{os.environ.get("POSTGRES_PORT", "5432")}/{os.environ.get("POSTGRES_DB")}'
    )


def load_inventory(conn_str: str, schema: str, item_id: int) -> dict:
    """
    Query Postgres for the current inventory state of this product.
    Returns a dict with keys: quantity_on_hand, reorder_threshold, lead_time_days.
    """
    query = f"""
        SELECT quantity_on_hand, reorder_threshold, lead_time_days
        FROM {schema}.inventory_history
        WHERE item_id = %s
    """
    with psycopg2.connect(conn_str) as conn, conn.cursor() as cur:
        cur.execute(query, (item_id,))
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"No inventory record found for item_id={item_id}")
        quantity_on_hand, reorder_threshold, lead_time_days = row

    return {
        "quantity_on_hand": quantity_on_hand,
        "reorder_threshold": reorder_threshold,
        "lead_time_days": lead_time_days,
    }


def load_api_parameters():
    """
    Parse REST‐style inputs into Python values.
    --schema:      database schema (default "public")
    --item_id:  which product to look up
    --reorder_quantity: how many units we’d like to reorder
    """
    parser = argparse.ArgumentParser(
        description="Load prediction parameters and inventory state"
    )
    parser.add_argument(
        "--schema",
        type=str,
        default="public",
        help="Database schema containing the inventory table",
    )
    parser.add_argument(
        "--item_id",
        type=str,
        required=True,
        help="Product ID to fetch inventory for",
    )
    parser.add_argument(
        "--reorder_quantity",
        type=int,
        required=True,
        help="How many units to reorder when threshold is hit",
    )
    args = parser.parse_args()
    return args.schema, args.item_id, args.reorder_quantity


def main():
    # 1. Load API parameters
    schema, item_id, reorder_quantity = load_api_parameters()
    conn_str = build_conn_str()

    print("\n=== Loaded API Parameters ===")
    print(f"  schema:            {schema}")
    print(f"  item_id:        {item_id}")
    print(f"  reorder_quantity:  {reorder_quantity}")

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

    print("\n=== Combined Payload Preview ===")
    payload = {
        "parameters": params.__dict__,
        "inventory": inv,
    }
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
