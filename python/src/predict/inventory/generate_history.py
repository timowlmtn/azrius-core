import pandas as pd
import random
import argparse
import os
from datetime import datetime, date, timedelta
import math


def generate_demand_pattern(weeks, base_demand=50, amplitude=20, noise_level=5):
    """
    Generate a seasonal weekly demand pattern over a number of weeks.
    Seasonal variation follows a sinusoidal curve on a 52-week cycle,
    with optional random noise per week.

    Returns:
        List[int]: Demand for each week.
    """
    pattern = []
    for i in range(weeks):
        season_factor = math.sin(2 * math.pi * (i % 52) / 52)
        demand = base_demand + amplitude * season_factor
        demand += random.uniform(-noise_level, noise_level)
        pattern.append(max(int(round(demand)), 0))
    return pattern


def generate_history(
    start_date: datetime,
    weeks: int,
    initial_inventory: int,
    replenishment_qty: int,
    replenish_every: int = 2,
    max_shrinkage: int = 4,
    item_id: str = None,
) -> pd.DataFrame:
    """
    Generates a weekly history of inventory levels and sales based on a weekly demand pattern.
    If item_id is provided, adds it as a column.

    Returns:
      pd.DataFrame: Columns [date, sales, shrinkage, quantity_on_hand, item_id].
    """
    dates = [start_date + timedelta(weeks=i) for i in range(weeks)]
    demand_pattern = generate_demand_pattern(weeks)
    records = []
    inventory = initial_inventory

    for idx, date_point in enumerate(dates):
        demand = demand_pattern[idx]
        sales = min(demand, inventory)
        shrinkage = random.randint(0, max_shrinkage)
        inventory = max(inventory - sales - shrinkage, 0)
        if (idx + 1) % replenish_every == 0:
            inventory += replenishment_qty
        record = {
            "date": date_point,
            "sales": sales,
            "shrinkage": shrinkage,
            "quantity_on_hand": inventory,
        }
        if item_id is not None:
            record["item_id"] = item_id
        records.append(record)

    df = pd.DataFrame(records)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate weekly inventory history for one or more items up to today."
    )
    parser.add_argument(
        "--item_ids",
        nargs="+",
        required=True,
        help="One or more item IDs to generate history for",
    )
    parser.add_argument(
        "--weeks", type=int, default=26, help="Number of weeks of history to generate"
    )
    parser.add_argument(
        "--initial_inventory", type=int, default=500, help="Starting inventory on hand"
    )
    parser.add_argument(
        "--replenishment_qty",
        type=int,
        default=200,
        help="Quantity to add at each replenishment",
    )
    parser.add_argument(
        "--replenish_every", type=int, default=2, help="Replenishment cadence in weeks"
    )
    parser.add_argument(
        "--max_shrinkage", type=int, default=4, help="Maximum random weekly shrinkage"
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="data/inventory_history",
        help="Directory to save per-item history CSVs",
    )

    args = parser.parse_args()
    today = date.today()
    start = today - timedelta(weeks=args.weeks)

    # Ensure output directory
    os.makedirs(args.output_dir, exist_ok=True)

    all_dfs = []
    for item in args.item_ids:
        # Create subfolder for item
        item_dir = os.path.join(args.output_dir, item)
        os.makedirs(item_dir, exist_ok=True)

        # Generate history
        df_hist = generate_history(
            start_date=start,
            weeks=args.weeks,
            initial_inventory=args.initial_inventory,
            replenishment_qty=args.replenishment_qty,
            replenish_every=args.replenish_every,
            max_shrinkage=args.max_shrinkage,
            item_id=item,
        )

        # Save per-item CSV
        item_file = os.path.join(item_dir, f"{item}_history.csv")
        df_hist.to_csv(item_file, index=False)
        print(f"✅ {item}: {len(df_hist)} rows saved to {item_file}")

        all_dfs.append(df_hist)

    # Save combined history
    combined = pd.concat(all_dfs, ignore_index=True)
    combined_file = os.path.join(args.output_dir, "combined_inventory_history.csv")
    combined.to_csv(combined_file, index=False)
    print(
        f"✅ Combined history for {len(args.item_ids)} items saved to {combined_file}"
    )
