import os
import pandas as pd
import numpy as np
import xgboost as xgb
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sqlalchemy import create_engine

# --- Configuration ---
DB_CONFIG = {
    "dbname": os.environ.get("POSTGRES_DB"),
    "user": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
    "host": os.environ.get("POSTGRES_HOST"),
    "port": os.environ.get("POSTGRES_PORT", "5432"),
}

SCHEMA = "smith_street_barbershop"
TABLE_NAME = "inventory_history"


def get_engine():
    """Build a SQLAlchemy engine from DB_CONFIG."""
    url = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    return create_engine(url, echo=False)


# --- 1. Load Data ---
def load_inventory_history():
    engine = get_engine()
    query = f"""
    SELECT item_id,
           item_name,
           date,
           quantity_on_hand,
           units_sold,
           units_ordered,
           price_per_unit
    FROM {SCHEMA}.{TABLE_NAME}
    ORDER BY date ASC
    """
    df = pd.read_sql_query(query, con=engine)
    df["date"] = pd.to_datetime(df["date"])
    return df


# --- 2. Create Time Features ---
def create_time_features(df):
    df["weekofyear"] = df["date"].dt.isocalendar().week
    df["month"] = df["date"].dt.month
    df["dayofweek"] = df["date"].dt.dayofweek
    df["year"] = df["date"].dt.year
    return df


# --- 3. Create Lag Features ---
def create_lag_features(df, lags=[1, 2, 4, 8]):
    df = df.sort_values(["item_id", "date"])
    for lag in lags:
        df[f"units_sold_lag_{lag}"] = df.groupby("item_id")["units_sold"].shift(lag)
    return df


# --- 4. Build and Train Model ---
def train_xgb(df):
    feature_cols = ["weekofyear", "month", "dayofweek", "year"] + [
        col for col in df.columns if col.startswith("units_sold_lag_")
    ]

    df = df.dropna(subset=feature_cols + ["units_sold"])
    X = df[feature_cols]
    y = df["units_sold"]

    # preserve time order by not shuffling
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, shuffle=False
    )

    model = xgb.XGBRegressor(
        objective="reg:squarederror",
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        random_state=42,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    print(f"Test RMSE: {rmse:.2f}")

    return model, X_test, y_test, y_pred, df


# --- 5. Visualize Results ---
def plot_predictions(y_test, y_pred, df):
    plt.figure(figsize=(14, 6))
    test_dates = df.loc[y_test.index, "date"]
    plt.plot(test_dates, y_test.values, label="Actual Units Sold", marker="o")
    plt.plot(test_dates, y_pred, label="Predicted Units Sold", marker="x")
    plt.title("Actual vs Predicted Units Sold")
    plt.xlabel("Date")
    plt.ylabel("Units Sold")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()


# --- Main Flow ---
if __name__ == "__main__":
    df = load_inventory_history()
    df = create_time_features(df)
    df = create_lag_features(df)

    model, X_test, y_test, y_pred, df = train_xgb(df)
    plot_predictions(y_test, y_pred, df)
