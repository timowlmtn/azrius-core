import os
import argparse

import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sqlalchemy import create_engine

# Configuration defaults pulled from environment
DB_CONFIG = {
    "dbname": os.environ.get("POSTGRES_DB"),
    "user": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
    "host": os.environ.get("POSTGRES_HOST"),
    "port": os.environ.get("POSTGRES_PORT", "5432"),
}


def run_xgb_with_conformal(
    db_url: str,
    table_name: str,
    output_dir: str,
    target_column: str,
    test_size: float = 0.2,
    cal_size: float = 0.2,
    random_state: int = 42,
    alpha: float = 0.1,
    xgb_params: dict = None,
):
    os.makedirs(output_dir, exist_ok=True)

    # Load data via SQL
    engine = create_engine(db_url)
    df = pd.read_sql_query(
        f"SELECT to_char(date, 'YYYYMMDD')::int AS date, {target_column} FROM {table_name}",
        con=engine,
    )
    if target_column not in df.columns:
        raise ValueError(
            f"Target column '{target_column}' not found in table '{table_name}'"
        )

    # Split features/target
    X = df.drop(columns=[target_column])
    y = df[target_column]
    X_train_full, X_test, y_train_full, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    X_train, X_cal, y_train, y_cal = train_test_split(
        X_train_full, y_train_full, test_size=cal_size, random_state=random_state
    )

    # Train XGBoost
    params = {
        "n_estimators": 100,
        "max_depth": 4,
        "learning_rate": 0.1,
        "objective": "reg:squarederror",
        "random_state": random_state,
    }
    if xgb_params:
        params.update(xgb_params)
    model = xgb.XGBRegressor(**params)
    model.fit(X_train, y_train)

    # Calibrate residuals
    y_cal_pred = model.predict(X_cal)
    residuals = np.abs(y_cal - y_cal_pred)
    q = np.quantile(residuals, 1 - alpha)

    # Predict on test set + intervals
    y_pred = model.predict(X_test)
    lower, upper = y_pred - q, y_pred + q

    # Save outputs
    pd.DataFrame(
        {
            "actual": y_test.values,
            "predicted": y_pred,
            "lower": lower,
            "upper": upper,
        },
        index=y_test.index,
    ).to_csv(os.path.join(output_dir, "predictions_with_intervals.csv"))

    pd.DataFrame({"predicted": y_pred}, index=y_test.index).to_csv(
        os.path.join(output_dir, "raw_predictions.csv")
    )

    pd.DataFrame({"residual": residuals}, index=y_cal.index).to_csv(
        os.path.join(output_dir, "calibration_residuals.csv")
    )

    fi = pd.DataFrame(
        {"feature": X.columns, "importance": model.feature_importances_}
    ).sort_values("importance", ascending=False)
    fi.to_csv(os.path.join(output_dir, "feature_importances.csv"), index=False)

    print(f"✔️ Results written to {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="Train an XGBoost model with split-conformal intervals using Postgres data."
    )

    # Database connection args, with defaults from environment
    parser.add_argument(
        "--db_host",
        default=DB_CONFIG["host"],
        help=f"Postgres host (default from POSTGRES_HOST env: {DB_CONFIG['host']})",
    )
    parser.add_argument(
        "--db_port",
        default=DB_CONFIG["port"],
        help=f"Postgres port (default from POSTGRES_PORT env: {DB_CONFIG['port']})",
    )
    parser.add_argument(
        "--db_name",
        default=DB_CONFIG["dbname"],
        help=f"Postgres database name (default from POSTGRES_DB env: {DB_CONFIG['dbname']})",
    )
    parser.add_argument(
        "--db_user",
        default=DB_CONFIG["user"],
        help=f"Postgres user (default from POSTGRES_USER env: {DB_CONFIG['user']})",
    )
    parser.add_argument(
        "--db_password",
        default=DB_CONFIG["password"],
        help=f"Postgres password (default from POSTGRES_PASSWORD env)",
    )
    parser.add_argument(
        "--db_schema", default="public", help="Postgres schema (default: public)"
    )
    parser.add_argument(
        "--table_name",
        required=True,
        help="Postgres table to query (schema.table or table)",
    )

    # Modeling & conformal args
    parser.add_argument(
        "--target_column", "-t", required=True, help="Name of the column to predict"
    )
    parser.add_argument(
        "--output_dir",
        "-o",
        required=True,
        help="Directory where result CSVs will be written",
    )
    parser.add_argument(
        "--test_size",
        type=float,
        default=0.2,
        help="Fraction of data to hold out for testing (default: 0.2)",
    )
    parser.add_argument(
        "--cal_size",
        type=float,
        default=0.2,
        help="Fraction of training data to hold out for calibration (default: 0.2)",
    )
    parser.add_argument(
        "--random_state",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--alpha",
        type=float,
        default=0.1,
        help="Miscoverage level for intervals (default: 0.1 → 90% coverage)",
    )

    args = parser.parse_args()

    # Build SQLAlchemy URL
    db_url = (
        f"postgresql://{args.db_user}:{args.db_password}"
        f"@{args.db_host}:{args.db_port}/{args.db_name}"
    )

    # Qualify table name with schema if needed
    table_ref = (
        args.table_name
        if "." in args.table_name
        else f"{args.db_schema}.{args.table_name}"
    )

    run_xgb_with_conformal(
        db_url=db_url,
        table_name=table_ref,
        output_dir=args.output_dir,
        target_column=args.target_column,
        test_size=args.test_size,
        cal_size=args.cal_size,
        random_state=args.random_state,
        alpha=args.alpha,
    )


if __name__ == "__main__":
    main()
