import streamlit as st
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from sqlalchemy import create_engine
from predict import seasonal_forecasts as sf
from core import db_postgres as db
import holidays
import matplotlib.cm as cm
import matplotlib.colors as mcolors

# --- Streamlit UI ---
st.set_page_config(
    page_title="Azrius Forecast Agent",
    page_icon="web/frontend/public/favicon.png",
    layout="wide",
)

# Custom CSS for sidebar multiselect tags
st.markdown(
    """
    <style>
section[data-testid="stSidebar"] .stMultiSelect span,
section[data-testid="stSidebar"] .stSelectbox div[data-baseweb="tag"] {
    background-color: #007bff !important;
    color: white !important;
    border: none !important;
}
</style>
""",
    unsafe_allow_html=True,
)

st.title("📈 Azrius Forecast Agent Builder")

# Description of the forecasting logic
with st.expander("ℹ️ What does this app do?"):
    st.markdown(
        """
    This app reads weekly sales data from a SQL database and uses statistical time series models (SARIMAX)
    to forecast future sales based on seasonal patterns.

    Here's what happens under the hood:
    - 🧾 **Data Loading**: It pulls sales records based on year/week columns and builds a clean weekly time series.
    - 🔄 **Seasonal Modeling**: It fits SARIMAX models using different seasonal cycles (e.g. 1, 13, or 52 weeks).
    - 📊 **Forecasting**: It predicts future sales for each seasonal pattern for the number of weeks you choose.
    - 📈 **Trend Specification**: Choose whether to include an intercept (drift), a linear time trend, both, or none.
    - 🎨 **Visualization**: It overlays the actual data and forecasts, and optionally highlights holiday weeks.

    Trend options:
    - **n**: No trend component
    - **c**: Constant only (intercept or drift if differenced)
    - **t**: Linear time trend only
    - **ct**: Constant and linear time trend together
    """
    )

# Sidebar controls
st.sidebar.header("Database Configuration")
db_url = st.sidebar.text_input("Database URL", value=db.get_db_url(), type="password")
schema = st.sidebar.text_input("Schema", value="staging_lincoln_liquors")
table = st.sidebar.text_input("Table", value="weekly_sales")

lags = st.sidebar.multiselect(
    "Seasonal Lags (weeks)", [1, 2, 4, 13, 26, 52], default=[52]
)
forecast_horizon = st.sidebar.slider("Forecast Horizon (weeks)", 4, 52, 12)

# Trend specification control
st.sidebar.header("Trend Specification")
trend = st.sidebar.selectbox(
    "Model Trend Component",
    options=["n", "c", "t", "ct"],
    index=1,
    format_func=lambda x: {
        "n": "No trend",
        "c": "Constant only",
        "t": "Time trend only",
        "ct": "Constant + Time trend",
    }[x],
    help="Select trend term: 'n' none, 'c' intercept/drift, 't' time slope, 'ct' both",
)

# Holiday highlight options
st.sidebar.header("Holiday Highlights")
current_year = datetime.now().year
us_holidays = holidays.country_holidays("US", years=[current_year - 1, current_year])
holiday_names = sorted(set(us_holidays.values()))
selected_holidays = st.sidebar.multiselect(
    "Select Holidays to Highlight", holiday_names
)

# Assign vibrant colors
color_map = cm.get_cmap("tab10", len(selected_holidays))
holiday_colors = {
    name: mcolors.to_hex(color_map(i)) for i, name in enumerate(selected_holidays)
}

# Run forecast
if st.sidebar.button("Run Forecast"):
    if not db_url:
        st.error("Please enter a valid database URL.")
    else:
        with st.spinner("Generating forecasts..."):
            engine = create_engine(db_url)
            actual, seasonal_forecasts, predicted_mean = sf.generate_seasonal_forecasts(
                engine=engine,
                schema=schema,
                table=table,
                lags=lags,
                forecast_horizon=forecast_horizon,
                trend=trend,  # pass the chosen trend to model
            )

            # --- Plotting ---
            fig = plt.figure(figsize=(14, 10))
            gs = gridspec.GridSpec(2, 2, height_ratios=[3, 1], hspace=0.4, wspace=0.3)

            # Top: Actual + Forecast Means
            ax_top = fig.add_subplot(gs[0, :])
            last = actual.tail(52)
            ax_top.plot(last.index, last.values, color="black", label="Actual")

            annotated_points = {}
            for lag in lags:
                fc_mean = predicted_mean[lag]
                ax_top.plot(
                    fc_mean.index,
                    fc_mean.values,
                    linestyle="--",
                    marker="o",
                    markersize=4,
                    label=f"{lag}-week Forecast",
                )

                # annotate critical points
                annotated = sf.find_annotated_critical_points(
                    predicted_mean[lag], keep=max(2, round(forecast_horizon / 12))
                )
                annotated_points[lag] = annotated
                for value in annotated:
                    offset_y = 10 if value["type"] == "max" else -10
                    ax_top.annotate(
                        value["label"],
                        xy=(value["timestamp"], value["value"]),
                        xytext=(0, offset_y),
                        textcoords="offset points",
                        ha="center",
                        fontsize=7,
                        color="gray",
                    )

            # Highlight selected holidays
            years = sorted(set(last.index.year))
            for yr in years:
                for name in selected_holidays:
                    for date, holiday_name in us_holidays.items():
                        if holiday_name == name and date.year == yr:
                            week_start = date - pd.Timedelta(days=date.weekday())
                            week_end = week_start + pd.Timedelta(days=6)
                            ax_top.axvspan(
                                week_start,
                                week_end,
                                color=holiday_colors[name],
                                alpha=0.6,
                                label=name if yr == years[0] else "",
                            )

            ax_top.set_title(
                "Weekly Sales Forecast with Seasonal Models, Trend & Holidays"
            )
            ax_top.set_ylabel("Sales Quantity")
            ax_top.legend(loc="upper left", fontsize="small", ncol=2)
            ax_top.grid(True)

            # Bottom-left: ACF
            ax_acf = fig.add_subplot(gs[1, 0])
            plot_acf(actual, lags=52, ax=ax_acf)
            ax_acf.set_title("ACF (Actual Series)")

            # Bottom-right: PACF
            ax_pacf = fig.add_subplot(gs[1, 1])
            plot_pacf(actual, lags=52, ax=ax_pacf)
            ax_pacf.set_title("PACF (Actual Series)")

            st.pyplot(fig)

            st.markdown("### 🔗 Agent Definitions from Critical Points")
            for lag in lags:
                st.markdown(f"**Lag: {lag} weeks**")
                for point in annotated_points.get(lag, []):
                    date_str = point["timestamp"].strftime("%Y-%m-%d")
                    label_str = point.get("label", f"{date_str} ({point['type']})")
                    value = round(point["value"], 2)
                    url = (
                        f"https://yourdomain.com/agent_definition?"
                        f"date={date_str}&value={value}&label={label_str}&type={point['type']}&global={point['is_global']}"
                    )
                    st.markdown(f"- [{label_str} → Agent Definition]({url})")

            st.success("Forecasting complete.")
