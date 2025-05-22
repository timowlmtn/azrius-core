# azrius-core

Thank you for your interest in Azrius Core! 
This repository is the core library for the Azrius project, 
which provides a set of tools and utilities for integrating
forecasting into learning models and agentic tools.

It is designed to be used either locally using a Postgres
database or in the cloud using AWS.

## Getting Started

To get started with Azrius Core, you will need to install the
requirements. You can do this using pip:

```bash
pip install -r requirements.txt
```

### Loading the Data

The example data data is from the Iowa Liquor Sales dataset.

The data dictionary can be found here:

https://data.iowa.gov/Sales-Distribution/Iowa-Liquor-Sales/m3tr-qhgy/about_data

1. Download the data from the link above.
2. Run the migration script to create the database and tables.
3. Load the data into the database using Spark load-data target.
4. Run the UI with `make run-ui` or `streamlit run python/src/ui/streamlit_forecast_ui.py`.
