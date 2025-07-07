Project: CoinGecko ETL with Delta Lake

This project performs an ELT (Extract, Load, Transform) pipeline on cryptocurrency data obtained from the public CoinGecko API, storing the data in Delta Lake format using a partitioned structure.

📂 Project Structure

elt_coingecko/
│
├── main.py                       # Main script to orchestrate the pipeline
├── extract.py                    # Data extraction from the CoinGecko API
├── process_markets.py           # Transformation and loading of market data
├── process_coinlist.py          # Transformation and loading of coin list data
├── schema.py                    # Schema and column/type definitions
├── delta_utils.py               # Helper functions to save/upsert to Delta Lake
├── utils/
│   └── data_validation.py       # Column validation and conversion for datasets
├── datalake/
│   ├── bronze/                  # Raw partitioned data
│   └── silver/                  # Transformed data for analysis
└── state/                       # Last run control
    └── last_extraction.json






⚙️ Features

Extraction

/coins/list: Static reference data for cryptocurrencies (FULL extraction)

/markets: Market data (price, volume, etc.) per coin (INCREMENTAL extraction)

Transformation (Silver Layer)

Validation of required columns (REQUIRED_COLUMNS)

Column typing (TYPE_MAP)

Null value imputation (IMPUTATION_MAP)

Duplicate removal using primary key: id, last_updated

Metric aggregation (e.g., average price per coin)

Derived flags (e.g., is_high_value if price exceeds $50,000)

🧪 Pipeline Execution

python main.py

The script performs:

Check the time since the last run (every 1 hour)

Extraction and storage in bronze

Processing and saving to silver

🪙 Delta Lake Partitions

Markets: coin, date, day, hour

Coinlist: date, hour

📌 Requirements (requirements.txt)

Make sure the following packages are installed:

pandas

pyarrow

deltalake

requests

✨ Possible Improvements

Unit tests for processing functions

Scheduling with Airflow

Enrichment with more APIs

Dashboard visualizations

📅 Last Run

The state of the last run is saved in state/last_extraction.json, used to control incremental extractions.

Implementation: The project must be implemented to run periodically every hour or less.

Author: German Rodriguez. Academic project for Data Engineer Certification.

