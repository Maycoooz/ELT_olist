ELT Olist — Brazilian E-Commerce Pipeline
An end-to-end ELT pipeline using the Olist Brazilian E-Commerce dataset from Kaggle.
Tech Stack
Tool	Purpose
Supabase (PostgreSQL)	Source system — holds raw CSV data
Meltano	Extract from Supabase, load into BigQuery
BigQuery	Data warehouse
dbt	Transformations, testing, star schema
Great Expectations	Raw layer data quality validation
Dagster	Orchestration — daily automated pipeline
All of the above are installed in a single conda environment — see Setup below.
---
Part 1 — Setup
Prerequisites
Anaconda or Miniconda installed
A Google Cloud Platform account with billing enabled (BigQuery has a generous free tier)
A free Supabase account — Meltano extracts from a live PostgreSQL source, so this is required, not optional
---
Step 1 — Clone the repo
```bash
git clone <repo-url>
cd ELT_olist
```
Step 2 — Create and activate the conda environment
This single environment includes everything: dbt, Great Expectations, Dagster, and all Python dependencies.
```bash
conda env create -f m2-environment.yml
conda activate m2
```
> `great_expectations/ge_olist_raw.py` connects to BigQuery via SQLAlchemy, which needs one extra package not listed in `m2-environment.yml`. If you hit `NoSuchModuleError: Can't load plugin: sqlalchemy.dialects:bigquery` when running GE, install it manually:
> ```bash
> pip install sqlalchemy-bigquery
> ```
Step 3 — Create your own GCP project and BigQuery datasets
This repo was built against a specific GCP project (`olist-498903`). To run it yourself:
Create a new project in the GCP Console
Enable the BigQuery API for that project
Create a service account with the BigQuery Data Editor and BigQuery Job User roles, and download its JSON key
Create two empty BigQuery datasets named exactly `olist_raw` and `olist_transformed` — these names are hardcoded throughout the dbt sources, the GE script, and the Dagster assets, so using different names will break the pipeline
Save the JSON key file somewhere on your machine outside the repo (e.g. `~/.gcp/olist-key.json`).
Step 4 — Point every script at your own project and key file
Seven files hardcode the original project ID and key file path. Update each to match what you created in Step 3:
File	What to update
`olist_transform/profiles.yml`	`project:`, `keyfile:`
`olist_transform/models/staging/sources.yml`	`database:` (under the `olist_raw` source)
`olist_meltano/meltano.yml`	`project:`, `credentials_path:`
`report/generate_dashboard.py`	`PROJECT`, `KEY_FILE`
`great_expectations/ge_olist_raw.py`	`PROJECT`, `KEYFILE`
`olist_dagster/olist_dagster/assets/pipeline.py`	`BQ_PROJECT`, `KEYFILE`
`olist_meltano/notebook/olist_analysis.ipynb`	`PROJECT`, `KEY_FILE` (in the first config cell)
Step 5 — Load the raw data via Meltano
The pipeline is built around Meltano as the extraction step — there is no supported path that skips it.
Create a free Supabase project
Load the 9 Olist CSVs into PostgreSQL tables in the `public` schema, using the original Kaggle file names as table names
Copy `olist_meltano/.env.example` to `olist_meltano/.env` and fill in your Supabase connection string
Run:
```bash
cd olist_meltano
meltano run tap-postgres target-bigquery
```
Step 6 — Build the dbt project
```bash
cd olist_transform
dbt deps
dbt parse
dbt build
```
`dbt build` runs every model and its tests in dependency order — staging, then marts. `dbt parse` generates the manifest that Dagster needs to recognise the project; run it once after any model change before starting Dagster.
Step 7 — Set up environment variables for alerts
Create a `.env` file at the repo root (never commit this file):
```
GMAIL_ADDRESS=your-gmail@gmail.com
GMAIL_APP_PASSWORD=your-app-password
ALERT_RECIPIENT=your-gmail@gmail.com
```
Generate a Gmail App Password at: Google Account → Security → 2-Step Verification → App Passwords.
Step 8 — Install and start Dagster
```bash
cd ../olist_dagster
pip install -e ".[dev]"
dagster dev
```
Open `http://localhost:3000` — you should see all 8 pipeline assets.
Step 9 (optional) — Run the analysis notebook
```bash
cd ~/ELT_olist   # repo root — adjust if Dagster left you in olist_dagster/
conda activate m2
jupyter notebook olist_meltano/notebook/olist_analysis.ipynb
```
Kernel → Restart & Run All to refresh every chart and number against your own BigQuery data.
---
Part 2 — Documentation
> Looking for the project presentation? View it live at `https://<github-username>.github.io/ELT_olist/olist_slide_deck.html`
📁 Project Structure
```
ELT_olist/
├── m2-environment.yml          # Conda environment (includes all dependencies)
├── olist_meltano/              # Meltano EL pipeline (Supabase → BigQuery)
│   ├── meltano.yml
│   ├── .env.example
│   └── notebook/
│       └── olist_analysis.ipynb   # Exploratory analysis notebook
├── olist_transform/            # dbt transformation layer
│   ├── profiles.yml
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── snapshots/
│   │   └── snap_dim_sellers.sql   # SCD Type 2 seller history
│   └── models/
│       ├── staging/            # 8 staging views (one per source table)
│       └── marts/              # 10 mart models (star schema + analytical marts)
├── great_expectations/         # Raw layer data quality validation
│   └── ge_olist_raw.py
├── olist_dagster/              # Dagster orchestration
│   ├── olist_dagster/
│   │   ├── assets/pipeline.py  # 8 pipeline assets
│   │   └── definitions.py
│   └── olist_dagster_tests/
│       └── test_assets.py      # Smoke test — verifies all 8 assets load correctly
├── report/
│   └── generate_dashboard.py   # Generates docs/index.html from BigQuery marts
└── docs/
    ├── index.html              # Auto-generated dashboard (GitHub Pages)
    └── customers.json          # Customer-level data for dashboard's interactive table
```
🗂️ Data Model
The raw data consists of 9 tables loaded into BigQuery under the `olist_raw` dataset.
Staging Layer (`olist_transformed_staging`)
8 views — one per source table. Light cleaning only: type casting, column renaming, zip code padding (Brazilian CEP codes are always 5 digits), and product category translation joined into `stg_products`.
Marts Layer (`olist_transformed_marts`)
Model	Description	Sample Questions
`dim_customers`	Customers enriched with lat/lng from geolocation	Customer distribution by state, repeat vs one-time buyers
`dim_products`	Products with English category name, photos, dimensions	Best performing categories, does photo count affect sales?
`dim_sellers`	Sellers enriched with lat/lng — reads from `snap_dim_sellers` (active record only)	Seller distribution by state, top sellers by revenue
`dim_sellers_history`	Full SCD Type 2 history of seller location changes	Point-in-time seller attribution
`fact_orders`	One row per order item. PK: `order_item_sk`. Includes `delivery_days`, `is_late`	Revenue by month/category/state, late delivery rate
`fact_reviews`	One row per review with `sentiment` derived from `review_score`	Average score by seller/product, delivery vs rating correlation
`mart_seller_health`	Composite seller health score (40% review, 35% on-time, 25% delivery rate) with 90-day trend window	Early warning for declining sellers
`mart_customer_summary`	One row per `customer_unique_id` with order history and churn segment	Repeat purchase rate, days since last order
`mart_rfm_scores`	RFM segmentation with deterministic NTILE scoring and campaign assignment	Champions, at-risk, lost segments
`mart_cohort_retention`	Monthly cohort retention rates	When do customers disengage?
Joining the marts
Join	Column to use
`fact_orders` → `dim_customers`	`customer_id`
`fact_orders` → `dim_products`	`product_id`
`fact_orders` → `dim_sellers`	`seller_id`
`fact_reviews` → `fact_orders`	`order_id`
`fact_reviews` → `dim_customers`	`customer_id`
> `order_item_sk` is the row identifier for `fact_orders` — use it as the PK in tests, not as a join key.
Date-agnostic design
Every mart that needs a reference point for "now" (`mart_customer_summary`, `mart_rfm_scores`, `mart_seller_health`) calls a single macro, `current_analysis_date()`, instead of `CURRENT_DATE()`. By default it resolves to `MAX(order_purchase_timestamp) + 1 day` from `fact_orders` — necessary here since the dataset is historical (2016–2018) and `CURRENT_DATE()` would put every customer hundreds of days into "recency."
For point-in-time analysis or to simulate a live pipeline, override it:
```bash
dbt build --vars 'analysis_date: 2018-08-01'
```
No model code changes are needed — this is what makes the pipeline ready to swap from historical data to a live feed.
Seller history — SCD Type 2
`snap_dim_sellers` is a dbt snapshot watching `zip_code`, `city`, and `state` on `stg_sellers`. Each run, if any of those fields changed since the last snapshot, the old record is closed (`dbt_valid_to` set) and a new active record is inserted. `dim_sellers` reads only the current record (`dbt_valid_to IS NULL`); `dim_sellers_history` exposes the full timeline, enabling point-in-time attribution of any historical sale to the seller's location at the time of the order.
Deterministic RFM scoring
`mart_rfm_scores.sql` originally produced different RFM segment counts on every `dbt build`, even on identical data. `NTILE(5)` has no defined behaviour for tied values — when multiple customers shared the same `recency_days`, `frequency`, or `monetary` value, BigQuery could assign them to different buckets across runs. We confirmed this by building twice in a row and seeing `champions` shift by dozens of customers with no underlying data change.
The fix was a secondary sort key on all three window functions:
```sql
NTILE(5) OVER (ORDER BY recency_days DESC, customer_unique_id) AS recency_score
```
Since `customer_unique_id` is unique per row, it breaks every tie deterministically — the value itself carries no business meaning, it's purely there to make the sort stable. Two consecutive `dbt build` runs on the same data now produce identical segment counts. We audited the rest of the marts layer for the same pattern and found no other window functions ranking or bucketing rows without a fully unique `ORDER BY`.
---
🔄 Meltano — Extract & Load
Meltano extracts all 9 source tables from Supabase PostgreSQL and loads them into BigQuery `olist_raw`.
Replication mode: FULL_TABLE with overwrite
We use `overwrite: true` in `target-bigquery`, which replaces the entire raw table on every run rather than appending. This keeps `olist_raw` clean and prevents row accumulation across runs.
Why not INCREMENTAL?
Meltano's INCREMENTAL mode requires a replication key — a column in the source table that increases monotonically with each change (e.g. `updated_at`). The Olist source tables were loaded from static Kaggle CSVs and have no such business timestamp. The only available candidate, `_sdc_sequence`, is a metadata field stamped by `tap-postgres` at extraction time — it does not exist in the source schema and cannot be used as a replication key.
---
✅ Great Expectations — Data Quality Validation
`ge_olist_raw.py` validates all 9 raw tables before dbt transformation. It runs 7 suites covering 44 structural expectations (not_null, unique, accepted_values, row count bounds, value ranges) plus 20 cross-table anomaly checks (orphan records, time inversions, duplicate reviews, geolocation coverage gaps).
If any of the 44 GE suite expectations fail, the script exits with code 1 — Dagster treats this as an asset failure and halts the pipeline before dbt runs. The anomaly checks are informational and do not halt the pipeline.
To run manually:
```bash
conda activate m2
cd ~/ELT_olist
python great_expectations/ge_olist_raw.py
```
---
⚡ Dagster — Orchestration
Dagster orchestrates the full pipeline as 8 assets connected via `deps=[]`. If any asset fails, all downstream assets are skipped automatically.
```
meltano_extract_load
        ↓
ge_raw_validation
        ↓
   dbt_staging
        ↓
  dbt_snapshot
        ↓
   dbt_marts
        ↓
   ┌────┴────────────────┐
   ↓                      ↓
generate_dashboard   alert_declining_sellers
   ↓
git_push_dashboard
```
Asset	What it does
`meltano_extract_load`	tap-postgres → BigQuery `olist_raw`
`ge_raw_validation`	GE gate — halts pipeline if any expectation fails
`dbt_staging`	`dbt build --select staging` (includes dbt tests)
`dbt_snapshot`	`dbt snapshot` — SCD Type 2 for `snap_dim_sellers`
`dbt_marts`	`dbt build --select marts` (includes dbt tests)
`alert_declining_sellers`	Emails alert if any seller `trend_status != 'stable'`
`generate_dashboard`	`python report/generate_dashboard.py` → `docs/index.html` + `docs/customers.json`
`git_push_dashboard`	`git add` + commit + push → GitHub Pages auto-deploys
`alert_declining_sellers` and `generate_dashboard` both depend only on `dbt_marts` and run in parallel — a failure in one does not block the other.
Schedule: daily at SGT 02:00 (UTC 18:00).
A smoke test (`olist_dagster_tests/test_assets.py`) verifies all 8 asset names load correctly:
```bash
cd olist_dagster
pytest
```
---
🌐 Dashboard
The pipeline auto-generates `docs/index.html` (and the supporting `docs/customers.json` data file) and pushes both to GitHub Pages on every run.
To enable GitHub Pages: repo Settings → Pages → Source: `main` branch, folder: `/docs`.
Live dashboard: `https://<github-username>.github.io/ELT_olist/`
> `docs/customers.json` is whitelisted in `.gitignore` (`!docs/customers.json`) since the repo otherwise ignores `*.json` files — this is required for the dashboard's interactive customer table to load correctly on GitHub Pages.
---
📓 Analysis Notebook
`olist_meltano/notebook/olist_analysis.ipynb` queries the same `olist_transformed_marts` tables for deeper, ad-hoc analysis — RFM segmentation, seller health distribution, cohort retention, and more. It is not part of the automated pipeline; run it manually whenever you want a fresh view of the current data.
---
Part 3 — Future Enhancements
🔐 Move credentials and project config out of source files
Project ID, dataset names, and key file paths are currently hardcoded across seven files (see Step 4). The cleaner fix is to centralise all of it in `.env` and have every script read from environment variables instead:
```
GCP_PROJECT=your-project-id
GCP_RAW_DATASET=olist_raw
GCP_TRANSFORMED_DATASET=olist_transformed
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your-key.json
```
`GOOGLE_APPLICATION_CREDENTIALS` is the standard variable Google's client libraries look for automatically — using it would remove the need for a `KEY_FILE` / `KEYFILE` constant in every script entirely. dbt's `profiles.yml` already supports `env_var()` for the same purpose. This would take a clone-and-run repo from "edit seven files" down to "edit one `.env`."
⏱️ Speed up Great Expectations
`ge_olist_raw.py` pulls every raw table fully into a pandas DataFrame (`SELECT * FROM ...`) before validating — on the full Olist dataset this takes several minutes, almost entirely in the BigQuery-to-pandas transfer rather than the checks themselves. Running the expectations directly against BigQuery via a SQL-native GE datasource (instead of round-tripping through pandas) would cut this down significantly, at the cost of a more complex GE configuration.
📦 Reduce dashboard payload size
`docs/customers.json`, used to power the dashboard's interactive customer table, is roughly 11 MB at current data volume and will keep growing as more orders accrue. Paginating the data server-side (or pre-aggregating to only the fields the table actually displays) would keep page-load times reasonable as the dataset scales beyond the current ~96K customers.
🔁 Add retry logic to the Dagster pipeline
None of the 8 assets currently retry on transient failures (e.g. a momentary BigQuery timeout or a flaky Meltano sync). Dagster supports per-op `RetryPolicy` natively — wrapping `meltano_extract_load` and the dbt-running assets with a short retry window would make the nightly schedule more resilient without masking genuine data quality failures.
🔌 Alternative ingestion approaches
The project currently uses Meltano because it's open-source, runs locally, and uses the Singer tap ecosystem with zero licensing cost — a good fit for this scope. A natural next step would be evaluating managed alternatives (Fivetran, Airbyte Cloud) for production deployment, where their managed reliability guarantees, schema drift handling, and reduced operational overhead may outweigh the cost of a paid connector.
