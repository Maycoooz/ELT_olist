from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from olist_dagster.assets import pipeline

all_assets = load_assets_from_modules([pipeline])

olist_job = define_asset_job(
    name="olist_pipeline_job",
    selection=AssetSelection.all(),
)

# SGT 02:00 = UTC 18:00
olist_schedule = ScheduleDefinition(
    job=olist_job,
    cron_schedule="0 18 * * *",
)

defs = Definitions(
    assets=all_assets,
    schedules=[olist_schedule],
)
