"""
Daily ELT pipeline: EIA grid demand + NLR EV stations -> Snowflake -> dbt.
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

REPO = "/Users/niromikha/ev-grid-pipeline"
PYTHON = "/opt/anaconda3/envs/evpipe/bin/python"
DBT = "/opt/anaconda3/envs/evpipe/bin/dbt"
DBT_DIR = f"{REPO}/dbt_project/ev_grid"

default_args = {
    "owner": "romi",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ev_grid_pipeline",
    description="EIA demand + NLR EV stations -> Snowflake RAW -> dbt marts",
    schedule="0 8 * * *",          # daily 08:00 UTC
    start_date=datetime(2026, 7, 1),
    catchup=False,
    default_args=default_args,
    tags=["elt", "snowflake", "dbt"],
) as dag:

    extract_eia = BashOperator(
        task_id="extract_eia_demand",
        bash_command=(
            f"cd {REPO} && {PYTHON} extract/eia_extract.py "
            "--start {{ macros.ds_add(ds, -1) }} --end {{ macros.ds_add(ds, -1) }}"
        ),
    )

    extract_nlr = BashOperator(
        task_id="extract_nlr_stations",
        bash_command=f"cd {REPO} && {PYTHON} extract/nlr_stations_extract.py",
    )

    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_DIR} && {DBT} source freshness --profiles-dir .",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && {DBT} run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && {DBT} test --profiles-dir .",
    )

    [extract_eia, extract_nlr] >> dbt_freshness >> dbt_build >> dbt_test
