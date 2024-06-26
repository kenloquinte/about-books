import os

from dagster import Definitions
from dagster_dbt import DbtCliResource
from dagster_embedded_elt.dlt import DagsterDltResource

from openlibrary_dagster.assets import dagster_openlibrary_assets, openlibrary_dbt_assets

from .constants import dbt_project_dir

from openlibrary_dagster import assets
from dagster import load_assets_from_modules

dlt_resource = DagsterDltResource()

defs = Definitions(
    assets=[*load_assets_from_modules([assets])],
    resources={
        "dlt": dlt_resource,
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
)