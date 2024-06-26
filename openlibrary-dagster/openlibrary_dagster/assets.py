from dagster import AssetExecutionContext, SourceAsset
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
from dlt import pipeline
from dlt_sources.openlibrary import openlibrary_source

from .constants import dbt_manifest_path

@dlt_assets(
    dlt_source=openlibrary_source(),
    dlt_pipeline=pipeline(
        pipeline_name="openlibrary",
        dataset_name="openlibrary_data",
        destination="duckdb",
    ),
    name="openlibrary",
    group_name="openlibrary"
)
def dagster_openlibrary_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    
    yield from dlt.run(context=context)

openlibrary_source_assets = [
    SourceAsset(key, group_name="openlibrary") for key in dagster_openlibrary_assets.dependency_keys
]

@dbt_assets(manifest=dbt_manifest_path)
def openlibrary_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()