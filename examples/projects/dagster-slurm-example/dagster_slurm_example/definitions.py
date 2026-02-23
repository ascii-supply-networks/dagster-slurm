# ruff: noqa: E402
import warnings

import dagster as dg
from dagster._utils import warnings as dagster_warnings

warnings.filterwarnings("ignore", category=dagster_warnings.BetaWarning)
warnings.filterwarnings("ignore", category=dagster_warnings.PreviewWarning)

import os
from pathlib import Path
from typing import Any

import metaxy as mx
import metaxy.ext.dagster as mxd

from . import defs as example_defs
from .defs import metaxy as metaxy_defs
from .defs import ray as ray_defs
from .defs import shell as shell_defs
from .resources import get_resources


@dg.definitions
def defs():
    mx.init()

    compute_resource_defs = get_resources()
    resource_defs: dict[str, Any] = dict(compute_resource_defs)

    deployment = os.getenv("DAGSTER_DEPLOYMENT", "development")
    deployment_lc = deployment.lower()

    # Per-example metaxy stores (see examples/metaxy.toml)
    store_simple = mxd.MetaxyStoreFromConfigResource(name="simple")
    ray_store_name = (
        "ray_embeddings_prod"
        if (
            "supercomputer" in deployment_lc
            or "staging" in deployment_lc
            or "production" in deployment_lc
            or "prod" in deployment_lc
        )
        else "ray_embeddings_dev"
    )
    store_ray = mxd.MetaxyStoreFromConfigResource(name=ray_store_name)

    # Example 3 demonstrates env switching: DuckDB in dev, DeltaLake in prod
    docling_store_name = (
        "docling_prod"
        if (
            "supercomputer" in deployment_lc
            or "staging" in deployment_lc
            or "production" in deployment_lc
            or "prod" in deployment_lc
        )
        else "docling_dev"
    )
    store_docling = mxd.MetaxyStoreFromConfigResource(name=docling_store_name)

    resource_defs["store_simple"] = store_simple
    resource_defs["store_ray"] = store_ray
    resource_defs["store_docling"] = store_docling
    resource_defs["metaxy_io_manager"] = mxd.MetaxyIOManager(store=store_simple)

    all_assets = dg.with_source_code_references(
        [
            *dg.load_assets_from_package_module(
                shell_defs,
                automation_condition=dg.AutomationCondition.eager()
                | dg.AutomationCondition.on_missing(),
                group_name="shell_example",
            ),
            *dg.load_assets_from_package_module(
                ray_defs,
                automation_condition=dg.AutomationCondition.eager()
                | dg.AutomationCondition.on_missing(),
            ),
            *dg.load_assets_from_package_module(
                metaxy_defs,
                automation_condition=dg.AutomationCondition.eager()
                | dg.AutomationCondition.on_missing(),
            ),
        ]
    )

    all_asset_checks = [*dg.load_asset_checks_from_package_module(example_defs)]  # ty: ignore[invalid-argument-type]

    all_assets = dg.link_code_references_to_git(
        assets_defs=all_assets,
        git_url="https://github.com/ascii-supply-networks/dagster-slurm",
        git_branch="main",
        file_path_mapping=dg.AnchorBasedFilePathMapping(
            local_file_anchor=Path(__file__).parent,
            file_anchor_path_in_repository="examples/dagster_slurm_example",
        ),
    )

    return dg.Definitions(
        assets=[
            *all_assets,
        ],
        asset_checks=[*all_asset_checks],
        resources=resource_defs,
    )
