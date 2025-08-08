# ruff: noqa: E402
import warnings

import dagster as dg

warnings.filterwarnings("ignore", category=dg._utils.warnings.BetaWarning)
warnings.filterwarnings("ignore", category=dg.PreviewWarning)

from pathlib import Path

from dagster_slurm_example import defs as example_defs
from dagster_slurm_example.defs import ray as ray_defs
from dagster_slurm_example.defs import shell as shell_defs
from dagster_slurm_example.resources import get_resources_for_deployment


@dg.definitions
def defs():
    resource_defs = get_resources_for_deployment()

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
                group_name="ray_example",
            ),
        ]
    )

    all_asset_checks = [*dg.load_asset_checks_from_package_module(example_defs)]

    all_assets = dg.link_code_references_to_git(
        assets_defs=all_assets,
        git_url="https://github.com/ascii-supply-networks/dagster-slurm",
        git_branch="main",
        file_path_mapping=dg.AnchorBasedFilePathMapping(
            local_file_anchor=Path(__file__).parent,
            file_anchor_path_in_repository="dagster-slurm/examples/dagster_slurm_example",
        ),
    )

    return dg.Definitions(
        assets=[
            *all_assets,
        ],
        asset_checks=[*all_asset_checks],
        resources=resource_defs,
    )
