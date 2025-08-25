import dagster as dg
from dagster_slurm_example import defs


def test_project_loads():
    # will raise errors if the project can't load
    # similar to loading a failing project in dagster-webserver
    # prevents fatal error in dagster-webserver
    # implied_repo = defs.get_repository_def()
    # implied_repo.load_all_definitions()
    actual_defs = defs()
    dg.Definitions.validate_loadable(actual_defs)
