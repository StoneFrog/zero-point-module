"""dbt project location, shared between definitions.py and assets/gold_dbt.py.

The dbt project lives at `dbt_project/` (repo root, sibling to src/) rather
than under src/energy_pipeline/, so it can be edited and mounted the same way
`src/` is — see docker-compose.yml, which mounts it read-write (dbt needs to
write its own target/ artifacts and scratch DuckDB file there) and runs
`dbt parse` before starting dagster-webserver/dagster-daemon, regenerating
manifest.json from whatever's currently mounted. `@dbt_assets` reads that
manifest at Python-import time, so it must already exist on disk before
Dagster loads definitions.py — this module doesn't generate it itself.

`DbtProject` doesn't take a `profiles_dir` argument (that's a `DbtCliResource`
concern, not a project-location concern) — `profiles.yml` is co-located with
`dbt_project.yml` here, and `DbtCliResource` in definitions.py is told that
explicitly rather than relying on cwd-defaulting.
"""

from pathlib import Path

from dagster_dbt import DbtProject

DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent / "dbt_project"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
