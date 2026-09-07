"""dbt renders YAML `description:` fields through Jinja before parsing them.

That is not obvious from reading the file, and it has already cost a code
location: a `{{ source(...) }}` written into a description as documentation
was evaluated as code, `dbt parse` failed, and the only visible symptom was
the Dagster gRPC server timing out after 180s with no mention of dbt at all.
Anything Jinja-shaped in these files has to sit inside {% raw %}, so this
walks the project and says so directly instead of leaving the next reader to
rediscover it through that timeout.
"""

import re
from pathlib import Path

import pytest

DBT_MODELS_DIR = Path(__file__).resolve().parents[1] / "dbt_project" / "models"

# Non-greedy, DOTALL: {% raw %} blocks span lines in _sources.yml.
RAW_BLOCK = re.compile(r"\{%-?\s*raw\s*-?%\}.*?\{%-?\s*endraw\s*-?%\}", re.DOTALL)
JINJA_EXPRESSION = re.compile(r"\{\{.*?\}\}", re.DOTALL)


def _yaml_files() -> list[Path]:
    return sorted(DBT_MODELS_DIR.rglob("*.yml"))


def test_the_project_actually_has_yaml_to_check():
    """Guards against this whole module passing because rglob found nothing."""
    assert _yaml_files()


@pytest.mark.parametrize("path", _yaml_files(), ids=lambda p: p.name)
def test_yaml_carries_no_unescaped_jinja(path):
    stripped = RAW_BLOCK.sub("", path.read_text())
    leaked = JINJA_EXPRESSION.findall(stripped)
    assert not leaked, (
        f"{path.name} has Jinja outside {{% raw %}}: {leaked}. dbt evaluates "
        "these fields, so this fails `dbt parse` and the code location will "
        "not load."
    )


def test_dbt_window_hours_default_matches_the_python_definition():
    """The one list that has to exist in two languages, pinned together.

    WINDOW_HOURS drives the Iceberg publish in assets/gold_windows.py and is
    handed to dbt per run via --vars, so at runtime Python wins. But
    dbt_project.yml's default is what a bare `dbt build` uses — the way this
    project is run by hand, and the way both gold tests are exercised — so a
    drift between them means the model, its coverage test, and the published
    table can each be talking about a different set of windows while every one
    of them passes.

    dbt can't import the Python value, so this is the seam that holds them
    together.
    """
    import yaml  # ships with dbt-core; not otherwise a runtime dependency

    from energy_pipeline.assets.gold_windows import WINDOW_HOURS

    project = yaml.safe_load(
        (DBT_MODELS_DIR.parent / "dbt_project.yml").read_text()
    )
    assert project["vars"]["window_hours"] == list(WINDOW_HOURS)
