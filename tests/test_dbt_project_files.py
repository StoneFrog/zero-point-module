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
