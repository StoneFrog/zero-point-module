"""The dbt->Dagster translator decides where dbt models land in the graph.

Two overrides carry real weight and neither shows up until a code location
loads: asset keys must stay bare (so the publish assets can `deps=` on them)
and groups must follow the models/ subfolder (so gold models sit with the
gold assets instead of in `default`). The dicts below mirror the shape
dbt writes into target/manifest.json — fqn is
["<project>", "<subfolder>", ..., "<name>"].
"""

from dagster import AssetKey

from energy_pipeline.assets.gold_dbt import _BareNameDbtTranslator

TRANSLATOR = _BareNameDbtTranslator()


def _model(name: str, *folders: str) -> dict:
    return {
        "resource_type": "model",
        "name": name,
        "fqn": ["energy_pipeline", *folders, name],
        "config": {},
        "meta": {},
    }


def test_gold_models_are_grouped_with_the_gold_assets():
    """Both gold models must reach the same group as their publish assets."""
    for name in ("int_gold_prices_daily_stats", "int_gold_cheapest_windows"):
        assert TRANSLATOR.get_group_name(_model(name, "gold")) == "gold"


def test_staging_models_are_grouped_by_their_folder_too():
    assert TRANSLATOR.get_group_name(_model("stg_silver_prices_15min", "staging")) == "staging"


def test_nested_folders_use_the_top_level_one():
    """fqn[1] is the models/ subfolder however deep the model sits below it."""
    assert TRANSLATOR.get_group_name(_model("int_gold_x", "gold", "nested")) == "gold"


def test_resource_sitting_at_the_project_root_falls_back_to_the_default():
    """e.g. tests/assert_gold_daily_stats_bounds.sql, whose fqn has no folder.

    There is no subfolder to name a group after, so the base translator
    decides — which for a node carrying no group metadata means None.
    """
    props = {
        "resource_type": "test",
        "name": "assert_gold_daily_stats_bounds",
        "fqn": ["energy_pipeline", "assert_gold_daily_stats_bounds"],
        "config": {},
        "meta": {},
    }
    assert TRANSLATOR.get_group_name(props) is None


def test_explicit_dbt_group_config_still_wins_at_the_root():
    props = {
        "resource_type": "model",
        "name": "rootlevel",
        "fqn": ["energy_pipeline", "rootlevel"],
        "config": {"group": "explicit"},
        "meta": {},
    }
    assert TRANSLATOR.get_group_name(props) == "explicit"


def test_model_asset_key_is_the_bare_model_name():
    """Single-segment, so the publish assets in gold_dbt.py can deps= on it."""
    assert TRANSLATOR.get_asset_key(_model("int_gold_cheapest_windows", "gold")) == AssetKey(
        "int_gold_cheapest_windows"
    )


def test_source_asset_key_still_comes_from_meta_dagster_asset_key():
    """The one exception to the bare-name rule.

    The `lake.silver_prices_hourly` source has to resolve to the *same* key as
    the Dagster-native silver asset, or gold_dbt_assets records no dependency
    on silver and the two can run in either order. Bare-naming it would yield
    the same string here by luck; going through the base translator is what
    keeps _sources.yml's meta.dagster.asset_key authoritative.
    """
    props = {
        "resource_type": "source",
        "source_name": "lake",
        "name": "silver_prices_hourly",
        "fqn": ["energy_pipeline", "staging", "lake", "silver_prices_hourly"],
        "config": {},
        "meta": {"dagster": {"asset_key": ["silver_prices_hourly"]}},
    }
    assert TRANSLATOR.get_asset_key(props) == AssetKey("silver_prices_hourly")
