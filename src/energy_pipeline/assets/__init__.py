"""Dagster assets — software-defined datasets.

Each asset is a Python function that produces a piece of data. Dagster derives
the lineage graph from how assets reference one another via the `deps` arg or
positional inputs. Materializing an asset = running its function and persisting
its output.

Layers (medallion):
- bronze: raw API responses, stored byte-for-byte. Immutable.
- silver: parsed, typed, deduped, stored as Iceberg.
- gold: business-ready aggregates over silver, stored as Iceberg.
"""
