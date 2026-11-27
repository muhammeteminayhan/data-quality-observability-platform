"""Data contracts: the single source of truth for what each dataset promises.

A contract is a YAML file in ``contracts/``, one per dataset. Everything the
platform asserts about a dataset - freshness windows, volume baselines, the
expected schema, the quality rules and the scoring weights - comes from that
file.

The engine reads them dynamically. No dataset name, column name, threshold or
rule appears in the Python: adding ``contracts/<name>.yml`` is the only step
needed to start monitoring a new dataset.
"""
