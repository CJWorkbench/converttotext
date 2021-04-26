from pathlib import Path

from cjwmodule.spec.testing import param_factory

from converttotext import migrate_params

P = param_factory(Path(__name__).parent.parent / "converttotext.yaml")


def test_v0_no_colnames():
    assert migrate_params({"colnames": ""}) == P(colnames=[])


def test_v0():
    assert migrate_params({"colnames": "A,B"}) == P(colnames=["A", "B"])


def test_v1():
    assert migrate_params({"colnames": ["A", "B"]}) == P(colnames=["A", "B"])
