import datetime
import tempfile
import unittest
from pathlib import Path
from typing import Dict, NamedTuple, Optional

import pyarrow as pa
from converttotext import migrate_params, render


class RenderColumn(NamedTuple):
    name: str
    type: str = "text"
    format: Optional[str] = None


class MigrateParamsTest(unittest.TestCase):
    def test_v0_no_colnames(self):
        self.assertEqual(migrate_params({"colnames": ""}), {"colnames": []})

    def test_v0(self):
        self.assertEqual(migrate_params({"colnames": "A,B"}), {"colnames": ["A", "B"]})

    def test_v1(self):
        self.assertEqual(
            migrate_params({"colnames": ["A", "B"]}), {"colnames": ["A", "B"]}
        )


def call_render(table, params, columns: Dict[str, RenderColumn]) -> pa.Table:
    with tempfile.NamedTemporaryFile(suffix=".arrow") as tf:
        output_path = Path(tf.name)
        errors = render(table, params, output_path, columns=columns)
        assert errors == []
        with pa.ipc.open_file(output_path) as reader:
            return reader.read_all()


def assert_arrow_table_equal(actual, expected):
    if isinstance(expected, dict):
        expected = pa.table(expected)
    assert actual.shape == expected.shape
    assert actual.column_names == expected.column_names
    assert [c.type for c in actual.columns] == [c.type for c in expected.columns]
    assert actual.to_pydict() == expected.to_pydict()


class RenderTest(unittest.TestCase):
    def test_NOP(self):
        # should NOP when first applied
        result = call_render(
            pa.table({"A": [0.006]}),
            {"colnames": []},
            [RenderColumn("A", "number", "{:.2f}")],
        )
        assert_arrow_table_equal(result, {"A": [0.006]})

    def test_convert_str(self):
        result = call_render(
            pa.table({"A": ["a"]}),
            {"colnames": ["A"]},
            [RenderColumn("A", "text", None)],
        )
        assert_arrow_table_equal(result, {"A": ["a"]})

    def test_convert_int(self):
        result = call_render(
            pa.table({"A": [1, 2], "B": [2, 3]}),
            {"colnames": ["A", "B"]},
            [
                RenderColumn("A", "number", "{:.2f}"),
                RenderColumn("B", "number", "{:d}"),
            ],
        )
        assert_arrow_table_equal(result, {"A": ["1.00", "2.00"], "B": ["2", "3"]})

    def test_convert_float(self):
        result = call_render(
            pa.table({"A": [1.111], "B": [2.6]}),
            {"colnames": ["A", "B"]},
            [
                RenderColumn("A", "number", "{:.2f}"),
                RenderColumn("B", "number", "{:d}"),
            ],
        )
        assert_arrow_table_equal(result, {"A": ["1.11"], "B": ["2"]})

    def test_convert_numbers_all_null(self):
        result = call_render(
            pa.table({"A": pa.array([None, None], pa.float64())}),
            {"colnames": ["A"]},
            [RenderColumn("A", "number", "{:d}")],
        )
        assert_arrow_table_equal(result, {"A": pa.array([None, None], pa.utf8())})

    def test_convert_datetime(self):
        result = call_render(
            pa.table(
                {
                    "A": pa.array(
                        [
                            datetime.datetime(2018, 1, 2, 3, 4),
                            datetime.datetime(2020, 1, 2),
                        ],
                        pa.timestamp("ns"),
                    ),
                },
            ),
            {"colnames": ["A"]},
            [RenderColumn("A", "datetime", None)],
        )
        assert_arrow_table_equal(result, {"A": ["2018-01-02T03:04Z", "2020-01-02"]})

    def test_convert_null(self):
        result = call_render(
            pa.table({"A": [1, None]}),
            {"colnames": ["A"]},
            [RenderColumn("A", "number", "{:,d}")],
        )
        assert_arrow_table_equal(result, {"A": ["1", None]})
