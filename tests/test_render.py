import datetime
from pathlib import Path

import pyarrow as pa
from cjwmodule.arrow.testing import assert_result_equals, make_column, make_table
from cjwmodule.arrow.types import ArrowRenderResult
from cjwmodule.spec.testing import param_factory

from converttotext import render_arrow_v1 as render

P = param_factory(Path(__name__).parent.parent / "converttotext.yaml")


def test_default_params_does_nothing():
    assert_result_equals(
        render(make_table(make_column("A", [1, 2, 3])), P()),
        ArrowRenderResult(make_table(make_column("A", [1, 2, 3]))),
    )


def test_convert_str_does_nothing():
    assert_result_equals(
        render(make_table(make_column("A", ["x"])), P(colnames=["A"])),
        ArrowRenderResult(make_table(make_column("A", ["x"]))),
    )


def test_convert_int():
    assert_result_equals(
        render(
            make_table(
                make_column("A", [1, 2, None], format="{:.2f}"),
                make_column("B", [2, 3, None], format="{:d}"),
            ),
            P(colnames=["A", "B"]),
        ),
        ArrowRenderResult(
            make_table(
                make_column("A", ["1.00", "2.00", None]),
                make_column("B", ["2", "3", None]),
            )
        ),
    )


def test_convert_float():
    assert_result_equals(
        render(
            make_table(
                make_column("A", [1.1, 2.888, None], format="{:.2f}"),
                make_column("B", [2.2, 3.7, None], format="{:d}"),
            ),
            P(colnames=["A", "B"]),
        ),
        ArrowRenderResult(
            make_table(
                make_column("A", ["1.10", "2.89", None]),
                make_column("B", ["2", "3", None]),
            )
        ),
    )


def test_convert_numbers_all_null():
    assert_result_equals(
        render(make_table(make_column("A", [None], pa.float64())), P(colnames=["A"])),
        ArrowRenderResult(make_table(make_column("A", [None], pa.utf8()))),
    )


def test_convert_no_arrays():
    assert_result_equals(
        render(
            pa.Table.from_batches(
                [],
                pa.schema(
                    [
                        pa.field("A", pa.float64(), metadata={"format": "{:,d}"}),
                        pa.field("B", pa.timestamp("ns"), metadata={"format": "{:,d}"}),
                    ]
                ),
            ),
            P(colnames=["A", "B"]),
        ),
        ArrowRenderResult(
            pa.Table.from_batches(
                [], pa.schema([pa.field("A", pa.utf8()), pa.field("B", pa.utf8())])
            )
        ),
    )


def test_convert_date():
    assert_result_equals(
        render(
            make_table(
                make_column(
                    "A",
                    [datetime.date(2018, 3, 1), datetime.date(2020, 12, 1), None],
                    unit="month",
                )
            ),
            P(colnames=["A"]),
        ),
        ArrowRenderResult(make_table(make_column("A", ["2018-03", "2020-12", None]))),
    )


def test_convert_timestamp():
    assert_result_equals(
        render(
            make_table(
                make_column(
                    "A",
                    [
                        datetime.datetime(2018, 1, 2, 3, 4),
                        datetime.datetime(2020, 1, 2),
                        None,
                    ],
                )
            ),
            P(colnames=["A"]),
        ),
        ArrowRenderResult(
            make_table(make_column("A", ["2018-01-02T03:04Z", "2020-01-02", None]))
        ),
    )


def test_dictionary_no_op():
    assert_result_equals(
        render(
            make_table(make_column("A", ["a", "b"], dictionary=True)),
            P(colnames=["A"]),
        ),
        ArrowRenderResult(make_table(make_column("A", ["a", "b"], dictionary=True))),
    )
