import pyarrow as pa
from cjwmodule.arrow.format import (
    format_date_array,
    format_number_array,
    format_timestamp_array,
    parse_number_format,
)
from cjwmodule.arrow.types import ArrowRenderResult


def format_chunked_array(
    chunked_array: pa.ChunkedArray, field: pa.Field
) -> pa.ChunkedArray:
    if pa.types.is_integer(field.type) or pa.types.is_floating(field.type):
        nf = parse_number_format(field.metadata[b"format"].decode("utf-8"))
        format_array = lambda chunk: format_number_array(chunk, nf)
    elif pa.types.is_timestamp(field.type):
        format_array = format_timestamp_array
    elif pa.types.is_date32(field.type):
        format_array = lambda chunk: format_date_array(
            chunk, field.metadata[b"unit"].decode("utf-8")
        )
    else:
        format_array = lambda chunk: chunk

    return pa.chunked_array(
        [format_array(chunk) for chunk in chunked_array.chunks], pa.utf8()
    )


def render_arrow_v1(table: pa.Table, params, **kwargs):
    todo = frozenset(params["colnames"])

    for i, colname in enumerate(table.column_names):
        if colname not in todo:
            continue

        table = table.set_column(
            i, colname, format_chunked_array(table.column(i), table.schema.field(i))
        )

    return ArrowRenderResult(table)


def _migrate_params_v0_to_v1(params):
    """v0: colnames is comma-separated str; v1: colnames is str."""
    return {"colnames": [c for c in params["colnames"].split(",") if c]}


def migrate_params(params):
    if isinstance(params["colnames"], str):
        params = _migrate_params_v0_to_v1(params)
    return params
