import pyarrow
from cjwmodule.arrow.format import (
    format_number_array,
    format_timestamp_array,
    parse_number_format,
)


def render(arrow_table, params, output_path, *, columns, **kwargs):
    data = dict(zip(arrow_table.column_names, arrow_table.columns))
    columns = {c.name: c for c in columns}
    for colname in params["colnames"]:
        chunked_array = data[colname]
        if pyarrow.types.is_integer(chunked_array.type) or pyarrow.types.is_floating(
            chunked_array.type
        ):
            fn = parse_number_format(columns[colname].type.format)
            data[colname] = pyarrow.chunked_array(
                [format_number_array(chunk, fn) for chunk in chunked_array.chunks],
                pyarrow.utf8(),
            )
        elif pyarrow.types.is_timestamp(chunked_array.type):
            data[colname] = pyarrow.chunked_array(
                [format_timestamp_array(chunk) for chunk in chunked_array.chunks],
                pyarrow.utf8(),
            )
        else:
            pass

    table = pyarrow.table(data)

    with pyarrow.ipc.RecordBatchFileWriter(output_path, table.schema) as writer:
        writer.write_table(table)

    return []  # No errors, ever


def _migrate_params_v0_to_v1(params):
    """v0: colnames is comma-separated str; v1: colnames is str."""
    return {"colnames": [c for c in params["colnames"].split(",") if c]}


def migrate_params(params):
    if isinstance(params["colnames"], str):
        params = _migrate_params_v0_to_v1(params)
    return params
