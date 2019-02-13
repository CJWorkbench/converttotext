import numpy as np


def render(table, params):
    # if no column has been selected, return table
    if not params['colnames']:
        return table

    columns = params['colnames'].split(',')
    columns = [c.strip() for c in columns]

    # Categories already sanitized to only contain strings
    non_text_columns = (
        table[columns]
        .select_dtypes(exclude=['object', 'category'])
        .columns
    )

    na = table[non_text_columns].isna()
    newdata = table[non_text_columns].astype(str)
    newdata[na] = np.nan
    table[non_text_columns] = newdata

    return table
