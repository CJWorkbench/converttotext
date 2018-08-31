import unittest
import pandas as pd
from converttotext import render
import numpy as np

class TestConvertText(unittest.TestCase):

    def setUp(self):
        self.table = pd.DataFrame([
            [99,   100,   '2018', None],
            [99,   100,   '2018', None]],
            columns=['intcol', 'floatcol', 'datecol', 'nullcol'])

        self.table['intcol'] = self.table['intcol'].astype(int)
        self.table['floatcol'] = self.table['floatcol'].astype(float)
        self.table['datecol'] = self.table['datecol'].astype(np.datetime64)
        self.table['nullcol'] = self.table['nullcol'].astype(float)

    def test_NOP(self):
        # should NOP when first applied
        params = {'colnames': ''}
        out = render(self.table.copy(), params)
        self.assertTrue(out.equals(self.table))

    def test_convert_number(self):
        colnames = 'intcol,floatcol'
        params = {'colnames': colnames}
        out = render(self.table, params)
        for y in out['intcol']:
            self.assertTrue(y == '99')
        # For now, just convert whole floats as-is
        for y in out['floatcol']:
            self.assertTrue(y == '100.0')

    def test_convert_datetime(self):
        colnames = 'datecol'
        params = {'colnames': colnames}
        out = render(self.table, params)
        for y in out['datecol']:
            self.assertTrue(y == '2018-01-01')

    def test_convert_null(self):
        colnames = 'nullcol'
        params = {'colnames': colnames}
        out = render(self.table, params)
        for y in out['nullcol']:
            self.assertTrue(y == '')

if __name__ == '__main__':
    unittest.main()


