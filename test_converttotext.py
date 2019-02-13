import unittest
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
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
        expected = pd.DataFrame({'A': [1, 2]})
        result = render(expected, {'colnames': ''})
        self.assertIs(result, expected)

    def test_convert_str(self):
        result = render(pd.DataFrame({'A': ['a', 'b']}), {'colnames': 'A'})
        expected = pd.DataFrame({'A': ['a', 'b']})
        assert_frame_equal(result, expected)

    def test_convert_int(self):
        result = render(pd.DataFrame({'A': [1, 2]}), {'colnames': 'A'})
        expected = pd.DataFrame({'A': ['1', '2']})
        assert_frame_equal(result, expected)

    def test_convert_float(self):
        result = render(pd.DataFrame({'A': [1, 2.1]}), {'colnames': 'A'})
        expected = pd.DataFrame({'A': ['1.0', '2.1']})
        assert_frame_equal(result, expected)

    def test_convert_datetime(self):
        result = render(pd.DataFrame({
            'A': [np.datetime64('2018-01-01'),
                  np.datetime64('2019-02-13')],
        }), {'colnames': 'A'})
        expected = pd.DataFrame({'A': ['2018-01-01', '2019-02-13']})
        assert_frame_equal(result, expected)

    def test_convert_null(self):
        result = render(pd.DataFrame({
            'A': [1, np.nan]
        }), {'colnames': 'A'})
        assert_frame_equal(result, pd.DataFrame({'A': ['1.0', np.nan]}))


if __name__ == '__main__':
    unittest.main()
