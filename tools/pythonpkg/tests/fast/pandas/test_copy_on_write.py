import duckdb
import pytest

# https://pandas.pydata.org/docs/dev/user_guide/copy_on_write.html
pandas = pytest.importorskip('pandas', '1.5', reason='copy_on_write does not exist in earlier versions')
import datetime


# Make sure the variable get's properly reset even in case of error
@pytest.fixture(autouse=True)
def scoped_copy_on_write_setting():
    old_value = pandas.options.mode.copy_on_write
    pandas.options.mode.copy_on_write = True
    yield
    # Reset it at the end of the function
    pandas.options.mode.copy_on_write = old_value
    return


def convert_to_result(col):
    return [(x,) for x in col]


class TestCopyOnWrite(object):
    @pytest.mark.parametrize(
        'col',
        [
            ['a', 'b', 'this is a long string'],
            [1.2334, None, 234.12],
            [123234, -213123, 2324234],
            [datetime.date(1990, 12, 7), None, datetime.date(1940, 1, 13)],
            [datetime.datetime(2012, 6, 21, 13, 23, 45, 328), None],
        ],
    )
    def test_copy_on_write(self, col):
        assert pandas.options.mode.copy_on_write == True
        con = duckdb.connect()
        df_in = pandas.DataFrame(
            {
                'numbers': col,
            }
        )
        rel = con.sql('select * from df_in')
        res = rel.fetchall()
        print(res)
        expected = convert_to_result(col)
        assert res == expected
