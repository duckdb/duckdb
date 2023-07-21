import duckdb
import pytest
import pandas

# Make sure the variable get's properly reset even in case of error
@pytest.fixture(autouse=True)
def scoped_copy_on_write_setting():
    old_value = pandas.options.mode.copy_on_write
    pandas.options.mode.copy_on_write = True
    yield
    # Reset it at the end of the function
    pandas.options.mode.copy_on_write = old_value
    return

class TestCopyOnWrite(object):
    def test_copy_on_write(self):
        assert pandas.options.mode.copy_on_write == True

        con = duckdb.connect()
        df_in = pandas.DataFrame({'numbers': [1,2,3,4,5],})
        rel = con.sql('select * from df_in')
        res = rel.fetchall()
        assert res == [
            (1,),
            (2,),
            (3,),
            (4,),
            (5,)
        ]
