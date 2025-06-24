import duckdb
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

pa = pytest.importorskip('pyarrow')


def test_5547():
    num_rows = 2**17 + 1

    tbl = pa.Table.from_pandas(
        pd.DataFrame.from_records(
            [
                dict(
                    id=i,
                    nested=dict(
                        a=i,
                    ),
                )
                for i in range(num_rows)
            ]
        )
    )

    con = duckdb.connect()
    expected = tbl.to_pandas()
    result = con.execute(
        """
		SELECT * FROM tbl
    """
    ).df()

    assert_frame_equal(expected, result)

    con.close()
