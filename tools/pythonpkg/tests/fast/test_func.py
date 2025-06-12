import inspect

import pandas as pd
from pandas.api.types import is_datetime64_dtype
import duckdb
from duckdb import ColumnExpression, ConstantExpression


def test_func():
    """Test all functions defined in func.py if they return the expected
    FunctionExpression.
    """
    functions_to_test = []
    for name, func in duckdb.func.__dict__.items():
        if (
            callable(func)
            # Only check the ones which are defined in func.py, not callables
            # which are imported from other modules.
            and func.__module__ == "duckdb.func"
            and name != "_UndefinedType"
        ):
            functions_to_test.append((name, func))
    assert len(functions_to_test) > 100, "Something seems wrong with the discovery of the functions."

    for name, func in functions_to_test:
        parameters = inspect.signature(func).parameters
        dummy_values = [f"param{i}" for i in range(len(parameters))]

        output = func(*dummy_values)

        expected_output = duckdb.FunctionExpression(name, *dummy_values)
        assert output == expected_output


def test_aliases():
    """Test that aliases for SQL function names also have a Python representation."""
    # We just test it for list_inner_product which has an alias list_dot_product.
    assert hasattr(duckdb.func, "list_inner_product")
    assert hasattr(duckdb.func, "list_dot_product")


def test_example_query():
    """Tests a few functions to see if we get the expected output. We can't exhaustively
    test all functions, as we'd need to know the exact input arguments and expected outputs
    for all of them.
    """

    input_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"], "k": [4, 3, 2, 1]})
    rel = duckdb.from_df(input_df)

    rel = rel.project(
        duckdb.func.string_split(ColumnExpression("j"), ConstantExpression("r")).alias("j_split"),
        "i",
        duckdb.func.greatest(ColumnExpression("i"), ColumnExpression("k")).alias("greatest_i_k"),
        duckdb.func.make_timestamp(ConstantExpression(2021)),
        duckdb.func.make_timestamp(
            ConstantExpression(2021),
            ConstantExpression(12),
            ColumnExpression("i"),
            ConstantExpression(10),
            ConstantExpression(30),
            ConstantExpression(0),
        ).alias("ts"),
    )
    output_df = rel.df()

    expected_df = pd.DataFrame.from_dict(
        {
            "j_split": [
                ["one"],
                ["two"],
                ["th", "ee"],
                ["fou", ""],
            ],
            "i": [1, 2, 3, 4],
            "greatest_i_k": [4, 3, 3, 4],
            "make_timestamp(2021)": [
                pd.Timestamp("1970-01-01 00:00:00.002021"),
                pd.Timestamp("1970-01-01 00:00:00.002021"),
                pd.Timestamp("1970-01-01 00:00:00.002021"),
                pd.Timestamp("1970-01-01 00:00:00.002021"),
            ],
            "ts": [
                pd.Timestamp("2021-12-01 10:30:00"),
                pd.Timestamp("2021-12-02 10:30:00"),
                pd.Timestamp("2021-12-03 10:30:00"),
                pd.Timestamp("2021-12-04 10:30:00"),
            ],
        }
    )

    # Convert both datetime columns to string to make the comparison with Pandas easier
    # as they can have different precisions such as [us] or [ns]
    for col in ("make_timestamp(2021)", "ts"):
        assert is_datetime64_dtype(output_df[col])
        assert is_datetime64_dtype(expected_df[col])
        output_df[col] = output_df[col].astype(str)
        expected_df[col] = expected_df[col].astype(str)

    # Compare as strings as much easier
    assert output_df.to_json() == expected_df.to_json()
