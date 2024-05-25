import inspect

import duckdb


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
        ):
            functions_to_test.append((name, func))
    assert (
        len(functions_to_test) > 20
    ), "Something seems wrong with the discovery of the functions."

    for name, func in functions_to_test:
        parameters = inspect.signature(func).parameters
        dummy_values = [f"param{i}" for i in range(len(parameters))]

        output = func(*dummy_values)

        expected_output = duckdb.FunctionExpression(name, *dummy_values)
        assert output == expected_output
