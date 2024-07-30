from typing import Any, Callable, Union, overload

from duckdb import (
    CaseExpression,
    CoalesceOperator,
    ColumnExpression,
    ConstantExpression,
    Expression,
    FunctionExpression,
)

from ..errors import PySparkTypeError
from ..exception import ContributionsAcceptedError
from ._typing import ColumnOrName
from .column import Column, _get_expr


def _invoke_function_over_columns(name: str, *cols: "ColumnOrName") -> Column:
    """
    Invokes n-ary JVM function identified by name
    and wraps the result with :class:`~pyspark.sql.Column`.
    """
    cols = [_to_column_expr(expr) for expr in cols]
    return _invoke_function(name, *cols)


def col(column: str):
    return Column(ColumnExpression(column))


def upper(col: "ColumnOrName") -> Column:
    """
    Converts a string expression to upper case.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        upper case values.

    Examples
    --------
    >>> df = spark.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
    >>> df.select(upper("value")).show()
    +------------+
    |upper(value)|
    +------------+
    |       SPARK|
    |     PYSPARK|
    |  PANDAS API|
    +------------+
    """
    return _invoke_function_over_columns("upper", col)


def when(condition: "Column", value: Any) -> Column:
    if not isinstance(condition, Column):
        raise TypeError("condition should be a Column")
    v = _get_expr(value)
    expr = CaseExpression(condition.expr, v)
    return Column(expr)


def _inner_expr_or_val(val):
    return val.expr if isinstance(val, Column) else val


def struct(*cols: Column) -> Column:
    return Column(
        FunctionExpression("struct_pack", *[_inner_expr_or_val(x) for x in cols])
    )


def lit(col: Any) -> Column:
    return col if isinstance(col, Column) else Column(ConstantExpression(col))


def _invoke_function(function: str, *arguments):
    return Column(FunctionExpression(function, *arguments))


def _to_column_expr(col: ColumnOrName) -> Expression:
    if isinstance(col, Column):
        return col.expr
    elif isinstance(col, str):
        return ColumnExpression(col)
    else:
        raise PySparkTypeError(
            error_class="NOT_COLUMN_OR_STR",
            message_parameters={"arg_name": "col", "arg_type": type(col).__name__},
        )

def regexp_replace(str: "ColumnOrName", pattern: str, replacement: str) -> Column:
    r"""Replace all substrings of the specified string value that match regexp with rep.

    .. versionadded:: 1.5.0

    Examples
    --------
    >>> df = spark.createDataFrame([('100-200',)], ['str'])
    >>> df.select(regexp_replace('str', r'(\d+)', '--').alias('d')).collect()
    [Row(d='-----')]
    """
    return _invoke_function(
        "regexp_replace",
        _to_column_expr(str),
        ConstantExpression(pattern),
        ConstantExpression(replacement),
        ConstantExpression("g"),
    )


def array_contains(col: "ColumnOrName", value: Any) -> Column:
    """
    Collection function: returns null if the array is null, true if the array contains the
    given value, and false otherwise.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        name of column containing array
    value :
        value or column to check for in array

    Returns
    -------
    :class:`~pyspark.sql.Column`
        a column of Boolean type.

    Examples
    --------
    >>> df = spark.createDataFrame([(["a", "b", "c"],), ([],)], ['data'])
    >>> df.select(array_contains(df.data, "a")).collect()
    [Row(array_contains(data, a)=True), Row(array_contains(data, a)=False)]
    >>> df.select(array_contains(df.data, lit("a"))).collect()
    [Row(array_contains(data, a)=True), Row(array_contains(data, a)=False)]
    """
    value = _get_expr(value)
    return _invoke_function("array_contains", _to_column_expr(col), value)


def avg(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the average of the values in a group.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(avg(col("id"))).show()
    +-------+
    |avg(id)|
    +-------+
    |    4.5|
    +-------+
    """
    return _invoke_function_over_columns("avg", col)


def sum(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the sum of all values in the expression.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(sum(df["id"])).show()
    +-------+
    |sum(id)|
    +-------+
    |     45|
    +-------+
    """
    return _invoke_function_over_columns("sum", col)


def max(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the maximum value of the expression in a group.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        column for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(max(col("id"))).show()
    +-------+
    |max(id)|
    +-------+
    |      9|
    +-------+
    """
    return _invoke_function_over_columns("max", col)


def mean(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the average of the values in a group.
    An alias of :func:`avg`.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(mean(df.id)).show()
    +-------+
    |avg(id)|
    +-------+
    |    4.5|
    +-------+
    """
    return _invoke_function_over_columns("mean", col)


def min(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the minimum value of the expression in a group.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        column for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(min(df.id)).show()
    +-------+
    |min(id)|
    +-------+
    |      0|
    +-------+
    """
    return _invoke_function_over_columns("min", col)


def count(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the number of items in a group.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        column for computed results.

    Examples
    --------
    Count by all columns (start), and by a column that does not count ``None``.

    >>> df = spark.createDataFrame([(None,), ("a",), ("b",), ("c",)], schema=["alphabets"])
    >>> df.select(count(expr("*")), count(df.alphabets)).show()
    +--------+----------------+
    |count(1)|count(alphabets)|
    +--------+----------------+
    |       4|               3|
    +--------+----------------+
    """
    return _invoke_function_over_columns("count", col)


@overload
def transform(col: "ColumnOrName", f: Callable[[Column], Column]) -> Column: ...


@overload
def transform(col: "ColumnOrName", f: Callable[[Column, Column], Column]) -> Column: ...


def transform(
    col: "ColumnOrName",
    f: Union[Callable[[Column], Column], Callable[[Column, Column], Column]],
) -> Column:
    """
    Returns an array of elements after applying a transformation to each element in the input array.

    .. versionadded:: 3.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        name of column or expression
    f : function
        a function that is applied to each element of the input array.
        Can take one of the following forms:

        - Unary ``(x: Column) -> Column: ...``
        - Binary ``(x: Column, i: Column) -> Column...``, where the second argument is
            a 0-based index of the element.

        and can use methods of :class:`~pyspark.sql.Column`, functions defined in
        :py:mod:`pyspark.sql.functions` and Scala ``UserDefinedFunctions``.
        Python ``UserDefinedFunctions`` are not supported
        (`SPARK-27052 <https://issues.apache.org/jira/browse/SPARK-27052>`__).

    Returns
    -------
    :class:`~pyspark.sql.Column`
        a new array of transformed elements.

    Examples
    --------
    >>> df = spark.createDataFrame([(1, [1, 2, 3, 4])], ("key", "values"))
    >>> df.select(transform("values", lambda x: x * 2).alias("doubled")).show()
    +------------+
    |     doubled|
    +------------+
    |[2, 4, 6, 8]|
    +------------+

    >>> def alternate(x, i):
    ...     return when(i % 2 == 0, x).otherwise(-x)
    ...
    >>> df.select(transform("values", alternate).alias("alternated")).show()
    +--------------+
    |    alternated|
    +--------------+
    |[1, -2, 3, -4]|
    +--------------+
    """
    raise NotImplementedError


def concat_ws(sep: str, *cols: "ColumnOrName") -> "Column":
    """
    Concatenates multiple input string columns together into a single string column,
    using the given separator.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    sep : str
        words separator.
    cols : :class:`~pyspark.sql.Column` or str
        list of columns to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        string of concatenated words.

    Examples
    --------
    >>> df = spark.createDataFrame([('abcd','123')], ['s', 'd'])
    >>> df.select(concat_ws('-', df.s, df.d).alias('s')).collect()
    [Row(s='abcd-123')]
    """
    cols = [_to_column_expr(expr) for expr in cols]
    return _invoke_function("concat_ws", ConstantExpression(sep), *cols)


def lower(col: "ColumnOrName") -> Column:
    """
    Converts a string expression to lower case.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        lower case values.

    Examples
    --------
    >>> df = spark.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
    >>> df.select(lower("value")).show()
    +------------+
    |lower(value)|
    +------------+
    |       spark|
    |     pyspark|
    |  pandas api|
    +------------+
    """
    return _invoke_function_over_columns("lower", col)


def ceil(col: "ColumnOrName") -> Column:
    """
    Computes the ceiling of the given value.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(ceil(lit(-0.1))).show()
    +----------+
    |CEIL(-0.1)|
    +----------+
    |         0|
    +----------+
    """
    return _invoke_function_over_columns("ceil", col)


def ceiling(col: "ColumnOrName") -> Column:
    return ceil(col)


def floor(col: "ColumnOrName") -> Column:
    """
    Computes the floor of the given value.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column to find floor for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        nearest integer that is less than or equal to given value.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(floor(lit(2.5))).show()
    +----------+
    |FLOOR(2.5)|
    +----------+
    |         2|
    +----------+
    """
    return _invoke_function_over_columns("floor", col)


def abs(col: "ColumnOrName") -> Column:
    """
    Computes the absolute value.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        column for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(abs(lit(-1))).show()
    +-------+
    |abs(-1)|
    +-------+
    |      1|
    +-------+
    """
    return _invoke_function_over_columns("abs", col)


def isnan(col: "ColumnOrName") -> Column:
    """An expression that returns true if the column is NaN.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        True if value is NaN and False otherwise.

    Examples
    --------
    >>> df = spark.createDataFrame([(1.0, float('nan')), (float('nan'), 2.0)], ("a", "b"))
    >>> df.select("a", "b", isnan("a").alias("r1"), isnan(df.b).alias("r2")).show()
    +---+---+-----+-----+
    |  a|  b|   r1|   r2|
    +---+---+-----+-----+
    |1.0|NaN|false| true|
    |NaN|2.0| true|false|
    +---+---+-----+-----+
    """
    return _invoke_function_over_columns("isnan", col)


def isnull(col: "ColumnOrName") -> Column:
    """An expression that returns true if the column is null.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        True if value is null and False otherwise.

    Examples
    --------
    >>> df = spark.createDataFrame([(1, None), (None, 2)], ("a", "b"))
    >>> df.select("a", "b", isnull("a").alias("r1"), isnull(df.b).alias("r2")).show()
    +----+----+-----+-----+
    |   a|   b|   r1|   r2|
    +----+----+-----+-----+
    |   1|NULL|false| true|
    |NULL|   2| true|false|
    +----+----+-----+-----+
    """
    return _invoke_function_over_columns("isnull", col)


def sqrt(col: "ColumnOrName") -> Column:
    """
    Computes the square root of the specified float value.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        column for computed results.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(sqrt(lit(4))).show()
    +-------+
    |SQRT(4)|
    +-------+
    |    2.0|
    +-------+
    """
    return _invoke_function_over_columns("sqrt", col)


def greatest(*cols: "ColumnOrName") -> Column:
    """
    Returns the greatest value of the list of column names, skipping null values.
    This function takes at least 2 parameters. It will return null if all parameters are null.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        columns to check for gratest value.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        gratest value.

    Examples
    --------
    >>> df = spark.createDataFrame([(1, 4, 3)], ['a', 'b', 'c'])
    >>> df.select(greatest(df.a, df.b, df.c).alias("greatest")).collect()
    [Row(greatest=4)]
    """

    if len(cols) < 2:
        raise ValueError("greatest should take at least 2 columns")

    cols = [_to_column_expr(expr) for expr in cols]
    return _invoke_function("greatest", *cols)


def least(*cols: "ColumnOrName") -> Column:
    """
    Returns the least value of the list of column names, skipping null values.
    This function takes at least 2 parameters. It will return null if all parameters are null.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    cols : :class:`~pyspark.sql.Column` or str
        column names or columns to be compared

    Returns
    -------
    :class:`~pyspark.sql.Column`
        least value.

    Examples
    --------
    >>> df = spark.createDataFrame([(1, 4, 3)], ['a', 'b', 'c'])
    >>> df.select(least(df.a, df.b, df.c).alias("least")).collect()
    [Row(least=1)]
    """
    if len(cols) < 2:
        raise ValueError("least should take at least 2 columns")

    cols = [_to_column_expr(expr) for expr in cols]
    return _invoke_function("least", *cols)


def trim(col: "ColumnOrName") -> Column:
    """
    Trim the spaces from left end for the specified string value.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        left trimmed values.

    Examples
    --------
    >>> df = spark.createDataFrame(["   Spark", "Spark  ", " Spark"], "STRING")
    >>> df.select(ltrim("value").alias("r")).withColumn("length", length("r")).show()
    +-------+------+
    |      r|length|
    +-------+------+
    |  Spark|     5|
    |Spark  |     7|
    |  Spark|     5|
    +-------+------+
    """
    return _invoke_function_over_columns("trim", col)


def rtrim(col: "ColumnOrName") -> Column:
    """
    Trim the spaces from right end for the specified string value.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        right trimmed values.

    Examples
    --------
    >>> df = spark.createDataFrame(["   Spark", "Spark  ", " Spark"], "STRING")
    >>> df.select(rtrim("value").alias("r")).withColumn("length", length("r")).show()
    +--------+------+
    |       r|length|
    +--------+------+
    |   Spark|     8|
    |   Spark|     5|
    |   Spark|     6|
    +--------+------+
    """
    return _invoke_function_over_columns("rtrim", col)


def ltrim(col: "ColumnOrName") -> Column:
    """
    Trim the spaces from left end for the specified string value.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        left trimmed values.

    Examples
    --------
    >>> df = spark.createDataFrame(["   Spark", "Spark  ", " Spark"], "STRING")
    >>> df.select(ltrim("value").alias("r")).withColumn("length", length("r")).show()
    +-------+------+
    |      r|length|
    +-------+------+
    |  Spark|     5|
    |Spark  |     7|
    |  Spark|     5|
    +-------+------+
    """
    return _invoke_function_over_columns("ltrim", col)


def length(col: "ColumnOrName") -> Column:
    """Computes the character length of string data or number of bytes of binary data.
    The length of character data includes the trailing spaces. The length of binary data
    includes binary zeros.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        length of the value.

    Examples
    --------
    >>> spark.createDataFrame([('ABC ',)], ['a']).select(length('a').alias('length')).collect()
    [Row(length=4)]
    """
    return _invoke_function_over_columns("length", col)


def coalesce(*cols: "ColumnOrName") -> Column:
    """Returns the first column that is not null.
    .. versionadded:: 1.4.0
    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    Parameters
    ----------
    cols : :class:`~pyspark.sql.Column` or str
        list of columns to work on.
    Returns
    -------
    :class:`~pyspark.sql.Column`
        value of the first column that is not null.
    Examples
    --------
    >>> cDf = spark.createDataFrame([(None, None), (1, None), (None, 2)], ("a", "b"))
    >>> cDf.show()
    +----+----+
    |   a|   b|
    +----+----+
    |NULL|NULL|
    |   1|NULL|
    |NULL|   2|
    +----+----+
    >>> cDf.select(coalesce(cDf["a"], cDf["b"])).show()
    +--------------+
    |coalesce(a, b)|
    +--------------+
    |          NULL|
    |             1|
    |             2|
    +--------------+
    >>> cDf.select('*', coalesce(cDf["a"], lit(0.0))).show()
    +----+----+----------------+
    |   a|   b|coalesce(a, 0.0)|
    +----+----+----------------+
    |NULL|NULL|             0.0|
    |   1|NULL|             1.0|
    |NULL|   2|             0.0|
    +----+----+----------------+
    """

    cols = [_to_column_expr(expr) for expr in cols]
    return Column(CoalesceOperator(*cols))


def nvl(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    """
    Returns `col2` if `col1` is null, or `col1` otherwise.
    .. versionadded:: 3.5.0
    Parameters
    ----------
    col1 : :class:`~pyspark.sql.Column` or str
    col2 : :class:`~pyspark.sql.Column` or str
    Examples
    --------
    >>> df = spark.createDataFrame([(None, 8,), (1, 9,)], ["a", "b"])
    >>> df.select(nvl(df.a, df.b).alias('r')).collect()
    [Row(r=8), Row(r=1)]
    """

    return coalesce(col1, col2)


def ifnull(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    """
    Returns `col2` if `col1` is null, or `col1` otherwise.
    .. versionadded:: 3.5.0
    Parameters
    ----------
    col1 : :class:`~pyspark.sql.Column` or str
    col2 : :class:`~pyspark.sql.Column` or str
    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> df = spark.createDataFrame([(None,), (1,)], ["e"])
    >>> df.select(sf.ifnull(df.e, sf.lit(8))).show()
    +------------+
    |ifnull(e, 8)|
    +------------+
    |           8|
    |           1|
    +------------+
    """
    return coalesce(col1, col2)

 
def md5(col: "ColumnOrName") -> Column:
    """Calculates the MD5 digest and returns the value as a 32 character hex string.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> spark.createDataFrame([('ABC',)], ['a']).select(md5('a').alias('hash')).collect()
    [Row(hash='902fbdd2b1df0c4f70b4a5d23525e932')]
    """
    return _invoke_function_over_columns("md5", col)


def sha2(col: "ColumnOrName", numBits: int) -> Column:
    """Returns the hex string result of SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384,
    and SHA-512). The numBits indicates the desired bit length of the result, which must have a
    value of 224, 256, 384, 512, or 0 (which is equivalent to 256).

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.
    numBits : int
        the desired bit length of the result, which must have a
        value of 224, 256, 384, 512, or 0 (which is equivalent to 256).

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.createDataFrame([["Alice"], ["Bob"]], ["name"])
    >>> df.withColumn("sha2", sha2(df.name, 256)).show(truncate=False)
    +-----+----------------------------------------------------------------+
    |name |sha2                                                            |
    +-----+----------------------------------------------------------------+
    |Alice|3bc51062973c458d5a6f2d8d64a023246354ad7e064b1e4e009ec8a0699a3043|
    |Bob  |cd9fb1e148ccd8442e5aa74904cc73bf6fb54d1d54d333bd596aa9bb4bb4e961|
    +-----+----------------------------------------------------------------+
    """

    if numBits not in {224, 256, 384, 512, 0}:
        raise ValueError("numBits should be one of {224, 256, 384, 512, 0}")

    if numBits == 256:
        return _invoke_function_over_columns("sha256", col)

    raise ContributionsAcceptedError(
        "SHA-224, SHA-384, and SHA-512 are not supported yet."
    )


def curdate() -> Column:
    """
    Returns the current date at the start of query evaluation as a :class:`DateType` column.
    All calls of current_date within the same query return the same value.

    .. versionadded:: 3.5.0

    Returns
    -------
    :class:`~pyspark.sql.Column`
        current date.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.range(1).select(sf.curdate()).show() # doctest: +SKIP
    +--------------+
    |current_date()|
    +--------------+
    |    2022-08-26|
    +--------------+
    """
    return _invoke_function("today")


def current_date() -> Column:
    """
    Returns the current date at the start of query evaluation as a :class:`DateType` column.
    All calls of current_date within the same query return the same value.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        current date.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(current_date()).show() # doctest: +SKIP
    +--------------+
    |current_date()|
    +--------------+
    |    2022-08-26|
    +--------------+
    """
    return curdate()


def now() -> Column:
    """
    Returns the current timestamp at the start of query evaluation.

    .. versionadded:: 3.5.0

    Returns
    -------
    :class:`~pyspark.sql.Column`
        current timestamp at the start of query evaluation.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(now()).show(truncate=False) # doctest: +SKIP
    +-----------------------+
    |now()    |
    +-----------------------+
    |2022-08-26 21:23:22.716|
    +-----------------------+
    """
    return _invoke_function("now")


def date_trunc(format: str, timestamp: "ColumnOrName") -> Column:
    """
    Returns timestamp truncated to the unit specified by the format.

    .. versionadded:: 2.3.0

    Parameters
    ----------
    format : str
        'year', 'yyyy', 'yy', 'month', 'mon', 'mm',
        'day', 'dd', 'hour', 'minute', 'second', 'week', 'quarter'
    timestamp : :class:`~pyspark.sql.Column` or str

    Examples
    --------
    >>> df = spark.createDataFrame([('1997-02-28 05:02:11',)], ['t'])
    >>> df.select(date_trunc('year', df.t).alias('year')).collect()
    [Row(year=datetime.datetime(1997, 1, 1, 0, 0))]
    >>> df.select(date_trunc('mon', df.t).alias('month')).collect()
    [Row(month=datetime.datetime(1997, 2, 1, 0, 0))]
    """
    format = format.lower()
    if format in ["yyyy", "yy"]:
        format = "year"
    elif format in ["mon", "mm"]:
        format = "month"
    elif format == "dd":
        format = "day"
    elif format == "hh":
        format = "hour"
    return _invoke_function_over_columns("date_trunc", lit(format), timestamp)


def date_part(field: "ColumnOrName", source: "ColumnOrName") -> Column:
    """
    Extracts a part of the date/timestamp or interval source.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    field : :class:`~pyspark.sql.Column` or str
        selects which part of the source should be extracted, and supported string values
        are as same as the fields of the equivalent function `extract`.
    source : :class:`~pyspark.sql.Column` or str
        a date/timestamp or interval column from where `field` should be extracted.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        a part of the date/timestamp or interval source.

    Examples
    --------
    >>> import datetime
    >>> df = spark.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ['ts'])
    >>> df.select(
    ...     date_part(lit('YEAR'), 'ts').alias('year'),
    ...     date_part(lit('month'), 'ts').alias('month'),
    ...     date_part(lit('WEEK'), 'ts').alias('week'),
    ...     date_part(lit('D'), 'ts').alias('day'),
    ...     date_part(lit('M'), 'ts').alias('minute'),
    ...     date_part(lit('S'), 'ts').alias('second')
    ... ).collect()
    [Row(year=2015, month=4, week=15, day=8, minute=8, second=Decimal('15.000000'))]
    """
    return _invoke_function_over_columns("date_part", field, source)


def extract(field: "ColumnOrName", source: "ColumnOrName") -> Column:
    """
    Extracts a part of the date/timestamp or interval source.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    field : :class:`~pyspark.sql.Column` or str
        selects which part of the source should be extracted.
    source : :class:`~pyspark.sql.Column` or str
        a date/timestamp or interval column from where `field` should be extracted.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        a part of the date/timestamp or interval source.

    Examples
    --------
    >>> import datetime
    >>> df = spark.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ['ts'])
    >>> df.select(
    ...     extract(lit('YEAR'), 'ts').alias('year'),
    ...     extract(lit('month'), 'ts').alias('month'),
    ...     extract(lit('WEEK'), 'ts').alias('week'),
    ...     extract(lit('D'), 'ts').alias('day'),
    ...     extract(lit('M'), 'ts').alias('minute'),
    ...     extract(lit('S'), 'ts').alias('second')
    ... ).collect()
    [Row(year=2015, month=4, week=15, day=8, minute=8, second=Decimal('15.000000'))]
    """
    return date_part(field, source)


def datepart(field: "ColumnOrName", source: "ColumnOrName") -> Column:
    """
    Extracts a part of the date/timestamp or interval source.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    field : :class:`~pyspark.sql.Column` or str
        selects which part of the source should be extracted, and supported string values
        are as same as the fields of the equivalent function `extract`.
    source : :class:`~pyspark.sql.Column` or str
        a date/timestamp or interval column from where `field` should be extracted.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        a part of the date/timestamp or interval source.

    Examples
    --------
    >>> import datetime
    >>> df = spark.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ['ts'])
    >>> df.select(
    ...     datepart(lit('YEAR'), 'ts').alias('year'),
    ...     datepart(lit('month'), 'ts').alias('month'),
    ...     datepart(lit('WEEK'), 'ts').alias('week'),
    ...     datepart(lit('D'), 'ts').alias('day'),
    ...     datepart(lit('M'), 'ts').alias('minute'),
    ...     datepart(lit('S'), 'ts').alias('second')
    ... ).collect()
    [Row(year=2015, month=4, week=15, day=8, minute=8, second=Decimal('15.000000'))]
    """
    return date_part(field, source)


def year(col: "ColumnOrName") -> Column:
    """
    Extract the year of a given date/timestamp as integer.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date/timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        year part of the date/timestamp as integer.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(year('dt').alias('year')).collect()
    [Row(year=2015)]
    """
    return _invoke_function_over_columns("year", col)


def quarter(col: "ColumnOrName") -> Column:
    """
    Extract the quarter of a given date/timestamp as integer.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date/timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        quarter of the date/timestamp as integer.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(quarter('dt').alias('quarter')).collect()
    [Row(quarter=2)]
    """
    return _invoke_function_over_columns("quarter", col)


def month(col: "ColumnOrName") -> Column:
    """
    Extract the month of a given date/timestamp as integer.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date/timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        month part of the date/timestamp as integer.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(month('dt').alias('month')).collect()
    [Row(month=4)]
    """
    return _invoke_function_over_columns("month", col)


def dayofweek(col: "ColumnOrName") -> Column:
    """
    Extract the day of the week of a given date/timestamp as integer.
    Ranges from 1 for a Sunday through to 7 for a Saturday

    .. versionadded:: 2.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date/timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        day of the week for given date/timestamp as integer.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(dayofweek('dt').alias('day')).collect()
    [Row(day=4)]
    """
    return _invoke_function_over_columns("dayofweek", col) + lit(1)


def day(col: "ColumnOrName") -> Column:
    """
    Extract the day of the month of a given date/timestamp as integer.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date/timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        day of the month for given date/timestamp as integer.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(day('dt').alias('day')).collect()
    [Row(day=8)]
    """
    return _invoke_function_over_columns("day", col)


def dayofmonth(col: "ColumnOrName") -> Column:
    """
    Extract the day of the month of a given date/timestamp as integer.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date/timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        day of the month for given date/timestamp as integer.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(dayofmonth('dt').alias('day')).collect()
    [Row(day=8)]
    """
    return day(col)


def dayofyear(col: "ColumnOrName") -> Column:
    """
    Extract the day of the year of a given date/timestamp as integer.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date/timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        day of the year for given date/timestamp as integer.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(dayofyear('dt').alias('day')).collect()
    [Row(day=98)]
    """
    return _invoke_function_over_columns("dayofyear", col)


def hour(col: "ColumnOrName") -> Column:
    """
    Extract the hours of a given timestamp as integer.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date/timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        hour part of the timestamp as integer.

    Examples
    --------
    >>> import datetime
    >>> df = spark.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ['ts'])
    >>> df.select(hour('ts').alias('hour')).collect()
    [Row(hour=13)]
    """
    return _invoke_function_over_columns("hour", col)


def minute(col: "ColumnOrName") -> Column:
    """
    Extract the minutes of a given timestamp as integer.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date/timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        minutes part of the timestamp as integer.

    Examples
    --------
    >>> import datetime
    >>> df = spark.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ['ts'])
    >>> df.select(minute('ts').alias('minute')).collect()
    [Row(minute=8)]
    """
    return _invoke_function_over_columns("minute", col)


def second(col: "ColumnOrName") -> Column:
    """
    Extract the seconds of a given date as integer.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date/timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        `seconds` part of the timestamp as integer.

    Examples
    --------
    >>> import datetime
    >>> df = spark.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ['ts'])
    >>> df.select(second('ts').alias('second')).collect()
    [Row(second=15)]
    """
    return _invoke_function_over_columns("second", col)


def weekofyear(col: "ColumnOrName") -> Column:
    """
    Extract the week number of a given date as integer.
    A week is considered to start on a Monday and week 1 is the first week with more than 3 days,
    as defined by ISO 8601

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        `week` of the year for given date as integer.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(weekofyear(df.dt).alias('week')).collect()
    [Row(week=15)]
    """
    return _invoke_function_over_columns("weekofyear", col)
