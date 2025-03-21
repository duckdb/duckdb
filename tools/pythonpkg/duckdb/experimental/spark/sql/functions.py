import warnings
from typing import Any, Callable, Union, overload, Optional, List, Tuple, TYPE_CHECKING

from duckdb import (
    CaseExpression,
    CoalesceOperator,
    ColumnExpression,
    ConstantExpression,
    Expression,
    FunctionExpression,
    LambdaExpression,
    SQLExpression,
)
if TYPE_CHECKING:
    from .dataframe import DataFrame

from ..errors import PySparkTypeError
from ..exception import ContributionsAcceptedError
from ._typing import ColumnOrName
from .column import Column, _get_expr
from . import types as _types


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


def ucase(str: "ColumnOrName") -> Column:
    """
    Returns `str` with all characters changed to uppercase.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        Input column or strings.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.range(1).select(sf.ucase(sf.lit("Spark"))).show()
    +------------+
    |ucase(Spark)|
    +------------+
    |       SPARK|
    +------------+
    """
    return upper(str)


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


def array(
    *cols: Union["ColumnOrName", Union[List["ColumnOrName"], Tuple["ColumnOrName", ...]]]
) -> Column:
    """Creates a new array column.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    cols : :class:`~pyspark.sql.Column` or str
        column names or :class:`~pyspark.sql.Column`\\s that have
        the same data type.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        a column of array type.

    Examples
    --------
    >>> df = spark.createDataFrame([("Alice", 2), ("Bob", 5)], ("name", "age"))
    >>> df.select(array('age', 'age').alias("arr")).collect()
    [Row(arr=[2, 2]), Row(arr=[5, 5])]
    >>> df.select(array([df.age, df.age]).alias("arr")).collect()
    [Row(arr=[2, 2]), Row(arr=[5, 5])]
    >>> df.select(array('age', 'age').alias("col")).printSchema()
    root
     |-- col: array (nullable = false)
     |    |-- element: long (containsNull = true)
    """
    if len(cols) == 1 and isinstance(cols[0], (list, set)):
        cols = cols[0]
    return _invoke_function_over_columns("list_value", *cols)


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


def slice(
    x: "ColumnOrName", start: Union["ColumnOrName", int], length: Union["ColumnOrName", int]
) -> Column:
    """
    Collection function: returns an array containing all the elements in `x` from index `start`
    (array indices start at 1, or from the end if `start` is negative) with the specified `length`.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    x : :class:`~pyspark.sql.Column` or str
        column name or column containing the array to be sliced
    start : :class:`~pyspark.sql.Column` or str or int
        column name, column, or int containing the starting index
    length : :class:`~pyspark.sql.Column` or str or int
        column name, column, or int containing the length of the slice

    Returns
    -------
    :class:`~pyspark.sql.Column`
        a column of array type. Subset of array.

    Examples
    --------
    >>> df = spark.createDataFrame([([1, 2, 3],), ([4, 5],)], ['x'])
    >>> df.select(slice(df.x, 2, 2).alias("sliced")).collect()
    [Row(sliced=[2, 3]), Row(sliced=[5])]
    """
    start = ConstantExpression(start) if isinstance(start, int) else _to_column_expr(start)
    length = ConstantExpression(length) if isinstance(length, int) else _to_column_expr(length)

    end = start + length

    return _invoke_function("list_slice", _to_column_expr(x), start, end)


def asc(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the ascending order of the given column name.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the ascending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    Sort by the column 'id' in the descending order.

    >>> df = spark.range(5)
    >>> df = df.sort(desc("id"))
    >>> df.show()
    +---+
    | id|
    +---+
    |  4|
    |  3|
    |  2|
    |  1|
    |  0|
    +---+

    Sort by the column 'id' in the ascending order.

    >>> df.orderBy(asc("id")).show()
    +---+
    | id|
    +---+
    |  0|
    |  1|
    |  2|
    |  3|
    |  4|
    +---+
    """
    return Column(_to_column_expr(col)).asc()


def asc_nulls_first(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the ascending order of the given
    column name, and null values return before non-null values.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the ascending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    >>> df1 = spark.createDataFrame([(1, "Bob"),
    ...                              (0, None),
    ...                              (2, "Alice")], ["age", "name"])
    >>> df1.sort(asc_nulls_first(df1.name)).show()
    +---+-----+
    |age| name|
    +---+-----+
    |  0| NULL|
    |  2|Alice|
    |  1|  Bob|
    +---+-----+

    """
    return asc(col).nulls_first()


def asc_nulls_last(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the ascending order of the given
    column name, and null values appear after non-null values.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the ascending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    >>> df1 = spark.createDataFrame([(0, None),
    ...                              (1, "Bob"),
    ...                              (2, "Alice")], ["age", "name"])
    >>> df1.sort(asc_nulls_last(df1.name)).show()
    +---+-----+
    |age| name|
    +---+-----+
    |  2|Alice|
    |  1|  Bob|
    |  0| NULL|
    +---+-----+

    """
    return asc(col).nulls_last()


def desc(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the descending order of the given column name.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the descending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    Sort by the column 'id' in the descending order.

    >>> spark.range(5).orderBy(desc("id")).show()
    +---+
    | id|
    +---+
    |  4|
    |  3|
    |  2|
    |  1|
    |  0|
    +---+
    """
    return Column(_to_column_expr(col)).desc()


def desc_nulls_first(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the descending order of the given
    column name, and null values appear before non-null values.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the descending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    >>> df1 = spark.createDataFrame([(0, None),
    ...                              (1, "Bob"),
    ...                              (2, "Alice")], ["age", "name"])
    >>> df1.sort(desc_nulls_first(df1.name)).show()
    +---+-----+
    |age| name|
    +---+-----+
    |  0| NULL|
    |  1|  Bob|
    |  2|Alice|
    +---+-----+

    """
    return desc(col).nulls_first()


def desc_nulls_last(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the descending order of the given
    column name, and null values appear after non-null values.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the descending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    >>> df1 = spark.createDataFrame([(0, None),
    ...                              (1, "Bob"),
    ...                              (2, "Alice")], ["age", "name"])
    >>> df1.sort(desc_nulls_last(df1.name)).show()
    +---+-----+
    |age| name|
    +---+-----+
    |  1|  Bob|
    |  2|Alice|
    |  0| NULL|
    +---+-----+

    """
    return desc(col).nulls_last()


def left(str: "ColumnOrName", len: "ColumnOrName") -> Column:
    """
    Returns the leftmost `len`(`len` can be string type) characters from the string `str`,
    if `len` is less or equal than 0 the result is an empty string.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        Input column or strings.
    len : :class:`~pyspark.sql.Column` or str
        Input column or strings, the leftmost `len`.

    Examples
    --------
    >>> df = spark.createDataFrame([("Spark SQL", 3,)], ['a', 'b'])
    >>> df.select(left(df.a, df.b).alias('r')).collect()
    [Row(r='Spa')]
    """
    len = _to_column_expr(len)
    return Column(
        CaseExpression(len <= ConstantExpression(0), ConstantExpression("")).otherwise(
            FunctionExpression(
                "array_slice", _to_column_expr(str), ConstantExpression(0), len
            )
        )
    )


def right(str: "ColumnOrName", len: "ColumnOrName") -> Column:
    """
    Returns the rightmost `len`(`len` can be string type) characters from the string `str`,
    if `len` is less or equal than 0 the result is an empty string.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        Input column or strings.
    len : :class:`~pyspark.sql.Column` or str
        Input column or strings, the rightmost `len`.

    Examples
    --------
    >>> df = spark.createDataFrame([("Spark SQL", 3,)], ['a', 'b'])
    >>> df.select(right(df.a, df.b).alias('r')).collect()
    [Row(r='SQL')]
    """
    len = _to_column_expr(len)
    return Column(
        CaseExpression(len <= ConstantExpression(0), ConstantExpression("")).otherwise(
            FunctionExpression(
                "array_slice", _to_column_expr(str), -len, ConstantExpression(-1)
            )
        )
    )


def levenshtein(
    left: "ColumnOrName", right: "ColumnOrName", threshold: Optional[int] = None
) -> Column:
    """Computes the Levenshtein distance of the two given strings.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    left : :class:`~pyspark.sql.Column` or str
        first column value.
    right : :class:`~pyspark.sql.Column` or str
        second column value.
    threshold : int, optional
        if set when the levenshtein distance of the two given strings
        less than or equal to a given threshold then return result distance, or -1

        .. versionchanged: 3.5.0
            Added ``threshold`` argument.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        Levenshtein distance as integer value.

    Examples
    --------
    >>> df0 = spark.createDataFrame([('kitten', 'sitting',)], ['l', 'r'])
    >>> df0.select(levenshtein('l', 'r').alias('d')).collect()
    [Row(d=3)]
    >>> df0.select(levenshtein('l', 'r', 2).alias('d')).collect()
    [Row(d=-1)]
    """
    distance = _invoke_function_over_columns("levenshtein", left, right)
    if threshold is None:
        return distance
    else:
        distance = _to_column_expr(distance)
        return Column(CaseExpression(distance <= ConstantExpression(threshold), distance).otherwise(ConstantExpression(-1)))


def lpad(col: "ColumnOrName", len: int, pad: str) -> Column:
    """
    Left-pad the string column to width `len` with `pad`.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.
    len : int
        length of the final string.
    pad : str
        chars to prepend.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        left padded result.

    Examples
    --------
    >>> df = spark.createDataFrame([('abcd',)], ['s',])
    >>> df.select(lpad(df.s, 6, '#').alias('s')).collect()
    [Row(s='##abcd')]
    """
    return _invoke_function("lpad", _to_column_expr(col), ConstantExpression(len), ConstantExpression(pad))


def rpad(col: "ColumnOrName", len: int, pad: str) -> Column:
    """
    Right-pad the string column to width `len` with `pad`.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.
    len : int
        length of the final string.
    pad : str
        chars to append.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        right padded result.

    Examples
    --------
    >>> df = spark.createDataFrame([('abcd',)], ['s',])
    >>> df.select(rpad(df.s, 6, '#').alias('s')).collect()
    [Row(s='abcd##')]
    """
    return _invoke_function("rpad", _to_column_expr(col), ConstantExpression(len), ConstantExpression(pad))


def ascii(col: "ColumnOrName") -> Column:
    """
    Computes the numeric value of the first character of the string column.

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
        numeric value.

    Examples
    --------
    >>> df = spark.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
    >>> df.select(ascii("value")).show()
    +------------+
    |ascii(value)|
    +------------+
    |          83|
    |          80|
    |          80|
    +------------+
    """
    return _invoke_function_over_columns("ascii", col)


def asin(col: "ColumnOrName") -> Column:
    """
    Computes inverse sine of the input column.

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
        inverse sine of `col`, as if computed by `java.lang.Math.asin()`

    Examples
    --------
    >>> df = spark.createDataFrame([(0,), (2,)])
    >>> df.select(asin(df.schema.fieldNames()[0])).show()
    +--------+
    |ASIN(_1)|
    +--------+
    |     0.0|
    |     NaN|
    +--------+
    """
    col = _to_column_expr(col)
    # FIXME: ConstantExpression(float("nan")) gives NULL and not NaN
    return Column(CaseExpression((col < -1.0) | (col > 1.0), ConstantExpression(float("nan"))).otherwise(FunctionExpression("asin", col)))


def like(
    str: "ColumnOrName", pattern: "ColumnOrName", escapeChar: Optional["Column"] = None
) -> Column:
    """
    Returns true if str matches `pattern` with `escape`,
    null if any arguments are null, false otherwise.
    The default escape character is the '\'.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        A string.
    pattern : :class:`~pyspark.sql.Column` or str
        A string. See the DuckDB documentation on like_escape for more information.
    escape : :class:`~pyspark.sql.Column`
        An character added since Spark 3.0. The default escape character is the '\'.
        If an escape character precedes a special symbol or another escape character, the
        following character is matched literally. It is invalid to escape any other character.

    Examples
    --------
    >>> df = spark.createDataFrame([("Spark", "_park")], ['a', 'b'])
    >>> df.select(like(df.a, df.b).alias('r')).collect()
    [Row(r=True)]

    >>> df = spark.createDataFrame(
    ...     [("%SystemDrive%/Users/John", "/%SystemDrive/%//Users%")],
    ...     ['a', 'b']
    ... )
    >>> df.select(like(df.a, df.b, lit('/')).alias('r')).collect()
    [Row(r=True)]
    """
    if escapeChar is None:
        escapeChar = ConstantExpression("\\")
    else:
        escapeChar = _to_column_expr(escapeChar)
    return _invoke_function("like_escape", _to_column_expr(str), _to_column_expr(pattern), escapeChar)


def ilike(
    str: "ColumnOrName", pattern: "ColumnOrName", escapeChar: Optional["Column"] = None
) -> Column:
    """
    Returns true if str matches `pattern` with `escape` case-insensitively,
    null if any arguments are null, false otherwise.
    The default escape character is the '\'.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        A string.
    pattern : :class:`~pyspark.sql.Column` or str
        A string. See the DuckDB documentation on ilike_escape for more information.
    escape : :class:`~pyspark.sql.Column`
        An character added since Spark 3.0. The default escape character is the '\'.
        If an escape character precedes a special symbol or another escape character, the
        following character is matched literally. It is invalid to escape any other character.

    Examples
    --------
    >>> df = spark.createDataFrame([("Spark", "_park")], ['a', 'b'])
    >>> df.select(ilike(df.a, df.b).alias('r')).collect()
    [Row(r=True)]

    >>> df = spark.createDataFrame(
    ...     [("%SystemDrive%/Users/John", "/%SystemDrive/%//Users%")],
    ...     ['a', 'b']
    ... )
    >>> df.select(ilike(df.a, df.b, lit('/')).alias('r')).collect()
    [Row(r=True)]
    """
    if escapeChar is None:
        escapeChar = ConstantExpression("\\")
    else:
        escapeChar = _to_column_expr(escapeChar)
    return _invoke_function("ilike_escape", _to_column_expr(str), _to_column_expr(pattern), escapeChar)


def array_agg(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns a list of objects with duplicates.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        list of objects with duplicates.

    Examples
    --------
    >>> df = spark.createDataFrame([[1],[1],[2]], ["c"])
    >>> df.agg(array_agg('c').alias('r')).collect()
    [Row(r=[1, 1, 2])]
    """
    return _invoke_function_over_columns("list", col)


def collect_list(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns a list of objects with duplicates.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    The function is non-deterministic because the order of collected results depends
    on the order of the rows which may be non-deterministic after a shuffle.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        list of objects with duplicates.

    Examples
    --------
    >>> df2 = spark.createDataFrame([(2,), (5,), (5,)], ('age',))
    >>> df2.agg(collect_list('age')).collect()
    [Row(collect_list(age)=[2, 5, 5])]
    """
    return array_agg(col)


def array_append(col: "ColumnOrName", value: Any) -> Column:
    """
    Collection function: returns an array of the elements in col1 along
    with the added element in col2 at the last of the array.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        name of column containing array
    value :
        a literal value, or a :class:`~pyspark.sql.Column` expression.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        an array of values from first array along with the element.

    Notes
    -----
    Supports Spark Connect.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([Row(c1=["b", "a", "c"], c2="c")])
    >>> df.select(array_append(df.c1, df.c2)).collect()
    [Row(array_append(c1, c2)=['b', 'a', 'c', 'c'])]
    >>> df.select(array_append(df.c1, 'x')).collect()
    [Row(array_append(c1, x)=['b', 'a', 'c', 'x'])]
    """
    return _invoke_function("list_append", _to_column_expr(col), _get_expr(value))


def array_insert(
    arr: "ColumnOrName", pos: Union["ColumnOrName", int], value: Any
) -> Column:
    """
    Collection function: adds an item into a given array at a specified array index.
    Array indices start at 1, or start from the end if index is negative.
    Index above array size appends the array, or prepends the array if index is negative,
    with 'null' elements.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    arr : :class:`~pyspark.sql.Column` or str
        name of column containing an array
    pos : :class:`~pyspark.sql.Column` or str or int
        name of Numeric type column indicating position of insertion
        (starting at index 1, negative position is a start from the back of the array)
    value :
        a literal value, or a :class:`~pyspark.sql.Column` expression.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        an array of values, including the new specified value

    Notes
    -----
    Supports Spark Connect.

    Examples
    --------
    >>> df = spark.createDataFrame(
    ...     [(['a', 'b', 'c'], 2, 'd'), (['c', 'b', 'a'], -2, 'd')],
    ...     ['data', 'pos', 'val']
    ... )
    >>> df.select(array_insert(df.data, df.pos.cast('integer'), df.val).alias('data')).collect()
    [Row(data=['a', 'd', 'b', 'c']), Row(data=['c', 'b', 'd', 'a'])]
    >>> df.select(array_insert(df.data, 5, 'hello').alias('data')).collect()
    [Row(data=['a', 'b', 'c', None, 'hello']), Row(data=['c', 'b', 'a', None, 'hello'])]
    """
    pos = _get_expr(pos)
    arr = _to_column_expr(arr)
    # Depending on if the position is positive or not, we need to interpret it differently.
    # This is because negative numbers are relative to the end of the NEW list.
    # For example, if it's -2, it's expected that the inserted value is at the second position
    # in the NEW list.
    pos_is_positive = pos > 0

    list_length_plus_1 = FunctionExpression("add", FunctionExpression("len", arr), 1)

    # If the position is above the list size plus 1, first extend the list with the
    # relevant number of nulls to get the list to the size of (pos - 1).
    list_ = CaseExpression(
        pos > list_length_plus_1,
        FunctionExpression("list_resize", arr, FunctionExpression("subtract", pos, 1)),
    ).otherwise(
        CaseExpression(
            (pos < 0) & (FunctionExpression("abs", pos) > list_length_plus_1),
            FunctionExpression(
                "list_concat",
                FunctionExpression(
                    "list_resize",
                    FunctionExpression("list_value", None),
                    FunctionExpression(
                        "subtract", FunctionExpression("abs", pos), list_length_plus_1
                    ),
                ),
                arr,
            ),
        ).otherwise(arr)
    )

    # We slice the array into two parts, insert the value in between and concatenate the two parts
    # together again.
    return _invoke_function(
        "list_concat",
        FunctionExpression(
            "list_concat",
            # First part of the list
            FunctionExpression(
                "list_slice",
                list_,
                1,
                CaseExpression(
                    pos_is_positive, FunctionExpression("subtract", pos, 1)
                ).otherwise(pos),
            ),
            # Here we insert the value at the specified position
            FunctionExpression("list_value", _get_expr(value)),
        ),
        # The remainder of the list
        FunctionExpression(
            "list_slice",
            list_,
            CaseExpression(pos_is_positive, pos).otherwise(
                FunctionExpression("add", pos, 1)
            ),
            -1,
        ),
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


def array_distinct(col: "ColumnOrName") -> Column:
    """
    Collection function: removes duplicate values from the array.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        name of column or expression

    Returns
    -------
    :class:`~pyspark.sql.Column`
        an array of unique values.

    Examples
    --------
    >>> df = spark.createDataFrame([([1, 2, 3, 2],), ([4, 5, 5, 4],)], ['data'])
    >>> df.select(array_distinct(df.data)).collect()
    [Row(array_distinct(data)=[1, 2, 3]), Row(array_distinct(data)=[4, 5])]
    """
    return _invoke_function_over_columns("array_distinct", col)


def array_intersect(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    """
    Collection function: returns an array of the elements in the intersection of col1 and col2,
    without duplicates.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col1 : :class:`~pyspark.sql.Column` or str
        name of column containing array
    col2 : :class:`~pyspark.sql.Column` or str
        name of column containing array

    Returns
    -------
    :class:`~pyspark.sql.Column`
        an array of values in the intersection of two arrays.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([Row(c1=["b", "a", "c"], c2=["c", "d", "a", "f"])])
    >>> df.select(array_intersect(df.c1, df.c2)).collect()
    [Row(array_intersect(c1, c2)=['a', 'c'])]
    """
    return _invoke_function_over_columns("array_intersect", col1, col2)


def array_union(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    """
    Collection function: returns an array of the elements in the union of col1 and col2,
    without duplicates.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col1 : :class:`~pyspark.sql.Column` or str
        name of column containing array
    col2 : :class:`~pyspark.sql.Column` or str
        name of column containing array

    Returns
    -------
    :class:`~pyspark.sql.Column`
        an array of values in union of two arrays.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([Row(c1=["b", "a", "c"], c2=["c", "d", "a", "f"])])
    >>> df.select(array_union(df.c1, df.c2)).collect()
    [Row(array_union(c1, c2)=['b', 'a', 'c', 'd', 'f'])]
    """
    return _invoke_function_over_columns("array_distinct", _invoke_function_over_columns("array_concat", col1, col2))


def array_max(col: "ColumnOrName") -> Column:
    """
    Collection function: returns the maximum value of the array.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        name of column or expression

    Returns
    -------
    :class:`~pyspark.sql.Column`
        maximum value of an array.

    Examples
    --------
    >>> df = spark.createDataFrame([([2, 1, 3],), ([None, 10, -1],)], ['data'])
    >>> df.select(array_max(df.data).alias('max')).collect()
    [Row(max=3), Row(max=10)]
    """
    return _invoke_function("array_extract", _to_column_expr(_invoke_function_over_columns("array_sort", col)), _get_expr(-1))


def array_min(col: "ColumnOrName") -> Column:
    """
    Collection function: returns the minimum value of the array.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        name of column or expression

    Returns
    -------
    :class:`~pyspark.sql.Column`
        minimum value of array.

    Examples
    --------
    >>> df = spark.createDataFrame([([2, 1, 3],), ([None, 10, -1],)], ['data'])
    >>> df.select(array_min(df.data).alias('min')).collect()
    [Row(min=1), Row(min=-1)]
    """
    return _invoke_function("array_extract", _to_column_expr(_invoke_function_over_columns("array_sort", col)), _get_expr(1))


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


def median(col: "ColumnOrName") -> Column:
    """
    Returns the median of the values in a group.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the median of the values in a group.

    Notes
    -----
    Supports Spark Connect.

    Examples
    --------
    >>> df = spark.createDataFrame([
    ...     ("Java", 2012, 20000), ("dotNET", 2012, 5000),
    ...     ("Java", 2012, 22000), ("dotNET", 2012, 10000),
    ...     ("dotNET", 2013, 48000), ("Java", 2013, 30000)],
    ...     schema=("course", "year", "earnings"))
    >>> df.groupby("course").agg(median("earnings")).show()
    +------+----------------+
    |course|median(earnings)|
    +------+----------------+
    |  Java|         22000.0|
    |dotNET|         10000.0|
    +------+----------------+
    """
    return _invoke_function_over_columns("median", col)


def mode(col: "ColumnOrName") -> Column:
    """
    Returns the most frequent value in a group.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the most frequent value in a group.

    Notes
    -----
    Supports Spark Connect.

    Examples
    --------
    >>> df = spark.createDataFrame([
    ...     ("Java", 2012, 20000), ("dotNET", 2012, 5000),
    ...     ("Java", 2012, 20000), ("dotNET", 2012, 5000),
    ...     ("dotNET", 2013, 48000), ("Java", 2013, 30000)],
    ...     schema=("course", "year", "earnings"))
    >>> df.groupby("course").agg(mode("year")).show()
    +------+----------+
    |course|mode(year)|
    +------+----------+
    |  Java|      2012|
    |dotNET|      2012|
    +------+----------+
    """
    return _invoke_function_over_columns("mode", col)


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


def any_value(col: "ColumnOrName") -> Column:
    """Returns some value of `col` for a group of rows.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.
    ignorenulls : :class:`~pyspark.sql.Column` or bool
        if first value is null then look for first non-null value.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        some value of `col` for a group of rows.

    Examples
    --------
    >>> df = spark.createDataFrame([(None, 1),
    ...                             ("a", 2),
    ...                             ("a", 3),
    ...                             ("b", 8),
    ...                             ("b", 2)], ["c1", "c2"])
    >>> df.select(any_value('c1'), any_value('c2')).collect()
    [Row(any_value(c1)=None, any_value(c2)=1)]
    >>> df.select(any_value('c1', True), any_value('c2', True)).collect()
    [Row(any_value(c1)='a', any_value(c2)=1)]
    """
    return _invoke_function_over_columns("any_value", col)


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


def approx_count_distinct(col: "ColumnOrName", rsd: Optional[float] = None) -> Column:
    """Aggregate function: returns a new :class:`~pyspark.sql.Column` for approximate distinct count
    of column `col`.

    .. versionadded:: 2.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
    rsd : float, optional
        maximum relative standard deviation allowed (default = 0.05).
        For rsd < 0.01, it is more efficient to use :func:`count_distinct`

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column of computed results.

    Examples
    --------
    >>> df = spark.createDataFrame([1,2,2,3], "INT")
    >>> df.agg(approx_count_distinct("value").alias('distinct_values')).show()
    +---------------+
    |distinct_values|
    +---------------+
    |              3|
    +---------------+
    """
    if rsd is not None:
        raise ValueError("rsd is not supported by DuckDB")
    return _invoke_function_over_columns("approx_count_distinct", col)


def approxCountDistinct(col: "ColumnOrName", rsd: Optional[float] = None) -> Column:
    """
    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    .. deprecated:: 2.1.0
        Use :func:`approx_count_distinct` instead.
    """
    warnings.warn("Deprecated in 2.1, use approx_count_distinct instead.", FutureWarning)
    return approx_count_distinct(col, rsd)


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


def lcase(str: "ColumnOrName") -> Column:
    """
    Returns `str` with all characters changed to lowercase.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        Input column or strings.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.range(1).select(sf.lcase(sf.lit("Spark"))).show()
    +------------+
    |lcase(Spark)|
    +------------+
    |       spark|
    +------------+
    """
    return lower(str)


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
    return Column(_to_column_expr(col).isnull())


def isnotnull(col: "ColumnOrName") -> Column:
    """
    Returns true if `col` is not null, or false otherwise.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str

    Examples
    --------
    >>> df = spark.createDataFrame([(None,), (1,)], ["e"])
    >>> df.select(isnotnull(df.e).alias('r')).collect()
    [Row(r=False), Row(r=True)]
    """
    return Column(_to_column_expr(col).isnotnull())


def flatten(col: "ColumnOrName") -> Column:
    """
    Collection function: creates a single array from an array of arrays.
    If a structure of nested arrays is deeper than two levels,
    only one level of nesting is removed.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        name of column or expression

    Returns
    -------
    :class:`~pyspark.sql.Column`
        flattened array.

    Examples
    --------
    >>> df = spark.createDataFrame([([[1, 2, 3], [4, 5], [6]],), ([None, [4, 5]],)], ['data'])
    >>> df.show(truncate=False)
    +------------------------+
    |data                    |
    +------------------------+
    |[[1, 2, 3], [4, 5], [6]]|
    |[NULL, [4, 5]]          |
    +------------------------+
    >>> df.select(flatten(df.data).alias('r')).show()
    +------------------+
    |                 r|
    +------------------+
    |[1, 2, 3, 4, 5, 6]|
    |              NULL|
    +------------------+
    """
    col = _to_column_expr(col)
    contains_null = _list_contains_null(col)
    return Column(
        CaseExpression(contains_null, None).otherwise(
            FunctionExpression("flatten", col)
        )
    )


def array_compact(col: "ColumnOrName") -> Column:
    """
    Collection function: removes null values from the array.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        name of column or expression

    Returns
    -------
    :class:`~pyspark.sql.Column`
        an array by excluding the null values.

    Notes
    -----
    Supports Spark Connect.

    Examples
    --------
    >>> df = spark.createDataFrame([([1, None, 2, 3],), ([4, 5, None, 4],)], ['data'])
    >>> df.select(array_compact(df.data)).collect()
    [Row(array_compact(data)=[1, 2, 3]), Row(array_compact(data)=[4, 5, 4])]
    """
    return _invoke_function(
        "list_filter", _to_column_expr(col), LambdaExpression("x", ColumnExpression("x").isnotnull())
    )


def array_remove(col: "ColumnOrName", element: Any) -> Column:
    """
    Collection function: Remove all elements that equal to element from the given array.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        name of column containing array
    element :
        element to be removed from the array

    Returns
    -------
    :class:`~pyspark.sql.Column`
        an array excluding given value.

    Examples
    --------
    >>> df = spark.createDataFrame([([1, 2, 3, 1, 1],), ([],)], ['data'])
    >>> df.select(array_remove(df.data, 1)).collect()
    [Row(array_remove(data, 1)=[2, 3]), Row(array_remove(data, 1)=[])]
    """
    return _invoke_function("list_filter", _to_column_expr(col), LambdaExpression("x", ColumnExpression("x") != ConstantExpression(element)))


def last_day(date: "ColumnOrName") -> Column:
    """
    Returns the last day of the month which the given date belongs to.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    date : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        last day of the month.

    Examples
    --------
    >>> df = spark.createDataFrame([('1997-02-10',)], ['d'])
    >>> df.select(last_day(df.d).alias('date')).collect()
    [Row(date=datetime.date(1997, 2, 28))]
    """
    return _invoke_function("last_day", _to_column_expr(date))



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


def cbrt(col: "ColumnOrName") -> Column:
    """
    Computes the cube-root of the given value.

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
    >>> df.select(cbrt(lit(27))).show()
    +--------+
    |CBRT(27)|
    +--------+
    |     3.0|
    +--------+
    """
    return _invoke_function_over_columns("cbrt", col)


def char(col: "ColumnOrName") -> Column:
    """
    Returns the ASCII character having the binary equivalent to `col`. If col is larger than 256 the
    result is equivalent to char(col % 256)

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        Input column or strings.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.range(1).select(sf.char(sf.lit(65))).show()
    +--------+
    |char(65)|
    +--------+
    |       A|
    +--------+
    """
    col = _to_column_expr(col)
    return Column(FunctionExpression("chr", CaseExpression(col > 256, col % 256).otherwise(col)))


def corr(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    """Returns a new :class:`~pyspark.sql.Column` for the Pearson Correlation Coefficient for
    ``col1`` and ``col2``.

    .. versionadded:: 1.6.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col1 : :class:`~pyspark.sql.Column` or str
        first column to calculate correlation.
    col1 : :class:`~pyspark.sql.Column` or str
        second column to calculate correlation.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        Pearson Correlation Coefficient of these two column values.

    Examples
    --------
    >>> a = range(20)
    >>> b = [2 * x for x in range(20)]
    >>> df = spark.createDataFrame(zip(a, b), ["a", "b"])
    >>> df.agg(corr("a", "b").alias('c')).collect()
    [Row(c=1.0)]
    """
    return _invoke_function_over_columns("corr", col1, col2)


def cot(col: "ColumnOrName") -> Column:
    """
    Computes cotangent of the input column.

    .. versionadded:: 3.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        angle in radians.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        cotangent of the angle.

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(cot(lit(math.radians(45)))).first()
    Row(COT(0.78539...)=1.00000...)
    """
    return _invoke_function_over_columns("cot", col)


def e() -> Column:
    """Returns Euler's number.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> spark.range(1).select(e()).show()
    +-----------------+
    |              E()|
    +-----------------+
    |2.718281828459045|
    +-----------------+
    """
    return lit(2.718281828459045)


def pi() -> Column:
    """Returns Pi.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> spark.range(1).select(pi()).show()
    +-----------------+
    |             PI()|
    +-----------------+
    |3.141592653589793|
    +-----------------+
    """
    return _invoke_function("pi")


def positive(col: "ColumnOrName") -> Column:
    """
    Returns the value.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        input value column.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        value.

    Examples
    --------
    >>> df = spark.createDataFrame([(-1,), (0,), (1,)], ['v'])
    >>> df.select(positive("v").alias("p")).show()
    +---+
    |  p|
    +---+
    | -1|
    |  0|
    |  1|
    +---+
    """
    return Column(_to_column_expr(col))


def pow(col1: Union["ColumnOrName", float], col2: Union["ColumnOrName", float]) -> Column:
    """
    Returns the value of the first argument raised to the power of the second argument.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col1 : str, :class:`~pyspark.sql.Column` or float
        the base number.
    col2 : str, :class:`~pyspark.sql.Column` or float
        the exponent number.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the base rased to the power the argument.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(pow(lit(3), lit(2))).first()
    Row(POWER(3, 2)=9.0)
    """
    return _invoke_function_over_columns("pow", col1, col2)


def printf(format: "ColumnOrName", *cols: "ColumnOrName") -> Column:
    """
    Formats the arguments in printf-style and returns the result as a string column.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    format : :class:`~pyspark.sql.Column` or str
        string that can contain embedded format tags and used as result column's value
    cols : :class:`~pyspark.sql.Column` or str
        column names or :class:`~pyspark.sql.Column`\\s to be used in formatting

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("aa%d%s", 123, "cc",)], ["a", "b", "c"]
    ... ).select(sf.printf("a", "b", "c")).show()
    +---------------+
    |printf(a, b, c)|
    +---------------+
    |        aa123cc|
    +---------------+
    """
    return _invoke_function_over_columns("printf", format, *cols)


def product(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the product of the values in a group.

    .. versionadded:: 3.2.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : str, :class:`Column`
        column containing values to be multiplied together

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(1, 10).toDF('x').withColumn('mod3', col('x') % 3)
    >>> prods = df.groupBy('mod3').agg(product('x').alias('product'))
    >>> prods.orderBy('mod3').show()
    +----+-------+
    |mod3|product|
    +----+-------+
    |   0|  162.0|
    |   1|   28.0|
    |   2|   80.0|
    +----+-------+
    """
    return _invoke_function_over_columns("product", col)


def rand(seed: Optional[int] = None) -> Column:
    """Generates a random column with independent and identically distributed (i.i.d.) samples
    uniformly distributed in [0.0, 1.0).

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    The function is non-deterministic in general case.

    Parameters
    ----------
    seed : int (default: None)
        seed value for random generator.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        random values.

    Examples
    --------
    >>> from pyspark.sql import functions as sf
    >>> spark.range(0, 2, 1, 1).withColumn('rand', sf.rand(seed=42) * 3).show()
    +---+------------------+
    | id|              rand|
    +---+------------------+
    |  0|1.8575681106759028|
    |  1|1.5288056527339444|
    +---+------------------+
    """
    if seed is not None:
        # Maybe call setseed just before but how do we know when it is executed?
        raise ContributionsAcceptedError("Seed is not yet implemented")
    return _invoke_function("random")


def regexp(str: "ColumnOrName", regexp: "ColumnOrName") -> Column:
    r"""Returns true if `str` matches the Java regex `regexp`, or false otherwise.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        target column to work on.
    regexp : :class:`~pyspark.sql.Column` or str
        regex pattern to apply.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        true if `str` matches a Java regex, or false otherwise.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("1a 2b 14m", r"(\d+)")], ["str", "regexp"]
    ... ).select(sf.regexp('str', sf.lit(r'(\d+)'))).show()
    +------------------+
    |REGEXP(str, (\d+))|
    +------------------+
    |              true|
    +------------------+

    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("1a 2b 14m", r"(\d+)")], ["str", "regexp"]
    ... ).select(sf.regexp('str', sf.lit(r'\d{2}b'))).show()
    +-------------------+
    |REGEXP(str, \d{2}b)|
    +-------------------+
    |              false|
    +-------------------+

    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("1a 2b 14m", r"(\d+)")], ["str", "regexp"]
    ... ).select(sf.regexp('str', sf.col("regexp"))).show()
    +-------------------+
    |REGEXP(str, regexp)|
    +-------------------+
    |               true|
    +-------------------+
    """
    return _invoke_function_over_columns("regexp_matches", str, regexp)


def regexp_count(str: "ColumnOrName", regexp: "ColumnOrName") -> Column:
    r"""Returns a count of the number of times that the Java regex pattern `regexp` is matched
    in the string `str`.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        target column to work on.
    regexp : :class:`~pyspark.sql.Column` or str
        regex pattern to apply.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the number of times that a Java regex pattern is matched in the string.

    Examples
    --------
    >>> df = spark.createDataFrame([("1a 2b 14m", r"\d+")], ["str", "regexp"])
    >>> df.select(regexp_count('str', lit(r'\d+')).alias('d')).collect()
    [Row(d=3)]
    >>> df.select(regexp_count('str', lit(r'mmm')).alias('d')).collect()
    [Row(d=0)]
    >>> df.select(regexp_count("str", col("regexp")).alias('d')).collect()
    [Row(d=3)]
    """
    return _invoke_function_over_columns("len", _invoke_function_over_columns("regexp_extract_all", str, regexp))


def regexp_extract(str: "ColumnOrName", pattern: str, idx: int) -> Column:
    r"""Extract a specific group matched by the Java regex `regexp`, from the specified string column.
    If the regex did not match, or the specified group did not match, an empty string is returned.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        target column to work on.
    pattern : str
        regex pattern to apply.
    idx : int
        matched group id.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        matched value specified by `idx` group id.

    Examples
    --------
    >>> df = spark.createDataFrame([('100-200',)], ['str'])
    >>> df.select(regexp_extract('str', r'(\d+)-(\d+)', 1).alias('d')).collect()
    [Row(d='100')]
    >>> df = spark.createDataFrame([('foo',)], ['str'])
    >>> df.select(regexp_extract('str', r'(\d+)', 1).alias('d')).collect()
    [Row(d='')]
    >>> df = spark.createDataFrame([('aaaac',)], ['str'])
    >>> df.select(regexp_extract('str', '(a+)(b)?(c)', 2).alias('d')).collect()
    [Row(d='')]
    """
    return _invoke_function("regexp_extract", _to_column_expr(str), ConstantExpression(pattern), ConstantExpression(idx))


def regexp_extract_all(
    str: "ColumnOrName", regexp: "ColumnOrName", idx: Optional[Union[int, Column]] = None
) -> Column:
    r"""Extract all strings in the `str` that match the Java regex `regexp`
    and corresponding to the regex group index.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        target column to work on.
    regexp : :class:`~pyspark.sql.Column` or str
        regex pattern to apply.
    idx : int
        matched group id.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        all strings in the `str` that match a Java regex and corresponding to the regex group index.

    Examples
    --------
    >>> df = spark.createDataFrame([("100-200, 300-400", r"(\d+)-(\d+)")], ["str", "regexp"])
    >>> df.select(regexp_extract_all('str', lit(r'(\d+)-(\d+)')).alias('d')).collect()
    [Row(d=['100', '300'])]
    >>> df.select(regexp_extract_all('str', lit(r'(\d+)-(\d+)'), 1).alias('d')).collect()
    [Row(d=['100', '300'])]
    >>> df.select(regexp_extract_all('str', lit(r'(\d+)-(\d+)'), 2).alias('d')).collect()
    [Row(d=['200', '400'])]
    >>> df.select(regexp_extract_all('str', col("regexp")).alias('d')).collect()
    [Row(d=['100', '300'])]
    """
    if idx is None:
        idx = 1
    return _invoke_function("regexp_extract_all", _to_column_expr(str), _to_column_expr(regexp), ConstantExpression(idx))


def regexp_like(str: "ColumnOrName", regexp: "ColumnOrName") -> Column:
    r"""Returns true if `str` matches the Java regex `regexp`, or false otherwise.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        target column to work on.
    regexp : :class:`~pyspark.sql.Column` or str
        regex pattern to apply.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        true if `str` matches a Java regex, or false otherwise.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("1a 2b 14m", r"(\d+)")], ["str", "regexp"]
    ... ).select(sf.regexp_like('str', sf.lit(r'(\d+)'))).show()
    +-----------------------+
    |REGEXP_LIKE(str, (\d+))|
    +-----------------------+
    |                   true|
    +-----------------------+

    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("1a 2b 14m", r"(\d+)")], ["str", "regexp"]
    ... ).select(sf.regexp_like('str', sf.lit(r'\d{2}b'))).show()
    +------------------------+
    |REGEXP_LIKE(str, \d{2}b)|
    +------------------------+
    |                   false|
    +------------------------+

    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("1a 2b 14m", r"(\d+)")], ["str", "regexp"]
    ... ).select(sf.regexp_like('str', sf.col("regexp"))).show()
    +------------------------+
    |REGEXP_LIKE(str, regexp)|
    +------------------------+
    |                    true|
    +------------------------+
    """
    return _invoke_function_over_columns("regexp_matches", str, regexp)


def regexp_substr(str: "ColumnOrName", regexp: "ColumnOrName") -> Column:
    r"""Returns the substring that matches the Java regex `regexp` within the string `str`.
    If the regular expression is not found, the result is null.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        target column to work on.
    regexp : :class:`~pyspark.sql.Column` or str
        regex pattern to apply.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the substring that matches a Java regex within the string `str`.

    Examples
    --------
    >>> df = spark.createDataFrame([("1a 2b 14m", r"\d+")], ["str", "regexp"])
    >>> df.select(regexp_substr('str', lit(r'\d+')).alias('d')).collect()
    [Row(d='1')]
    >>> df.select(regexp_substr('str', lit(r'mmm')).alias('d')).collect()
    [Row(d=None)]
    >>> df.select(regexp_substr("str", col("regexp")).alias('d')).collect()
    [Row(d='1')]
    """
    return Column(FunctionExpression("nullif", FunctionExpression("regexp_extract", _to_column_expr(str), _to_column_expr(regexp)), ConstantExpression("")))


def repeat(col: "ColumnOrName", n: int) -> Column:
    """
    Repeats a string column n times, and returns it as a new string column.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.
    n : int
        number of times to repeat value.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        string with repeated values.

    Examples
    --------
    >>> df = spark.createDataFrame([('ab',)], ['s',])
    >>> df.select(repeat(df.s, 3).alias('s')).collect()
    [Row(s='ababab')]
    """
    return _invoke_function("repeat", _to_column_expr(col), ConstantExpression(n))


def sequence(
    start: "ColumnOrName", stop: "ColumnOrName", step: Optional["ColumnOrName"] = None
) -> Column:
    """
    Generate a sequence of integers from `start` to `stop`, incrementing by `step`.
    If `step` is not set, incrementing by 1 if `start` is less than or equal to `stop`,
    otherwise -1.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    start : :class:`~pyspark.sql.Column` or str
        starting value (inclusive)
    stop : :class:`~pyspark.sql.Column` or str
        last values (inclusive)
    step : :class:`~pyspark.sql.Column` or str, optional
        value to add to current to get next element (default is 1)

    Returns
    -------
    :class:`~pyspark.sql.Column`
        an array of sequence values

    Examples
    --------
    >>> df1 = spark.createDataFrame([(-2, 2)], ('C1', 'C2'))
    >>> df1.select(sequence('C1', 'C2').alias('r')).collect()
    [Row(r=[-2, -1, 0, 1, 2])]
    >>> df2 = spark.createDataFrame([(4, -4, -2)], ('C1', 'C2', 'C3'))
    >>> df2.select(sequence('C1', 'C2', 'C3').alias('r')).collect()
    [Row(r=[4, 2, 0, -2, -4])]
    """
    if step is None:
        return _invoke_function_over_columns("generate_series", start, stop)
    else:
        return _invoke_function_over_columns("generate_series", start, stop, step)


def sign(col: "ColumnOrName") -> Column:
    """
    Computes the signum of the given value.

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
    >>> import pyspark.sql.functions as sf
    >>> spark.range(1).select(
    ...     sf.sign(sf.lit(-5)),
    ...     sf.sign(sf.lit(6))
    ... ).show()
    +--------+-------+
    |sign(-5)|sign(6)|
    +--------+-------+
    |    -1.0|    1.0|
    +--------+-------+
    """
    return _invoke_function_over_columns("sign", col)


def signum(col: "ColumnOrName") -> Column:
    """
    Computes the signum of the given value.

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
    >>> import pyspark.sql.functions as sf
    >>> spark.range(1).select(
    ...     sf.signum(sf.lit(-5)),
    ...     sf.signum(sf.lit(6))
    ... ).show()
    +----------+---------+
    |SIGNUM(-5)|SIGNUM(6)|
    +----------+---------+
    |      -1.0|      1.0|
    +----------+---------+
    """
    return sign(col)


def sin(col: "ColumnOrName") -> Column:
    """
    Computes sine of the input column.

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
        sine of the angle, as if computed by `java.lang.Math.sin()`

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(sin(lit(math.radians(90)))).first()
    Row(SIN(1.57079...)=1.0)
    """
    return _invoke_function_over_columns("sin", col)


def skewness(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the skewness of the values in a group.

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
        skewness of given column.

    Examples
    --------
    >>> df = spark.createDataFrame([[1],[1],[2]], ["c"])
    >>> df.select(skewness(df.c)).first()
    Row(skewness(c)=0.70710...)
    """
    return _invoke_function_over_columns("skewness", col)


def encode(col: "ColumnOrName", charset: str) -> Column:
    """
    Computes the first argument into a binary from a string using the provided character set
    (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.
    charset : str
        charset to use to encode.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.createDataFrame([('abcd',)], ['c'])
    >>> df.select(encode("c", "UTF-8")).show()
    +----------------+
    |encode(c, UTF-8)|
    +----------------+
    |   [61 62 63 64]|
    +----------------+
    """
    if charset != "UTF-8":
        raise ContributionsAcceptedError("Only UTF-8 charset is supported right now")
    return _invoke_function("encode", _to_column_expr(col))


def find_in_set(str: "ColumnOrName", str_array: "ColumnOrName") -> Column:
    """
    Returns the index (1-based) of the given string (`str`) in the comma-delimited
    list (`strArray`). Returns 0, if the string was not found or if the given string (`str`)
    contains a comma.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        The given string to be found.
    str_array : :class:`~pyspark.sql.Column` or str
        The comma-delimited list.

    Examples
    --------
    >>> df = spark.createDataFrame([("ab", "abc,b,ab,c,def")], ['a', 'b'])
    >>> df.select(find_in_set(df.a, df.b).alias('r')).collect()
    [Row(r=3)]
    """
    str_array = _to_column_expr(str_array)
    str = _to_column_expr(str)
    return Column(
        CaseExpression(
            FunctionExpression("contains", str, ConstantExpression(",")), 0
        ).otherwise(
            CoalesceOperator(
                FunctionExpression(
                    "list_position",
                    FunctionExpression("string_split", str_array, ConstantExpression(",")),
                    str
                ),
                # If the element cannot be found, list_position returns null but we want to return 0
                ConstantExpression(0)
            )
        )
    )


def first(col: "ColumnOrName", ignorenulls: bool = False) -> Column:
    """Aggregate function: returns the first value in a group.

    The function by default returns the first values it sees. It will return the first non-null
    value it sees when ignoreNulls is set to true. If all values are null, then null is returned.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    The function is non-deterministic because its results depends on the order of the
    rows which may be non-deterministic after a shuffle.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column to fetch first value for.
    ignorenulls : :class:`~pyspark.sql.Column` or str
        if first value is null then look for first non-null value.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        first value of the group.

    Examples
    --------
    >>> df = spark.createDataFrame([("Alice", 2), ("Bob", 5), ("Alice", None)], ("name", "age"))
    >>> df = df.orderBy(df.age)
    >>> df.groupby("name").agg(first("age")).orderBy("name").show()
    +-----+----------+
    | name|first(age)|
    +-----+----------+
    |Alice|      NULL|
    |  Bob|         5|
    +-----+----------+

    Now, to ignore any nulls we needs to set ``ignorenulls`` to `True`

    >>> df.groupby("name").agg(first("age", ignorenulls=True)).orderBy("name").show()
    +-----+----------+
    | name|first(age)|
    +-----+----------+
    |Alice|         2|
    |  Bob|         5|
    +-----+----------+
    """
    if ignorenulls:
        raise ContributionsAcceptedError()
    return _invoke_function_over_columns("first", col)


def last(col: "ColumnOrName", ignorenulls: bool = False) -> Column:
    """Aggregate function: returns the last value in a group.

    The function by default returns the last values it sees. It will return the last non-null
    value it sees when ignoreNulls is set to true. If all values are null, then null is returned.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    The function is non-deterministic because its results depends on the order of the
    rows which may be non-deterministic after a shuffle.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column to fetch last value for.
    ignorenulls : :class:`~pyspark.sql.Column` or str
        if last value is null then look for non-null value.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        last value of the group.

    Examples
    --------
    >>> df = spark.createDataFrame([("Alice", 2), ("Bob", 5), ("Alice", None)], ("name", "age"))
    >>> df = df.orderBy(df.age.desc())
    >>> df.groupby("name").agg(last("age")).orderBy("name").show()
    +-----+---------+
    | name|last(age)|
    +-----+---------+
    |Alice|     NULL|
    |  Bob|        5|
    +-----+---------+

    Now, to ignore any nulls we needs to set ``ignorenulls`` to `True`

    >>> df.groupby("name").agg(last("age", ignorenulls=True)).orderBy("name").show()
    +-----+---------+
    | name|last(age)|
    +-----+---------+
    |Alice|        2|
    |  Bob|        5|
    +-----+---------+
    """
    if ignorenulls:
        raise ContributionsAcceptedError()
    return _invoke_function_over_columns("last", col)



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


def btrim(str: "ColumnOrName", trim: Optional["ColumnOrName"] = None) -> Column:
    """
    Remove the leading and trailing `trim` characters from `str`.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        Input column or strings.
    trim : :class:`~pyspark.sql.Column` or str
        The trim string characters to trim, the default value is a single space

    Examples
    --------
    >>> df = spark.createDataFrame([("SSparkSQLS", "SL", )], ['a', 'b'])
    >>> df.select(btrim(df.a, df.b).alias('r')).collect()
    [Row(r='parkSQ')]

    >>> df = spark.createDataFrame([("    SparkSQL   ",)], ['a'])
    >>> df.select(btrim(df.a).alias('r')).collect()
    [Row(r='SparkSQL')]
    """
    if trim is not None:
        return _invoke_function_over_columns("trim", str, trim)
    else:
        return _invoke_function_over_columns("trim", str)


def endswith(str: "ColumnOrName", suffix: "ColumnOrName") -> Column:
    """
    Returns a boolean. The value is True if str ends with suffix.
    Returns NULL if either input expression is NULL. Otherwise, returns False.
    Both str or suffix must be of STRING or BINARY type.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        A column of string.
    suffix : :class:`~pyspark.sql.Column` or str
        A column of string, the suffix.

    Examples
    --------
    >>> df = spark.createDataFrame([("Spark SQL", "Spark",)], ["a", "b"])
    >>> df.select(endswith(df.a, df.b).alias('r')).collect()
    [Row(r=False)]

    >>> df = spark.createDataFrame([("414243", "4243",)], ["e", "f"])
    >>> df = df.select(to_binary("e").alias("e"), to_binary("f").alias("f"))
    >>> df.printSchema()
    root
     |-- e: binary (nullable = true)
     |-- f: binary (nullable = true)
    >>> df.select(endswith("e", "f"), endswith("f", "e")).show()
    +--------------+--------------+
    |endswith(e, f)|endswith(f, e)|
    +--------------+--------------+
    |          true|         false|
    +--------------+--------------+
    """
    return _invoke_function_over_columns("suffix", str, suffix)


def startswith(str: "ColumnOrName", prefix: "ColumnOrName") -> Column:
    """
    Returns a boolean. The value is True if str starts with prefix.
    Returns NULL if either input expression is NULL. Otherwise, returns False.
    Both str or prefix must be of STRING or BINARY type.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        A column of string.
    prefix : :class:`~pyspark.sql.Column` or str
        A column of string, the prefix.

    Examples
    --------
    >>> df = spark.createDataFrame([("Spark SQL", "Spark",)], ["a", "b"])
    >>> df.select(startswith(df.a, df.b).alias('r')).collect()
    [Row(r=True)]

    >>> df = spark.createDataFrame([("414243", "4142",)], ["e", "f"])
    >>> df = df.select(to_binary("e").alias("e"), to_binary("f").alias("f"))
    >>> df.printSchema()
    root
     |-- e: binary (nullable = true)
     |-- f: binary (nullable = true)
    >>> df.select(startswith("e", "f"), startswith("f", "e")).show()
    +----------------+----------------+
    |startswith(e, f)|startswith(f, e)|
    +----------------+----------------+
    |            true|           false|
    +----------------+----------------+
    """
    return _invoke_function_over_columns("starts_with", str, prefix)


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


def nvl2(col1: "ColumnOrName", col2: "ColumnOrName", col3: "ColumnOrName") -> Column:
    """
    Returns `col2` if `col1` is not null, or `col3` otherwise.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col1 : :class:`~pyspark.sql.Column` or str
    col2 : :class:`~pyspark.sql.Column` or str
    col3 : :class:`~pyspark.sql.Column` or str

    Examples
    --------
    >>> df = spark.createDataFrame([(None, 8, 6,), (1, 9, 9,)], ["a", "b", "c"])
    >>> df.select(nvl2(df.a, df.b, df.c).alias('r')).collect()
    [Row(r=6), Row(r=9)]
    """
    col1 = _to_column_expr(col1)
    col2 = _to_column_expr(col2)
    col3 = _to_column_expr(col3)
    return Column(CaseExpression(col1.isnull(), col3).otherwise(col2))


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


def nullif(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    """
    Returns null if `col1` equals to `col2`, or `col1` otherwise.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col1 : :class:`~pyspark.sql.Column` or str
    col2 : :class:`~pyspark.sql.Column` or str

    Examples
    --------
    >>> df = spark.createDataFrame([(None, None,), (1, 9,)], ["a", "b"])
    >>> df.select(nullif(df.a, df.b).alias('r')).collect()
    [Row(r=None), Row(r=1)]
    """
    return _invoke_function_over_columns("nullif", col1, col2)


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

def desc(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the descending order of the given column name.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the descending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    Sort by the column 'id' in the descending order.

    >>> spark.range(5).orderBy(desc("id")).show()
    +---+
    | id|
    +---+
    |  4|
    |  3|
    |  2|
    |  1|
    |  0|
    +---+
    """
    return Column(_to_column_expr(col).desc())

def asc(col: "ColumnOrName") -> Column:
    """
    Returns a sort expression based on the ascending order of the given column name.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to sort by in the ascending order.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column specifying the order.

    Examples
    --------
    Sort by the column 'id' in the descending order.

    >>> df = spark.range(5)
    >>> df = df.sort(desc("id"))
    >>> df.show()
    +---+
    | id|
    +---+
    |  4|
    |  3|
    |  2|
    |  1|
    |  0|
    +---+

    Sort by the column 'id' in the ascending order.

    >>> df.orderBy(asc("id")).show()
    +---+
    | id|
    +---+
    |  0|
    |  1|
    |  2|
    |  3|
    |  4|
    +---+
    """
    return Column(_to_column_expr(col).asc())

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


def cos(col: "ColumnOrName") -> Column:
    """
    Computes cosine of the input column.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        angle in radians

    Returns
    -------
    :class:`~pyspark.sql.Column`
        cosine of the angle, as if computed by `java.lang.Math.cos()`.

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(cos(lit(math.pi))).first()
    Row(COS(3.14159...)=-1.0)
    """
    return _invoke_function_over_columns("cos", col)


def acos(col: "ColumnOrName") -> Column:
    """
    Computes inverse cosine of the input column.

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
        inverse cosine of `col`, as if computed by `java.lang.Math.acos()`

    Examples
    --------
    >>> df = spark.range(1, 3)
    >>> df.select(acos(df.id)).show()
    +--------+
    |ACOS(id)|
    +--------+
    |     0.0|
    |     NaN|
    +--------+
    """
    return _invoke_function_over_columns("acos", col)


def call_function(funcName: str, *cols: "ColumnOrName") -> Column:
    """
    Call a SQL function.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    funcName : str
        function name that follows the SQL identifier syntax (can be quoted, can be qualified)
    cols : :class:`~pyspark.sql.Column` or str
        column names or :class:`~pyspark.sql.Column`\\s to be used in the function

    Returns
    -------
    :class:`~pyspark.sql.Column`
        result of executed function.

    Examples
    --------
    >>> from pyspark.sql.functions import call_udf, col
    >>> from pyspark.sql.types import IntegerType, StringType
    >>> df = spark.createDataFrame([(1, "a"),(2, "b"), (3, "c")],["id", "name"])
    >>> _ = spark.udf.register("intX2", lambda i: i * 2, IntegerType())
    >>> df.select(call_function("intX2", "id")).show()
    +---------+
    |intX2(id)|
    +---------+
    |        2|
    |        4|
    |        6|
    +---------+
    >>> _ = spark.udf.register("strX2", lambda s: s * 2, StringType())
    >>> df.select(call_function("strX2", col("name"))).show()
    +-----------+
    |strX2(name)|
    +-----------+
    |         aa|
    |         bb|
    |         cc|
    +-----------+
    >>> df.select(call_function("avg", col("id"))).show()
    +-------+
    |avg(id)|
    +-------+
    |    2.0|
    +-------+
    >>> _ = spark.sql("CREATE FUNCTION custom_avg AS 'test.org.apache.spark.sql.MyDoubleAvg'")
    ... # doctest: +SKIP
    >>> df.select(call_function("custom_avg", col("id"))).show()
    ... # doctest: +SKIP
    +------------------------------------+
    |spark_catalog.default.custom_avg(id)|
    +------------------------------------+
    |                               102.0|
    +------------------------------------+
    >>> df.select(call_function("spark_catalog.default.custom_avg", col("id"))).show()
    ... # doctest: +SKIP
    +------------------------------------+
    |spark_catalog.default.custom_avg(id)|
    +------------------------------------+
    |                               102.0|
    +------------------------------------+
    """
    return _invoke_function_over_columns(funcName, *cols)


def covar_pop(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    """Returns a new :class:`~pyspark.sql.Column` for the population covariance of ``col1`` and
    ``col2``.

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col1 : :class:`~pyspark.sql.Column` or str
        first column to calculate covariance.
    col1 : :class:`~pyspark.sql.Column` or str
        second column to calculate covariance.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        covariance of these two column values.

    Examples
    --------
    >>> a = [1] * 10
    >>> b = [1] * 10
    >>> df = spark.createDataFrame(zip(a, b), ["a", "b"])
    >>> df.agg(covar_pop("a", "b").alias('c')).collect()
    [Row(c=0.0)]
    """
    return _invoke_function_over_columns("covar_pop", col1, col2)


def covar_samp(col1: "ColumnOrName", col2: "ColumnOrName") -> Column:
    """Returns a new :class:`~pyspark.sql.Column` for the sample covariance of ``col1`` and
    ``col2``.

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col1 : :class:`~pyspark.sql.Column` or str
        first column to calculate covariance.
    col1 : :class:`~pyspark.sql.Column` or str
        second column to calculate covariance.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        sample covariance of these two column values.

    Examples
    --------
    >>> a = [1] * 10
    >>> b = [1] * 10
    >>> df = spark.createDataFrame(zip(a, b), ["a", "b"])
    >>> df.agg(covar_samp("a", "b").alias('c')).collect()
    [Row(c=0.0)]
    """
    return _invoke_function_over_columns("covar_samp", col1, col2)


def exp(col: "ColumnOrName") -> Column:
    """
    Computes the exponential of the given value.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column to calculate exponential for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        exponential of the given value.

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(exp(lit(0))).show()
    +------+
    |EXP(0)|
    +------+
    |   1.0|
    +------+
    """
    return _invoke_function_over_columns("exp", col)


def factorial(col: "ColumnOrName") -> Column:
    """
    Computes the factorial of the given value.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        a column to calculate factorial for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        factorial of given value.

    Examples
    --------
    >>> df = spark.createDataFrame([(5,)], ['n'])
    >>> df.select(factorial(df.n).alias('f')).collect()
    [Row(f=120)]
    """
    return _invoke_function_over_columns("factorial", col)


def log2(col: "ColumnOrName") -> Column:
    """Returns the base-2 logarithm of the argument.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        a column to calculate logariphm for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        logariphm of given value.

    Examples
    --------
    >>> df = spark.createDataFrame([(4,)], ['a'])
    >>> df.select(log2('a').alias('log2')).show()
    +----+
    |log2|
    +----+
    | 2.0|
    +----+
    """
    return _invoke_function_over_columns("log2", col)


def ln(col: "ColumnOrName") -> Column:
    """Returns the natural logarithm of the argument.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        a column to calculate logariphm for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        natural logarithm of given value.

    Examples
    --------
    >>> df = spark.createDataFrame([(4,)], ['a'])
    >>> df.select(ln('a')).show()
    +------------------+
    |             ln(a)|
    +------------------+
    |1.3862943611198906|
    +------------------+
    """
    return _invoke_function_over_columns("ln", col)


def degrees(col: "ColumnOrName") -> Column:
    """
    Converts an angle measured in radians to an approximately equivalent angle
    measured in degrees.

    .. versionadded:: 2.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        angle in radians

    Returns
    -------
    :class:`~pyspark.sql.Column`
        angle in degrees, as if computed by `java.lang.Math.toDegrees()`

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(degrees(lit(math.pi))).first()
    Row(DEGREES(3.14159...)=180.0)
    """
    return _invoke_function_over_columns("degrees", col)



def radians(col: "ColumnOrName") -> Column:
    """
    Converts an angle measured in degrees to an approximately equivalent angle
    measured in radians.

    .. versionadded:: 2.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        angle in degrees

    Returns
    -------
    :class:`~pyspark.sql.Column`
        angle in radians, as if computed by `java.lang.Math.toRadians()`

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(radians(lit(180))).first()
    Row(RADIANS(180)=3.14159...)
    """
    return _invoke_function_over_columns("radians", col)


def atan(col: "ColumnOrName") -> Column:
    """
    Compute inverse tangent of the input column.

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
        inverse tangent of `col`, as if computed by `java.lang.Math.atan()`

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(atan(df.id)).show()
    +--------+
    |ATAN(id)|
    +--------+
    |     0.0|
    +--------+
    """
    return _invoke_function_over_columns("atan", col)


def atan2(col1: Union["ColumnOrName", float], col2: Union["ColumnOrName", float]) -> Column:
    """
    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col1 : str, :class:`~pyspark.sql.Column` or float
        coordinate on y-axis
    col2 : str, :class:`~pyspark.sql.Column` or float
        coordinate on x-axis

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the `theta` component of the point
        (`r`, `theta`)
        in polar coordinates that corresponds to the point
        (`x`, `y`) in Cartesian coordinates,
        as if computed by `java.lang.Math.atan2()`

    Examples
    --------
    >>> df = spark.range(1)
    >>> df.select(atan2(lit(1), lit(2))).first()
    Row(ATAN2(1, 2)=0.46364...)
    """
    def lit_or_column(x: Union["ColumnOrName", float]) -> Column:
        if isinstance(x, (int, float)):
            return lit(x)
        return x
    return _invoke_function_over_columns("atan2", lit_or_column(col1), lit_or_column(col2))


def tan(col: "ColumnOrName") -> Column:
    """
    Computes tangent of the input column.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        angle in radians

    Returns
    -------
    :class:`~pyspark.sql.Column`
        tangent of the given value, as if computed by `java.lang.Math.tan()`

    Examples
    --------
    >>> import math
    >>> df = spark.range(1)
    >>> df.select(tan(lit(math.radians(45)))).first()
    Row(TAN(0.78539...)=0.99999...)
    """
    return _invoke_function_over_columns("tan", col)


def round(col: "ColumnOrName", scale: int = 0) -> Column:
    """
    Round the given value to `scale` decimal places using HALF_UP rounding mode if `scale` >= 0
    or at integral part when `scale` < 0.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        input column to round.
    scale : int optional default 0
        scale value.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        rounded values.

    Examples
    --------
    >>> spark.createDataFrame([(2.5,)], ['a']).select(round('a', 0).alias('r')).collect()
    [Row(r=3.0)]
    """
    return _invoke_function_over_columns("round", col, lit(scale))


def bround(col: "ColumnOrName", scale: int = 0) -> Column:
    """
    Round the given value to `scale` decimal places using HALF_EVEN rounding mode if `scale` >= 0
    or at integral part when `scale` < 0.

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        input column to round.
    scale : int optional default 0
        scale value.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        rounded values.

    Examples
    --------
    >>> spark.createDataFrame([(2.5,)], ['a']).select(bround('a', 0).alias('r')).collect()
    [Row(r=2.0)]
    """
    return _invoke_function_over_columns("round_even", col, lit(scale))


def get(col: "ColumnOrName", index: Union["ColumnOrName", int]) -> Column:
    """
    Collection function: Returns element of array at given (0-based) index.
    If the index points outside of the array boundaries, then this function
    returns NULL.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        name of column containing array
    index : :class:`~pyspark.sql.Column` or str or int
        index to check for in array

    Returns
    -------
    :class:`~pyspark.sql.Column`
        value at given position.

    Notes
    -----
    The position is not 1 based, but 0 based index.
    Supports Spark Connect.

    See Also
    --------
    :meth:`element_at`

    Examples
    --------
    >>> df = spark.createDataFrame([(["a", "b", "c"], 1)], ['data', 'index'])
    >>> df.select(get(df.data, 1)).show()
    +------------+
    |get(data, 1)|
    +------------+
    |           b|
    +------------+

    >>> df.select(get(df.data, -1)).show()
    +-------------+
    |get(data, -1)|
    +-------------+
    |         NULL|
    +-------------+

    >>> df.select(get(df.data, 3)).show()
    +------------+
    |get(data, 3)|
    +------------+
    |        NULL|
    +------------+

    >>> df.select(get(df.data, "index")).show()
    +----------------+
    |get(data, index)|
    +----------------+
    |               b|
    +----------------+

    >>> df.select(get(df.data, col("index") - 1)).show()
    +----------------------+
    |get(data, (index - 1))|
    +----------------------+
    |                     a|
    +----------------------+
    """
    index = ConstantExpression(index) if isinstance(index, int) else _to_column_expr(index)
    # Spark uses 0-indexing, DuckDB 1-indexing
    index = index + 1

    return _invoke_function("list_extract", _to_column_expr(col), index)


def initcap(col: "ColumnOrName") -> Column:
    """Translate the first letter of each word to upper case in the sentence.

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
        string with all first letters are uppercase in each word.

    Examples
    --------
    >>> spark.createDataFrame([('ab cd',)], ['a']).select(initcap("a").alias('v')).collect()
    [Row(v='Ab Cd')]
    """
    return Column(
        FunctionExpression(
            "array_to_string",
            FunctionExpression(
                "list_transform",
                FunctionExpression(
                    "string_split", _to_column_expr(col), ConstantExpression(" ")
                ),
                LambdaExpression(
                    "x",
                    FunctionExpression(
                        "concat",
                        FunctionExpression(
                            "upper",
                            FunctionExpression(
                                "array_extract", ColumnExpression("x"), 1
                            ),
                        ),
                        FunctionExpression("array_slice", ColumnExpression("x"), 2, -1),
                    ),
                ),
            ),
            ConstantExpression(" "),
        )
    )


def octet_length(col: "ColumnOrName") -> Column:
    """
    Calculates the byte length for the specified string column.

    .. versionadded:: 3.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        Source column or strings

    Returns
    -------
    :class:`~pyspark.sql.Column`
        Byte length of the col

    Examples
    --------
    >>> from pyspark.sql.functions import octet_length
    >>> spark.createDataFrame([('cat',), ( '\U0001F408',)], ['cat']) \\
    ...      .select(octet_length('cat')).collect()
        [Row(octet_length(cat)=3), Row(octet_length(cat)=4)]
    """
    return _invoke_function("octet_length", _to_column_expr(col).cast("blob"))


def hex(col: "ColumnOrName") -> Column:
    """
    Computes hex value of the given column, which could be :class:`~pyspark.sql.types.StringType`, :class:`~pyspark.sql.types.BinaryType`, :class:`~pyspark.sql.types.IntegerType` or :class:`~pyspark.sql.types.LongType`.

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
        hexadecimal representation of given value as string.

    Examples
    --------
    >>> spark.createDataFrame([('ABC', 3)], ['a', 'b']).select(hex('a'), hex('b')).collect()
    [Row(hex(a)='414243', hex(b)='3')]
    """
    return _invoke_function_over_columns("hex", col)


def unhex(col: "ColumnOrName") -> Column:
    """
    Inverse of hex. Interprets each pair of characters as a hexadecimal number and converts to the byte representation of number. column and returns it as a binary column.

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
        string representation of given hexadecimal value.

    Examples
    --------
    >>> spark.createDataFrame([('414243',)], ['a']).select(unhex('a')).collect()
    [Row(unhex(a)=bytearray(b'ABC'))]
    """
    return _invoke_function_over_columns("unhex", col)


def base64(col: "ColumnOrName") -> Column:
    """
    Computes the BASE64 encoding of a binary column and returns it as a string column.

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
        BASE64 encoding of string value.

    Examples
    --------
    >>> df = spark.createDataFrame(["Spark", "PySpark", "Pandas API"], "STRING")
    >>> df.select(base64("value")).show()
    +----------------+
    |   base64(value)|
    +----------------+
    |        U3Bhcms=|
    |    UHlTcGFyaw==|
    |UGFuZGFzIEFQSQ==|
    +----------------+
    """
    if isinstance(col,str):
        col = Column(ColumnExpression(col))
    return _invoke_function_over_columns("base64", col.cast("BLOB"))


def unbase64(col: "ColumnOrName") -> Column:
    """
    Decodes a BASE64 encoded string column and returns it as a binary column.

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
        encoded string value.

    Examples
    --------
    >>> df = spark.createDataFrame(["U3Bhcms=",
    ...                            "UHlTcGFyaw==",
    ...                            "UGFuZGFzIEFQSQ=="], "STRING")
    >>> df.select(unbase64("value")).show()
    +--------------------+
    |     unbase64(value)|
    +--------------------+
    |    [53 70 61 72 6B]|
    |[50 79 53 70 61 7...|
    |[50 61 6E 64 61 7...|
    +--------------------+
    """
    return _invoke_function_over_columns("from_base64", col)


def add_months(start: "ColumnOrName", months: Union["ColumnOrName", int]) -> Column:
    """
    Returns the date that is `months` months after `start`. If `months` is a negative value
    then these amount of months will be deducted from the `start`.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    start : :class:`~pyspark.sql.Column` or str
        date column to work on.
    months : :class:`~pyspark.sql.Column` or str or int
        how many months after the given date to calculate.
        Accepts negative value as well to calculate backwards.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        a date after/before given number of months.

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08', 2)], ['dt', 'add'])
    >>> df.select(add_months(df.dt, 1).alias('next_month')).collect()
    [Row(next_month=datetime.date(2015, 5, 8))]
    >>> df.select(add_months(df.dt, df.add.cast('integer')).alias('next_month')).collect()
    [Row(next_month=datetime.date(2015, 6, 8))]
    >>> df.select(add_months('dt', -2).alias('prev_month')).collect()
    [Row(prev_month=datetime.date(2015, 2, 8))]
    """
    months = ConstantExpression(months) if isinstance(months, int) else _to_column_expr(months)
    return _invoke_function("date_add", _to_column_expr(start), FunctionExpression("to_months", months)).cast("date")


def array_join(
    col: "ColumnOrName", delimiter: str, null_replacement: Optional[str] = None
) -> Column:
    """
    Concatenates the elements of `column` using the `delimiter`. Null values are replaced with
    `null_replacement` if set, otherwise they are ignored.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.
    delimiter : str
        delimiter used to concatenate elements
    null_replacement : str, optional
        if set then null values will be replaced by this value

    Returns
    -------
    :class:`~pyspark.sql.Column`
        a column of string type. Concatenated values.

    Examples
    --------
    >>> df = spark.createDataFrame([(["a", "b", "c"],), (["a", None],)], ['data'])
    >>> df.select(array_join(df.data, ",").alias("joined")).collect()
    [Row(joined='a,b,c'), Row(joined='a')]
    >>> df.select(array_join(df.data, ",", "NULL").alias("joined")).collect()
    [Row(joined='a,b,c'), Row(joined='a,NULL')]
    """
    col = _to_column_expr(col)
    if null_replacement is not None:
        col = FunctionExpression(
            "list_transform", col, LambdaExpression("x", CaseExpression(ColumnExpression("x").isnull(), ConstantExpression(null_replacement)).otherwise(ColumnExpression("x")))
        )
    return _invoke_function("array_to_string", col, ConstantExpression(delimiter))


def array_position(col: "ColumnOrName", value: Any) -> Column:
    """
    Collection function: Locates the position of the first occurrence of the given value
    in the given array. Returns null if either of the arguments are null.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    The position is not zero based, but 1 based index. Returns 0 if the given
    value could not be found in the array.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to work on.
    value : Any
        value to look for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        position of the value in the given array if found and 0 otherwise.

    Examples
    --------
    >>> df = spark.createDataFrame([(["c", "b", "a"],), ([],)], ['data'])
    >>> df.select(array_position(df.data, "a")).collect()
    [Row(array_position(data, a)=3), Row(array_position(data, a)=0)]
    """
    return Column(CoalesceOperator(_to_column_expr(_invoke_function_over_columns("list_position", col, lit(value))), ConstantExpression(0)))


def array_prepend(col: "ColumnOrName", value: Any) -> Column:
    """
    Collection function: Returns an array containing element as
    well as all elements from array. The new element is positioned
    at the beginning of the array.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        name of column containing array
    value :
        a literal value, or a :class:`~pyspark.sql.Column` expression.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        an array excluding given value.

    Examples
    --------
    >>> df = spark.createDataFrame([([2, 3, 4],), ([],)], ['data'])
    >>> df.select(array_prepend(df.data, 1)).collect()
    [Row(array_prepend(data, 1)=[1, 2, 3, 4]), Row(array_prepend(data, 1)=[1])]
    """
    return _invoke_function_over_columns("list_prepend", lit(value), col)


def array_repeat(col: "ColumnOrName", count: Union["ColumnOrName", int]) -> Column:
    """
    Collection function: creates an array containing a column repeated count times.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column name or column that contains the element to be repeated
    count : :class:`~pyspark.sql.Column` or str or int
        column name, column, or int containing the number of times to repeat the first argument

    Returns
    -------
    :class:`~pyspark.sql.Column`
        an array of repeated elements.

    Examples
    --------
    >>> df = spark.createDataFrame([('ab',)], ['data'])
    >>> df.select(array_repeat(df.data, 3).alias('r')).collect()
    [Row(r=['ab', 'ab', 'ab'])]
    """
    count = lit(count) if isinstance(count, int) else count

    return _invoke_function_over_columns("list_resize", _invoke_function_over_columns("list_value", col), count, col)


def array_size(col: "ColumnOrName") -> Column:
    """
    Returns the total number of elements in the array. The function returns null for null input.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        total number of elements in the array.

    Examples
    --------
    >>> df = spark.createDataFrame([([2, 1, 3],), (None,)], ['data'])
    >>> df.select(array_size(df.data).alias('r')).collect()
    [Row(r=3), Row(r=None)]
    """
    return _invoke_function_over_columns("len", col)

def array_sort(
    col: "ColumnOrName", comparator: Optional[Callable[[Column, Column], Column]] = None
) -> Column:
    """
    Collection function: sorts the input array in ascending order. The elements of the input array
    must be orderable. Null elements will be placed at the end of the returned array.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Can take a `comparator` function.

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        name of column or expression
    comparator : callable, optional
        A binary ``(Column, Column) -> Column: ...``.
        The comparator will take two
        arguments representing two elements of the array. It returns a negative integer, 0, or a
        positive integer as the first element is less than, equal to, or greater than the second
        element. If the comparator function returns null, the function will fail and raise an error.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        sorted array.

    Examples
    --------
    >>> df = spark.createDataFrame([([2, 1, None, 3],),([1],),([],)], ['data'])
    >>> df.select(array_sort(df.data).alias('r')).collect()
    [Row(r=[1, 2, 3, None]), Row(r=[1]), Row(r=[])]
    >>> df = spark.createDataFrame([(["foo", "foobar", None, "bar"],),(["foo"],),([],)], ['data'])
    >>> df.select(array_sort(
    ...     "data",
    ...     lambda x, y: when(x.isNull() | y.isNull(), lit(0)).otherwise(length(y) - length(x))
    ... ).alias("r")).collect()
    [Row(r=['foobar', 'foo', None, 'bar']), Row(r=['foo']), Row(r=[])]
    """
    if comparator is not None:
        raise ContributionsAcceptedError("comparator is not yet supported")
    else:
        return _invoke_function_over_columns("list_sort", col, lit("ASC"), lit("NULLS LAST"))


def sort_array(col: "ColumnOrName", asc: bool = True) -> Column:
    """
    Collection function: sorts the input array in ascending or descending order according
    to the natural ordering of the array elements. Null elements will be placed at the beginning
    of the returned array in ascending order or at the end of the returned array in descending
    order.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        name of column or expression
    asc : bool, optional
        whether to sort in ascending or descending order. If `asc` is True (default)
        then ascending and if False then descending.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        sorted array.

    Examples
    --------
    >>> df = spark.createDataFrame([([2, 1, None, 3],),([1],),([],)], ['data'])
    >>> df.select(sort_array(df.data).alias('r')).collect()
    [Row(r=[None, 1, 2, 3]), Row(r=[1]), Row(r=[])]
    >>> df.select(sort_array(df.data, asc=False).alias('r')).collect()
    [Row(r=[3, 2, 1, None]), Row(r=[1]), Row(r=[])]
    """
    if asc:
        order = "ASC"
        null_order = "NULLS FIRST"
    else:
        order = "DESC"
        null_order = "NULLS LAST"
    return _invoke_function_over_columns("list_sort", col, lit(order), lit(null_order))


def split(str: "ColumnOrName", pattern: str, limit: int = -1) -> Column:
    """
    Splits str around matches of the given pattern.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        a string expression to split
    pattern : str
        a string representing a regular expression. The regex string should be
        a Java regular expression.
    limit : int, optional
        an integer which controls the number of times `pattern` is applied.

        * ``limit > 0``: The resulting array's length will not be more than `limit`, and the
                         resulting array's last entry will contain all input beyond the last
                         matched pattern.
        * ``limit <= 0``: `pattern` will be applied as many times as possible, and the resulting
                          array can be of any size.

        .. versionchanged:: 3.0
           `split` now takes an optional `limit` field. If not provided, default limit value is -1.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        array of separated strings.

    Examples
    --------
    >>> df = spark.createDataFrame([('oneAtwoBthreeC',)], ['s',])
    >>> df.select(split(df.s, '[ABC]', 2).alias('s')).collect()
    [Row(s=['one', 'twoBthreeC'])]
    >>> df.select(split(df.s, '[ABC]', -1).alias('s')).collect()
    [Row(s=['one', 'two', 'three', ''])]
    """
    if limit > 0:
        # Unclear how to implement this in DuckDB as we'd need to map back from the split array
        # to the original array which is tricky with regular expressions.
        raise ContributionsAcceptedError("limit is not yet supported")
    return _invoke_function_over_columns("regexp_split_to_array", str, lit(pattern))


def split_part(src: "ColumnOrName", delimiter: "ColumnOrName", partNum: "ColumnOrName") -> Column:
    """
    Splits `str` by delimiter and return requested part of the split (1-based).
    If any input is null, returns null. if `partNum` is out of range of split parts,
    returns empty string. If `partNum` is 0, throws an error. If `partNum` is negative,
    the parts are counted backward from the end of the string.
    If the `delimiter` is an empty string, the `str` is not split.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    src : :class:`~pyspark.sql.Column` or str
        A column of string to be splited.
    delimiter : :class:`~pyspark.sql.Column` or str
        A column of string, the delimiter used for split.
    partNum : :class:`~pyspark.sql.Column` or str
        A column of string, requested part of the split (1-based).

    Examples
    --------
    >>> df = spark.createDataFrame([("11.12.13", ".", 3,)], ["a", "b", "c"])
    >>> df.select(split_part(df.a, df.b, df.c).alias('r')).collect()
    [Row(r='13')]
    """
    src = _to_column_expr(src)
    delimiter = _to_column_expr(delimiter)
    partNum = _to_column_expr(partNum)
    part = FunctionExpression("split_part", src, delimiter, partNum)

    return Column(CaseExpression(src.isnull() | delimiter.isnull() | partNum.isnull(), ConstantExpression(None)).otherwise(CaseExpression(delimiter == ConstantExpression(""), ConstantExpression("")).otherwise(part)))


def stddev_samp(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the unbiased sample standard deviation of
    the expression in a group.

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
        standard deviation of given column.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.range(6).select(sf.stddev_samp("id")).show()
    +------------------+
    |   stddev_samp(id)|
    +------------------+
    |1.8708286933869...|
    +------------------+
    """
    return _invoke_function_over_columns("stddev_samp", col)


def stddev(col: "ColumnOrName") -> Column:
    """
    Aggregate function: alias for stddev_samp.

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
        standard deviation of given column.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.range(6).select(sf.stddev("id")).show()
    +------------------+
    |        stddev(id)|
    +------------------+
    |1.8708286933869...|
    +------------------+
    """
    return stddev_samp(col)

def std(col: "ColumnOrName") -> Column:
    """
    Aggregate function: alias for stddev_samp.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        standard deviation of given column.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.range(6).select(sf.std("id")).show()
    +------------------+
    |           std(id)|
    +------------------+
    |1.8708286933869...|
    +------------------+
    """
    return stddev_samp(col)


def stddev_pop(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns population standard deviation of
    the expression in a group.

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
        standard deviation of given column.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.range(6).select(sf.stddev_pop("id")).show()
    +-----------------+
    |   stddev_pop(id)|
    +-----------------+
    |1.707825127659...|
    +-----------------+
    """
    return _invoke_function_over_columns("stddev_pop", col)


def var_pop(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the population variance of the values in a group.

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
        variance of given column.

    Examples
    --------
    >>> df = spark.range(6)
    >>> df.select(var_pop(df.id)).first()
    Row(var_pop(id)=2.91666...)
    """
    return _invoke_function_over_columns("var_pop", col)


def var_samp(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the unbiased sample variance of
    the values in a group.

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
        variance of given column.

    Examples
    --------
    >>> df = spark.range(6)
    >>> df.select(var_samp(df.id)).show()
    +------------+
    |var_samp(id)|
    +------------+
    |         3.5|
    +------------+
    """
    return _invoke_function_over_columns("var_samp", col)


def variance(col: "ColumnOrName") -> Column:
    """
    Aggregate function: alias for var_samp

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
        variance of given column.

    Examples
    --------
    >>> df = spark.range(6)
    >>> df.select(variance(df.id)).show()
    +------------+
    |var_samp(id)|
    +------------+
    |         3.5|
    +------------+
    """
    return var_samp(col)


def weekday(col: "ColumnOrName") -> Column:
    """
    Returns the day of the week for date/timestamp (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target date/timestamp column to work on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the day of the week for date/timestamp (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).

    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(weekday('dt').alias('day')).show()
    +---+
    |day|
    +---+
    |  2|
    +---+
    """
    return _invoke_function_over_columns("isodow", col) - 1


def zeroifnull(col: "ColumnOrName") -> Column:
    """
    Returns zero if `col` is null, or `col` otherwise.

    .. versionadded:: 4.0.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str

    Examples
    --------
    >>> df = spark.createDataFrame([(None,), (1,)], ["a"])
    >>> df.select(zeroifnull(df.a).alias("result")).show()
    +------+
    |result|
    +------+
    |     0|
    |     1|
    +------+
    """
    return coalesce(col, lit(0))

def _to_date_or_timestamp(col: "ColumnOrName", spark_datatype: _types.DataType, format: Optional[str] = None) -> Column:
    if format is not None:
        raise ContributionsAcceptedError(
            "format is not yet supported as DuckDB and PySpark use a different way of specifying them."
            + " As a workaround, you can use F.call_function('strptime', col, format)"
        )
    return Column(_to_column_expr(col)).cast(spark_datatype)


def to_date(col: "ColumnOrName", format: Optional[str] = None) -> Column:
    """Converts a :class:`~pyspark.sql.Column` into :class:`pyspark.sql.types.DateType`
    using the optionally specified format. Specify formats according to `datetime pattern`_.
    By default, it follows casting rules to :class:`pyspark.sql.types.DateType` if the format
    is omitted. Equivalent to ``col.cast("date")``.

    .. _datetime pattern: https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

    .. versionadded:: 2.2.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        input column of values to convert.
    format: str, optional
        format to use to convert date values.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        date value as :class:`pyspark.sql.types.DateType` type.

    Examples
    --------
    >>> df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
    >>> df.select(to_date(df.t).alias('date')).collect()
    [Row(date=datetime.date(1997, 2, 28))]

    >>> df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
    >>> df.select(to_date(df.t, 'yyyy-MM-dd HH:mm:ss').alias('date')).collect()
    [Row(date=datetime.date(1997, 2, 28))]
    """
    return _to_date_or_timestamp(col, _types.DateType(), format)


def to_timestamp(col: "ColumnOrName", format: Optional[str] = None) -> Column:
    """Converts a :class:`~pyspark.sql.Column` into :class:`pyspark.sql.types.TimestampType`
    using the optionally specified format. Specify formats according to `datetime pattern`_.
    By default, it follows casting rules to :class:`pyspark.sql.types.TimestampType` if the format
    is omitted. Equivalent to ``col.cast("timestamp")``.

    .. _datetime pattern: https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html

    .. versionadded:: 2.2.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        column values to convert.
    format: str, optional
        format to use to convert timestamp values.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        timestamp value as :class:`pyspark.sql.types.TimestampType` type.

    Examples
    --------
    >>> df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
    >>> df.select(to_timestamp(df.t).alias('dt')).collect()
    [Row(dt=datetime.datetime(1997, 2, 28, 10, 30))]

    >>> df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
    >>> df.select(to_timestamp(df.t, 'yyyy-MM-dd HH:mm:ss').alias('dt')).collect()
    [Row(dt=datetime.datetime(1997, 2, 28, 10, 30))]
    """
    return _to_date_or_timestamp(col, _types.TimestampNTZType(), format)


def to_timestamp_ltz(
    timestamp: "ColumnOrName",
    format: Optional["ColumnOrName"] = None,
) -> Column:
    """
    Parses the `timestamp` with the `format` to a timestamp without time zone.
    Returns null with invalid input.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    timestamp : :class:`~pyspark.sql.Column` or str
        Input column or strings.
    format : :class:`~pyspark.sql.Column` or str, optional
        format to use to convert type `TimestampType` timestamp values.

    Examples
    --------
    >>> df = spark.createDataFrame([("2016-12-31",)], ["e"])
    >>> df.select(to_timestamp_ltz(df.e, lit("yyyy-MM-dd")).alias('r')).collect()
    ... # doctest: +SKIP
    [Row(r=datetime.datetime(2016, 12, 31, 0, 0))]

    >>> df = spark.createDataFrame([("2016-12-31",)], ["e"])
    >>> df.select(to_timestamp_ltz(df.e).alias('r')).collect()
    ... # doctest: +SKIP
    [Row(r=datetime.datetime(2016, 12, 31, 0, 0))]
    """
    return _to_date_or_timestamp(timestamp, _types.TimestampNTZType(), format)


def to_timestamp_ntz(
    timestamp: "ColumnOrName",
    format: Optional["ColumnOrName"] = None,
) -> Column:
    """
    Parses the `timestamp` with the `format` to a timestamp without time zone.
    Returns null with invalid input.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    timestamp : :class:`~pyspark.sql.Column` or str
        Input column or strings.
    format : :class:`~pyspark.sql.Column` or str, optional
        format to use to convert type `TimestampNTZType` timestamp values.

    Examples
    --------
    >>> df = spark.createDataFrame([("2016-04-08",)], ["e"])
    >>> df.select(to_timestamp_ntz(df.e, lit("yyyy-MM-dd")).alias('r')).collect()
    ... # doctest: +SKIP
    [Row(r=datetime.datetime(2016, 4, 8, 0, 0))]

    >>> df = spark.createDataFrame([("2016-04-08",)], ["e"])
    >>> df.select(to_timestamp_ntz(df.e).alias('r')).collect()
    ... # doctest: +SKIP
    [Row(r=datetime.datetime(2016, 4, 8, 0, 0))]
    """
    return _to_date_or_timestamp(timestamp, _types.TimestampNTZType(), format)


def substr(
    str: "ColumnOrName", pos: "ColumnOrName", len: Optional["ColumnOrName"] = None
) -> Column:
    """
    Returns the substring of `str` that starts at `pos` and is of length `len`,
    or the slice of byte array that starts at `pos` and is of length `len`.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    src : :class:`~pyspark.sql.Column` or str
        A column of string.
    pos : :class:`~pyspark.sql.Column` or str
        A column of string, the substring of `str` that starts at `pos`.
    len : :class:`~pyspark.sql.Column` or str, optional
        A column of string, the substring of `str` is of length `len`.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("Spark SQL", 5, 1,)], ["a", "b", "c"]
    ... ).select(sf.substr("a", "b", "c")).show()
    +---------------+
    |substr(a, b, c)|
    +---------------+
    |              k|
    +---------------+

    >>> import pyspark.sql.functions as sf
    >>> spark.createDataFrame(
    ...     [("Spark SQL", 5, 1,)], ["a", "b", "c"]
    ... ).select(sf.substr("a", "b")).show()
    +------------------------+
    |substr(a, b, 2147483647)|
    +------------------------+
    |                   k SQL|
    +------------------------+
    """
    if len is not None:
        return _invoke_function_over_columns("substring", str, pos, len)
    else:
        return _invoke_function_over_columns("substring", str, pos)


def _unix_diff(col: "ColumnOrName", part: str) -> Column:
    return _invoke_function_over_columns("date_diff", lit(part), lit("1970-01-01 00:00:00+00:00").cast("timestamp"), col)

def unix_date(col: "ColumnOrName") -> Column:
    """Returns the number of days since 1970-01-01.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    >>> df = spark.createDataFrame([('1970-01-02',)], ['t'])
    >>> df.select(unix_date(to_date(df.t)).alias('n')).collect()
    [Row(n=1)]
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """
    return _unix_diff(col, "days")


def unix_micros(col: "ColumnOrName") -> Column:
    """Returns the number of microseconds since 1970-01-01 00:00:00 UTC.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    >>> df = spark.createDataFrame([('2015-07-22 10:00:00',)], ['t'])
    >>> df.select(unix_micros(to_timestamp(df.t)).alias('n')).collect()
    [Row(n=1437584400000000)]
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """
    return _unix_diff(col, "microseconds")


def unix_millis(col: "ColumnOrName") -> Column:
    """Returns the number of milliseconds since 1970-01-01 00:00:00 UTC.
    Truncates higher levels of precision.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    >>> df = spark.createDataFrame([('2015-07-22 10:00:00',)], ['t'])
    >>> df.select(unix_millis(to_timestamp(df.t)).alias('n')).collect()
    [Row(n=1437584400000)]
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """
    return _unix_diff(col, "milliseconds")


def unix_seconds(col: "ColumnOrName") -> Column:
    """Returns the number of seconds since 1970-01-01 00:00:00 UTC.
    Truncates higher levels of precision.

    .. versionadded:: 3.5.0

    Examples
    --------
    >>> spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    >>> df = spark.createDataFrame([('2015-07-22 10:00:00',)], ['t'])
    >>> df.select(unix_seconds(to_timestamp(df.t)).alias('n')).collect()
    [Row(n=1437584400)]
    >>> spark.conf.unset("spark.sql.session.timeZone")
    """
    return _unix_diff(col, "seconds")


def arrays_overlap(a1: "ColumnOrName", a2: "ColumnOrName") -> Column:
    """
    Collection function: returns true if the arrays contain any common non-null element; if not,
    returns null if both the arrays are non-empty and any of them contains a null element; returns
    false otherwise.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        a column of Boolean type.

    Examples
    --------
    >>> df = spark.createDataFrame([(["a", "b"], ["b", "c"]), (["a"], ["b", "c"])], ['x', 'y'])
    >>> df.select(arrays_overlap(df.x, df.y).alias("overlap")).collect()
    [Row(overlap=True), Row(overlap=False)]
    """
    a1 = _to_column_expr(a1)
    a2 = _to_column_expr(a2)

    a1_has_null = _list_contains_null(a1)
    a2_has_null = _list_contains_null(a2)

    return Column(
        CaseExpression(
            FunctionExpression("list_has_any", a1, a2), ConstantExpression(True)
        ).otherwise(
            CaseExpression(
                (FunctionExpression("len", a1) > 0) & (FunctionExpression("len", a2) > 0) & (a1_has_null | a2_has_null), ConstantExpression(None)
                ).otherwise(ConstantExpression(False)))
    )


def _list_contains_null(c: ColumnExpression) -> Expression:
    return FunctionExpression(
        "list_contains",
        FunctionExpression(
            "list_transform", c, LambdaExpression("x", ColumnExpression("x").isnull())
        ),
        True,
    )


def arrays_zip(*cols: "ColumnOrName") -> Column:
    """
    Collection function: Returns a merged array of structs in which the N-th struct contains all
    N-th values of input arrays. If one of the arrays is shorter than others then
    resulting struct type value will be a `null` for missing elements.

    .. versionadded:: 2.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    cols : :class:`~pyspark.sql.Column` or str
        columns of arrays to be merged.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        merged array of entries.

    Examples
    --------
    >>> from pyspark.sql.functions import arrays_zip
    >>> df = spark.createDataFrame([([1, 2, 3], [2, 4, 6], [3, 6])], ['vals1', 'vals2', 'vals3'])
    >>> df = df.select(arrays_zip(df.vals1, df.vals2, df.vals3).alias('zipped'))
    >>> df.show(truncate=False)
    +------------------------------------+
    |zipped                              |
    +------------------------------------+
    |[{1, 2, 3}, {2, 4, 6}, {3, 6, NULL}]|
    +------------------------------------+
    >>> df.printSchema()
    root
     |-- zipped: array (nullable = true)
     |    |-- element: struct (containsNull = false)
     |    |    |-- vals1: long (nullable = true)
     |    |    |-- vals2: long (nullable = true)
     |    |    |-- vals3: long (nullable = true)
    """
    return _invoke_function_over_columns("list_zip", *cols)


def substring(str: "ColumnOrName", pos: int, len: int) -> Column:
    """
    Substring starts at `pos` and is of length `len` when str is String type or
    returns the slice of byte array that starts at `pos` in byte and is of length `len`
    when str is Binary type.
    .. versionadded:: 1.5.0
    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    Notes
    -----
    The position is not zero based, but 1 based index.
    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        target column to work on.
    pos : int
        starting position in str.
    len : int
        length of chars.
    Returns
    -------
    :class:`~pyspark.sql.Column`
        substring of given value.
    Examples
    --------
    >>> df = spark.createDataFrame([('abcd',)], ['s',])
    >>> df.select(substring(df.s, 1, 2).alias('s')).collect()
    [Row(s='ab')]
    """
    return _invoke_function(
        "substring",
        _to_column_expr(str),
        ConstantExpression(pos),
        ConstantExpression(len),
    )


def contains(left: "ColumnOrName", right: "ColumnOrName") -> Column:
    """
    Returns a boolean. The value is True if right is found inside left.
    Returns NULL if either input expression is NULL. Otherwise, returns False.
    Both left or right must be of STRING or BINARY type.
    .. versionadded:: 3.5.0
    Parameters
    ----------
    left : :class:`~pyspark.sql.Column` or str
        The input column or strings to check, may be NULL.
    right : :class:`~pyspark.sql.Column` or str
        The input column or strings to find, may be NULL.
    Examples
    --------
    >>> df = spark.createDataFrame([("Spark SQL", "Spark")], ['a', 'b'])
    >>> df.select(contains(df.a, df.b).alias('r')).collect()
    [Row(r=True)]
    >>> df = spark.createDataFrame([("414243", "4243",)], ["c", "d"])
    >>> df = df.select(to_binary("c").alias("c"), to_binary("d").alias("d"))
    >>> df.printSchema()
    root
     |-- c: binary (nullable = true)
     |-- d: binary (nullable = true)
    >>> df.select(contains("c", "d"), contains("d", "c")).show()
    +--------------+--------------+
    |contains(c, d)|contains(d, c)|
    +--------------+--------------+
    |          true|         false|
    +--------------+--------------+
    """
    return _invoke_function_over_columns("contains", left, right)


def reverse(col: "ColumnOrName") -> Column:
    """
    Collection function: returns a reversed string or an array with reverse order of elements.
    .. versionadded:: 1.5.0
    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        name of column or expression
    Returns
    -------
    :class:`~pyspark.sql.Column`
        array of elements in reverse order.
    Examples
    --------
    >>> df = spark.createDataFrame([('Spark SQL',)], ['data'])
    >>> df.select(reverse(df.data).alias('s')).collect()
    [Row(s='LQS krapS')]
    >>> df = spark.createDataFrame([([2, 1, 3],) ,([1],) ,([],)], ['data'])
    >>> df.select(reverse(df.data).alias('r')).collect()
    [Row(r=[3, 1, 2]), Row(r=[1]), Row(r=[])]
    """
    return _invoke_function("reverse", _to_column_expr(col))

def concat(*cols: "ColumnOrName") -> Column:
    """
    Concatenates multiple input columns together into a single column.
    The function works with strings, numeric, binary and compatible array columns.
    .. versionadded:: 1.5.0
    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    Parameters
    ----------
    cols : :class:`~pyspark.sql.Column` or str
        target column or columns to work on.
    Returns
    -------
    :class:`~pyspark.sql.Column`
        concatenated values. Type of the `Column` depends on input columns' type.
    See Also
    --------
    :meth:`pyspark.sql.functions.array_join` : to concatenate string columns with delimiter
    Examples
    --------
    >>> df = spark.createDataFrame([('abcd','123')], ['s', 'd'])
    >>> df = df.select(concat(df.s, df.d).alias('s'))
    >>> df.collect()
    [Row(s='abcd123')]
    >>> df
    DataFrame[s: string]
    >>> df = spark.createDataFrame([([1, 2], [3, 4], [5]), ([1, 2], None, [3])], ['a', 'b', 'c'])
    >>> df = df.select(concat(df.a, df.b, df.c).alias("arr"))
    >>> df.collect()
    [Row(arr=[1, 2, 3, 4, 5]), Row(arr=None)]
    >>> df
    DataFrame[arr: array<bigint>]
    """
    return _invoke_function_over_columns("concat", *cols)


def instr(str: "ColumnOrName", substr: str) -> Column:
    """
    Locate the position of the first occurrence of substr column in the given string.
    Returns null if either of the arguments are null.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    The position is not zero based, but 1 based index. Returns 0 if substr
    could not be found in str.

    Parameters
    ----------
    str : :class:`~pyspark.sql.Column` or str
        target column to work on.
    substr : str
        substring to look for.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        location of the first occurrence of the substring as integer.

    Examples
    --------
    >>> df = spark.createDataFrame([('abcd',)], ['s',])
    >>> df.select(instr(df.s, 'b').alias('s')).collect()
    [Row(s=2)]
    """
    return _invoke_function("instr", _to_column_expr(str), ConstantExpression(substr))

def expr(str: str) -> Column:
    """Parses the expression string into the column that it represents

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    str : str
        expression defined in string.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        column representing the expression.

    Examples
    --------
    >>> df = spark.createDataFrame([["Alice"], ["Bob"]], ["name"])
    >>> df.select("name", expr("length(name)")).show()
    +-----+------------+
    | name|length(name)|
    +-----+------------+
    |Alice|           5|
    |  Bob|           3|
    +-----+------------+
    """
    return Column(SQLExpression(str))

def broadcast(df: "DataFrame") -> "DataFrame":
    """
    The broadcast function in Spark is used to optimize joins by broadcasting a smaller
    dataset to all the worker nodes. However, DuckDB operates on a single-node architecture .
    As a result, the function simply returns the input DataFrame without applying any modifications
    or optimizations, since broadcasting is not applicable in the DuckDB context.
    """
    return df
