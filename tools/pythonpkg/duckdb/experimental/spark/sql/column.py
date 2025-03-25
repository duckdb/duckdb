from typing import Union, TYPE_CHECKING, Any, cast, Callable, Tuple
from ..exception import ContributionsAcceptedError

from .types import DataType

if TYPE_CHECKING:
    from ._typing import ColumnOrName, LiteralType, DecimalLiteral, DateTimeLiteral

from duckdb import ConstantExpression, ColumnExpression, FunctionExpression, Expression

from duckdb.typing import DuckDBPyType

__all__ = ["Column"]


def _get_expr(x) -> Expression:
    return x.expr if isinstance(x, Column) else ConstantExpression(x)


def _func_op(name: str, doc: str = "") -> Callable[["Column"], "Column"]:
    def _(self: "Column") -> "Column":
        njc = getattr(self.expr, name)()
        return Column(njc)

    _.__doc__ = doc
    return _


def _unary_op(
    name: str,
    doc: str = "unary operator",
) -> Callable[["Column"], "Column"]:
    """Create a method for given unary operator"""

    def _(self: "Column") -> "Column":
        # Call the function identified by 'name' on the internal Expression object
        expr = getattr(self.expr, name)()
        return Column(expr)

    _.__doc__ = doc
    return _


def _bin_op(
    name: str,
    doc: str = "binary operator",
) -> Callable[["Column", Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]], "Column"]:
    """Create a method for given binary operator"""

    def _(
        self: "Column",
        other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"],
    ) -> "Column":
        jc = _get_expr(other)
        njc = getattr(self.expr, name)(jc)
        return Column(njc)

    _.__doc__ = doc
    return _


def _bin_func(
    name: str,
    doc: str = "binary function",
) -> Callable[["Column", Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]], "Column"]:
    """Create a function expression for the given binary function"""

    def _(
        self: "Column",
        other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"],
    ) -> "Column":
        other = _get_expr(other)
        func = FunctionExpression(name, self.expr, other)
        return Column(func)

    _.__doc__ = doc
    return _


class Column:
    """
    A column in a DataFrame.

    :class:`Column` instances can be created by::

        # 1. Select a column out of a DataFrame

        df.colName
        df["colName"]

        # 2. Create from an expression
        df.colName + 1
        1 / df.colName

    .. versionadded:: 1.3.0
    """

    def __init__(self, expr: Expression):
        self.expr = expr

    # arithmetic operators
    def __neg__(self):
        return Column(-self.expr)

    # `and`, `or`, `not` cannot be overloaded in Python,
    # so use bitwise operators as boolean operators
    __and__ = _bin_op("__and__")
    __or__ = _bin_op("__or__")
    __invert__ = _func_op("__invert__")
    __rand__ = _bin_op("__rand__")
    __ror__ = _bin_op("__ror__")

    __add__ = _bin_op("__add__")

    __sub__ = _bin_op("__sub__")

    __mul__ = _bin_op("__mul__")

    __div__ = _bin_op("__div__")

    __truediv__ = _bin_op("__truediv__")

    __mod__ = _bin_op("__mod__")

    __pow__ = _bin_op("__pow__")

    __radd__ = _bin_op("__radd__")

    __rsub__ = _bin_op("__rsub__")

    __rmul__ = _bin_op("__rmul__")

    __rdiv__ = _bin_op("__rdiv__")

    __rtruediv__ = _bin_op("__rtruediv__")

    __rmod__ = _bin_op("__rmod__")

    __rpow__ = _bin_op("__rpow__")

    def __getitem__(self, k: Any) -> "Column":
        """
        An expression that gets an item at position ``ordinal`` out of a list,
        or gets an item by key out of a dict.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        k
            a literal value, or a slice object without step.

        Returns
        -------
        :class:`Column`
            Column representing the item got by key out of a dict, or substrings sliced by
            the given slice object.

        Examples
        --------
        >>> df = spark.createDataFrame([('abcedfg', {"key": "value"})], ["l", "d"])
        >>> df.select(df.l[slice(1, 3)], df.d['key']).show()
        +------------------+------+
        |substring(l, 1, 3)|d[key]|
        +------------------+------+
        |               abc| value|
        +------------------+------+
        """
        if isinstance(k, slice):
            raise ContributionsAcceptedError
            # if k.step is not None:
            #    raise ValueError("Using a slice with a step value is not supported")
            # return self.substr(k.start, k.stop)
        else:
            # FIXME: this is super hacky
            expr_str = str(self.expr) + "." + str(k)
            return Column(ColumnExpression(expr_str))

    def __getattr__(self, item: Any) -> "Column":
        """
        An expression that gets an item at position ``ordinal`` out of a list,
        or gets an item by key out of a dict.

        Parameters
        ----------
        item
            a literal value.

        Returns
        -------
        :class:`Column`
            Column representing the item got by key out of a dict.

        Examples
        --------
        >>> df = spark.createDataFrame([('abcedfg', {"key": "value"})], ["l", "d"])
        >>> df.select(df.d.key).show()
        +------+
        |d[key]|
        +------+
        | value|
        +------+
        """
        if item.startswith("__"):
            raise AttributeError("Can not access __ (dunder) method")
        return self[item]

    def alias(self, alias: str):
        return Column(self.expr.alias(alias))

    def when(self, condition: "Column", value: Any):
        if not isinstance(condition, Column):
            raise TypeError("condition should be a Column")
        v = _get_expr(value)
        expr = self.expr.when(condition.expr, v)
        return Column(expr)

    def otherwise(self, value: Any):
        v = _get_expr(value)
        expr = self.expr.otherwise(v)
        return Column(expr)

    def cast(self, dataType: Union[DataType, str]) -> "Column":
        if isinstance(dataType, str):
            # Try to construct a default DuckDBPyType from it
            internal_type = DuckDBPyType(dataType)
        else:
            internal_type = dataType.duckdb_type
        return Column(self.expr.cast(internal_type))

    def isin(self, *cols: Any) -> "Column":
        if len(cols) == 1 and isinstance(cols[0], (list, set)):
            # Only one argument supplied, it's a list
            cols = cast(Tuple, cols[0])

        cols = cast(
            Tuple,
            [_get_expr(c) for c in cols],
        )
        return Column(self.expr.isin(*cols))

    # logistic operators
    def __eq__(  # type: ignore[override]
        self,
        other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"],
    ) -> "Column":
        """binary function"""
        return Column(self.expr == (_get_expr(other)))

    def __ne__(  # type: ignore[override]
        self,
        other: Any,
    ) -> "Column":
        """binary function"""
        return Column(self.expr != (_get_expr(other)))

    __lt__ = _bin_op("__lt__")

    __le__ = _bin_op("__le__")

    __ge__ = _bin_op("__ge__")

    __gt__ = _bin_op("__gt__")

    # String interrogation methods

    contains = _bin_func("contains")
    rlike = _bin_func("regexp_matches")
    like = _bin_func("~~")
    ilike = _bin_func("~~*")
    startswith = _bin_func("starts_with")
    endswith = _bin_func("suffix")

    # order
    _asc_doc = """
    Returns a sort expression based on the ascending order of the column.
    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.asc()).collect()
    [Row(name='Alice'), Row(name='Tom')]
    """

    _asc_nulls_first_doc = """
    Returns a sort expression based on ascending order of the column, and null values
    return before non-null values.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), (None, 60), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.asc_nulls_first()).collect()
    [Row(name=None), Row(name='Alice'), Row(name='Tom')]

    """
    _asc_nulls_last_doc = """
    Returns a sort expression based on ascending order of the column, and null values
    appear after non-null values.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), (None, 60), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.asc_nulls_last()).collect()
    [Row(name='Alice'), Row(name='Tom'), Row(name=None)]

    """
    _desc_doc = """
    Returns a sort expression based on the descending order of the column.
    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.desc()).collect()
    [Row(name='Tom'), Row(name='Alice')]
    """
    _desc_nulls_first_doc = """
    Returns a sort expression based on the descending order of the column, and null values
    appear before non-null values.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), (None, 60), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.desc_nulls_first()).collect()
    [Row(name=None), Row(name='Tom'), Row(name='Alice')]

    """
    _desc_nulls_last_doc = """
    Returns a sort expression based on the descending order of the column, and null values
    appear after non-null values.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> df = spark.createDataFrame([('Tom', 80), (None, 60), ('Alice', None)], ["name", "height"])
    >>> df.select(df.name).orderBy(df.name.desc_nulls_last()).collect()
    [Row(name='Tom'), Row(name='Alice'), Row(name=None)]
    """

    asc = _unary_op("asc", _asc_doc)
    desc = _unary_op("desc", _desc_doc)
    nulls_first = _unary_op("nulls_first")
    nulls_last = _unary_op("nulls_last")


    def asc_nulls_first(self) -> "Column":
        return self.asc().nulls_first()

    def asc_nulls_last(self) -> "Column":
        return self.asc().nulls_last()

    def desc_nulls_first(self) -> "Column":
        return self.desc().nulls_first()

    def desc_nulls_last(self) -> "Column":
        return self.desc().nulls_last()

    def isNull(self) -> "Column":
        return Column(self.expr.isnull())

    def isNotNull(self) -> "Column":
        return Column(self.expr.isnotnull())

