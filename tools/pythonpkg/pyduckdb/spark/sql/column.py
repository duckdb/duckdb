from typing import Union, TYPE_CHECKING, Any, Callable

from pyduckdb.spark.sql.types import DataType

if TYPE_CHECKING:
    from ._typing import ColumnOrName, LiteralType, DecimalLiteral, DateTimeLiteral

from duckdb import ConstantExpression, ColumnExpression, FunctionExpression, Expression

from duckdb.typing import DuckDBPyType

__all__ = ["Column"]


def _bin_op(
    name: str,
    doc: str = "binary operator",
) -> Callable[["Column", Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"]], "Column"]:
    """Create a method for given binary operator"""

    def _(
        self: "Column",
        other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"],
    ) -> "Column":
        jc = other.expr if isinstance(other, Column) else other
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
        other = other.expr if isinstance(other, Column) else other
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

    def alias(self, alias: str):
        return Column(self.expr.alias(alias))

    def when(self, condition: "Column", value: Any):
        if not isinstance(condition, Column):
            raise TypeError("condition should be a Column")
        v = value.expr if isinstance(value, Column) else value
        expr = self.expr.when(condition.expr, v)
        return Column(expr)

    def otherwise(self, value: Any):
        v = value.expr if isinstance(value, Column) else value
        expr = self.expr.otherwise(v)
        return Column(expr)

    def cast(self, dataType: Union[DataType, str]) -> "Column":
        if isinstance(dataType, str):
            # Try to construct a default DuckDBPyType from it
            internal_type = DuckDBPyType(dataType)
        else:
            internal_type = dataType.duckdb_type
        return Column(self.expr.cast(internal_type))

    # logistic operators
    def __eq__(  # type: ignore[override]
        self,
        other: Union["Column", "LiteralType", "DecimalLiteral", "DateTimeLiteral"],
    ) -> "Column":
        """binary function"""
        return Column(self.expr == (other.expr if isinstance(other, Column) else other))

    def __ne__(  # type: ignore[override]
        self,
        other: Any,
    ) -> "Column":
        """binary function"""
        return Column(self.expr != (other.expr if isinstance(other, Column) else other))

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
