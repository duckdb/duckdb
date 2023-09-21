from typing import TYPE_CHECKING, Iterable, Union, List, Optional, cast
from .types import StructType
from ..exception import ContributionsAcceptedError

PrimitiveType = Union[bool, float, int, str]
OptionalPrimitiveType = Optional[PrimitiveType]

if TYPE_CHECKING:
    from duckdb.experimental.spark.sql.dataframe import DataFrame
    from duckdb.experimental.spark.sql.session import SparkSession


class DataFrameWriter:
    def __init__(self, dataframe: "DataFrame"):
        self.dataframe = dataframe

    def saveAsTable(self, table_name: str) -> None:
        relation = self.dataframe.relation
        relation.create(table_name)


class DataFrameReader:
    def __init__(self, session: "SparkSession"):
        self.session = session

    def load(
        self,
        path: Optional[Union[str, List[str]]] = None,
        format: Optional[str] = None,
        schema: Optional[Union[StructType, str]] = None,
        **options: OptionalPrimitiveType,
    ) -> "DataFrame":
        from duckdb.experimental.spark.sql.dataframe import DataFrame

        if not isinstance(path, str):
            raise ImportError
        if options:
            raise ContributionsAcceptedError

        rel = None
        if format:
            format = format.lower()
            if format == 'csv' or format == 'tsv':
                rel = self.session.conn.read_csv(path)
            elif format == 'json':
                rel = self.session.conn.read_json(path)
            elif format == 'parquet':
                rel = self.session.conn.read_parquet(path)
            else:
                raise ContributionsAcceptedError
        else:
            rel = self.session.conn.sql(f'select * from {path}')
        df = DataFrame(rel, self.session)
        if schema:
            if not isinstance(schema, StructType):
                raise ContributionsAcceptedError
            schema = cast(StructType, schema)
            types, names = schema.extract_types_and_names()
            df = df._cast_types(types)
            df = df.toDF(names)
        raise NotImplementedError

    def csv(
        self,
        path: Union[str, List[str]],
        schema: Optional[Union[StructType, str]] = None,
        sep: Optional[str] = None,
        encoding: Optional[str] = None,
        quote: Optional[str] = None,
        escape: Optional[str] = None,
        comment: Optional[str] = None,
        header: Optional[Union[bool, str]] = None,
        inferSchema: Optional[Union[bool, str]] = None,
        ignoreLeadingWhiteSpace: Optional[Union[bool, str]] = None,
        ignoreTrailingWhiteSpace: Optional[Union[bool, str]] = None,
        nullValue: Optional[str] = None,
        nanValue: Optional[str] = None,
        positiveInf: Optional[str] = None,
        negativeInf: Optional[str] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        maxColumns: Optional[Union[int, str]] = None,
        maxCharsPerColumn: Optional[Union[int, str]] = None,
        maxMalformedLogPerPartition: Optional[Union[int, str]] = None,
        mode: Optional[str] = None,
        columnNameOfCorruptRecord: Optional[str] = None,
        multiLine: Optional[Union[bool, str]] = None,
        charToEscapeQuoteEscaping: Optional[str] = None,
        samplingRatio: Optional[Union[float, str]] = None,
        enforceSchema: Optional[Union[bool, str]] = None,
        emptyValue: Optional[str] = None,
        locale: Optional[str] = None,
        lineSep: Optional[str] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        modifiedBefore: Optional[Union[bool, str]] = None,
        modifiedAfter: Optional[Union[bool, str]] = None,
        unescapedQuoteHandling: Optional[str] = None,
    ) -> "DataFrame":
        if not isinstance(path, str):
            raise NotImplementedError
        if schema and not isinstance(schema, StructType):
            raise ContributionsAcceptedError
        if comment:
            raise ContributionsAcceptedError
        if inferSchema:
            raise ContributionsAcceptedError
        if ignoreLeadingWhiteSpace:
            raise ContributionsAcceptedError
        if ignoreTrailingWhiteSpace:
            raise ContributionsAcceptedError
        if nanValue:
            raise ConnectionAbortedError
        if positiveInf:
            raise ConnectionAbortedError
        if negativeInf:
            raise ConnectionAbortedError
        if negativeInf:
            raise ConnectionAbortedError
        if maxColumns:
            raise ContributionsAcceptedError
        if maxCharsPerColumn:
            raise ContributionsAcceptedError
        if maxMalformedLogPerPartition:
            raise ContributionsAcceptedError
        if mode:
            raise ContributionsAcceptedError
        if columnNameOfCorruptRecord:
            raise ContributionsAcceptedError
        if multiLine:
            raise ContributionsAcceptedError
        if charToEscapeQuoteEscaping:
            raise ContributionsAcceptedError
        if samplingRatio:
            raise ContributionsAcceptedError
        if enforceSchema:
            raise ContributionsAcceptedError
        if emptyValue:
            raise ContributionsAcceptedError
        if locale:
            raise ContributionsAcceptedError
        if pathGlobFilter:
            raise ContributionsAcceptedError
        if recursiveFileLookup:
            raise ContributionsAcceptedError
        if modifiedBefore:
            raise ContributionsAcceptedError
        if modifiedAfter:
            raise ContributionsAcceptedError
        if unescapedQuoteHandling:
            raise ContributionsAcceptedError
        if lineSep:
            # We have support for custom newline, just needs to be ported to 'read_csv'
            raise NotImplementedError

        dtype = None
        names = None
        if schema:
            schema = cast(StructType, schema)
            dtype, names = schema.extract_types_and_names()

        rel = self.session.conn.read_csv(
            path,
            header=header if isinstance(header, bool) else header == "True",
            sep=sep,
            dtype=dtype,
            na_values=nullValue,
            quotechar=quote,
            escapechar=escape,
            encoding=encoding,
            date_format=dateFormat,
            timestamp_format=timestampFormat,
        )
        df = DataFrame(rel, self.session)
        if names:
            df = df.toDF(*names)
        return df


__all__ = ["DataFrameWriter", "DataFrameReader"]
