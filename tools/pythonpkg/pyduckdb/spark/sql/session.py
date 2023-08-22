from typing import Optional, List, Tuple, Any, Union, Iterable, TYPE_CHECKING
import uuid

if TYPE_CHECKING:
    from pyduckdb.spark.sql.catalog import Catalog
    from pandas.core.frame import DataFrame as PandasDataFrame

from pyduckdb.spark.exception import ContributionsAcceptedError

from pyduckdb.spark.sql.types import StructType, AtomicType, DataType
from pyduckdb.spark.conf import SparkConf
from pyduckdb.spark.sql.dataframe import DataFrame
from pyduckdb.spark.sql.conf import RuntimeConfig
from pyduckdb.spark.sql.readwriter import DataFrameReader
from pyduckdb.spark.context import SparkContext
from pyduckdb.spark.sql.udf import UDFRegistration
from pyduckdb.spark.sql.streaming import DataStreamReader
import duckdb

# In spark:
# SparkSession holds a SparkContext
# SparkContext gets created from SparkConf
# At this level the check is made to determine whether the instance already exists and just needs to be retrieved or it needs to be created

# For us this is done inside of `duckdb.connect`, based on the passed in path + configuration
# SparkContext can be compared to our Connection class, and SparkConf to our ClientContext class


# data is a List of rows
# every value in each row needs to be turned into a Value
def _combine_data_and_schema(data: Iterable[Any], schema: StructType):
    from pyduckdb import Value

    new_data = []
    for row in data:
        new_row = [Value(x, dtype.duckdb_type) for x, dtype in zip(row, [y.dataType for y in schema])]
        new_data.append(new_row)
    return new_data


class SparkSession:
    def __init__(self, context: SparkContext):
        self.conn = context.connection
        self._context = context
        self._conf = RuntimeConfig(self.conn)

    def _create_dataframe(self, data: Union[Iterable[Any], "PandasDataFrame"]) -> DataFrame:
        try:
            import pandas

            has_pandas = True
        except:
            has_pandas = False
        if has_pandas and isinstance(data, pandas.DataFrame):
            unique_name = f'pyspark_pandas_df_{uuid.uuid1()}'
            self.conn.register(unique_name, data)
            return DataFrame(self.conn.sql(f'select * from "{unique_name}"'), self)

        def verify_tuple_integrity(tuples):
            if len(tuples) <= 1:
                return
            assert all([len(x) == len(tuples[0]) for x in tuples[1:]])

        if not isinstance(data, list):
            data = list(data)
        verify_tuple_integrity(data)

        def construct_query(tuples) -> str:
            def construct_values_list(row, start_param_idx):
                parameter_count = len(row)
                parameters = [f'${x+start_param_idx}' for x in range(parameter_count)]
                parameters = '(' + ', '.join(parameters) + ')'
                return parameters

            row_size = len(tuples[0])
            values_list = [construct_values_list(x, 1 + (i * row_size)) for i, x in enumerate(tuples)]
            values_list = ', '.join(values_list)

            query = f"""
                select * from (values {values_list})
            """
            return query

        query = construct_query(data)

        def construct_parameters(tuples):
            parameters = []
            for row in tuples:
                parameters.extend(list(row))
            return parameters

        parameters = construct_parameters(data)

        rel = self.conn.sql(query, params=parameters)
        return DataFrame(rel, self)

    def createDataFrame(
        self,
        data: Union["PandasDataFrame", Iterable[Any]],
        schema: Optional[Union[StructType, List[str]]] = None,
        samplingRatio: Optional[float] = None,
        verifySchema: bool = True,
    ) -> DataFrame:
        if samplingRatio:
            raise NotImplementedError
        if not verifySchema:
            raise NotImplementedError
        types = None
        names = None
        if schema:
            if isinstance(schema, StructType):
                types, names = schema.extract_types_and_names()
            else:
                names = schema

        try:
            import pandas

            has_pandas = True
        except:
            has_pandas = False
        # Falsey check on pandas dataframe is not defined, so first check if it's not a pandas dataframe
        # Then check if 'data' is None or []
        # Finally check if a schema was provided
        is_empty = False
        if (not has_pandas or (has_pandas and not isinstance(data, pandas.DataFrame))) and not data and names:
            # Create NULLs for every type in our the dataframe
            is_empty = True
            data = [tuple(None for _ in names)]

        if schema and isinstance(schema, StructType):
            # Transform the data into Values to combine the data+schema
            data = _combine_data_and_schema(data, schema)

        df = self._create_dataframe(data)
        if is_empty:
            rel = df.relation
            # Add impossible where clause
            rel = rel.filter('1=0')
            df = DataFrame(rel, self)

        # Cast to types
        if types:
            df = df._cast_types(*types)
        # Alias to names
        if names:
            df = df.toDF(*names)
        return df

    def newSession(self) -> "SparkSession":
        return SparkSession(self._context)

    def range(
        self, start: int, end: Optional[int] = None, step: int = 1, numPartitions: Optional[int] = None
    ) -> "DataFrame":
        raise ContributionsAcceptedError

    def sql(self, sqlQuery: str, **kwargs: Any) -> DataFrame:
        if kwargs:
            raise NotImplementedError
        relation = self.conn.sql(sqlQuery)
        return DataFrame(relation, self)

    def stop(self) -> None:
        self._context.stop()

    def table(self, tableName: str) -> DataFrame:
        relation = self.conn.table(tableName)
        return DataFrame(relation, self)

    def getActiveSession(self) -> "SparkSession":
        return self

    @property
    def catalog(self) -> "Catalog":
        if not hasattr(self, "_catalog"):
            from pyduckdb.spark.sql.catalog import Catalog

            self._catalog = Catalog(self)
        return self._catalog

    @property
    def conf(self) -> RuntimeConfig:
        return self._conf

    @property
    def read(self) -> DataFrameReader:
        return DataFrameReader(self)

    @property
    def readStream(self) -> DataStreamReader:
        return DataStreamReader(self)

    @property
    def sparkContext(self) -> SparkContext:
        return self._context

    @property
    def streams(self) -> Any:
        raise ContributionsAcceptedError

    @property
    def udf(self) -> UDFRegistration:
        return UDFRegistration()

    @property
    def version(self) -> str:
        return '1.0.0'

    class Builder:
        def __init__(self):
            self.name = "builder"
            self._master = ':memory:'
            self._config = {}

        def master(self, name: str) -> "SparkSession.Builder":
            self._master = name
            return self

        def appName(self, name: str) -> "SparkSession.Builder":
            # no-op
            return self

        def remote(self, url: str) -> "SparkSession.Builder":
            # no-op
            return self

        def getOrCreate(self) -> "SparkSession":
            # TODO: use the config to pass in methods to 'connect'
            context = SparkContext(self._master)
            return SparkSession(context)

        def config(
            self, key: Optional[str] = None, value: Optional[Any] = None, conf: Optional[SparkConf] = None
        ) -> "SparkSession.Builder":
            if conf:
                raise NotImplementedError
            if key and value:
                self._config[key] = value
            return self

        def enableHiveSupport(self) -> "SparkSession.Builder":
            # no-op
            return self

    builder = Builder()


__all__ = ["SparkSession"]
