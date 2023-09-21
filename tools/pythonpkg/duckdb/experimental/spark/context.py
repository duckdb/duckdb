from typing import Optional
import duckdb
from duckdb import DuckDBPyConnection

from duckdb.experimental.spark.exception import ContributionsAcceptedError
from duckdb.experimental.spark.conf import SparkConf


class SparkContext:
    def __init__(self, master: str):
        self._connection = duckdb.connect(':memory:')

    @property
    def connection(self) -> DuckDBPyConnection:
        return self._connection

    def stop(self) -> None:
        self._connection.close()

    @classmethod
    def getOrCreate(cls, conf: Optional[SparkConf] = None) -> "SparkContext":
        raise ContributionsAcceptedError

    @classmethod
    def setSystemProperty(cls, key: str, value: str) -> None:
        raise ContributionsAcceptedError

    @property
    def applicationId(self) -> str:
        raise ContributionsAcceptedError

    @property
    def defaultMinPartitions(self) -> int:
        raise ContributionsAcceptedError

    @property
    def defaultParallelism(self) -> int:
        raise ContributionsAcceptedError

    # @property
    # def resources(self) -> Dict[str, ResourceInformation]:
    # 	raise ContributionsAcceptedError

    @property
    def startTime(self) -> str:
        raise ContributionsAcceptedError

    @property
    def uiWebUrl(self) -> str:
        raise ContributionsAcceptedError

    @property
    def version(self) -> str:
        raise ContributionsAcceptedError

    def __repr__(self) -> str:
        raise ContributionsAcceptedError

    # def accumulator(self, value: ~T, accum_param: Optional[ForwardRef('AccumulatorParam[T]')] = None) -> 'Accumulator[T]':
    # 	pass

    def addArchive(self, path: str) -> None:
        raise ContributionsAcceptedError

    def addFile(self, path: str, recursive: bool = False) -> None:
        raise ContributionsAcceptedError

    def addPyFile(self, path: str) -> None:
        raise ContributionsAcceptedError

    # def binaryFiles(self, path: str, minPartitions: Optional[int] = None) -> duckdb.experimental.spark.rdd.RDD[typing.Tuple[str, bytes]]:
    # 	pass

    # def binaryRecords(self, path: str, recordLength: int) -> duckdb.experimental.spark.rdd.RDD[bytes]:
    # 	pass

    # def broadcast(self, value: ~T) -> 'Broadcast[T]':
    # 	pass

    def cancelAllJobs(self) -> None:
        raise ContributionsAcceptedError

    def cancelJobGroup(self, groupId: str) -> None:
        raise ContributionsAcceptedError

    def dump_profiles(self, path: str) -> None:
        raise ContributionsAcceptedError

    # def emptyRDD(self) -> duckdb.experimental.spark.rdd.RDD[typing.Any]:
    # 	pass

    def getCheckpointDir(self) -> Optional[str]:
        raise ContributionsAcceptedError

    def getConf(self) -> SparkConf:
        raise ContributionsAcceptedError

    def getLocalProperty(self, key: str) -> Optional[str]:
        raise ContributionsAcceptedError

    # def hadoopFile(self, path: str, inputFormatClass: str, keyClass: str, valueClass: str, keyConverter: Optional[str] = None, valueConverter: Optional[str] = None, conf: Optional[Dict[str, str]] = None, batchSize: int = 0) -> pyspark.rdd.RDD[typing.Tuple[~T, ~U]]:
    # 	pass

    # def hadoopRDD(self, inputFormatClass: str, keyClass: str, valueClass: str, keyConverter: Optional[str] = None, valueConverter: Optional[str] = None, conf: Optional[Dict[str, str]] = None, batchSize: int = 0) -> pyspark.rdd.RDD[typing.Tuple[~T, ~U]]:
    # 	pass

    # def newAPIHadoopFile(self, path: str, inputFormatClass: str, keyClass: str, valueClass: str, keyConverter: Optional[str] = None, valueConverter: Optional[str] = None, conf: Optional[Dict[str, str]] = None, batchSize: int = 0) -> pyspark.rdd.RDD[typing.Tuple[~T, ~U]]:
    # 	pass

    # def newAPIHadoopRDD(self, inputFormatClass: str, keyClass: str, valueClass: str, keyConverter: Optional[str] = None, valueConverter: Optional[str] = None, conf: Optional[Dict[str, str]] = None, batchSize: int = 0) -> pyspark.rdd.RDD[typing.Tuple[~T, ~U]]:
    # 	pass

    # def parallelize(self, c: Iterable[~T], numSlices: Optional[int] = None) -> pyspark.rdd.RDD[~T]:
    # 	pass

    # def pickleFile(self, name: str, minPartitions: Optional[int] = None) -> pyspark.rdd.RDD[typing.Any]:
    # 	pass

    # def range(self, start: int, end: Optional[int] = None, step: int = 1, numSlices: Optional[int] = None) -> pyspark.rdd.RDD[int]:
    # 	pass

    # def runJob(self, rdd: pyspark.rdd.RDD[~T], partitionFunc: Callable[[Iterable[~T]], Iterable[~U]], partitions: Optional[Sequence[int]] = None, allowLocal: bool = False) -> List[~U]:
    # 	pass

    # def sequenceFile(self, path: str, keyClass: Optional[str] = None, valueClass: Optional[str] = None, keyConverter: Optional[str] = None, valueConverter: Optional[str] = None, minSplits: Optional[int] = None, batchSize: int = 0) -> pyspark.rdd.RDD[typing.Tuple[~T, ~U]]:
    # 	pass

    def setCheckpointDir(self, dirName: str) -> None:
        raise ContributionsAcceptedError

    def setJobDescription(self, value: str) -> None:
        raise ContributionsAcceptedError

    def setJobGroup(self, groupId: str, description: str, interruptOnCancel: bool = False) -> None:
        raise ContributionsAcceptedError

    def setLocalProperty(self, key: str, value: str) -> None:
        raise ContributionsAcceptedError

    def setLogLevel(self, logLevel: str) -> None:
        raise ContributionsAcceptedError

    def show_profiles(self) -> None:
        raise ContributionsAcceptedError

    def sparkUser(self) -> str:
        raise ContributionsAcceptedError

    # def statusTracker(self) -> duckdb.experimental.spark.status.StatusTracker:
    # 	raise ContributionsAcceptedError

    # def textFile(self, name: str, minPartitions: Optional[int] = None, use_unicode: bool = True) -> pyspark.rdd.RDD[str]:
    # 	pass

    # def union(self, rdds: List[pyspark.rdd.RDD[~T]]) -> pyspark.rdd.RDD[~T]:
    # 	pass

    # def wholeTextFiles(self, path: str, minPartitions: Optional[int] = None, use_unicode: bool = True) -> pyspark.rdd.RDD[typing.Tuple[str, str]]:
    # 	pass


__all__ = ["SparkContext"]
