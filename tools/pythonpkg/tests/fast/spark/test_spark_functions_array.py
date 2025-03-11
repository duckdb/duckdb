import pytest
import platform

_ = pytest.importorskip("duckdb.experimental.spark")
from spark_namespace.sql import functions as F
from spark_namespace.sql.types import Row
from spark_namespace import USE_ACTUAL_SPARK

pytestmark = pytest.mark.skipif(
    platform.system() == "Emscripten",
    reason="This Spark experimental test is not supported on Emscripten",
)


class TestSparkFunctionsArray:
    def test_array_distinct(self, spark):
        data = [
            ([1, 2, 2], 2),
            ([2, 4, 5], 3),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])
        df = df.withColumn("distinct_values", F.array_distinct(F.col("firstColumn")))
        res = df.select("distinct_values").collect()
        # Output order can vary across platforms which is why we sort it first
        assert len(res) == 2
        assert sorted(res[0].distinct_values) == [1, 2]
        assert sorted(res[1].distinct_values) == [2, 4, 5]

    def test_array_intersect(self, spark):
        data = [
            (["b", "a", "c"], ["c", "d", "a", "f"]),
        ]
        df = spark.createDataFrame(data, ["c1", "c2"])
        df = df.withColumn("intersect_values", F.array_intersect(F.col("c1"), F.col("c2")))
        res = df.select("intersect_values").collect()
        # Output order can vary across platforms which is why we sort it first
        assert len(res) == 1
        assert sorted(res[0].intersect_values) == ["a", "c"]

    def test_array_union(self, spark):
        data = [
            (["b", "a", "c"], ["c", "d", "a", "f"]),
        ]
        df = spark.createDataFrame(data, ["c1", "c2"])
        df = df.withColumn("union_values", F.array_union(F.col("c1"), F.col("c2")))
        res = df.select("union_values").collect()
        # Output order can vary across platforms which is why we sort it first
        assert len(res) == 1
        assert sorted(res[0].union_values) == ["a", "b", "c", "d", "f"]

    def test_array_max(self, spark):
        data = [
            ([1, 2, 3], 3),
            ([4, 2, 5], 5),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])
        df = df.withColumn("max_value", F.array_max(F.col("firstColumn")))
        res = df.select("max_value").collect()
        assert res == [
            Row(max_value=3),
            Row(max_value=5),
        ]

    def test_array_min(self, spark):
        data = [
            ([2, 1, 3], 3),
            ([2, 4, 5], 5),
        ]
        df = spark.createDataFrame(data, ["firstColumn", "secondColumn"])
        df = df.withColumn("min_value", F.array_min(F.col("firstColumn")))
        res = df.select("min_value").collect()
        assert res == [
            Row(max_value=1),
            Row(max_value=2),
        ]

    def test_get(self, spark):
        df = spark.createDataFrame([(["a", "b", "c"], 1)], ['data', 'index'])

        res = df.select(F.get(df.data, 1).alias("r")).collect()
        assert res == [Row(r="b")]

        res = df.select(F.get(df.data, -1).alias("r")).collect()
        assert res == [Row(r=None)]

        res = df.select(F.get(df.data, 3).alias("r")).collect()
        assert res == [Row(r=None)]

        res = df.select(F.get(df.data, "index").alias("r")).collect()
        assert res == [Row(r='b')]

        res = df.select(F.get(df.data, F.col("index") - 1).alias("r")).collect()
        assert res == [Row(r='a')]

    def test_flatten(self, spark):
        df = spark.createDataFrame([([[1, 2, 3], [4, 5], [6]],), ([None, [4, 5]],)], ['data'])

        res = df.select(F.flatten(df.data).alias("r")).collect()
        assert res == [Row(r=[1, 2, 3, 4, 5, 6]), Row(r=None)]

    def test_array_compact(self, spark):
        df = spark.createDataFrame([([1, None, 2, 3],), ([4, 5, None, 4],)], ['data'])

        res = df.select(F.array_compact(df.data).alias("v")).collect()
        assert [Row(v=[1, 2, 3]), Row(v=[4, 5, 4])]

    def test_array_remove(self, spark):
        df = spark.createDataFrame([([1, 2, 3, 1, 1],), ([],)], ['data'])

        res = df.select(F.array_remove(df.data, 1).alias("v")).collect()
        assert res == [Row(v=[2, 3]), Row(v=[])]

    def test_array_agg(self, spark):
        df = spark.createDataFrame([[1, "A"], [1, "A"], [2, "A"]], ["c", "group"])

        res = df.groupBy("group").agg(F.array_agg("c").alias("r")).collect()
        assert res[0] == Row(group="A", r=[1, 1, 2])

    def test_collect_list(self, spark):
        df = spark.createDataFrame([[1, "A"], [1, "A"], [2, "A"]], ["c", "group"])

        res = df.groupBy("group").agg(F.collect_list("c").alias("r")).collect()
        assert res[0] == Row(group="A", r=[1, 1, 2])

    def test_array_append(self, spark):
        df = spark.createDataFrame([Row(c1=["b", "a", "c"], c2="c")], ["c1", "c2"])

        res = df.select(F.array_append(df.c1, df.c2).alias("r")).collect()
        assert res == [Row(r=['b', 'a', 'c', 'c'])]

        res = df.select(F.array_append(df.c1, 'x')).collect()
        assert res == [Row(r=['b', 'a', 'c', 'x'])]

    def test_array_insert(self, spark):
        df = spark.createDataFrame(
            [(['a', 'b', 'c'], 2, 'd'), (['a', 'b', 'c', 'e'], 2, 'd'), (['c', 'b', 'a'], -2, 'd')],
            ['data', 'pos', 'val'],
        )

        res = df.select(F.array_insert(df.data, df.pos.cast('integer'), df.val).alias('data')).collect()
        assert res == [
            Row(data=['a', 'd', 'b', 'c']),
            Row(data=['a', 'd', 'b', 'c', 'e']),
            Row(data=['c', 'b', 'd', 'a']),
        ]

        res = df.select(F.array_insert(df.data, 5, 'hello').alias('data')).collect()
        assert res == [
            Row(data=['a', 'b', 'c', None, 'hello']),
            Row(data=['a', 'b', 'c', 'e', 'hello']),
            Row(data=['c', 'b', 'a', None, 'hello']),
        ]

        res = df.select(F.array_insert(df.data, -5, 'hello').alias('data')).collect()
        assert res == [
            Row(data=['hello', None, 'a', 'b', 'c']),
            Row(data=['hello', 'a', 'b', 'c', 'e']),
            Row(data=['hello', None, 'c', 'b', 'a']),
        ]

    def test_slice(self, spark):
        df = spark.createDataFrame([([1, 2, 3],), ([4, 5],)], ['x'])
        res = df.select(F.slice(df.x, 2, 2).alias("sliced")).collect()
        assert res == [Row(sliced=[2, 3]), Row(sliced=[5])]

    def test_sort_array(self, spark):
        df = spark.createDataFrame([([2, 1, None, 3],), ([1],), ([],)], ['data'])

        res = df.select(F.sort_array(df.data).alias('r')).collect()
        assert res == [Row(r=[None, 1, 2, 3]), Row(r=[1]), Row(r=[])]

        res = df.select(F.sort_array(df.data, asc=False).alias('r')).collect()
        assert res == [Row(r=[3, 2, 1, None]), Row(r=[1]), Row(r=[])]

    @pytest.mark.parametrize(("null_replacement", "expected_joined_2"), [(None, "a"), ("replaced", "a,replaced")])
    def test_array_join(self, spark, null_replacement, expected_joined_2):
        df = spark.createDataFrame([(["a", "b", "c"],), (["a", None],)], ['data'])

        res = df.select(F.array_join(df.data, ",", null_replacement=null_replacement).alias("joined")).collect()
        assert res == [Row(joined='a,b,c'), Row(joined=expected_joined_2)]

    def test_array_position(self, spark):
        df = spark.createDataFrame([(["c", "b", "a"],), ([],)], ['data'])

        res = df.select(F.array_position(df.data, "a").alias("pos")).collect()
        assert res == [Row(pos=3), Row(pos=0)]

    def test_array_preprend(self, spark):
        df = spark.createDataFrame([([2, 3, 4],), ([],)], ['data'])

        res = df.select(F.array_prepend(df.data, 1).alias("pre")).collect()
        assert res == [Row(pre=[1, 2, 3, 4]), Row(pre=[1])]

    def test_array_repeat(self, spark):
        df = spark.createDataFrame([('ab',)], ['data'])

        res = df.select(F.array_repeat(df.data, 3).alias('r')).collect()
        assert res == [Row(r=['ab', 'ab', 'ab'])]

    def test_array_size(self, spark):
        df = spark.createDataFrame([([2, 1, 3],), (None,)], ['data'])

        res = df.select(F.array_size(df.data).alias('r')).collect()
        assert res == [Row(r=3), Row(r=None)]

    def test_array_sort(self, spark):
        df = spark.createDataFrame([([2, 1, None, 3],), ([1],), ([],)], ['data'])

        res = df.select(F.array_sort(df.data).alias('r')).collect()
        assert res == [Row(r=[1, 2, 3, None]), Row(r=[1]), Row(r=[])]

    def test_arrays_overlap(self, spark):
        df = spark.createDataFrame(
            [(["a", "b"], ["b", "c"]), (["a"], ["b", "c"]), ([None, "c"], ["a"]), ([None, "c"], [None])], ['x', 'y']
        )

        res = df.select(F.arrays_overlap(df.x, df.y).alias("overlap")).collect()
        assert res == [Row(overlap=True), Row(overlap=False), Row(overlap=None), Row(overlap=None)]

    def test_arrays_zip(self, spark):
        df = spark.createDataFrame([([1, 2, 3], [2, 4, 6], [3, 6])], ['vals1', 'vals2', 'vals3'])

        res = df.select(F.arrays_zip(df.vals1, df.vals2, df.vals3).alias('zipped')).collect()
        # FIXME: The structure of the results should be the same
        if USE_ACTUAL_SPARK:
            assert res == [
                Row(
                    zipped=[
                        Row(vals1=1, vals2=2, vals3=3),
                        Row(vals1=2, vals2=4, vals3=6),
                        Row(vals1=3, vals2=6, vals3=None),
                    ]
                )
            ]
        else:
            assert res == [Row(zipped=[(1, 2, 3), (2, 4, 6), (3, 6, None)])]
