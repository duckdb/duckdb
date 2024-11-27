import pytest


from spark_namespace.sql.types import (
    Row,
)

_ = pytest.importorskip("duckdb.experimental.spark")


class TestDataFrameDropDuplicates(object):
    @pytest.mark.parametrize("method", ["dropDuplicates", "drop_duplicates"])
    def test_spark_drop_duplicates(self, method, spark):
        # Prepare Data
        data = [
            ("James", "Sales", 3000),
            ("Michael", "Sales", 4600),
            ("Robert", "Sales", 4100),
            ("Maria", "Finance", 3000),
            ("James", "Sales", 3000),
            ("Scott", "Finance", 3300),
            ("Jen", "Finance", 3900),
            ("Jeff", "Marketing", 3000),
            ("Kumar", "Marketing", 2000),
            ("Saif", "Sales", 4100),
        ]

        # Create DataFrame
        columns = ["employee_name", "department", "salary"]
        df = spark.createDataFrame(data=data, schema=columns)

        distinctDF = df.distinct().sort("employee_name")
        assert distinctDF.count() == 9
        res = distinctDF.collect()
        # James | Sales had a duplicate, has been removed
        expected = [
            Row(employee_name='James', department='Sales', salary=3000),
            Row(employee_name='Jeff', department='Marketing', salary=3000),
            Row(employee_name='Jen', department='Finance', salary=3900),
            Row(employee_name='Kumar', department='Marketing', salary=2000),
            Row(employee_name='Maria', department='Finance', salary=3000),
            Row(employee_name='Michael', department='Sales', salary=4600),
            Row(employee_name='Robert', department='Sales', salary=4100),
            Row(employee_name='Saif', department='Sales', salary=4100),
            Row(employee_name='Scott', department='Finance', salary=3300),
        ]
        assert res == expected

        df2 = getattr(df, method)().sort("employee_name")
        assert df2.count() == 9
        res2 = df2.collect()
        assert res2 == res

        expected_subset = [
            Row(department='Finance', salary=3000),
            Row(department='Finance', salary=3300),
            Row(department='Finance', salary=3900),
            Row(department='Marketing', salary=2000),
            Row(department='Marketing', salary=3000),
            Row(epartment='Sales', salary=3000),
            Row(department='Sales', salary=4100),
            Row(department='Sales', salary=4600),
        ]

        dropDisDF = getattr(df, method)(["department", "salary"]).sort("department", "salary")
        assert dropDisDF.columns == ["employee_name", "department", "salary"]
        assert dropDisDF.count() == len(expected_subset)
        res = dropDisDF.select("department", "salary").collect()
        assert res == expected_subset

    def test_spark_drop_duplicates_with_keywords_cols(self, spark):
        data = [
            ("abc", 1, ""),
            ("abc", 1, ""),
            ("def", 2, ""),
            ("def", 2, ""),
            ("def", 2, ""),
            ("def", 3, ""),
            ("def", 3, ""),
            ("def", 3, ""),
            ("def", 3, ""),
        ]

        columns = ["table", "min", "null"]
        df = spark.createDataFrame(data=data, schema=columns)

        dropDisDF = df.dropDuplicates(["table", "min"]).sort("table", "min")
        assert dropDisDF.count() == 3
