import pytest

pd = pytest.importorskip("pandas")
np = pytest.importorskip("numpy")

NUMBER_OF_ROWS = 200000
NUMBER_OF_COLUMNS = 1


class TestMetaTransaction(object):
    def test_fetchmany(self, duckdb_cursor):
        duckdb_cursor.execute("CREATE SEQUENCE id_seq")
        column_names = ',\n'.join([f'column_{i} FLOAT' for i in range(1, NUMBER_OF_COLUMNS + 1)])
        create_table_query = f"""
        CREATE TABLE my_table (
            id INTEGER DEFAULT nextval('id_seq'),
            {column_names}
        )
        """
        # Create a table containing a sequence
        duckdb_cursor.execute(create_table_query)

        for i in range(20):
            # Then insert a large amount of tuples, triggering a parallel execution
            data = np.random.rand(NUMBER_OF_ROWS, NUMBER_OF_COLUMNS)
            columns = [f'Column_{i+1}' for i in range(NUMBER_OF_COLUMNS)]
            df = pd.DataFrame(data, columns=columns)
            df_columns = ", ".join(df.columns)
            # This gets executed in parallel, causing NextValFunction to be called in parallel
            # stressing the MetaTransaction::Get concurrency
            duckdb_cursor.execute(f"INSERT INTO my_table ({df_columns}) SELECT * FROM df")
            print(f"inserted {i}")
            duckdb_cursor.commit()
