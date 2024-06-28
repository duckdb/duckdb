import duckdb


class Test3728(object):
    def test_3728_describe_enum(self, duckdb_cursor):
        # Create an in-memory database, but the problem is also present in file-backed DBs
        cursor = duckdb.connect(":memory:")

        # Create an arbitrary enum type
        cursor.execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');")

        # Create a table where one or more columns are enum typed
        cursor.execute("CREATE TABLE person (name text, current_mood mood);")

        # This fails with "RuntimeError: Not implemented Error: unsupported type: mood"
        assert cursor.table("person").execute().description == [
            ('name', 'STRING', None, None, None, None, None),
            ('current_mood', "ENUM('sad', 'ok', 'happy')", None, None, None, None, None),
        ]
