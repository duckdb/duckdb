import duckdb


class TestModule:
    def test_paramstyle(self):
        assert duckdb.paramstyle == "qmark"

    def test_threadsafety(self):
        assert duckdb.threadsafety == 1

    def test_apilevel(self):
        assert duckdb.apilevel == "1.0"
