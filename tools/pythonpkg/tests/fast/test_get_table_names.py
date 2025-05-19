import duckdb
import pytest
import os
import tempfile


class TestGetTableNames(object):
    def test_table_success(self, duckdb_cursor):
        conn = duckdb.connect()
        table_names = conn.get_table_names("SELECT * FROM my_table1, my_table2, my_table3")
        assert table_names == {'my_table2', 'my_table3', 'my_table1'}

    def test_table_fail(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.close()
        with pytest.raises(duckdb.ConnectionException, match="Connection already closed"):
            table_names = conn.get_table_names("SELECT * FROM my_table1, my_table2, my_table3")
            
    def test_qualified_parameter_basic(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE test_table(i INTEGER)")
        
        # Default (qualified=False)
        table_names = conn.get_table_names("SELECT * FROM test_table")
        assert table_names == {'test_table'}
        
        # Explicit qualified=False
        table_names = conn.get_table_names("SELECT * FROM test_table", qualified=False)
        assert table_names == {'test_table'}
        
    def test_qualified_parameter_schemas(self):
        conn = duckdb.connect()
        conn.execute("CREATE SCHEMA test_schema")
        conn.execute("CREATE TABLE test_schema.schema_table(i INTEGER)")
        conn.execute("CREATE TABLE main_table(i INTEGER)")
        
        # Test with unqualified names (default)
        query = "SELECT * FROM test_schema.schema_table, main_table"
        table_names = conn.get_table_names(query)
        assert table_names == {'schema_table', 'main_table'}
        
        # Test with qualified names
        table_names = conn.get_table_names(query, qualified=True)
        assert table_names == {'test_schema.schema_table', 'main_table'}
        
    def test_qualified_parameter_catalogs(self):
        conn = duckdb.connect()
        # Create a temporary database for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "temp.db")
            conn.execute(f"ATTACH '{db_path}' AS catalog1")
            conn.execute("CREATE SCHEMA catalog1.test_schema")
            conn.execute("CREATE TABLE catalog1.test_schema.catalog_table(i INTEGER)")
            conn.execute("CREATE TABLE regular_table(i INTEGER)")
            
            # Test with qualified names including catalogs
            query = "SELECT * FROM catalog1.test_schema.catalog_table, regular_table"
            
            # Default (qualified=False)
            table_names = conn.get_table_names(query)
            assert table_names == {'catalog_table', 'regular_table'}
            
            # With qualified=True
            table_names = conn.get_table_names(query, qualified=True)
            assert table_names == {'catalog1.test_schema.catalog_table', 'regular_table'}
            
    def test_qualified_parameter_quoted_identifiers(self):
        conn = duckdb.connect()
        conn.execute('CREATE SCHEMA "Schema.With.Dots"')
        conn.execute('CREATE TABLE "Schema.With.Dots"."Table.With.Dots"(i INTEGER)')
        conn.execute('CREATE TABLE "Table With Spaces"(i INTEGER)')
        
        # Test with quoted identifiers
        query = 'SELECT * FROM "Schema.With.Dots"."Table.With.Dots", "Table With Spaces"'
        
        # With qualified=False
        table_names = conn.get_table_names(query)
        assert 'Table.With.Dots' in table_names
        assert 'Table With Spaces' in table_names
        
        # With qualified=True
        table_names = conn.get_table_names(query, qualified=True)
        assert '"Schema.With.Dots"."Table.With.Dots"' in table_names
        assert '"Table With Spaces"' in table_names
