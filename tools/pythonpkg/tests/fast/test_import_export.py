import duckdb
import pytest
from os import path
import shutil
import unittest

@pytest.fixture(scope="session")
def export_path(tmp_path_factory):
    database = tmp_path_factory.mktemp("export_dbs", numbered=True)
    return str(database)

@pytest.fixture(scope="session")
def import_path(tmp_path_factory):
    database = tmp_path_factory.mktemp("import_dbs", numbered=True)
    return str(database)

class TestDuckDBImportExport():
    def export_database(self):
        # Create the db
        duckdb.execute("create table tbl (a integer, b integer);");
        duckdb.execute("insert into tbl values (5,1);");

        # Export the db
        duckdb.execute(f"export database '{self.export_location}';");
        print(f"Exported database to {self.export_location}")

        # Destroy the db
        duckdb.execute("drop table tbl");

    def import_database(self):
        duckdb.execute(f"import database '{self.import_location}'")
        print(f"Imported database from {self.import_location}");

        res = duckdb.query("select * from tbl").fetchall()
        assert res == [(5,1),]
        print("Successfully queried an imported database that was moved from its original export location!")

    def move_database(self):
        assert path.exists(self.export_location)
        assert path.exists(self.import_location)

        # Move the contents of the exported database
        for file_name in ['load.sql', 'tbl.csv', 'schema.sql']:
            shutil.move(path.join(self.export_location, file_name), self.import_location)

    def test_import_and_export(self, export_path, import_path):
        self.export_location = export_path;
        self.import_location = import_path;

        self.export_database()
        self.move_database()
        self.import_database()


