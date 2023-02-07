import duckdb
import pytest
from os import path
import shutil
import os

@pytest.fixture(scope="session")
def export_path(tmp_path_factory):
    database = tmp_path_factory.mktemp("export_dbs", numbered=True)
    return str(database)

@pytest.fixture(scope="session")
def import_path(tmp_path_factory):
    database = tmp_path_factory.mktemp("import_dbs", numbered=True)
    return str(database)

def export_database(export_location):
    # Create the db
    duckdb.execute("create table tbl (a integer, b integer);");
    duckdb.execute("insert into tbl values (5,1);");

    # Export the db
    duckdb.execute(f"export database '{export_location}';");
    print(f"Exported database to {export_location}")

    # Destroy the db
    duckdb.execute("drop table tbl");

def import_database(import_location):
    duckdb.execute(f"import database '{import_location}'")
    print(f"Imported database from {import_location}");

    res = duckdb.query("select * from tbl").fetchall()
    assert res == [(5,1),]
    print("Successfully queried an imported database that was moved from its original export location!")

    # Destroy the db
    duckdb.execute("drop table tbl");

def move_database(export_location, import_location):
    assert path.exists(export_location)
    assert path.exists(import_location)

    for file in ['schema.sql', 'load.sql', 'tbl.csv']:
        shutil.move(path.join(export_location, file), import_location)

class TestDuckDBImportExport():
	
    def test_import_and_export(self, export_path, import_path):
        export_database(export_path)
        move_database(export_path, import_path)
        import_database(import_path)
