import duckdb
import pytest
from os import path
import shutil
import os
from pathlib import Path


def export_database(export_location):
    # Create the db
    con = duckdb.connect()
    con.execute("create table tbl (a integer, b integer);")
    con.execute("insert into tbl values (5,1);")

    # Export the db
    con.execute(f"export database '{export_location}';")
    print(f"Exported database to {export_location}")


def import_database(import_location):
    con = duckdb.connect()
    con.execute(f"import database '{import_location}'")
    print(f"Imported database from {import_location}")

    res = con.query("select * from tbl").fetchall()
    assert res == [
        (5, 1),
    ]
    print("Successfully queried an imported database that was moved from its original export location!")


def move_database(export_location, import_location):
    assert path.exists(export_location)
    assert path.exists(import_location)

    for file in ['schema.sql', 'load.sql', 'tbl.csv']:
        shutil.move(path.join(export_location, file), import_location)


def export_move_and_import(export_path, import_path):
    export_database(export_path)
    move_database(export_path, import_path)
    import_database(import_path)


def export_and_import_empty_db(db_path, _):
    con = duckdb.connect()

    # Export the db
    con.execute(f"export database '{db_path}';")
    print(f"Exported database to {db_path}")

    con.close()
    con = duckdb.connect()
    con.execute(f"import database '{db_path}'")


class TestDuckDBImportExport:
    @pytest.mark.parametrize('routine', [export_move_and_import, export_and_import_empty_db])
    def test_import_and_export(self, routine, tmp_path_factory):
        export_path = str(tmp_path_factory.mktemp("export_dbs", numbered=True))
        import_path = str(tmp_path_factory.mktemp("import_dbs", numbered=True))
        routine(export_path, import_path)

    def test_import_empty_db(self, tmp_path_factory):
        import_path = str(tmp_path_factory.mktemp("empty_db", numbered=True))

        # Create an empty db folder structure
        Path(Path(import_path) / 'load.sql').touch()
        Path(Path(import_path) / 'schema.sql').touch()

        con = duckdb.connect()
        con.execute(f"import database '{import_path}'")

        # Put a single comment into the 'schema.sql' file
        with open(Path(import_path) / 'schema.sql', 'w') as f:
            f.write('--\n')

        con.close()
        con = duckdb.connect()
        con.execute(f"import database '{import_path}'")
