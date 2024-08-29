import sqllogictest
from sqllogictest import SQLParserException, SQLLogicParser, SQLLogicTest
import duckdb
from typing import Optional
import argparse
import shutil
import os

# example usage: python3 scripts/test_serialization_bwc.py --old-source ../duckdb-bugfix --test-file test/sql/aggregate/aggregates/test_median.test

serialized_path = os.path.join('test', 'api', 'serialized_plans')
db_load_path = os.path.join(serialized_path, 'db_load.sql')
queries_path = os.path.join(serialized_path, 'queries.sql')
result_binary = os.path.join(serialized_path, 'serialized_plans.binary')
unittest_binary = os.path.join('build', 'debug', 'test', 'unittest')


def complete_query(q):
    q = q.strip()
    if q.endswith(';'):
        return q
    return q + ';'


def main():
    parser = argparse.ArgumentParser(description="Test serialization")
    parser.add_argument("--new-source", type=str, help="Path to the new source", default='.')
    parser.add_argument("--old-source", type=str, help="Path to the old source")
    parser.add_argument("--test-file", type=str, help="Path to the SQL logic file")
    args = parser.parse_args()

    old_source = args.old_source
    new_source = args.new_source
    filename = args.test_file

    parser = SQLLogicParser()
    out: Optional[SQLLogicTest] = parser.parse(filename)
    if not out:
        raise SQLParserException(f"Test {filename} could not be parsed")
    loop_count = 0
    load_statements = []
    query_statements = []
    for stmt in out.statements:
        if type(stmt) is sqllogictest.statement.loop.Loop or type(stmt) is sqllogictest.statement.foreach.Foreach:
            loop_count += 1
        if type(stmt) is sqllogictest.statement.endloop.Endloop:
            loop_count -= 1
        if loop_count > 0:
            # loops are ignored currently
            continue
        if not (
            type(stmt) is sqllogictest.statement.query.Query or type(stmt) is sqllogictest.statement.statement.Statement
        ):
            # only handle query and statement nodes for now
            continue
        if type(stmt) is sqllogictest.statement.statement.Statement:
            # skip expected errors
            if stmt.expected_result.type == sqllogictest.ExpectedResult.Type.ERROR:
                continue
        query = ' '.join(stmt.lines)
        sql_stmt_list = duckdb.extract_statements(query)
        for sql_stmt in sql_stmt_list:
            if sql_stmt.type == duckdb.StatementType.SELECT:
                query_statements.append(query)
            elif sql_stmt.type == duckdb.StatementType.PRAGMA:
                continue
            else:
                load_statements.append(query)

    # generate the sources
    current_path = os.getcwd()
    os.chdir(old_source)
    try:
        # build if not yet build
        if not os.path.isfile(unittest_binary):
            res = os.system('make debug')
            if res != 0:
                raise Exception("Failed to build old sources")

        # write the files
        with open(os.path.join(old_source, db_load_path), 'w+') as f:
            for stmt in load_statements:
                f.write(complete_query(stmt) + '\n')

        with open(os.path.join(old_source, queries_path), 'w+') as f:
            for stmt in query_statements:
                f.write(complete_query(stmt) + '\n')

        # generate the serialization
        res = os.system('GEN_PLAN_STORAGE=1 build/debug/test/unittest "Generate serialized plans file"')
        if res != 0:
            raise Exception("Failed to generate serialized plans file")
    except:
        raise
    finally:
        os.chdir(current_path)

    # copy over the files
    for f in [db_load_path, queries_path, result_binary]:
        shutil.copy(os.path.join(old_source, f), os.path.join(new_source, f))

    # run the verification
    os.chdir(new_source)
    try:
        # build if not yet build
        if not os.path.isfile(unittest_binary):
            res = os.system('make debug')
            if res != 0:
                raise Exception("Failed to build new sources")

        res = os.system('build/debug/test/unittest "Test deserialized plans from file"')
        if res != 0:
            raise Exception("Failed to run deserialized plan test")
    except:
        raise
    finally:
        os.chdir(current_path)


if __name__ == "__main__":
    main()
