import sys
import subprocess
import tempfile
import os
import shutil

if len(sys.argv) < 2:
    raise Exception('need shell binary as parameter')


def test_exception(command, input, stdout, stderr, errmsg):
    print('--- COMMAND --')
    print(' '.join(command))
    print('--- INPUT --')
    print(input)
    print('--- STDOUT --')
    print(stdout)
    print('--- STDERR --')
    print(stderr)
    raise Exception(errmsg)


def test(cmd, out=None, err=None, extra_commands=None, input_file=None, output_file=None):
    command = [sys.argv[1], '--batch', '-init', '/dev/null']
    if extra_commands:
        command += extra_commands

    if input_file:
        command += [cmd]
        input_data = open(input_file, 'rb').read()
    else:
        input_data = bytearray(cmd, 'utf8')
    output_pipe = subprocess.PIPE
    if output_file:
        output_pipe = open(output_file, 'w+')

    res = subprocess.run(command, input=input_data, stdout=output_pipe, stderr=subprocess.PIPE)
    if output_file:
        stdout = open(output_file, 'r').read()
    else:
        stdout = res.stdout.decode('utf8').strip()
    stderr = res.stderr.decode('utf8').strip()

    if out and out not in stdout:
        test_exception(command, cmd, stdout, stderr, 'out test failed')

    if err and err not in stderr:
        test_exception(command, cmd, stdout, stderr, f"err test failed, error does not contain: '{err}'")

    if not err and stderr != '':
        test_exception(command, cmd, stdout, stderr, 'got err test failed')

    if err is None and res.returncode != 0:
        test_exception(command, cmd, stdout, stderr, 'process returned non-zero exit code but no error was specified')


def tf():
    return tempfile.mktemp().replace('\\', '/')


# basic test
test('select \'asdf\' as a;', out='asdf')

test('select * from range(10000);', out='9999')

import_basic_csv_table = tf()
print("col_1,col_2\n1,2\n10,20", file=open(import_basic_csv_table, 'w'))
# test create missing table with import
test(
    """
.mode csv
.import "%s" test_table
SELECT * FROM test_table;
"""
    % import_basic_csv_table,
    out="col_1,col_2\n1,2\n10,20",
)

# test pragma
test(
    """
.mode csv
.headers off
.sep |
CREATE TABLE t0(c0 INT);
PRAGMA table_info('t0');
""",
    out='0|c0|INTEGER|false||false',
)

datafile = tf()
print("42\n84", file=open(datafile, 'w'))
test(
    '''
CREATE TABLE a (i INTEGER);
.import "%s" a
SELECT SUM(i) FROM a;
'''
    % datafile,
    out='126',
)

# system functions
test('SELECT 1, current_query() as my_column', out='SELECT 1, current_query() as my_column')

# nested types
test('select LIST_VALUE(1, 2);', out='[1, 2]')
test("select STRUCT_PACK(x := 3, y := 3);", out="{'x': 3, 'y': 3}")
test("select STRUCT_PACK(x := 3, y := LIST_VALUE(1, 2));", out="{'x': 3, 'y': [1, 2]}")

test(
    '''
CREATE TABLE a (i STRING);
INSERT INTO a VALUES ('XXXX');
SELECT CAST(i AS INTEGER) FROM a;
''',
    err='Could not convert',
)

test('.auth ON', err='sqlite3_set_authorizer')
test('.auth OFF', err='sqlite3_set_authorizer')
test('.backup %s' % tf(), err='sqlite3_backup_init')

# test newline in value
test(
    '''select 'hello
world' as a;''',
    out='hello\\nworld',
)

# test newline in column name
test(
    '''select 42 as "hello
world";''',
    out='hello\\nworld',
)

test(
    '''
.bail on
.bail off
.binary on
SELECT 42;
.binary off
SELECT 42;
'''
)

test(
    '''
.cd %s
.cd %s
'''
    % (tempfile.gettempdir().replace('\\', '/'), os.getcwd().replace('\\', '/'))
)

test(
    '''
CREATE TABLE a (I INTEGER);
.changes on
INSERT INTO a VALUES (42);
DROP TABLE a;
''',
    out="total_changes: 1",
)

test(
    '''
CREATE TABLE a (I INTEGER);
.changes on
INSERT INTO a VALUES (42);
INSERT INTO a VALUES (42);
INSERT INTO a VALUES (42);
DROP TABLE a;
''',
    out="total_changes: 3",
)

test(
    '''
CREATE TABLE a (I INTEGER);
.changes off
INSERT INTO a VALUES (42);
DROP TABLE a;
'''
)

# maybe at some point we can do something meaningful here
# test('.dbinfo', err='unable to read database header')

test(
    '''
.echo on
SELECT 42;
''',
    out="SELECT 42",
)


test('.exit')
test('.quit')

test('.print asdf', out='asdf')

test(
    '''
.headers on
SELECT 42 as wilbur;
''',
    out="wilbur",
)


test(
    '''
.nullvalue wilbur
SELECT NULL;
''',
    out="wilbur",
)

test("select 'yo' where 'abc' like 'a%c';", out='yo')

test("select regexp_matches('abc','abc')", out='true')

test('.help', 'Show help text for PATTERN')

test('.load %s' % tf(), err="Error")

# error in streaming result
test(
    '''
SELECT x::INT FROM (SELECT x::VARCHAR x FROM range(10) tbl(x) UNION ALL SELECT 'hello' x) tbl(x);
''',
    err='Could not convert string',
)

# test explain
test('explain select sum(i) from range(1000) tbl(i)', out='RANGE')
test('explain analyze select sum(i) from range(1000) tbl(i)', out='RANGE')

# test returning insert
test(
    '''
CREATE TABLE table1 (a INTEGER DEFAULT -1, b INTEGER DEFAULT -2, c INTEGER DEFAULT -3);
INSERT INTO table1 VALUES (1, 2, 3) RETURNING *;
SELECT COUNT(*) FROM table1;
''',
    out='1',
)

# test display of pragmas
test(
    '''
CREATE TABLE table1 (mylittlecolumn INTEGER);
pragma table_info('table1');
''',
    out='mylittlecolumn',
)

# test display of show
test(
    '''
CREATE TABLE table1 (mylittlecolumn INTEGER);
show table1;
''',
    out='mylittlecolumn',
)

# test display of call
test(
    '''
CALL range(4);
''',
    out='3',
)

# test display of prepare/execute
test(
    '''
PREPARE v1 AS SELECT ?::INT;
EXECUTE v1(42);
''',
    out='42',
)


# this should be fixed
test('.selftest', err='sqlite3_table_column_metadata')

scriptfile = tf()
print("select 42", file=open(scriptfile, 'w'))
test('.read %s' % scriptfile, out='42')


test('.show', out='rowseparator')

test('.limit length 42', err='sqlite3_limit')

# ???
# FIXME
# Parser Error: syntax error at or near "["
# LINE 1: ...concat(quote(s.name) || '.' || quote(f.[from]) || '=?'   || fkey_collate_claus...
# test('.lint fkey-indexes')

test('.timeout', err='sqlite3_busy_timeout')


test('.save %s' % tf(), err='sqlite3_backup_init')
test('.restore %s' % tf(), err='sqlite3_backup_init')


# don't crash plz
test('.vfsinfo')
test('.vfsname')
test('.vfslist')

test('.stats', err="sqlite3_status64")
test('.stats on')
test('.stats off')

test(
    '''
create table test (a int, b varchar);
insert into test values (1, 'hello');
.schema test
''',
    out="CREATE TABLE test(a INTEGER, b VARCHAR);",
)

test(
    '''
create table test (a int, b varchar);
insert into test values (1, 'hello');
.schema tes%
''',
    out="CREATE TABLE test(a INTEGER, b VARCHAR);",
)

test(
    '''
create table test (a int, b varchar);
insert into test values (1, 'hello');
.schema tes*
''',
    out="CREATE TABLE test(a INTEGER, b VARCHAR);",
)

test(
    '''
create table test (a int, b varchar);
CREATE TABLE test2(a INTEGER, b VARCHAR);
.schema
''',
    out="CREATE TABLE test2(a INTEGER, b VARCHAR);",
)

test('.fullschema', 'No STAT tables available', '')

test(
    '''
CREATE TABLE asda (i INTEGER);
CREATE TABLE bsdf (i INTEGER);
CREATE TABLE csda (i INTEGER);
.tables
''',
    out="asda  bsdf  csda",
)

test(
    '''
CREATE TABLE asda (i INTEGER);
CREATE TABLE bsdf (i INTEGER);
CREATE TABLE csda (i INTEGER);
.tables %da
''',
    out="asda  csda",
)

test('.indexes', out="")

test(
    '''
CREATE TABLE a (i INTEGER);
CREATE INDEX a_idx ON a(i);
.indexes a%
''',
    out="a_idx",
)

test('.schema %p%', out='')

test(
    '''
create table duckdb_p (a int, b varchar, c BIT);
create table p_duck(d INT, f DATE);
.schema %p
''',
    out='CREATE TABLE duckdb_p(a INTEGER, b VARCHAR, c BIT);',
)

test(
    '''
create table duckdb_p (a int, b varchar, c BIT);
create table p_duck(d INT, f DATE);
.schema p%
''',
    out='CREATE TABLE p_duck(d INTEGER, f DATE);',
)

# below test fails on windows I think because of how newlines are interpreted
if os.name != 'nt':
    test(
        '''
create table duckdb_p (a int, b varchar, c BIT);
create table p_duck(d INT, f DATE);
.schema %p%''',
        out='''CREATE TABLE duckdb_p(a INTEGER, b VARCHAR, c BIT);
CREATE TABLE p_duck(d INTEGER, f DATE);''',
    )


test('''.clone''', err='Error: unknown command or invalid arguments:  "clone". Enter ".help" for help')

# this does not seem to output anything
test('.sha3sum')

test(
    '''
.mode jsonlines
SELECT 42,43;
''',
    out='{"42":42,"43":43}',
)

test(
    '''
.mode csv
.separator XX
SELECT 42,43;
''',
    out="42XX43",
)

test(
    '''
.timer on
SELECT NULL;
''',
    out="Run Time (s):",
)

test(
    '''
.scanstats on
SELECT NULL;
''',
    err='scanstats',
)

test('.trace %s\n; SELECT 42;' % tf(), err='sqlite3_trace_v2')

outfile = tf()
test(
    '''
.mode csv
.output %s
SELECT 42;
'''
    % outfile
)
outstr = open(outfile, 'rb').read()
if b'42' not in outstr:
    raise Exception('.output test failed')

# issue 6204
test(
    '''
.output foo.txt
select * from range(2049);
'''
)

outfile = tf()
test(
    '''
.once %s
SELECT 43;
'''
    % outfile
)
outstr = open(outfile, 'rb').read()
if b'43' not in outstr:
    raise Exception('.once test failed')

# This somehow does not log nor fail. works for me.
test(
    '''
.log %s
SELECT 42;
.log off
'''
    % tf()
)

test(
    '''
.mode ascii
SELECT NULL, 42, 'fourty-two', 42.0;
''',
    out='fourty-two',
)

test(
    '''
.mode csv
SELECT NULL, 42, 'fourty-two', 42.0;
''',
    out=',fourty-two,',
)

test(
    '''
.mode column
.width 10 10 10 10
SELECT NULL, 42, 'fourty-two', 42.0;
''',
    out='  fourty-two  ',
)

test(
    '''
.mode html
SELECT NULL, 42, 'fourty-two', 42.0;
''',
    out='<TD>fourty-two</TD>',
)

# FIXME sqlite3_column_blob
# test('''
# .mode insert
# SELECT NULL, 42, 'fourty-two', 42.0;
# ''', out='fourty-two')

test(
    '''
.mode line
SELECT NULL, 42, 'fourty-two' x, 42.0;
''',
    out='x = fourty-two',
)

test(
    '''
.mode list
SELECT NULL, 42, 'fourty-two', 42.0;
''',
    out='|fourty-two|',
)

# FIXME sqlite3_column_blob and %! format specifier
# test('''
# .mode quote
# SELECT NULL, 42, 'fourty-two', 42.0;
# ''', out='fourty-two')

test(
    '''
.mode tabs
SELECT NULL, 42, 'fourty-two', 42.0;
''',
    out='fourty-two',
)


db1 = tf()
db2 = tf()

test(
    '''
.open %s
CREATE TABLE t1 (i INTEGER);
INSERT INTO t1 VALUES (42);
.open %s
CREATE TABLE t2 (i INTEGER);
INSERT INTO t2 VALUES (43);
.open %s
SELECT * FROM t1;
'''
    % (db1, db2, db1),
    out='42',
)

# open file that is not a database
duckdb_nonsense_db = 'duckdbtest_nonsensedb.db'
with open(duckdb_nonsense_db, 'w+') as f:
    f.write('blablabla')
test('', err='not a valid DuckDB database file', extra_commands=[duckdb_nonsense_db])
os.remove(duckdb_nonsense_db)

# enable_profiling doesn't result in any output
test(
    '''
PRAGMA enable_profiling
''',
    err="",
)

# only when we follow it up by an actual query does something get printed to the terminal
test(
    '''
PRAGMA enable_profiling;
SELECT 42;
''',
    out="42",
    err="Query Profiling Information",
)

# escapes in query profiling
test(
    """
PRAGMA enable_profiling=json;
CREATE TABLE "foo"("hello world" INT);
SELECT "hello world", '\r\t\n\b\f\\' FROM "foo";
""",
    err="""SELECT \\"hello world\\", '\\r\\t\\n\\b\\f\\\\' FROM \\"foo""",
)

test('.system echo 42', out="42")
test('.shell echo 42', out="42")

# query profiling that includes the optimizer
test(
    """
PRAGMA enable_profiling=query_tree_optimizer;
SELECT 42;
""",
    out="42",
    err="Optimizer",
)

# detailed also includes optimizer
test(
    """
PRAGMA enable_profiling;
PRAGMA profiling_mode=detailed;
SELECT 42;
""",
    out="42",
    err="Optimizer",
)

# even in json output mode
test(
    """
PRAGMA enable_profiling=json;
PRAGMA profiling_mode=detailed;
SELECT 42;
""",
    out="42",
    err="optimizer",
)

# this fails because db_config is missing
# test('''
# .eqp full
# SELECT 42;
# ''', out="DUMMY_SCAN")

# this fails because the sqlite printf accepts %w for table names

# test('''
# CREATE TABLE a (I INTEGER);
# INSERT INTO a VALUES (42);
# .clone %s
# ''' % tempfile.mktemp())


test('.databases', out='memory')

# .dump test
test(
    '''
CREATE TABLE a (i INTEGER);
.changes off
INSERT INTO a VALUES (42);
.dump
''',
    'CREATE TABLE a(i INTEGER)',
)

test(
    '''
CREATE TABLE a (i INTEGER);
.changes off
INSERT INTO a VALUES (42);
.dump
''',
    'COMMIT',
)

# .dump a specific table
test(
    '''
CREATE TABLE a (i INTEGER);
.changes off
INSERT INTO a VALUES (42);
.dump a
''',
    'CREATE TABLE a(i INTEGER);',
)

# .dump LIKE
test(
    '''
CREATE TABLE a (i INTEGER);
.changes off
INSERT INTO a VALUES (42);
.dump a%
''',
    'CREATE TABLE a(i INTEGER);',
)

# more types, tables and views
test(
    '''
CREATE TABLE a (d DATE, k FLOAT, t TIMESTAMP);
CREATE TABLE b (c INTEGER);
.changes off
INSERT INTO a VALUES (DATE '1992-01-01', 0.3, NOW());
INSERT INTO b SELECT * FROM range(0,10);
.dump
''',
    'CREATE TABLE a(d DATE, k FLOAT, t TIMESTAMP);',
)

# import/export database
target_dir = 'duckdb_shell_test_export_dir'
try:
    shutil.rmtree(target_dir)
except:
    pass
test(
    '''
.mode csv
.changes off
CREATE TABLE integers(i INTEGER);
CREATE TABLE integers2(i INTEGER);
INSERT INTO integers SELECT * FROM range(100);
INSERT INTO integers2 VALUES (1), (3), (99);
EXPORT DATABASE '%s';
DROP TABLE integers;
DROP TABLE integers2;
IMPORT DATABASE '%s';
SELECT SUM(i)*MAX(i) FROM integers JOIN integers2 USING (i);
'''
    % (target_dir, target_dir),
    '10197',
)

shutil.rmtree(target_dir)

# test using .import with a CSV file containing invalid UTF8

duckdb_nonsensecsv = 'duckdbtest_nonsensecsv.csv'
with open(duckdb_nonsensecsv, 'wb+') as f:
    f.write(b'\xFF\n')
test(
    '''
.nullvalue NULL
CREATE TABLE test(i INTEGER);
.import duckdbtest_nonsensecsv.csv test
SELECT * FROM test;
''',
    out="NULL",
)
os.remove(duckdb_nonsensecsv)

# .mode latex
test(
    '''
.mode latex
CREATE TABLE a (I INTEGER);
.changes off
INSERT INTO a VALUES (42);
SELECT * FROM a;
''',
    '\\begin{tabular}',
)

# .mode trash
test(
    '''
.mode trash
SELECT 1;
''',
    '',
)

# dump blobs: FIXME
# test('''
# CREATE TABLE a (b BLOB);
# .changes off
# INSERT INTO a VALUES (DATE '1992-01-01', 0.3, NOW());
# .dump
# ''', 'COMMIT')


# printf %q

# test('''
# CREATE TABLE a (i INTEGER);
# CREATE INDEX a_idx ON a(i);
# .imposter a_idx a_idx_imp
# ''')


# test that sqlite3_complete works somewhat correctly

test(
    '''/*
;
*/
select 42;
''',
    out='42',
)

test(
    '''-- this is a comment ;
select 42;
''',
    out='42',
)

test(
    '''--;;;;;;
select 42;
''',
    out='42',
)

test('/* ;;;;;; */ select 42;', out='42')


# sqlite udfs
test(
    '''
SELECT writefile();
''',
    err='wrong number of arguments to function writefile',
)

test(
    '''
SELECT writefile('hello');
''',
    err='wrong number of arguments to function writefile',
)

test(
    '''
SELECT writefile('duckdbtest_writefile', 'hello');
'''
)
test_writefile = 'duckdbtest_writefile'
if not os.path.exists(test_writefile):
    raise Exception(f"Failed to write file {test_writefile}")
with open(test_writefile, 'r') as f:
    text = f.read()
if text != 'hello':
    raise Exception("Incorrect contents for test writefile")
os.remove(test_writefile)

test(
    '''
SELECT lsmode(1) AS lsmode;
''',
    out='lsmode',
)


# test auto-complete
test(
    """
CALL sql_auto_complete('SEL')
""",
    out="SELECT",
)

test(
    """
CREATE TABLE my_table(my_column INTEGER);
SELECT * FROM sql_auto_complete('SELECT my_') LIMIT 1;
""",
    out="my_column",
)

test(
    """
CREATE TABLE my_table(my_column INTEGER);
SELECT * FROM sql_auto_complete('SELECT my_column FROM my_') LIMIT 1;
""",
    out="my_table",
)

test(
    """
CREATE TABLE my_table(my_column INTEGER);
SELECT * FROM sql_auto_complete('SELECT my_column FROM my_table WH') LIMIT 1;
""",
    out="WHERE",
)

test(
    """
CREATE TABLE my_table(my_column INTEGER);
SELECT * FROM sql_auto_complete('INS') LIMIT 1;
""",
    out="INSERT",
)

test(
    """
CREATE TABLE my_table(my_column INTEGER);
SELECT * FROM sql_auto_complete('INSERT IN') LIMIT 1;
""",
    out="INTO",
)

test(
    """
CREATE TABLE my_table(my_column INTEGER);
SELECT * FROM sql_auto_complete('INSERT INTO my_t') LIMIT 1;
""",
    out="my_table",
)

test(
    """
CREATE TABLE my_table(my_column INTEGER);
SELECT * FROM sql_auto_complete('INSERT INTO my_table VAL') LIMIT 1;
""",
    out="VALUES",
)

test(
    """
CREATE TABLE my_table(my_column INTEGER);
SELECT * FROM sql_auto_complete('DEL') LIMIT 1;
""",
    out="DELETE",
)

test(
    """
CREATE TABLE my_table(my_column INTEGER);
SELECT * FROM sql_auto_complete('DELETE F') LIMIT 1;
""",
    out="FROM",
)

test(
    """
CREATE TABLE my_table(my_column INTEGER);
SELECT * FROM sql_auto_complete('DELETE FROM m') LIMIT 1;
""",
    out="my_table",
)

test(
    """
CREATE TABLE my_table(my_column INTEGER);
SELECT * FROM sql_auto_complete('DELETE FROM my_table WHERE m') LIMIT 1;
""",
    out="my_column",
)

test(
    """
CREATE TABLE my_table(my_column INTEGER);
SELECT * FROM sql_auto_complete('U') LIMIT 1;
""",
    out="UPDATE",
)

test(
    """
CREATE TABLE my_table(my_column INTEGER);
SELECT * FROM sql_auto_complete('UPDATE m') LIMIT 1;
""",
    out="my_table",
)

test(
    """
CREATE TABLE my_table(my_column INTEGER);
SELECT * FROM sql_auto_complete('UPDATE "m') LIMIT 1;
""",
    out="my_table",
)

test(
    """
CREATE TABLE my_table(my_column INTEGER);
SELECT * FROM sql_auto_complete('UPDATE my_table SET m') LIMIT 1;
""",
    out="my_column",
)


test(
    """
CREATE TABLE "Funky Table With Spaces"(my_column INTEGER);
SELECT * FROM sql_auto_complete('SELECT * FROM F') LIMIT 1;
""",
    out="\"Funky Table With Spaces\"",
)

test(
    """
CREATE TABLE "Funky Table With Spaces"("Funky Column" int);
SELECT * FROM sql_auto_complete('select f') LIMIT 1;
""",
    out="\"Funky Column\"",
)

test(
    """
CREATE TABLE "Funky Table With Spaces"("Funky Column" int);
SELECT * FROM sql_auto_complete('select "Funky Column" FROM f') LIMIT 1;
""",
    out="\"Funky Table With Spaces\"",
)

# semicolon
test(
    """
SELECT * FROM sql_auto_complete('SELECT 42; SEL') LIMIT 1;
""",
    out="SELECT",
)

# comments
test(
    """
SELECT * FROM sql_auto_complete('--SELECT * FROM
SEL') LIMIT 1;
""",
    out="SELECT",
)

# scalar functions
test(
    """
SELECT * FROM sql_auto_complete('SELECT regexp_m') LIMIT 1;
""",
    out="regexp_matches",
)

# aggregate functions
test(
    """
SELECT * FROM sql_auto_complete('SELECT approx_c') LIMIT 1;
""",
    out="approx_count_distinct",
)

# built-in views
test(
    """
SELECT * FROM sql_auto_complete('SELECT * FROM sqlite_ma') LIMIT 1;
""",
    out="sqlite_master",
)

# table functions
test(
    """
SELECT * FROM sql_auto_complete('SELECT * FROM read_csv_a') LIMIT 1;
""",
    out="read_csv_auto",
)

test(
    """
CREATE TABLE partsupp(ps_suppkey int);
CREATE TABLE supplier(s_suppkey int);
CREATE TABLE nation(n_nationkey int);
SELECT * FROM sql_auto_complete('DROP TABLE na') LIMIT 1;
""",
    out="nation",
)

test(
    """
CREATE TABLE partsupp(ps_suppkey int);
CREATE TABLE supplier(s_suppkey int);
CREATE TABLE nation(n_nationkey int);
SELECT * FROM sql_auto_complete('SELECT s_supp') LIMIT 1;
""",
    out="s_suppkey",
)

# joins
test(
    """
CREATE TABLE partsupp(ps_suppkey int);
CREATE TABLE supplier(s_suppkey int);
CREATE TABLE nation(n_nationkey int);
SELECT * FROM sql_auto_complete('SELECT * FROM partsupp JOIN supp') LIMIT 1;
""",
    out="supplier",
)

test(
    """
CREATE TABLE partsupp(ps_suppkey int);
CREATE TABLE supplier(s_suppkey int);
CREATE TABLE nation(n_nationkey int);
.mode csv
SELECT l,l FROM sql_auto_complete('SELECT * FROM partsupp JOIN supplier ON (s_supp') t(l) LIMIT 1;
""",
    out="s_suppkey,s_suppkey",
)

test(
    """
CREATE TABLE partsupp(ps_suppkey int);
CREATE TABLE supplier(s_suppkey int);
CREATE TABLE nation(n_nationkey int);
SELECT * FROM sql_auto_complete('SELECT * FROM partsupp JOIN supplier USING (ps_') LIMIT 1;
""",
    out="ps_suppkey",
)

test(
    """
SELECT * FROM sql_auto_complete('SELECT * FR') LIMIT 1;
""",
    out="FROM",
)

test(
    """
CREATE TABLE MyTable(MyColumn Varchar);
SELECT * FROM sql_auto_complete('SELECT My') LIMIT 1;
""",
    out="MyColumn",
)

test(
    """
CREATE TABLE MyTable(MyColumn Varchar);
SELECT * FROM sql_auto_complete('SELECT MyColumn FROM My') LIMIT 1;
""",
    out="MyTable",
)

# duckbox renderer displays the number of rows if there are none
test(
    '''
.mode duckbox
select 42 limit 0;
''',
    out='0 rows',
)

# #5411 - with maxrows=2, we still display all 4 rows (hiding them would take up more space)
test(
    '''
.maxrows 2
select * from range(4);
''',
    out='1',
)

outfile = tf()
test(
    '''
.maxrows 2
.output %s
SELECT * FROM range(100);
'''
    % outfile
)
outstr = open(outfile, 'rb').read().decode('utf8')
if '50' not in outstr:
    raise Exception('.output test failed')

# we always display all columns when outputting to a file
columns = ', '.join([str(x) for x in range(100)])
outfile = tf()
test(
    '''
.output %s
SELECT %s
'''
    % (outfile, columns)
)
outstr = open(outfile, 'rb').read().decode('utf8')
if '99' not in outstr:
    raise Exception('.output test failed')

# columnar mode
test(
    '''
.col
select * from range(4);
''',
    out='Row 1',
)

columns = ','.join(["'MyValue" + str(x) + "'" for x in range(100)])
test(
    f'''
.col
select {columns};
''',
    out='MyValue50',
)

test(
    f'''
.col
select {columns}
from range(1000)
''',
    out='100 columns',
)

# test null-byte rendering
test('select varchar from test_all_types();', out='goo\\0se')

# null byte in error message
test('select chr(0)::int', err='INT32')

# test temp directory behavior (issue #5878)
# use an existing temp directory
temp_dir = tf()
temp_file = os.path.join(temp_dir, 'myfile')
os.mkdir(temp_dir)
with open(temp_file, 'w+') as f:
    f.write('hello world')

test(
    f'''
SET temp_directory='{temp_dir}';
PRAGMA memory_limit='2MB';
CREATE TABLE t1 AS SELECT * FROM range(1000000);
'''
)

# make sure the temp directory or existing files are not deleted
assert os.path.isdir(temp_dir)
with open(temp_file, 'r') as f:
    assert f.read() == "hello world"

# all other files are gone
assert os.listdir(temp_dir) == ['myfile']

os.remove(temp_file)
os.rmdir(temp_dir)

# now use a new temp directory
test(
    f'''
SET temp_directory='{temp_dir}';
PRAGMA memory_limit='2MB';
CREATE TABLE t1 AS SELECT * FROM range(1000000);
'''
)

# make sure the temp directory is deleted
assert not os.path.isdir(temp_dir)

if os.name != 'nt':
    shell_test_dir = 'shell_test_dir'
    try:
        os.mkdir(shell_test_dir)
    except:
        pass
    try:
        os.mkdir(os.path.join(shell_test_dir, 'extra_path'))
    except:
        pass

    base_files = ['extra.parquet', 'extra.file']
    for fname in base_files:
        with open(os.path.join(shell_test_dir, fname), 'w+') as f:
            f.write('')

    test(
        """
     CREATE TABLE MyTable(MyColumn Varchar);
     SELECT * FROM sql_auto_complete('SELECT * FROM ''shell_test') LIMIT 1;
     """,
        out="shell_test_dir/",
    )

    test(
        """
     CREATE TABLE MyTable(MyColumn Varchar);
     SELECT * FROM sql_auto_complete('SELECT * FROM ''shell_test_dir/extra') LIMIT 1;
     """,
        out="extra_path/",
    )

    test(
        """
     CREATE TABLE MyTable(MyColumn Varchar);
     SELECT * FROM sql_auto_complete('SELECT * FROM ''shell_test_dir/extra.par') LIMIT 1;
     """,
        out="extra.parquet",
    )

    shutil.rmtree(shell_test_dir)

# test backwards compatibility
test('.open test/storage/bc/db_dev.db', err='older development version')
test('.open test/storage/bc/db_031.db', err='v0.3.1')
test('.open test/storage/bc/db_032.db', err='v0.3.2')
test('.open test/storage/bc/db_04.db', err='v0.4.0')
test('.open test/storage/bc/db_051.db', err='v0.5.1')
test('.open test/storage/bc/db_060.db', err='v0.6.0')

# sqlite udfs
test('select decimal_mul(NULL, NULL);', out='NULL')
test('select decimal_mul(NULL, i) FROM range(3) t(i);', out='NULL')
test('select sha3(NULL);', out='NULL')
test('select sha3(256);', out='A7')
test("select sha3('hello world this is a long string');", out='D4')

if os.name != 'nt':
    test(
        '''
create table mytable as select * from
read_csv('/dev/stdin',
  columns=STRUCT_PACK(foo := 'INTEGER', bar := 'INTEGER', baz := 'VARCHAR'),
  AUTO_DETECT='false'
);
select * from mytable limit 1;''',
        extra_commands=['-csv', ':memory:'],
        input_file='test/sql/copy/csv/data/test/test.csv',
        out='''foo,bar,baz
0,0," test"''',
    )

    test(
        '''
create table mytable as select * from
read_csv_auto('/dev/stdin');
select * from mytable limit 1;
''',
        extra_commands=['-csv', ':memory:'],
        input_file='test/sql/copy/csv/data/test/test.csv',
        out='''column0,column1,column2
0,0," test"''',
    )

    test(
        '''create table mytable as select * from
read_csv_auto('/dev/stdin');
select channel,i_brand_id,sum_sales,number_sales from mytable;
          ''',
        extra_commands=['-csv', ':memory:'],
        input_file='data/csv/tpcds_14.csv',
        out='''web,8006004,844.21,21''',
    )

    test(
        '''create table mytable as select * from
read_ndjson_objects('/dev/stdin');
select * from mytable;
          ''',
        extra_commands=['-list', ':memory:'],
        input_file='data/json/example_rn.ndjson',
        out='''json
{"id":1,"name":"O Brother, Where Art Thou?"}
{"id":2,"name":"Home for the Holidays"}
{"id":3,"name":"The Firm"}
{"id":4,"name":"Broadcast News"}
{"id":5,"name":"Raising Arizona"}''',
    )

    test(
        '''create table mytable as select * from
read_ndjson_objects('/dev/stdin');
select * from mytable;
          ''',
        extra_commands=['-list', ':memory:'],
        input_file='data/json/example_rn.ndjson',
        out='''json
{"id":1,"name":"O Brother, Where Art Thou?"}
{"id":2,"name":"Home for the Holidays"}
{"id":3,"name":"The Firm"}
{"id":4,"name":"Broadcast News"}
{"id":5,"name":"Raising Arizona"}''',
    )

    test(
        '''create table mytable as select * from
read_json_auto('/dev/stdin');
select * from mytable;
          ''',
        extra_commands=['-list', ':memory:'],
        input_file='data/json/example_rn.ndjson',
        out='''id|name
1|O Brother, Where Art Thou?
2|Home for the Holidays
3|The Firm
4|Broadcast News
5|Raising Arizona''',
    )

    test(
        '''
     COPY (SELECT 42) TO '/dev/stdout' WITH (FORMAT 'csv');
     ''',
        extra_commands=['-csv', ':memory:'],
        out='''42''',
    )

    test(
        '''
     COPY (SELECT 42) TO stdout WITH (FORMAT 'csv');
     ''',
        extra_commands=['-csv', ':memory:'],
        out='''42''',
    )

    test(
        '''
     COPY (SELECT 42) TO '/dev/stderr' WITH (FORMAT 'csv');
     ''',
        extra_commands=['-csv', ':memory:'],
        err='''42''',
    )

    test(
        '''
     copy (select 42) to '/dev/stdout'
     ''',
        out='''42''',
    )

    test(
        '''
     select list(concat('thisisalongstring', range::VARCHAR)) i from range(10000)
     ''',
        out='''thisisalongstring''',
    )

    test("copy (select * from range(10000) tbl(i)) to '/dev/stdout' (format csv)", out='9999', output_file=tf())
