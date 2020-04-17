import sys
import subprocess
import tempfile
import os

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

def test(cmd, out=None, err=None, extra_commands=None):
     command = [sys.argv[1], '--batch', '-init', '/dev/null']
     if extra_commands:
          command += extra_commands
     res = subprocess.run(command, capture_output=True, input=bytearray(cmd, 'utf8'))
     stdout = res.stdout.decode('utf8').strip()
     stderr = res.stderr.decode('utf8').strip()

     if out and out not in stdout:
          test_exception(command, cmd, stdout, stderr, 'out test failed')

     if err and err not in stderr:
          test_exception(command, cmd, stdout, stderr, 'err test failed')

     if not err and stderr != '':
          test_exception(command, cmd, stdout, stderr, 'got err test failed')

def tf():
	return tempfile.mktemp().replace('\\','/')

# basic test
test('select \'asdf\' as a;', out='asdf')

# test pragma
test("CREATE TABLE t0(c0 INT);PRAGMA table_info('t0');", out='0|c0|INTEGER|false||false')

datafile = tf()
print("42\n84",  file=open(datafile, 'w'))
test('''
CREATE TABLE a (i INTEGER);
.import "%s" a
SELECT SUM(i) FROM a;
''' % datafile, out='126')


test('''
CREATE TABLE a (i STRING);
INSERT INTO a VALUES ('XXXX');
SELECT CAST(i AS INTEGER) FROM a;
''' , err='Could not convert')


test('.auth ON', err='sqlite3_set_authorizer')
test('.auth OFF', err='sqlite3_set_authorizer')
test('.backup %s' % tf(), err='sqlite3_backup_init')

test('''
.bail on
.bail off
.binary on
SELECT 42;
.binary off
SELECT 42;
''')

test('''
.cd %s
.cd %s
''' % (tempfile.gettempdir().replace('\\','/'), os.getcwd().replace('\\','/')))

test('''
CREATE TABLE a (I INTEGER);
.changes on
INSERT INTO a VALUES (42);
DROP TABLE a;
''', err="sqlite3_changes")

test('''
CREATE TABLE a (I INTEGER);
.changes off
INSERT INTO a VALUES (42);
DROP TABLE a;
''')

# maybe at some point we can do something meaningful here
test('.dbinfo', err='unable to read database header')

test('''
.echo on
SELECT 42;
''', out="SELECT 42")


test('.exit')
test('.quit')

test('.print asdf', out='asdf')

test('''
.headers on
SELECT 42 as wilbur;
''', out="wilbur")


test('''
.nullvalue wilbur
SELECT NULL;
''', out="wilbur")

test('.help', 'Show this message')

test('.load %s' % tf(), err="Error")

# this should be fixed
test('.selftest', err='sqlite3_table_column_metadata')

scriptfile = tf()
print("select 42", file=open(scriptfile, 'w'))
test('.read %s' % scriptfile, out='42')


test('.show', out='rowseparator')

test('.limit length 42', err='sqlite3_limit')

# ???
test('.lint fkey-indexes')

# this should probably be fixed, sqlite generates an internal query that duckdb does not like
test('.indexes', err='syntax error')


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

# FIXME
test('.schema', err="pragma_database_list")

# FIXME need sqlite3_strlike for this
test('''
CREATE TABLE asdf (i INTEGER);
.schema as%
''', err="pragma_database_list")

test('.fullschema')

test('.tables', err="syntax error")

test('''
CREATE TABLE asdf (i INTEGER);
.tables as%
''', err="syntax error")


test('.indexes',  err="syntax error")

test('''
CREATE TABLE a (i INTEGER);
CREATE INDEX a_idx ON a(i);
.indexes a_%
''',  err="syntax error")



# this does not seem to output anything
test('.sha3sum')


test('''
.separator XX
SELECT 42,43;
''', out="XX")

test('''
.timer on
SELECT NULL;
''', out="Run Time:")

test('''
.scanstats on
SELECT NULL;
''', err='scanstats')

test('.trace %s\n; SELECT 42;' % tf(), err='sqlite3_trace_v2')

outfile = tf()
test('''
.output %s
SELECT 42;
''' % outfile)
outstr = open(outfile,'r').read()
if '42' not in outstr:
     raise Exception('.output test failed')


outfile = tf()
test('''
.once %s
SELECT 43;
''' % outfile)
outstr = open(outfile,'r').read()
if '43' not in outstr:
     raise Exception('.once test failed')


# This somehow does not log nor fail. works for me.
test('''
.log %s
SELECT 42;
.log off
''' % tf())

test('''
.mode ascii
SELECT NULL, 42, 'fourty-two', 42.0;
''', out='fourty-two')

test('''
.mode csv
SELECT NULL, 42, 'fourty-two', 42.0;
''', out=',fourty-two,')

test('''
.mode column
.width 10 10 10 10
SELECT NULL, 42, 'fourty-two', 42.0;
''', out='  fourty-two  ')

test('''
.mode html
SELECT NULL, 42, 'fourty-two', 42.0;
''', out='<TD>fourty-two</TD>')

# FIXME sqlite3_column_blob
# test('''
# .mode insert
# SELECT NULL, 42, 'fourty-two', 42.0;
# ''', out='fourty-two')

test('''
.mode line
SELECT NULL, 42, 'fourty-two' x, 42.0;
''', out='x = fourty-two')

test('''
.mode list
SELECT NULL, 42, 'fourty-two', 42.0;
''', out='|fourty-two|')

# FIXME sqlite3_column_blob and %! format specifier
# test('''
# .mode quote
# SELECT NULL, 42, 'fourty-two', 42.0;
# ''', out='fourty-two')

test('''
.mode tabs
SELECT NULL, 42, 'fourty-two', 42.0;
''', out='fourty-two')


db1 = tf()
db2 = tf()

test('''
.open %s
CREATE TABLE t1 (i INTEGER);
INSERT INTO t1 VALUES (42);
.open %s
CREATE TABLE t2 (i INTEGER);
INSERT INTO t2 VALUES (43);
.open %s
SELECT * FROM t1;
''' % (db1, db2, db1), out='42')

# open file that is not a database
duckdb_nonsense_db = 'duckdbtest_nonsensedb.db'
with open(duckdb_nonsense_db, 'w+') as f:
     f.write('blablabla')
test('', err='unable to open', extra_commands=[duckdb_nonsense_db])
os.remove(duckdb_nonsense_db)




# not working for now, probably for the better

# test('.system echo 42', out="42")
# test('.shell echo 42', out="42")

# this fails because strlike and db_config are missing

# test('''
# .eqp full
# SELECT 42;
# ''', out="DUMMY_SCAN")

# this fails because the sqlite printf accepts %w for table names

# test('''
# CREATE VIEW sqlite_master AS SELECT * FROM sqlite_master(); -- hack!
# CREATE TABLE a (I INTEGER);
# INSERT INTO a VALUES (42);
# .clone %s
# ''' % tempfile.mktemp())



# fails because view pragma_database_list does not exist

# test('.databases')


# fails
# test('''
# CREATE TABLE a (I INTEGER);
# .changes off
# INSERT INTO a VALUES (42);
# .dump a
# ''')



# printf %q

# test('''
# CREATE TABLE a (i INTEGER);
# CREATE INDEX a_idx ON a(i);
# .imposter a_idx a_idx_imp
# ''')
