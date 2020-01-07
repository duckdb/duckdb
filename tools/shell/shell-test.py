import sys
import subprocess
import tempfile
import os

if len(sys.argv) < 2:
     raise Exception('need shell binary as parameter')

def test(cmd, out=None, err=None):
     res = subprocess.run([sys.argv[1], '--batch', '-init', '/dev/null'], capture_output=True, input=bytearray(cmd, 'utf8'))
     stdout = res.stdout.decode('utf8').strip()
     stderr = res.stderr.decode('utf8').strip()

     print(stdout)
     print(stderr)

     if out and out not in stdout:
          print(stdout)
          raise Exception('out test failed')

     if err and err not in stderr:
          print(stderr)
          raise Exception('err test failed')

     if not err and stderr != '':
          print(stderr)
          raise Exception('got err test failed')


# basic test
test('select \'asdf\' as a;', out='asdf')

test('.auth ON', err='sqlite3_set_authorizer')
test('.auth OFF', err='sqlite3_set_authorizer')
test('.backup %s' % tempfile.mktemp(), err='sqlite3_backup_init')

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
''' % (tempfile.gettempdir(), os.getcwd()))

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

test('.load %s' % tempfile.mktemp(), err="Error")

# this should be fixed
test('.selftest', err='sqlite3_table_column_metadata')

scriptfile = tempfile.mktemp()
print("select 42",  file=open(scriptfile, 'w'))
test('.read %s' % scriptfile, out='42')


test('.show', out='rowseparator')

test('.limit length 42', err='sqlite3_limit')

# ???
test('.lint fkey-indexes')

# this should probably be fixed, sqlite generates an internal query that duckdb does not like
test('.indexes', err='syntax error')


test('.timeout', err='sqlite3_busy_timeout')


test('.save %s' % tempfile.mktemp(), err='sqlite3_backup_init')
test('.restore %s' % tempfile.mktemp(), err='sqlite3_backup_init')


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

test('.trace %s\n; SELECT 42;' % tempfile.mktemp(), err='sqlite3_trace_v2')

outfile = tempfile.mktemp()
test('''
.output %s
SELECT 42;
''' % outfile)
outstr = open(outfile,'r').read()
if '42' not in outstr:
     raise Exception('.output test failed')


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

# datafile = tempfile.mktemp()
# print("42\n84",  file=open(datafile, 'w'))
# test('''
# CREATE TABLE a (i INTEGER);
# .import %s a
# SELECT SUM(i) FROM a;
# ''' % datafile, out='126')


# fails because view pragma_database_list does not exist

# test('.databases')


# fails because sqlite printf uses %Q, too

# test('''
# CREATE TABLE a (I INTEGER);
# .changes off
# INSERT INTO a VALUES (42);
# .dump a
# ''')


# this fails because sqlite printf uses %z, too. perhaps for the best

# test('.system echo 42', out="42")
# test('.shell echo 42', out="42")

# printf %q

# test('''
# CREATE TABLE a (i INTEGER);
# CREATE INDEX a_idx ON a(i);
# .imposter a_idx a_idx_imp
# ''')



# .fullschema ?--indent? Show schema and the content of sqlite_stat tables
# .indexes ?TABLE?       Show names of all indexes
#                          If TABLE specified, only show indexes for tables
#                          matching LIKE pattern TABLE.
# .log FILE|off          Turn logging on or off.  FILE can be stderr/stdout
# .mode MODE ?TABLE?     Set output mode where MODE is one of:
#                          ascii    Columns/rows delimited by 0x1F and 0x1E
#                          csv      Comma-separated values
#                          column   Left-aligned columns.  (See .width)
#                          html     HTML <table> code
#                          insert   SQL insert statements for TABLE
#                          line     One value per line
#                          list     Values delimited by "|"
#                          quote    Escape answers as for SQL
#                          tabs     Tab-separated values
#                          tcl      TCL list elements
# .once (-e|-x|FILE)     Output for the next SQL command only to FILE
#                          or invoke system text editor (-e) or spreadsheet (-x)
#                          on the output.
# .open ?OPTIONS? ?FILE? Close existing database and reopen FILE
#                          The --new option starts with an empty file
#                          Other options: --readonly --append --zip
# .schema ?PATTERN?      Show the CREATE statements matching PATTERN
#                           Add --indent for pretty-printing
# .sha3sum ?OPTIONS...?  Compute a SHA3 hash of database content
# .tables ?TABLE?        List names of tables
#                          If TABLE specified, only list tables matching
#                          LIKE pattern TABLE.
# .width NUM1 NUM2 ...   Set column widths for "column" mode
#                          Negative values right-justify
