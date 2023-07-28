import sys
import subprocess
import tempfile
import os
import shutil

if len(sys.argv) < 2:
    raise Exception('need shell binary as parameter')

extra_parameter = ""
if len(sys.argv) == 3:
    extra_parameter = sys.argv[2]


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


def test(cmd, out=None, err=None, extra_commands=None, input_file=None):
    #########  isql         "DSN=DuckDB;Database=test.db"   -k    -b    -d'|'  /dev/null
    command = [sys.argv[1], "DSN=DuckDB;Database=test.db", '-k', '-b', '-d|', '/dev/null']
    if extra_parameter:
        command.append(extra_parameter)

    if extra_commands:
        command += extra_commands
    if input_file:
        command += [cmd]
        res = subprocess.run(
            command, input=open(input_file, 'rb').read(), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
    else:
        res = subprocess.run(command, input=bytearray(cmd, 'utf8'), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = res.stdout.decode('utf8').strip()
    stderr = res.stderr.decode('utf8').strip()

    if out and out not in stdout:
        test_exception(command, cmd, stdout, stderr, 'out test failed')

    if err and err not in stderr:
        test_exception(command, cmd, stdout, stderr, 'err test failed')

    if not err and stderr != '':
        test_exception(command, cmd, stdout, stderr, 'got err test failed')

    if err is None and res.returncode != 0:
        test_exception(command, cmd, stdout, stderr, 'process returned non-zero exit code but no error was specified')


# basic tests
test('select 42;', out='42')

test('select 42, 43, 44;', out="42|43|44")

test(
    """CREATE TABLE people(id INTEGER, name VARCHAR);
INSERT INTO people VALUES (1, 'Mark'), (2, 'Hannes');
SELECT * FROM people;""",
    out=("1|Mark\n" "2|Hannes"),
)

range_out = ""
for i in range(10000):
    if i < 9999:
        range_out += str(i) + "\n"
    else:
        range_out += str(i)
test("SELECT * FROM range(10000);", out=range_out)


# ### FROM test/sql/projection/test_simple_projection.test #################################
test(
    """PRAGMA enable_verification
CREATE TABLE a (i integer, j integer);
SELECT * FROM a;
""",
    out="",
)

test(
    """INSERT INTO a VALUES (42, 84);
SELECT * FROM a;
""",
    out="42|84",
)

test(
    """CREATE TABLE test (a INTEGER, b INTEGER);
INSERT INTO test VALUES (11, 22), (12, 21), (13, 22);
""",
    out="",
)

test('SELECT a, b FROM test;', out=("11|22\n" "12|21\n" "13|22"))

test('SELECT a + 2, b FROM test WHERE a = 11', out='13|22')
test('SELECT a + 2, b FROM test WHERE a = 12', out='14|21')

test('SELECT cast(a AS VARCHAR) FROM test;', out=("11\n" "12\n" "13"))

test('SELECT cast(cast(a AS VARCHAR) as INTEGER) FROM test;', out=("11\n" "12\n" "13"))

### FROM test/sql/types/timestamp/test_timestamp.test #################################
test(
    """CREATE TABLE IF NOT EXISTS timestamp (t TIMESTAMP);
INSERT INTO timestamp VALUES ('2008-01-01 00:00:01'), (NULL), ('2007-01-01 00:00:01'), ('2008-02-01 00:00:01'), ('2008-01-02 00:00:01'), ('2008-01-01 10:00:00'), ('2008-01-01 00:10:00'), ('2008-01-01 00:00:10')
"""
)

test("SELECT timestamp '2017-07-23 13:10:11';", out='2017-07-23 13:10:11')
test(
    "SELECT timestamp '2017-07-23T13:10:11', timestamp '2017-07-23T13:10:11Z';",
    out='2017-07-23 13:10:11|2017-07-23 13:10:11',
)

test("SELECT timestamp '    2017-07-23     13:10:11    ';", out='2017-07-23 13:10:11')

test("SELECT timestamp '    2017-07-23     13:10:11    AA';", err="[ISQL]ERROR")
test("SELECT timestamp 'AA2017-07-23 13:10:11';", err="[ISQL]ERROR")
test("SELECT timestamp '2017-07-23A13:10:11';", err="[ISQL]ERROR")

test(
    'SELECT t FROM timestamp ORDER BY t;',
    out=(
        "2007-01-01 00:00:01\n"
        "2008-01-01 00:00:01\n"
        "2008-01-01 00:00:10\n"
        "2008-01-01 00:10:00\n"
        "2008-01-01 10:00:00\n"
        "2008-01-02 00:00:01\n"
        "2008-02-01 00:00:01"
    ),
)

test('SELECT MIN(t) FROM timestamp;', out='2007-01-01 00:00:01')
test('SELECT MAX(t) FROM timestamp;', out='2008-02-01 00:00:01')

test('SELECT SUM(t) FROM timestamp', err="[ISQL]ERROR")
test('SELECT AVG(t) FROM timestamp', err="[ISQL]ERROR")
test('SELECT t+t FROM timestamp', err="[ISQL]ERROR")
test('SELECT t*t FROM timestamp', err="[ISQL]ERROR")
test('SELECT t/t FROM timestamp', err="[ISQL]ERROR")
test('SELECT t%t FROM timestamp', err="[ISQL]ERROR")

test(
    'SELECT t-t FROM timestamp',
    out=("00:00:00\n" "\n" "00:00:00\n" "00:00:00\n" "00:00:00\n" "00:00:00\n" "00:00:00\n" "00:00:00"),
)

test("SELECT YEAR(TIMESTAMP '1992-01-01 01:01:01');", out='1992')
test("SELECT YEAR(TIMESTAMP '1992-01-01 01:01:01'::DATE);", out='1992')
test("SELECT (TIMESTAMP '1992-01-01 01:01:01')::DATE", out='1992-01-01')
test("SELECT (TIMESTAMP '1992-01-01 01:01:01')::TIME", out='01:01:01')
test('SELECT t::DATE FROM timestamp WHERE EXTRACT(YEAR from t)=2007 ORDER BY 1', out='2007-01-01')
test('SELECT t::TIME FROM timestamp WHERE EXTRACT(YEAR from t)=2007 ORDER BY 1', out='00:00:01')
test("SELECT (DATE '1992-01-01')::TIMESTAMP;", out='1992-01-01 00:00:00')
test("SELECT TIMESTAMP '2008-01-01 00:00:01.5'::VARCHAR", out='2008-01-01 00:00:01.5')
test("SELECT TIMESTAMP '-8-01-01 00:00:01.5'::VARCHAR", out='0009-01-01 (BC) 00:00:01.5')
test("SELECT TIMESTAMP '100000-01-01 00:00:01.5'::VARCHAR", out='100000-01-01 00:00:01.5')

test("UPDATE timestamp SET t = strptime('20221215101010','%Y%m%d%H%M%S') WHERE t='2008-01-01 00:00:01'::TIMESTAMP")

### FROM test/sql/types/time/test_time.test #################################
test(
    """CREATE TABLE times(i TIME);
INSERT INTO times VALUES ('00:01:20'), ('20:08:10.998'), ('20:08:10.33'), ('20:08:10.001'), (NULL);
"""
)

test("SELECT * FROM times", out=("00:01:20\n" "20:08:10.998\n" "20:08:10.33\n" "20:08:10.001"))

test("SELECT cast(i AS VARCHAR) FROM times", out=("00:01:20\n" "20:08:10.998\n" "20:08:10.33\n" "20:08:10.001"))

test("SELECT ''::TIME", err="[ISQL]ERROR")
test("SELECT '  '::TIME", err="[ISQL]ERROR")
test("SELECT '        '::TIME", err="[ISQL]ERROR")
test("SELECT '1'::TIME", err="[ISQL]ERROR")
test("SELECT '11'::TIME", err="[ISQL]ERROR")
test("SELECT '11:'::TIME", err="[ISQL]ERROR")
test("SELECT '11:11'::TIME", err="[ISQL]ERROR")
test("SELECT '11:11:f'::TIME", err="[ISQL]ERROR")

### FROM test/sql/types/time/test_time.test #################################
test("SELECT NULL", out='')
test("SELECT 3 + NULL", out='')
test("SELECT NULL + 3", out='')
test("SELECT NULL + NULL", out='')
test("SELECT 4 / 0", out='')

test(
    """DROP TABLE test
CREATE TABLE test (a INTEGER, b INTEGER);
INSERT INTO test VALUES (11, 22), (NULL, 21), (13, 22)
"""
)

test("SELECT a FROM test", out=("11\n" "\n" "13"))

test("SELECT cast(a AS BIGINT) FROM test;", out=("11\n" "\n" "13"))

test("SELECT a / 0 FROM test;", out='')
test("SELECT a / (a - a) FROM test;", out='')
test("SELECT a + b FROM test;", out=("33\n" "\n" "35"))

### FROM test/sql/types/decimal/test_decimal.test #################################
test("SELECT typeof('0.1'::DECIMAL);", out='DECIMAL(18,3)')
test("SELECT '0.1'::DECIMAL::VARCHAR, '922337203685478.758'::DECIMAL::VARCHAR;", out="0.100|922337203685478.758")
test("SELECT '-0.1'::DECIMAL::VARCHAR, '-922337203685478.758'::DECIMAL::VARCHAR;", out='-0.100|-922337203685478.758')
test("SELECT '   7   '::DECIMAL::VARCHAR, '9.'::DECIMAL::VARCHAR, '.1'::DECIMAL::VARCHAR;", out='7.000|9.000|0.100')
test("SELECT '0.123456789'::DECIMAL::VARCHAR, '-0.123456789'::DECIMAL::VARCHAR;", out='0.123|-0.123')

test("SELECT '9223372036854788.758'::DECIMAL;", err="[ISQL]ERROR")

test("SELECT '0.1'::DECIMAL(3, 0)::VARCHAR;", out='0')
test("SELECT '123.4'::DECIMAL(9)::VARCHAR;", out='123')
test("SELECT '0.1'::DECIMAL(3, 3)::VARCHAR, '-0.1'::DECIMAL(3, 3)::VARCHAR;", out='.100|-.100')

test("SELECT '1'::DECIMAL(3, 3)::VARCHAR;", err="[ISQL]ERROR")
test("SELECT '-1'::DECIMAL(3, 3)::VARCHAR;", err="[ISQL]ERROR")

test("SELECT '0.1'::DECIMAL::DECIMAL::DECIMAL;", out='0.1')

test("SELECT '123.4'::DECIMAL(4,1)::VARCHAR;", out='123.4')
test("SELECT '2.001'::DECIMAL(4,3)::VARCHAR;", out='2.001')
test("SELECT '123456.789'::DECIMAL(9,3)::VARCHAR;", out='123456.789')
test("SELECT '123456789'::DECIMAL(9,0)::VARCHAR;", out='123456789')
test("SELECT '123456789'::DECIMAL(18,3)::VARCHAR;", out='123456789.000')
test(
    "SELECT '1701411834604692317316873037.1588410572'::DECIMAL(38,10)::VARCHAR;",
    out='1701411834604692317316873037.1588410572',
)
test("SELECT '0'::DECIMAL(38,10)::VARCHAR;", out='0.0000000000')
test("SELECT '0.00003'::DECIMAL(38,10)::VARCHAR;", out='0.0000300000')

test("SELECT '0.1'::DECIMAL(3, 4);", err="[ISQL]ERROR")
test("SELECT '0.1'::DECIMAL('hello');", err="[ISQL]ERROR")
test("SELECT '0.1'::DECIMAL(-17);", err="[ISQL]ERROR")
test("SELECT '0.1'::DECIMAL(1000);", err="[ISQL]ERROR")
test("SELECT '0.1'::DECIMAL(1, 2, 3);", err="[ISQL]ERROR")
test("SELECT '1'::INTEGER(1000);", err="[ISQL]ERROR")

### FROM test/sql/types/date/test_date.test #################################
test(
    """CREATE TABLE dates(i DATE);
INSERT INTO dates VALUES ('1993-08-14'), (NULL);
"""
)

# NULL is print as an empty string, thus python removes it from the stoudt
test("SELECT * FROM dates", out='1993-08-14')
test("SELECT * FROM dates", out='1993-08-14')
test("SELECT cast(i AS VARCHAR) FROM dates", out='1993-08-14')
test("SELECT i + 5 FROM dates", out='1993-08-19')
test("SELECT i - 5 FROM dates", out='1993-08-09')

test("SELECT i * 3 FROM dates", err="[ISQL]ERROR")
test("SELECT i / 3 FROM dates", err="[ISQL]ERROR")
test("SELECT i % 3 FROM dates", err="[ISQL]ERROR")
test("SELECT i + i FROM dates", err="[ISQL]ERROR")

test("SELECT (i + 5) - i FROM dates", out='5')

test("SELECT ''::DATE", err="[ISQL]ERROR")
test("SELECT '  '::DATE", err="[ISQL]ERROR")
test("SELECT '1992'::DATE", err="[ISQL]ERROR")
test("SELECT '1992-'::DATE", err="[ISQL]ERROR")
test("SELECT '1992-01'::DATE", err="[ISQL]ERROR")
test("SELECT '1992-01-'::DATE", err="[ISQL]ERROR")
test("SELECT '30000307-01-01 (BC)'::DATE", err="[ISQL]ERROR")

### FROM test/sql/types/blob/test_blob.test #################################
test(
    """CREATE TABLE blobs (b BYTEA);
INSERT INTO blobs VALUES('\\xaa\\xff\\xaa'), ('\\xAA\\xFF\\xAA\\xAA\\xFF\\xAA'), ('\\xAA\\xFF\\xAA\\xAA\\xFF\\xAA\\xAA\\xFF\\xAA');
"""
)

test(
    "SELECT * FROM blobs",
    out=("\\xAA\\xFF\\xAA\n" "\\xAA\\xFF\\xAA\\xAA\\xFF\\xAA\n" "\\xAA\\xFF\\xAA\\xAA\\xFF\\xAA\\xAA\\xFF\\xAA"),
)

test(
    """DELETE FROM blobs;
INSERT INTO blobs VALUES('\\xaa\\xff\\xaa'), ('\\xaa\\xff\\xaa\\xaa\\xff\\xaa'), ('\\xaa\\xff\\xaa\\xaa\\xff\\xaa\\xaa\\xff\\xaa');
"""
)

test(
    "SELECT * FROM blobs",
    out=("\\xAA\\xFF\\xAA\n" "\\xAA\\xFF\\xAA\\xAA\\xFF\\xAA\n" "\\xAA\\xFF\\xAA\\xAA\\xFF\\xAA\\xAA\\xFF\\xAA"),
)

test(
    """DELETE FROM blobs;
INSERT INTO blobs VALUES('\\xaa1199'), ('\\xaa1199aa1199'), ('\\xaa1199aa1199aa1199');
"""
)

test("SELECT * FROM blobs", out=("\\xAA1199\n" "\\xAA1199aa1199\n" "\\xAA1199aa1199aa1199"))

test("INSERT INTO blobs VALUES('\\xGA\\xFF\\xAA')", err="[ISQL]ERROR")
test("INSERT INTO blobs VALUES('\\xA')", err="[ISQL]ERROR")
test("INSERT INTO blobs VALUES('\\xAA\\xA')", err="[ISQL]ERROR")
test("INSERT INTO blobs VALUES('blablabla\\x')", err="[ISQL]ERROR")
test("SELECT 'abc �'::BYTEA;", err="[ISQL]ERROR")

test("SELECT ''::BLOB;", out='')
test('SELECT NULL::BLOB', out='')

test(
    """CREATE TABLE blob_empty (b BYTEA);
INSERT INTO blob_empty VALUES(''), (''::BLOB);
INSERT INTO blob_empty VALUES(NULL), (NULL::BLOB);
"""
)

test("SELECT * FROM blob_empty", out='')

test("SELECT 'ü'::blob;", err="[ISQL]ERROR")

### FROM test/sql/types/string/test_unicode.test #################################
test(
    """CREATE TABLE emojis(id INTEGER, s VARCHAR);
INSERT INTO emojis VALUES (1, '🦆'), (2, '🦆🍞🦆')
"""
)

test("SELECT * FROM emojis ORDER BY id", out=("1|🦆\n" "2|🦆🍞🦆"))

test("SELECT substring(s, 1, 1), substring(s, 2, 1) FROM emojis ORDER BY id", out=("🦆|\n" "🦆|🍞"))

test("SELECT length(s) FROM emojis ORDER BY id", out="1\n3")
