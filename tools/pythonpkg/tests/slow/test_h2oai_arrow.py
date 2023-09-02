import duckdb
import os
import math
from pytest import mark, fixture, importorskip

read_csv = importorskip('pyarrow.csv').read_csv
requests = importorskip('requests')
np = importorskip('numpy')


def download_file(url, name):
    r = requests.get(url, allow_redirects=True)
    open(name, 'wb').write(r.content)


def group_by_q1(con):
    con.execute("CREATE TABLE ans AS SELECT id1, sum(v1) AS v1 FROM x GROUP BY id1")
    res = con.execute("SELECT COUNT(*), sum(v1)::varchar AS v1 FROM ans").fetchall()
    assert res == [(96, '28498857')]
    con.execute("DROP TABLE ans")


def group_by_q2(con):
    con.execute("CREATE TABLE ans AS SELECT id1, id2, sum(v1) AS v1 FROM x GROUP BY id1, id2;")
    res = con.execute("SELECT count(*), sum(v1) AS v1 FROM ans;").fetchall()
    assert res == [(9216, 28498857)]
    con.execute("DROP TABLE ans")


def group_by_q3(con):
    con.execute("CREATE TABLE ans AS SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM x GROUP BY id3;")
    res = con.execute("SELECT COUNT(*), sum(v1) AS v1, sum(v3) AS v3 FROM ans;").fetchall()
    assert res[0][0] == 95001
    assert res[0][1] == 28498857
    assert math.floor(res[0][2]) == 4749467
    con.execute("DROP TABLE ans")


def group_by_q4(con):
    con.execute("CREATE TABLE ans AS SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM x GROUP BY id4;")
    res = con.execute("SELECT COUNT(*), sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM ans").fetchall()
    assert res[0][0] == 96
    assert math.floor(res[0][1]) == 287
    assert math.floor(res[0][2]) == 767
    assert math.floor(res[0][3]) == 4799
    con.execute("DROP TABLE ans")


def group_by_q5(con):
    con.execute("CREATE TABLE ans AS SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM x GROUP BY id6;")
    res = con.execute("SELECT COUNT(*), sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM ans").fetchall()
    assert res[0][0] == 95001
    assert res[0][1] == 28498857
    assert res[0][2] == 75988394
    assert math.floor(res[0][3]) == 474969574
    con.execute("DROP TABLE ans")


def group_by_q6(con):
    con.execute(
        "CREATE TABLE ans AS SELECT id4, id5, quantile_cont(v3, 0.5) AS median_v3, stddev(v3) AS sd_v3 FROM x GROUP BY id4, id5;"
    )
    res = con.execute("SELECT COUNT(*), sum(median_v3) AS median_v3, sum(sd_v3) AS sd_v3 FROM ans").fetchall()
    assert res[0][0] == 9216
    assert math.floor(res[0][1]) == 460771
    assert math.floor(res[0][2]) == 266006
    con.execute("DROP TABLE ans")


def group_by_q7(con):
    con.execute("CREATE TABLE ans AS SELECT id3, max(v1)-min(v2) AS range_v1_v2 FROM x GROUP BY id3;")
    res = con.execute("SELECT count(*), sum(range_v1_v2) AS range_v1_v2 FROM ans;").fetchall()
    assert res[0][0] == 95001
    assert res[0][1] == 379850
    con.execute("DROP TABLE ans")


def group_by_q8(con):
    con.execute(
        "CREATE TABLE ans AS SELECT id6, v3 AS largest2_v3 FROM (SELECT id6, v3, row_number() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS order_v3 FROM x WHERE v3 IS NOT NULL) sub_query WHERE order_v3 <= 2"
    )
    res = con.execute("SELECT count(*), sum(largest2_v3) AS largest2_v3 FROM ans").fetchall()
    assert res[0][0] == 190002
    assert math.floor(res[0][1]) == 18700554
    con.execute("DROP TABLE ans")


def group_by_q9(con):
    con.execute("CREATE TABLE ans AS SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM x GROUP BY id2, id4;")
    res = con.execute("SELECT count(*), sum(r2) AS r2 FROM ans").fetchall()
    assert res[0][0] == 9216
    assert math.floor(res[0][1]) == 9
    con.execute("DROP TABLE ans")


def group_by_q10(con):
    con.execute(
        "CREATE TABLE ans AS SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count(*) AS count FROM x GROUP BY id1, id2, id3, id4, id5, id6;"
    )
    res = con.execute("SELECT sum(v3) AS v3, sum(count) AS count FROM ans;").fetchall()
    assert math.floor(res[0][0]) == 474969574
    assert res[0][1] == 10000000
    con.execute("DROP TABLE ans")


def join_by_q1(con):
    con.execute("CREATE TABLE ans AS SELECT x.*, small.id4 AS small_id4, v2 FROM x JOIN small USING (id1);")
    res = con.execute("SELECT COUNT(*), SUM(v1) AS v1, SUM(v2) AS v2 FROM ans;").fetchall()
    assert res[0][0] == 8998860
    assert math.floor(res[0][1]) == 450015153
    assert math.floor(res[0][2]) == 347720187
    con.execute("DROP TABLE ans")


def join_by_q2(con):
    con.execute(
        "CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x JOIN medium USING (id2);"
    )
    res = con.execute("SELECT COUNT(*), SUM(v1) AS v1, SUM(v2) AS v2 FROM ans;").fetchall()
    assert res[0][0] == 8998412
    assert math.floor(res[0][1]) == 449954076
    assert math.floor(res[0][2]) == 449999844
    con.execute("DROP TABLE ans")


def join_by_q3(con):
    con.execute(
        "CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 FROM x LEFT JOIN medium USING (id2);"
    )
    res = con.execute("SELECT COUNT(*), SUM(v1) AS v1, SUM(v2) AS v2 FROM ans;").fetchall()
    assert res[0][0] == 10000000
    assert math.floor(res[0][1]) == 500043740
    assert math.floor(res[0][2]) == 449999844
    con.execute("DROP TABLE ans")


def join_by_q4(con):
    con.execute(
        "CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 AS medium_id4, v2 FROM x JOIN medium USING (id5);"
    )
    res = con.execute("SELECT COUNT(*), SUM(v1) AS v1, SUM(v2) AS v2 FROM ans;").fetchall()
    assert res[0][0] == 8998412
    assert math.floor(res[0][1]) == 449954076
    assert math.floor(res[0][2]) == 449999844
    con.execute("DROP TABLE ans")


def join_by_q5(con):
    con.execute(
        "CREATE TABLE ans AS SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 AS big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM x JOIN big USING (id3);"
    )
    res = con.execute("SELECT COUNT(*), SUM(v1) AS v1, SUM(v2) AS v2 FROM ans;").fetchall()
    assert res[0][0] == 9000000
    assert math.floor(res[0][1]) == 450032091
    assert math.floor(res[0][2]) == 449860428
    con.execute("DROP TABLE ans")


class TestH2OAIArrow(object):
    @mark.parametrize(
        'function',
        [
            group_by_q1,
            group_by_q2,
            group_by_q3,
            group_by_q4,
            group_by_q5,
            group_by_q6,
            group_by_q7,
            group_by_q8,
            group_by_q9,
            group_by_q10,
        ],
    )
    @mark.parametrize('threads', [1, 4])
    def test_group_by(self, threads, function):
        con = duckdb.connect()
        download_file(
            'https://github.com/duckdb/duckdb-data/releases/download/v1.0/G1_1e7_1e2_5_0.csv.gz',
            'G1_1e7_1e2_5_0.csv.gz',
        )
        arrow_table = read_csv('G1_1e7_1e2_5_0.csv.gz')
        con.register("x", arrow_table)
        os.remove('G1_1e7_1e2_5_0.csv.gz')

        con.execute(f"PRAGMA threads={threads}")

        function(con)

    @mark.parametrize('threads', [1, 4])
    @mark.parametrize(
        'function',
        [
            join_by_q1,
            join_by_q2,
            join_by_q3,
            join_by_q4,
            join_by_q5,
        ],
    )
    @mark.usefixtures('large_data')
    def test_join(self, threads, function, large_data):
        large_data.execute(f"PRAGMA threads={threads}")

        function(large_data)


@fixture(scope="module")
def large_data():
    download_file(
        'https://github.com/duckdb/duckdb-data/releases/download/v1.0/J1_1e7_NA_0_0.csv.gz', 'J1_1e7_NA_0_0.csv.gz'
    )
    download_file(
        'https://github.com/duckdb/duckdb-data/releases/download/v1.0/J1_1e7_1e1_0_0.csv.gz', 'J1_1e7_1e1_0_0.csv.gz'
    )
    download_file(
        'https://github.com/duckdb/duckdb-data/releases/download/v1.0/J1_1e7_1e4_0_0.csv.gz', 'J1_1e7_1e4_0_0.csv.gz'
    )
    download_file(
        'https://github.com/duckdb/duckdb-data/releases/download/v1.0/J1_1e7_1e7_0_0.csv.gz', 'J1_1e7_1e7_0_0.csv.gz'
    )

    con = duckdb.connect()
    arrow_table = read_csv('J1_1e7_NA_0_0.csv.gz')
    con.register("x", arrow_table)

    arrow_table = read_csv('J1_1e7_1e1_0_0.csv.gz')
    con.register("small", arrow_table)

    arrow_table = read_csv('J1_1e7_1e4_0_0.csv.gz')
    con.register("medium", arrow_table)

    arrow_table = read_csv('J1_1e7_1e7_0_0.csv.gz')
    con.register("big", arrow_table)

    yield con

    os.remove('J1_1e7_NA_0_0.csv.gz')
    os.remove('J1_1e7_1e1_0_0.csv.gz')
    os.remove('J1_1e7_1e4_0_0.csv.gz')
    os.remove('J1_1e7_1e7_0_0.csv.gz')
