var duckdb = require('..');
var assert = require('assert');
var arrow = require('apache-arrow')

// const build = 'debug';
const build = 'debug';
const extension_path = `../../build/${build}/extension/arrow/arrow.duckdb_extension`;

// TODO move to duckdb src
class IpcResultStreamIterator {
    constructor(stream_result_p) {
        this._depleted = false;
        this.stream_result = stream_result_p;
    }

    async next() {
        if (this._depleted) {
            return { done: true, value: null };
        }

        const ipc_raw = await this.stream_result.nextIpcBuffer();
        const res = new Uint8Array(ipc_raw);

        this._depleted = res.length == 0;
        return {
            done: this._depleted,
            value: res,
        };
    }

    [Symbol.asyncIterator]() {
        return this;
    }

    // Materialize the IPC stream into a list of Uint8Arrays
    async toArray () {
        const retval = []

        for await (const ipc_buf of this) {
            retval.push(ipc_buf);
        }

        // Push EOS message containing 4 bytes of 0
        retval.push(new Uint8Array([0,0,0,0]));

        return retval;
    }
}

describe('Roundtrip DuckDB -> ArrowJS ipc -> DuckDB', () => {
    const total = 1000;

    let db;
    let conn;
    before((done) => {
        db = new duckdb.Database(':memory:', {"allow_unsigned_extensions":"true"}, () => {
            conn = new duckdb.Connection(db, () => {
                db.run(`LOAD '${extension_path}';`, function(err) {
                    if (err) {
                        throw err;
                    }
                    done();
                });
            });
        });
    });

    it('Simple int column', async () => {
        // Now we fetch the ipc stream object and construct the RecordBatchReader
        const result = await conn.arrrowIPCStream('SELECT * FROM range(1001, 2001) tbl(i)');

        // Create iterator from QueryResult, could
        const it = new IpcResultStreamIterator(result);

        // Materialize into list of Uint8Arrays containing the ipc stream
        const fully_materialized = await it.toArray();

        // We can now create a RecordBatchReader & Table from the materialized stream
        const reader = await arrow.RecordBatchReader.from(fully_materialized);

        // We now have an Arrow table containing the data
        // const table = arrow.tableFromIPC(reader);
        // console.log(table.toArray());

        // Now we can query the ipc buffer using DuckDB. It is available as "_arrow_ipc_stream"
        db.scanArrowIpc(`SELECT avg(i) as average, count(1) as total FROM _arrow_ipc_stream;`, fully_materialized, function(err, result) {
            if (err) {
                throw err;
            }
            assert.deepEqual(result, [{average: 1500.5, total:1000}]);
        });
    });
})

describe('[Benchmark] single int column load (50M tuples)',() => {
    // Config
    const column_size = 50*1000*1000;

    let db;
    let conn;

    before((done) => {
        db = new duckdb.Database(':memory:',  {"allow_unsigned_extensions":"true"}, () => {
            conn = new duckdb.Connection(db, () => {
                db.run("CREATE TABLE test AS select * FROM range(0,?) tbl(i);", column_size, (err) => {
                    if (err) throw err;
                    db.run(`LOAD '${extension_path}';`, function (err) {
                        if (err) throw err;
                        done();
                    });
                });
            });
        });
    });

    it('DuckDB table -> DuckDB table', (done) => {
        const batches = [];
        let got_rows = 0;

        conn.run('CREATE TABLE copy_table AS SELECT * FROM test;', (err, result) => {
            if (err) throw err;
            done();
        });
    });

    it('DuckDB table -> IPC buffer', async () => {
        let got_batches = 0;
        let got_rows = 0;
        const batches = [];

        const result = await conn.arrrowIPCStream('SELECT * FROM test;');
        const it = new IpcResultStreamIterator(result);
        const fully_materialized = await it.toArray();
        const reader = await arrow.RecordBatchReader.from(fully_materialized);
        const table = arrow.tableFromIPC(reader);

        assert.equal(table.numRows, column_size);
    });
});

describe('[Benchmark] TPC-H SF1 lineitem.parquet', () => {
	// Config
    // const tpch_q06 = "SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= CAST('1994-01-01' AS date) AND l_shipdate < CAST('1995-01-01' AS date) AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24; "
    const simple_query = "select sum(l_orderkey) as sum_orderkey FROM lineitem";

	// const expected_rows = 60175;
	// const expected_orderkey_sum = 1802759573;
	// const parquet_file_path = "/tmp/lineitem_sf0_01.parquet";
	const expected_rows = 6001215;
    const expected_orderkey_sum = 18005322964949;
	const parquet_file_path = "/tmp/lineitem_sf1.parquet";

    // Which query to run
    // const sql = tpch_q06;
    const sql = simple_query;


	let db;
	let conn;

    before((done) => {
        db = new duckdb.Database(':memory:',  {"allow_unsigned_extensions":"true"}, () => {
            conn = new duckdb.Connection(db, () => {
                db.run(`LOAD '${extension_path}';`, function (err) {
                    if (err) throw err;
                    done();
                });
            });
        });
    });

    it('lineitem.parquet -> DuckDB -> arrow IPC -> query from DuckDB', async () => {
        const batches = [];
        let got_rows = 0;

        const result = await conn.arrrowIPCStream('SELECT * FROM "' + parquet_file_path + '";');
        const it = new IpcResultStreamIterator(result);
        const fully_materialized = await it.toArray();

        // We can now create a RecordBatchReader & Table from the materialized stream
        const reader = await arrow.RecordBatchReader.from(fully_materialized);
        const table = arrow.tableFromIPC(reader);

        const query = sql.replace("lineitem", "_arrow_ipc_stream");
        await new Promise((resolve, reject) => {
            db.scanArrowIpc(query, fully_materialized , function (err, result) {
                if (err) {
                    reject(err)
                }

                assert.deepEqual(result, [{sum_orderkey: expected_orderkey_sum}]);
                resolve();
            })
        });
    });

    it('lineitem.parquet -> DuckDB table -> query from DuckDB', (done) => {
        const batches = [];
        let got_rows = 0;

        conn.run('CREATE TABLE load_parquet_directly AS SELECT * FROM "' + parquet_file_path + '";');
        const query = sql.replace("lineitem", "load_parquet_directly");
        db.all(query, function(err, result) {
            if (err) {
                throw err;
            }

            assert.deepEqual(result, [{sum_orderkey: expected_orderkey_sum}]);
            done()
        });
    });
});

describe.only('Validate with TPCH lineitem SF0.01', () => {
    const parquet_file_path = "/tmp/lineitem_sf0_01.parquet";

    const queries = [
        "select count(*) from table_name LIMIT 10;",
        "select sum(l_orderkey) as sum_orderkey FROM table_name",
        "select * from table_name LIMIT 10",
        "select l_orderkey from table_name WHERE l_orderkey=2 LIMIT 2",
        "select l_extendedprice from table_name",
        "select l_extendedprice from table_name WHERE l_extendedprice > 53468 and l_extendedprice < 53469  LIMIT 2",
        "select count(l_orderkey) from table_name where l_commitdate > '1996-10-28'",
    ];

    let db;
    let conn;
    before((done) => {
        db = new duckdb.Database(':memory:', {"allow_unsigned_extensions":"true"}, () => {
            conn = new duckdb.Connection(db, () => {
                db.run(`LOAD '${extension_path}';`, function(err) {
                    if (err) {
                        throw err;
                    }
                    done();
                });
            });
        });
    });

    for (const query of queries) {
        it(` ${query}`, async () => {
            const batches = [];
            // First do query directly on parquet file
            const expected_value = await new Promise((resolve, reject) => {
                db.all(query.replace("table_name", `'${parquet_file_path}'`), function (err, result) {
                    if (err) {
                        reject(err);
                    }

                    resolve(result);
                });
            });

            // Secondly copy parquet file completely into Arrow IPC format
            const result = await conn.arrrowIPCStream('SELECT * FROM "' + parquet_file_path + '";');
            const it = new IpcResultStreamIterator(result);
            const fully_materialized = await it.toArray();

            // Now re-run query on Arrow IPC stream
            const reader = await arrow.RecordBatchReader.from(fully_materialized);
            await new Promise((resolve, reject) => {
                db.scanArrowIpc(query.replace("table_name", '_arrow_ipc_stream'), fully_materialized, function (err, result) {
                    if (err) {
                        reject(err)
                    }

                    assert.deepEqual(result, expected_value, `Query failed: ${query}`);
                    resolve();
                })
            });
        });
    }
})

