var duckdb = require('..');
var assert = require('assert');
var arrow = require('apache-arrow')
const { performance } = require('perf_hooks');

const build = 'debug';
// const build = 'release';
const extension_path = `../../build/${build}/extension/arrow/arrow.duckdb_extension`;

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

    // TODO: this test should be done with a sanitizer instead of spraying memory
    it('test gc', async () => {
        // Now we fetch the ipc stream object and construct the RecordBatchReader
        const result = await conn.arrowIPCStream('SELECT * FROM range(1001, 2001) tbl(i)');

        // Materialize returned iterator into list of Uint8Arrays containing the ipc stream
        let ipc_buffers = await result.toArray();

        // Now to scan the buffer, we first need to register it
        db.register_buffer("ipc_table", ipc_buffers);

        // Delete JS reference to arrays
        ipc_buffers = 0;

        // Run GC to ensure file is deleted
        if (global.gc) {
            global.gc();
        } else {
            throw "should run with --expose-gc";
        }

        // Spray memory overwriting hopefully old buffer
        let spray_results = [];
        for (let i = 0; i < 3000; i++) {
            // Now we fetch the ipc stream object and construct the RecordBatchReader
            const resultinner = await conn.arrowIPCStream('SELECT * FROM range(2001, 3001) tbl(i)');
            // Materialize returned iterator into list of Uint8Arrays containing the ipc stream
            spray_results.push(await resultinner.toArray());
        }

        // Now we can query the ipc buffer using DuckDB by providing an object with an alias and the materialized ipc buffers
        db.all(`SELECT avg(i) as average, count(1) as total FROM ipc_table;`, function(err, result) {
            if (err) {
                throw err;
            }
            assert.deepEqual(result, [{average: 1500.5, total:1000}]);
        });
    });

    it('Simple int column', async () => {
        // Now we fetch the ipc stream object and construct the RecordBatchReader
        const result = await conn.arrowIPCStream('SELECT * FROM range(1001, 2001) tbl(i)');

        // Materialize returned iterator into list of Uint8Arrays containing the ipc stream
        const ipc_buffers = await result.toArray();

        //! NOTE: We can now create an Arrow RecordBatchReader and Table from the materialized stream
        // const reader = await arrow.RecordBatchReader.from(ipc_buffers);
        // const table = arrow.tableFromIPC(reader);
        // console.log(table.toArray());

        // Now to scan the buffer, we first need to register it
        db.register_buffer("ipc_table", ipc_buffers, true);

        // Now we can query the ipc buffer using DuckDB by providing an object with an alias and the materialized ipc buffers
        db.all(`SELECT avg(i) as average, count(1) as total FROM ipc_table;`, function(err, result) {
            if (err) {
                throw err;
            }
            assert.deepEqual(result, [{average: 1500.5, total:1000}]);
        });
    });

    it('Joining 2 IPC-based tables in DuckDB', async () => {
        // Insert first table
        const result1 = await conn.arrowIPCStream('SELECT * FROM range(1001, 2001) tbl(i)');
        const ipc_buffers1 = await result1.toArray();

        // Insert second table
        const result2 = await conn.arrowIPCStream('SELECT * FROM range(1999, 3000) tbl(i)');
        const ipc_buffers2 = await result2.toArray();

        // Register buffers for scanning from DuckDB
        db.register_buffer("table1", ipc_buffers1, true);
        db.register_buffer("table2", ipc_buffers2, true);

        db.all(`SELECT * FROM table1 JOIN table2 ON table1.i=table2.i;`, function(err, result) {
            if (err) {
                throw err;
            }
            assert.deepEqual(result, [{i: 1999}, {i: 2000}]);
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

        conn.run('CREATE TABLE copy_table AS SELECT * FROM test', (err, result) => {
            if (err) throw err;
            done();
        });
    });

    it('DuckDB table -> Stream IPC buffer', async () => {
        let got_batches = 0;
        let got_rows = 0;
        const batches = [];

        const result = await conn.arrowIPCStream('SELECT * FROM test');
        const ipc_buffers = await result.toArray();
        const reader = await arrow.RecordBatchReader.from(ipc_buffers);
        const table = arrow.tableFromIPC(reader);

        assert.equal(table.numRows, column_size);
    });

    it('DuckDB table -> Materialized IPC buffer',  (done) => {
        let got_batches = 0;
        let got_rows = 0;
        const batches = [];

        conn.arrowAll('SELECT * FROM test', (err,res) => {
            done();
        });
    });
});

describe('[Benchmark] TPC-H lineitem.parquet', () => {
    const sql = "SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= CAST('1994-01-01' AS date) AND l_shipdate < CAST('1995-01-01' AS date) AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"
	const parquet_file_path_sf0_01 = "/tmp/lineitem_sf0_01.parquet";
	const parquet_file_path_sf1 = "/tmp/lineitem_sf1.parquet";
    const tpch_answer_sf0_01 = [{revenue: 1193053.2253}];
    const tpch_answer_sf1 = [{revenue: 123141078.2283}];

    // Which query to run
    // const parquet_file_path = parquet_file_path_sf0_01;
    const parquet_file_path = parquet_file_path_sf1;
    // const answer = tpch_answer_sf0_01;
    const answer = tpch_answer_sf1;

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

    it('lineitem.parquet -> DuckDB -> streaming arrow IPC -> query from DuckDB', async () => {
        const startTimeLoad = performance.now()
        const result = await conn.arrowIPCStream('SELECT * FROM "' + parquet_file_path + '"');
        const ipc_buffers = await result.toArray();

        // We can now create a RecordBatchReader & Table from the materialized stream
        const reader = await arrow.RecordBatchReader.from(ipc_buffers);
        const table = arrow.tableFromIPC(reader);
        // console.log("Load time: " + Math.round(performance.now() - startTimeLoad) + "ms");

        const query = sql.replace("lineitem", "my_arrow_ipc_stream");
        const startTimeQuery = performance.now()

        // Register the ipc buffers as table in duckdb
        db.register_buffer("my_arrow_ipc_stream", ipc_buffers);

        await new Promise((resolve, reject) => {
            db.all(query, function (err, result) {
                if (err) {
                    reject(err)
                }

                assert.deepEqual(result, answer);
                resolve();
            })
        });
        // console.log("Query time: " + Math.round(performance.now() - startTimeQuery) + "ms");
    });

    it('lineitem.parquet -> DuckDB -> arrow IPC -> query from DuckDB', async () => {
        const startTimeLoad = performance.now()

        const ipc_buffers = await new Promise((resolve, reject) => {
            conn.arrowAll('SELECT * FROM "' + parquet_file_path + '"', function (err, result) {
                if (err) {
                    reject(err)
                }
                resolve(result)
            });
        });

        // We can now create a RecordBatchReader & Table from the materialized stream
        const reader = await arrow.RecordBatchReader.from(ipc_buffers);
        const table = arrow.tableFromIPC(reader);
        // console.log("Load time: " + Math.round(performance.now() - startTimeLoad) + "ms");

        const query = sql.replace("lineitem", "my_arrow_ipc_stream_2");
        const startTimeQuery = performance.now()

        // Register the ipc buffers as table in duckdb
        db.register_buffer("my_arrow_ipc_stream_2", ipc_buffers);

        await new Promise((resolve, reject) => {
            db.all(query, function (err, result) {
                if (err) {
                    reject(err)
                } else {
                    assert.deepEqual(result, answer);
                    resolve();
                }
            })
        });
        // console.log("Query time: " + Math.round(performance.now() - startTimeQuery) + "ms");
    });

    it('lineitem.parquet -> DuckDB table -> query from DuckDB', async () => {
        const batches = [];
        let got_rows = 0;

        const startTimeLoad = performance.now()
        await new Promise((resolve, reject) => {
            conn.run('CREATE TABLE load_parquet_directly AS SELECT * FROM "' + parquet_file_path + '";', (err) => {
                if (err) {
                    reject(err)
                }
                resolve()
            });
        });
        // console.log("Load time: " + Math.round(performance.now() - startTimeLoad) + "ms");

        const query = sql.replace("lineitem", "load_parquet_directly");
        const startTimeQuery = performance.now()

        const result = await new Promise((resolve, reject) => {
            db.all(query, function (err, result) {
                if (err) {
                    throw err;
                }
                resolve(result)
            });
        });
        // console.log("Query time: " + Math.round(performance.now() - startTimeQuery) + "ms");

        assert.deepEqual(result, answer);
    });
});

describe('Validate with TPCH lineitem SF0.01', () => {
    const parquet_file_path = "/tmp/lineitem_sf0_01.parquet";

    // `table_name` in these queries will be replaced by either the parquet file directly, or the ipc buffer
    const queries = [
        "select count(*) from table_name LIMIT 10",
        "select sum(l_orderkey) as sum_orderkey FROM table_name",
        "select * from table_name LIMIT 10",
        "select l_orderkey from table_name WHERE l_orderkey=2 LIMIT 2",
        "select l_extendedprice from table_name",
        "select l_extendedprice from table_name WHERE l_extendedprice > 53468 and l_extendedprice < 53469  LIMIT 2",
        "select count(l_orderkey) from table_name where l_commitdate > '1996-10-28'",
        "SELECT sum(l_extendedprice * l_discount) AS revenue FROM table_name WHERE l_shipdate >= CAST('1994-01-01' AS date) AND l_shipdate < CAST('1995-01-01' AS date) AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"
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
            const result = await conn.arrowIPCStream('SELECT * FROM "' + parquet_file_path + '"');
            const ipc_buffers = await result.toArray();

            // Now re-run query on Arrow IPC stream
            const reader = await arrow.RecordBatchReader.from(ipc_buffers);

            // Register the ipc buffers as table in duckdb, using force to override the previously registered buffers
            db.register_buffer("table_name", ipc_buffers, true);

            await new Promise((resolve, reject) => {
                db.all(query, function (err, result) {
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

