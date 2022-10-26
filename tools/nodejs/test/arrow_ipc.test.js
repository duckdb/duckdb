var duckdb = require('..');
var assert = require('assert');
var arrow = require('apache-arrow')
const { performance } = require('perf_hooks');

const build = 'debug';
// const build = 'release';
const extension_path = `../../build/${build}/extension/arrow/arrow.duckdb_extension`;

// Wrapper for tests, materializes whole stream
const arrow_ipc_stream = async (db, sql) => {
    const result_stream = await db.arrowIPCStream(sql);
    return await result_stream.toArray();
}

// Wrapper for tests
const arrow_ipc_materialized = async (db, sql) => {
    return await new Promise((resolve, reject) => {
        db.arrowIPCAll(sql, function (err, result) {
            if (err) {
                reject(err)
            }

            resolve(result);
        })
    });
}

const to_ipc_functions = {
    'streaming': arrow_ipc_stream,
    'materialized': arrow_ipc_materialized,
}

describe(`Arrow IPC Demo`, () => {
    let db;
    let conn;
    before((done) => {
        db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"}, () => {
            conn = new duckdb.Connection(db, () => {
                db.run(`LOAD '${extension_path}';`, function (err) {
                    if (err) {
                        throw err;
                    }
                    done();
                });
            });
        });
    });

    it(`Basic examples`, async () => {
        const query = "SELECT * FROM range(0,3) tbl(i)";
        const arrow_table_expected = new arrow.Table({
            i: new arrow.Vector([arrow.makeData({ type: new arrow.Int32, data: [0, 1, 2] })]),
        });

        // Can use Arrow to read from stream directly
        const result_stream = await db.arrowIPCStream(query);
        const reader = await arrow.RecordBatchReader.from(result_stream);
        const table = await arrow.tableFromIPC(reader);
        const array_from_arrow = table.toArray();
        assert.deepEqual(array_from_arrow, arrow_table_expected.toArray());

        // Can also fully materialize stream first, then pass to Arrow
        const result_stream2 = await db.arrowIPCStream(query);
        const reader2 = await arrow.RecordBatchReader.from(result_stream2.toArray());
        const table2 = await arrow.tableFromIPC(reader2);
        const array_from_arrow2 = table2.toArray();
        assert.deepEqual(array_from_arrow2, arrow_table_expected.toArray());

        // Can also fully materialize in DuckDB first (allowing parallel execution)
        const result_materialized = await new Promise((resolve, reject) => {
            db.arrowIPCAll(query, function (err, result) {
                if (err) {
                    reject(err)
                }

                resolve(result);
            })
        });
        const reader3 = await arrow.RecordBatchReader.from(result_materialized);
        const table3 = await arrow.tableFromIPC(reader3);
        const array_from_arrow3 = table3.toArray();
        assert.deepEqual(array_from_arrow3, arrow_table_expected.toArray());

        // Scanning materialized IPC buffers from DuckDB
        db.register_buffer("ipc_table", result_materialized, true);
        await new Promise((resolve, reject) => {
            db.all(`SELECT * FROM ipc_table`, function (err, result) {
                if (err) {
                    reject(err);
                }

                assert.deepEqual(result, [{i: 0}, { i: 1}, {i: 2}]);
                resolve()
            });
        });
    });
})

for (const [name, fun] of Object.entries(to_ipc_functions)) {
    describe(`DuckDB <-> Arrow IPC (${name})`, () => {
        const total = 1000;

        let db;
        let conn;
        before((done) => {
            db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"}, () => {
                conn = new duckdb.Connection(db, () => {
                    db.run(`LOAD '${extension_path}';`, function (err) {
                        if (err) {
                            throw err;
                        }
                        done();
                    });
                });
            });
        });

        it(`Buffers are not garbage collected`, async () => {
            let ipc_buffers = await fun(db, 'SELECT * FROM range(1001, 2001) tbl(i)');

            // Now to scan the buffer, we first need to register it
            db.register_buffer(`ipc_table_${name}`, ipc_buffers, true);

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
                spray_results.push(await fun(db, 'SELECT * FROM range(2001, 3001) tbl(i)'));
            }

            // Now we can query the ipc buffer using DuckDB by providing an object with an alias and the materialized ipc buffers
            await new Promise((resolve, reject) => {
                db.all(`SELECT avg(i) as average, count(1) as total
                        FROM ipc_table_${name};`, function (err, result) {
                    if (err) {
                        reject(err);
                    }
                    assert.deepEqual(result, [{average: 1500.5, total: 1000}]);
                    resolve();
                });
            });
        });

        it(`Round-trip int column`, async () => {
            // Now we fetch the ipc stream object and construct the RecordBatchReader
            const ipc_buffers = await fun(db, 'SELECT * FROM range(1001, 2001) tbl(i)');

            // Now to scan the buffer, we first need to register it
            db.register_buffer("ipc_table", ipc_buffers, true);

            // Now we can query the ipc buffer using DuckDB by providing an object with an alias and the materialized ipc buffers
            await new Promise((resolve, reject) => {
                db.all(`SELECT avg(i) as average, count(1) as total
                        FROM ipc_table;`, function (err, result) {
                    if (err) {
                        reject(err)
                    }
                    assert.deepEqual(result, [{average: 1500.5, total: 1000}]);
                    resolve();
                });
            });
        });


        it(`Joining 2 IPC buffers in DuckDB`, async () => {
            // Insert first table
            const ipc_buffers1 = await fun(db, 'SELECT * FROM range(1, 3) tbl(i)');

            // Insert second table
            const ipc_buffers2 = await fun(db, 'SELECT * FROM range(2, 4) tbl(i)');

            // Register buffers for scanning from DuckDB
            db.register_buffer("table1", ipc_buffers1, true);
            db.register_buffer("table2", ipc_buffers2, true);

            await new Promise((resolve, reject) => {
                db.all(`SELECT *
                        FROM table1
                                 JOIN table2 ON table1.i = table2.i;`, function (err, result) {
                    if (err) {
                        reject(err);
                    }
                    assert.deepEqual(result, [{i: 2}]);
                    resolve()
                });
            });
        });
    })
}

describe('[Benchmark] Arrow IPC Single Int Column (50M tuples)',() => {
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
        conn.run('CREATE TABLE copy_table AS SELECT * FROM test', (err) => {
            if (err) throw err;
            done();
        });
    });

    it('DuckDB table -> Stream IPC buffer', async () => {
        const result = await conn.arrowIPCStream('SELECT * FROM test');
        const ipc_buffers = await result.toArray();
        const reader = await arrow.RecordBatchReader.from(ipc_buffers);
        const table = arrow.tableFromIPC(reader);
        assert.equal(table.numRows, column_size);
    });

    it('DuckDB table -> Materialized IPC buffer',  (done) => {
        conn.arrowIPCAll('SELECT * FROM test', (err,res) => {
            done();
        });
    });
});

describe('[Benchmark] Arrow IPC TPC-H lineitem.parquet', () => {
    const sql = "SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= CAST('1994-01-01' AS date) AND l_shipdate < CAST('1995-01-01' AS date) AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"
	const parquet_file_path = "/tmp/lineitem_sf1.parquet";
    const answer = [{revenue: 123141078.2283}];

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

    it('Parquet -> DuckDB Streaming-> Arrow IPC -> DuckDB Query', async () => {
        const ipc_buffers = await arrow_ipc_stream(db, 'SELECT * FROM "' + parquet_file_path + '"');

        const query = sql.replace("lineitem", "my_arrow_ipc_stream");
        db.register_buffer("my_arrow_ipc_stream", ipc_buffers, true);

        await new Promise((resolve, reject) => {
            db.all(query, function (err, result) {
                if (err) {
                    reject(err)
                }

                assert.deepEqual(result, answer);
                resolve();
            })
        });
    });

    it('Parquet -> DuckDB Materialized -> Arrow IPC -> DuckDB' , async () => {
        const ipc_buffers = await arrow_ipc_materialized(db, 'SELECT * FROM "' + parquet_file_path + '"');

        const query = sql.replace("lineitem", "my_arrow_ipc_stream_2");
        db.register_buffer("my_arrow_ipc_stream_2", ipc_buffers, true);

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
    });

    it('Parquet -> DuckDB', async () => {
        await new Promise((resolve, reject) => {
            conn.run('CREATE TABLE load_parquet_directly AS SELECT * FROM "' + parquet_file_path + '";', (err) => {
                if (err) {
                    reject(err)
                }
                resolve()
            });
        });

        const query = sql.replace("lineitem", "load_parquet_directly");

        const result = await new Promise((resolve, reject) => {
            db.all(query, function (err, result) {
                if (err) {
                    throw err;
                }
                resolve(result)
            });
        });

        assert.deepEqual(result, answer);
    });
});

for (const [name, fun] of Object.entries(to_ipc_functions)) {
    describe(`Arrow IPC TPC-H lineitem SF0.01 (${name})`, () => {
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
            db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"}, () => {
                conn = new duckdb.Connection(db, () => {
                    db.run(`LOAD '${extension_path}';`, function (err) {
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
                // First do query directly on parquet file
                const expected_value = await new Promise((resolve, reject) => {
                    db.all(query.replace("table_name", `'${parquet_file_path}'`), function (err, result) {
                        if (err) {
                            reject(err);
                        }

                        resolve(result);
                    });
                });

                // Copy parquet file completely into Arrow IPC format
                const ipc_buffers = await fun(db, 'SELECT * FROM "' + parquet_file_path + '"');

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
}

