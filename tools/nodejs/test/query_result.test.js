var duckdb = require('..');
var assert = require('assert');
var arrow = require('apache-arrow')

const vector_size = 1024
const extension_path = '../../build/release/extension/arrow/arrow.duckdb_extension'

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

describe('[Benchmark] write single int column',() => {
    // Config
    const batch_size = vector_size*100;
    const column_size = 10*1000*1000;

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

    it('DuckDB + ipc conversion (batch_size=' + batch_size + ')', async () => {
        let got_batches = 0;
        let got_rows = 0;
        const batches = [];

        const result = await conn.arrowStream2('SELECT * FROM test;');
        const it = new IpcResultStreamIterator(result);
        const fully_materialized = await it.toArray();
        const reader = await arrow.RecordBatchReader.from(fully_materialized);
        const table = arrow.tableFromIPC(reader);

        assert.equal(table.numRows, column_size);
    });

    it('DuckDB materialize full table in JS', (done) => {
        conn.all('SELECT * FROM test;', (err, res) => {
            assert.equal(res.length, column_size);
            done()
        });
    });
});

describe.only('[Benchmark] TPC-H SF1 lineitem.parquet', () => {
	// Config
	const batch_size = vector_size*100;

	// const expected_rows = 60175;
	// const expected_orderkey_sum = 1802759573;
	// const parquet_file_path = "/tmp/lineitem_sf0_01.parquet";
	const expected_rows = 6001215;
    const expected_orderkey_sum = 18005322964949;
	const parquet_file_path = "/tmp/lineitem_sf1.parquet";

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

	it('Parquet -> DuckDB -> arrow IPC -> Query from DuckDB', async () => {
		const batches = [];
		let got_rows = 0;

		const result = await conn.arrowStream2('SELECT * FROM "' + parquet_file_path + '";');
        const it = new IpcResultStreamIterator(result);
        const fully_materialized = await it.toArray();

        await new Promise((resolve, reject) => {
            db.scanArrowIpc('select sum(l_orderkey) as sum_orderkey FROM _arrow_ipc_stream;', fully_materialized, function (err, result) {
                if (err) {
                    reject(err)
                }
                assert.deepEqual(result, [{sum_orderkey: expected_orderkey_sum}]);
                resolve();
            })
        });
    });

    it('Parquet -> DuckDB in-memory table -> Query from DuckDB', (done) => {
        const batches = [];
        let got_rows = 0;

        conn.run('CREATE TABLE load_parquet_directly AS SELECT * FROM "' + parquet_file_path + '";');
        db.all('select sum(l_orderkey) as sum_orderkey FROM load_parquet_directly;', function(err, result) {
            if (err) {
                throw err;
            }
            assert.deepEqual(result, [{sum_orderkey: expected_orderkey_sum}]);
            done()
        });
    });
});


describe('QueryResult', () => {
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

    it('streams results', async () => {
        let retrieved = 0;
        const stream = conn.stream('SELECT * FROM range(0, ?)', total);
        for await (const row of stream) {
            retrieved++;
        }
        assert.equal(total, retrieved)
    })

    it('round trip ipc buffers', async () => {
        // Now we fetch the ipc stream object and construct the RecordBatchReader
        const result = await conn.arrowStream2('SELECT * FROM range(1001, 2001) tbl(i)');

        // Create iterator from QueryResult, could
        const it = new IpcResultStreamIterator(result);

        // Materialize into list of Uint8Arrays containing the ipc stream
        const fully_materialized = await it.toArray();

        // We can now create a RecordBatchReader & Table from the materialized stream
        const reader = await arrow.RecordBatchReader.from(fully_materialized);
        const table = arrow.tableFromIPC(reader);

        // We now have an Arrow table containing the data
        // console.log((await table).toArray());

        // Now we can query the ipc buffer using DuckDB. It is available as "_arrow_ipc_stream"
        db.scanArrowIpc(`SELECT avg(i) as average, count(1) as total FROM _arrow_ipc_stream;`, fully_materialized, function(err, result) {
            if (err) {
                throw err;
            }
            assert.deepEqual(result, [{average: 1500.5, total:1000}]);
        });
    });
})