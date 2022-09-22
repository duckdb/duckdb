var duckdb = require('..');
var assert = require('assert');
var arrow = require('apache-arrow')

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

describe('QueryResult', () => {
    const total = 1000;

    let db;
    let conn;
    before((done) => {
        db = new duckdb.Database(':memory:', {"allow_unsigned_extensions":"true"}, () => {
            conn = new duckdb.Connection(db, () => {
                db.run(`LOAD '../../build/debug/extension/arrow/arrow.duckdb_extension';`, function(err) {
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
