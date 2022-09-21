var duckdb = require('..');
var assert = require('assert');
var arrow = require('apache-arrow')

//
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
}


describe('QueryResult', () => {
    const total = 1000;

    let db;
    let conn;
    before((done) => {
        db = new duckdb.Database(':memory:', {"allow_unsigned_extensions":"true"}, () => {
            conn = new duckdb.Connection(db, done);
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

    it('streams ipc buffers', async () => {

        // First load the arrow extension
        db.run(`LOAD '../../build/debug/extension/arrow/arrow.duckdb_extension';`, function(err) {
            if (err) {
                throw err;
            }
        });

        // Now we fetch the ipc stream object and construct the RecordBatchReader
        const result = await conn.arrowStream2('SELECT * FROM range(0, 5000) tbl(i)');
        const it = new IpcResultStreamIterator(result);
        const reader = await arrow.RecordBatchReader.from(it);
        const table = arrow.tableFromIPC(reader);

        // We now have an Arrow table
        console.log((await table).toArray());

        // Now we need to scan this thing with duckdb again

    });
})
