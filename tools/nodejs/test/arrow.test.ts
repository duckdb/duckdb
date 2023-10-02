import * as duckdb from '..';
import * as assert from 'assert';
import {ArrowArray} from "..";

describe('arrow IPC API fails neatly when extension not loaded', function() {
    // Note: arrow IPC api requires the arrow extension to be loaded. The tests for this functionality reside in:
    //       https://github.com/duckdblabs/arrow
    let db: duckdb.Database;
    let conn;
    before((done) => {
        db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"}, () => {
            done();
        });
    });

    it(`basic examples`, async () => {
        const range_size = 130000;
        const query = `SELECT * FROM range(0,${range_size}) tbl(i)`;

        db.arrowIPCStream(query).then(
            () => Promise.reject(new Error('Expected method to reject.')),
            err => {
                assert.ok(err.message.includes("arrow"))
            }
        );

        db.arrowIPCAll(`SELECT * FROM ipc_table`, function (err: null | Error, result: ArrowArray) {
            if (err) {
                assert.ok(err.message.includes("arrow"))
            } else {
                assert.fail("Expected error");
            }
        });

        // @ts-expect-error
        assert.throws(() => db.register_buffer("ipc_table", [1,'a',1], true), TypeError, "Incorrect parameters");
    });

    it('register buffer should be disabled currently', function(done) {
        db.register_buffer("test", [new Uint8Array(new ArrayBuffer(10))], true, (err: null | Error) => {
            assert.ok(err)
            assert.ok(err.toString().includes("arrow"));
            done()
        });
    });

    it('unregister will silently do nothing', function(done) {
        db.unregister_buffer("test", (err) => {
            assert.ok(!err)
            done()
        });
    });
});
