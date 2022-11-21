var duckdb = require('..');
var assert = require('assert');
var fs = require('fs');

describe('arrow IPC API fails neatly when extension not loaded', function() {
    // Note: arrow IPC api requires the arrow extension to be loaded. The tests for this functionality reside in:
    //       https://github.com/duckdblabs/arrow
    let db;
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
                assert(err.message.includes("Catalog Error: Function with name to_arrow_ipc is not on the catalog, but it exists in the arrow extension. To Install and Load the extension, run: INSTALL arrow; LOAD arrow;"))
            }
        );

        db.arrowIPCAll(`SELECT * FROM ipc_table`, function (err, result) {
            if (err) {
                assert(err.message.includes("Catalog Error: Function with name to_arrow_ipc is not on the catalog, but it exists in the arrow extension. To Install and Load the extension, run: INSTALL arrow; LOAD arrow;"))
            } else {
                assert.fail("Expected error");
            }
        });

        assert.throws(() => db.register_buffer("ipc_table", [1,'a',1], true), TypeError, "Incorrect parameters");
    });

    it('register buffer should be disabled currently', function(done) {
        db.register_buffer("test", [new Uint8Array(new ArrayBuffer(10))], true, (err) => {
            assert(err)
            assert(err.includes("Function with name scan_arrow_ipc is not on the catalog, but it exists in the arrow extension. To Install and Load the extension, run: INSTALL arrow; LOAD arrow;"));
            done()
        });
    });

    it('unregister will silently do nothing', function(done) {
        db.unregister_buffer("test", (err) => {
            assert(!err)
            done()
        });
    });
});
