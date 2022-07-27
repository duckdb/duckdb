var duckdb = require('..');
var assert = require('assert');

describe('QueryResult', () => {
    const total = 1000;

    let db;
    let conn;
    before((done) => {
        db = new duckdb.Database(':memory:', () => {
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
})
