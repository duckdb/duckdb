var sqlite3 = require('..');
var assert = require('assert');
var fs = require('fs');

describe('exec', function() {
    var db;
    before(function(done) {
        db = new sqlite3.Database(':memory:', done);
    });

    it('Database#exec', function(done) {
        var sql = fs.readFileSync('test/support/script.sql', 'utf8');
        db.exec(sql, done);
    });

    it('retrieve database structure', function(done) {
        db.all("SELECT type, name FROM sqlite_master ORDER BY type, name", function(err, rows) {
            if (err) throw err;
            assert.deepEqual(rows, [
                { type: 'index', name: 'grid_key_lookup' },
                { type: 'index', name: 'grid_utfgrid_lookup' },
                { type: 'index', name: 'images_id' },
                { type: 'index', name: 'keymap_lookup' },
                { type: 'index', name: 'map_index' },
                { type: 'index', name: 'name' },
                { type: 'table', name: 'grid_key' },
                { type: 'table', name: 'grid_utfgrid' },
                { type: 'table', name: 'images' },
                { type: 'table', name: 'keymap' },
                { type: 'table', name: 'map' },
                { type: 'table', name: 'metadata' },
                { type: 'view', name: 'grid_data' },
                { type: 'view', name: 'grids' },
                { type: 'view', name: 'tiles' }
            ]);
            done();
        });
    });
});
