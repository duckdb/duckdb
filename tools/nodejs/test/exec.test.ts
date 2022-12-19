import * as sqlite3 from '..';
import * as assert from 'assert';
import * as fs from 'fs';
import {TableData} from "..";

describe('exec', function() {
    var db: sqlite3.Database;
    before(function(done) {
        db = new sqlite3.Database(':memory:', done);
    });

    it('Database#exec', function(done) {
        var sql = fs.readFileSync('test/support/script.sql', 'utf8');
        db.exec(sql, function(err: null | Error) {
            if (err) throw err;
            done();
        });
    });

    it('retrieve database structure', function(done) {
        db.all("SELECT type, name FROM sqlite_master ORDER BY type, name", function(err: null | Error, rows: TableData) {
            if (err) throw err;
            assert.deepEqual(rows, [
               // { type: 'index', name: 'grid_key_lookup' },
               // { type: 'index', name: 'grid_utfgrid_lookup' },
               // { type: 'index', name: 'images_id' },
               // { type: 'index', name: 'keymap_lookup' },
              //  { type: 'index', name: 'map_index' },
               // { type: 'index', name: 'name' },
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
