import * as sqlite3 from '..';
import {TableData} from "..";
import * as assert from 'assert';

describe('unicode', function() {
    let i;
    let first_values: number[] = [],
      trailing_values: number[] = [],
      subranges = new Array(2),
      len = subranges.length,
      db: sqlite3.Database;

    before(function(done) { db = new sqlite3.Database(':memory:', done); });

    for (i = 0x20; i < 0x80; i++) {
        first_values.push(i);
    }

    for (i = 0xc2; i < 0xf0; i++) {
        first_values.push(i);
    }

    for (i = 0x80; i < 0xc0; i++) {
        trailing_values.push(i);
    }

    for (i = 0; i < len; i++) {
        subranges[i] = [];
    }

    for (i = 0xa0; i < 0xc0; i++) {
        subranges[0].push(i);
    }

    for (i = 0x80; i < 0xa0; i++) {
        subranges[1].push(i);
    }

    function random_choice(arr: number[]) {
        return arr[Math.random() * arr.length | 0];
    }

    function random_utf8() {
        const first = random_choice(first_values);

        if (first < 0x80) {
            return String.fromCharCode(first);
        } else if (first < 0xe0) {
            return String.fromCharCode((first & 0x1f) << 0x6 | random_choice(trailing_values) & 0x3f);
        } else if (first == 0xe0) {
             return String.fromCharCode(((first & 0xf) << 0xc) | ((random_choice(subranges[0]) & 0x3f) << 6) | random_choice(trailing_values) & 0x3f);
        } else if (first == 0xed) {
            return String.fromCharCode(((first & 0xf) << 0xc) | ((random_choice(subranges[1]) & 0x3f) << 6) | random_choice(trailing_values) & 0x3f);
        } else if (first < 0xf0) {
            return String.fromCharCode(((first & 0xf) << 0xc) | ((random_choice(trailing_values) & 0x3f) << 6) | random_choice(trailing_values) & 0x3f);
        }
    }

    function randomString() {
        let str = '',
          i;

        for (i = Math.random() * 300; i > 0; i--) {
            str += random_utf8();
        }

        return str;
    }


        // Generate random data.
    const data: string[] = [];
    const length = Math.floor(Math.random() * 1000) + 200;
    for (i = 0; i < length; i++) {
        data.push(randomString());
    }

    let inserted = 0;
    let retrieved = 0;

    it('should create the table', function(done) {
        db.run("CREATE TABLE foo (id int, txt text)", done);
    });

    it('should insert all values', function(done) {
        const stmt = db.prepare("INSERT INTO foo VALUES(?, ?)");
        for (let i = 0; i < data.length; i++) {
            stmt.run(i, data[i], function(err: null | Error) {
                if (err) throw err;
                inserted++;
            });
        }
        stmt.finalize(done);
    });

    it('should retrieve all values', function(done) {
        db.all("SELECT txt FROM foo ORDER BY id", function(err: null | Error, rows: TableData) {
            if (err) throw err;

            for (let i = 0; i < rows.length; i++) {
                assert.equal(rows[i].txt, data[i]);
                retrieved++;
            }
            done();
        });
    });

    it('should have inserted and retrieved the correct amount', function() {
        assert.equal(inserted, length);
        assert.equal(retrieved, length);
    });

    after(function(done) { db.close(done); });
});
