import * as sqlite3 from '..';
import * as helper from './support/helper';

describe('parallel', function() {
    var db: sqlite3.Database;
    before(function(done) {
        helper.deleteFile('test/tmp/test_parallel_inserts.db');
        helper.ensureExists('test/tmp');
        db = new sqlite3.Database('test/tmp/test_parallel_inserts.db', done);
    });

    var columns: string[] = [];
    for (var i = 0; i < 128; i++) {
        columns.push('id' + i + " INTEGER");
    }

    it('should create the table', function(done) {
        db.run("CREATE TABLE foo (" + columns + ")", function(err: null | Error) {
            done();
        });
    });

    it('should insert in parallel', function(done) {
        for (var i = 0; i < 1000; i++) {
            for (var values = [], j = 0; j < columns.length; j++) {
                values.push(i * j);
            }
            db.run("INSERT INTO foo VALUES (" + values + ")");
        }

        db.wait(done);
    });

    it('should close the database', function(done) {
        db.close(done);
    });

    it('should verify that the database exists', function() {
        helper.fileExists('test/tmp/test_parallel_inserts.db');
    });

    after(function() {
        helper.deleteFile('test/tmp/test_parallel_inserts.db');
    });
});
