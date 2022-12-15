import * as sqlite3 from '..';

describe('can query parquet', function() {
    var db: sqlite3.Database;

    before(function(done) {
        db = new sqlite3.Database(':memory:', done);
    });

    it('should be able to read parquet files', function(done) {
        db.run("select * from parquet_scan('../../data/parquet-testing/userdata1.parquet')", done);
    });

});
