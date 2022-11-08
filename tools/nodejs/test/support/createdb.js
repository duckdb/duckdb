#!/usr/bin/env node

function createdb(callback) {
    var existsSync = require('fs').existsSync || require('path').existsSync;
    var path = require('path');

    var sqlite3 = require('../../lib/duckdb');

    var count = 1000000;
    var db_path = path.join(__dirname,'big.db');

    function randomString() {
        var str = '';
        var chars = 'abcdefghijklmnopqrstuvwxzyABCDEFGHIJKLMNOPQRSTUVWXZY0123456789  ';
        for (var i = Math.random() * 100; i > 0; i--) {
            str += chars[Math.floor(Math.random() * chars.length)];
        }
        return str;
    };


    if (existsSync(db_path)) {
        console.log('okay: database already created (' + db_path + ')');
        if (callback) callback();
    } else {
        console.log("Creating test database... This may take several minutes.");
        var db = new sqlite3.Database(db_path, () => {
            db.serialize(function() {
                db.run("CREATE TABLE foo (id INT, txt TEXT)");
                db.run("BEGIN TRANSACTION");
                var stmt = db.prepare("INSERT INTO foo VALUES(?, ?)");
                for (var i = 0; i < count; i++) {
                    stmt.run(i, randomString());
                }

                stmt.finalize();
                db.run("COMMIT TRANSACTION", function () {
                    db.close(callback);
                });
            });
        });
    }
};

if (require.main === module) {
    createdb();
}

module.exports = createdb;
