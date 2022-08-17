var duckdb = require('..');
var fs = require('fs');
var assert = require('assert');
var path = require('path');

const extension_base_path = "../../../build/release/extension";

// Look for extensions that we can load and test
let extension_paths = [];
const extension_full_path = path.resolve(__dirname, extension_base_path);
if (fs.existsSync(extension_full_path)) {
    extension_paths = extension_paths.concat(fs.readdirSync(extension_full_path).map(function (file) {
        if (!fs.statSync(extension_full_path+'/'+file).isDirectory())
            return undefined;
        const potential_extension_path = extension_full_path+`/${file}/${file}.duckdb_extension`;
        if (fs.existsSync(potential_extension_path)) {
            return potential_extension_path;
        }
    }).filter(a=>a));
}

// Note: test will pass on http request failing due to connection issues.
const test_httpfs = async function (db, done) {
    db.all("SELECT id, first_name, last_name FROM PARQUET_SCAN('https://raw.githubusercontent.com/cwida/duckdb/master/data/parquet-testing/userdata1.parquet') LIMIT 3;", function(err, rows) {
        if (err) {
            if (err.message.startsWith("Unable to connect to URL")) {
                console.warn("Warning: HTTP request failed in extension.test.js");
                done();
            } else {
                throw err;
            }
        } else {
            assert.deepEqual(rows, [
                { id: 1, first_name: 'Amanda', last_name: 'Jordan'},
                { id: 2, first_name: 'Albert', last_name: 'Freeman'},
                { id: 3, first_name: 'Evelyn', last_name: 'Morgan'},
            ]);
            done();
        }
    });
};

const test_tpch = async function (db, done) {
    db.all("CALL DBGEN(sf=0.01);", function(err) {
        if (err) {
            throw err;
        }
        done();
    });
};

const test_extension = function(extension_name, db, done) {
    switch(extension_name) {
        case 'httpfs.duckdb_extension':
            test_httpfs(db, done);
            break;
        case 'tpch.duckdb_extension':
            test_tpch(db, done);
            break;
        default:
            done();
            break;
    }
};

describe('Extension loading', function() {
    var db;

    before(function(done) {
        db = new duckdb.Database(':memory:', {"allow_unsigned_extensions":"true"}, done);
    });

    for (ext of extension_paths) {
        const extension_path = ext;
        const extension_name = ext.replace(/^.*[\\\/]/, '');

        it(extension_name, function(done) {
            db.run(`LOAD '${extension_path}';`, function(err) {
                if (err) {
                    throw err;
                }
                test_extension(extension_name, db, done);
            });
        });
    }
});
