import * as duckdb from '..';
import {Database, TableData} from '..';
import * as fs from 'fs';
import * as assert from 'assert';
import * as path from 'path';
import {Done} from "mocha";

const extension_base_path = "../../../build/release/extension";

// Look for extensions that we can load and test
let extension_paths: string[] = [];
const extension_full_path = path.resolve(__dirname, extension_base_path);
if (fs.existsSync(extension_full_path)) {
    extension_paths = fs.readdirSync(extension_full_path).map(function (file) {
        if (!fs.statSync(extension_full_path + '/' + file).isDirectory())
            return undefined;
        const potential_extension_path = extension_full_path + `/${file}/${file}.duckdb_extension`;
        if (fs.existsSync(potential_extension_path)) {
            return potential_extension_path;
        }
    }).filter(a => a) as string[];
}

// Note: test will pass on http request failing due to connection issues.
const test_httpfs = async function (db: duckdb.Database, done: Done) {
    db.all("SELECT id, first_name, last_name FROM PARQUET_SCAN('https://raw.githubusercontent.com/cwida/duckdb/master/data/parquet-testing/userdata1.parquet') LIMIT 3;", function(err: null | Error, rows: TableData) {
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

const test_tpch = async function (db: Database, done:Done) {
    db.all("CALL DBGEN(sf=0.01);", function(err: null | Error) {
        if (err) {
            throw err;
        }
        done();
    });
};

const test_extension = function(extension_name: string, db: duckdb.Database, done: Done) {
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
    var db: Database;

    before(function(done) {
        db = new duckdb.Database(':memory:', {"allow_unsigned_extensions":"true"}, done);
    });

    for (let extension_path of extension_paths) {
        const extension_name = extension_path.replace(/^.*[\\\/]/, '');

        if (extension_name.startsWith('parquet')) { // Parquet is built-in in the Node client, so skip
            continue;
        }

        it(extension_name, function(done) {
            db.run(`LOAD '${extension_path}';`, function(err: null | Error) {
                if (err) {
                    throw err;
                }
                test_extension(extension_name, db, done);
            });
        });
    }
});
