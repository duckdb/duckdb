import * as duckdb from '..';
import {Database, DuckDbError, HttpError, TableData} from '..';
import * as fs from 'fs';
import * as assert from 'assert';
import * as path from 'path';
import chaiAsPromised from 'chai-as-promised';
import chai, {expect} from "chai";

chai.use(chaiAsPromised);

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

function isHTTPException(err: DuckDbError): err is HttpError {
    return err.errorType === 'HTTP';
}

// Note: test will pass on http request failing due to connection issues.
const test_httpfs = async function (db: duckdb.Database) {
    const promise = new Promise<void>((resolve, reject) =>
        db.exec(`SELECT *
                 FROM parquet_scan('http://localhost:1234/whatever.parquet')`, function (err: DuckDbError | null) {
            err ? reject(err) : resolve()
        }));
    await chai.assert.isRejected(promise, 'IO Error: Connection error for HTTP HEAD');

    await new Promise<void>((resolve, reject) => db.all("SELECT id, first_name, last_name FROM PARQUET_SCAN('https://raw.githubusercontent.com/duckdb/duckdb/main/data/parquet-testing/userdata1.parquet') LIMIT 3;", function (err: null | Error, rows: TableData) {
        if (err) {
            if (err.message.startsWith("Unable to connect to URL")) {
                console.warn("Warning: HTTP request failed in extension.test.js");
                resolve();
            } else {
                reject(err);
            }
        } else {
            assert.deepEqual(rows, [
                {id: 1, first_name: 'Amanda', last_name: 'Jordan'},
                {id: 2, first_name: 'Albert', last_name: 'Freeman'},
                {id: 3, first_name: 'Evelyn', last_name: 'Morgan'},
            ]);
            resolve();
        }
    }));

    await new Promise<void>((resolve) => {
        db.exec("select * from read_csv_auto('https://example.com/hello.csv')", (err: DuckDbError | null) => {
            assert.ok(err);
            assert.ok(isHTTPException(err));
            if (isHTTPException(err)) {
                assert.equal(err.statusCode, 404);
                assert.equal(err.reason, 'Not Found');
                assert.equal(err.response, '');
                assert.ok('Content-Length' in err.headers, JSON.stringify(err.headers));
            }
            resolve();
        });
    })
};

const test_tpch = async function (db: Database) {
    await new Promise<void>((resolve, reject) => db.all("CALL DBGEN(sf=0.01);", function (err: null | Error) {
        if (err) {
            reject(err);
        }
        resolve();
    }));
};

const test_extension = async function (extension_name: string, db: duckdb.Database) {
    switch (extension_name) {
        case 'httpfs.duckdb_extension':
            await test_httpfs(db);
            break;
        case 'tpch.duckdb_extension':
            await test_tpch(db);
            break;
        default:
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

        it(extension_name, async function () {
            await new Promise<void>((resolve, reject) => db.run(`LOAD '${extension_path}';`, function (err: null | Error) {
                if (err) {
                    reject(err);
                }
                resolve()
            }));

            await test_extension(extension_name, db);
        });
    }
});
