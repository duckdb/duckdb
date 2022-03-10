var duckdb = require('..');
var fs = require('fs');
var path = require('path');

const extension_base_path = "../../../build/release/extension";

describe('Extension loading', function() {
    var db;
    var extension_paths;

    before(function(done) {
        db = new duckdb.Database(':memory:', done);
        const extension_full_path = path.resolve(__dirname, extension_base_path);
        extension_paths = fs.readdirSync(extension_full_path).map(function (file) {

            if (!fs.statSync(extension_full_path+'/'+file).isDirectory())
                return undefined;

            const potential_extension_path = extension_full_path+`/${file}/${file}.duckdb_extension`;
            if (fs.existsSync(potential_extension_path)) {
                return potential_extension_path;
            }
        }).filter(a=>a);
    });

    it('load extensions from ' + extension_base_path, function(done) {
        for (extension_path in extension_paths) {
            db.run(`LOAD ${extension_path};`);
        }
        done();
    });
});
