var duckdb = require('..');
const assert = require('node:assert/strict');

describe('udf non-inlined string issue', function() {
	it('0ary string > 12 characters', function(done) {

		db = new duckdb.Database(':memory:');
		var con = db.connect();

		// Works if string is 12 characters or less
		const str = '012345678912345';

		con.register_udf("udf", "varchar", () => str);
		con.all("select udf() v", function(err, rows) {
			if (err) throw err;
			assert.equal(rows[0].v, str);
			done();
		});
	});
})
