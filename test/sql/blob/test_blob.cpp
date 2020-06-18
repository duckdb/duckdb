#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/prepared_statement.hpp"

#include <fstream>

using namespace duckdb;
using namespace std;

TEST_CASE("BLOB null and empty values", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	result = con.Query("SELECT ''::BLOB");
	REQUIRE(CHECK_COLUMN(result, 0, {""}));

	result = con.Query("SELECT NULL::BLOB");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr)}));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BYTEA);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES(''), (''::BLOB)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES(NULL), (NULL::BLOB)"));

	result = con.Query("SELECT * FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {"", "", Value(nullptr), Value(nullptr)}));
}

TEST_CASE("Test BLOBs with persistent storage", "[blob]") {
	auto config = GetTestConfig();
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("blob_storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BLOB);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES('a'), ('\\xAA'), ('\\xAAFFAA'),  (''),"
								  "(NULL), ('55AAFF55AAFF55AAFF01'), ('\\x55AAFF55AAFF55AAFF01'),"
								  "('abc \153\154\155 \052\251\124'::BLOB)"));
	}
	// reload the database from disk a few times
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT * FROM blobs");
		REQUIRE(CHECK_COLUMN(result, 0, {"a", Value::BLOB("\\xAA"), Value::BLOB("\\xAAFFAA"), (""),
							Value(nullptr), ("55AAFF55AAFF55AAFF01"), Value::BLOB("\\x55AAFF55AAFF55AAFF01"),
							Value::BLOB("abc \153\154\155 \052\251\124") }));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Cast BLOB values", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// BLOB to VARCHAR -> CastFromBlob, it always results in a hex representation
	result = con.Query("SELECT 'a'::BYTEA::VARCHAR");
	REQUIRE(CHECK_COLUMN(result, 0, {Value("\\x61")}));

	// VARCHAR to BLOB -> CastToBlob
	result = con.Query("SELECT 'a'::VARCHAR::BYTEA");
	REQUIRE(CHECK_COLUMN(result, 0, {"a"}));

	// Hex string with BLOB
	result = con.Query("SELECT '\\xAAFFAAAAFFAAAAFFAA'::BYTEA");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("\\xAAFFAAAAFFAAAAFFAA")}));

	// CastFromBlob with hex string
	result = con.Query("SELECT '\\xAAFFAAAAFFAAAAFFAA'::BLOB::VARCHAR");
	REQUIRE(CHECK_COLUMN(result, 0, {Value("\\xAAFFAAAAFFAAAAFFAA")}));

	// CastFromBlob and after CastToBlob with hex string
	result = con.Query("SELECT '\\xAAFFAAAAFFAAAAFFAA'::BLOB::VARCHAR::BLOB");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("\\xAAFFAAAAFFAAAAFFAA")}));

	// CastFromBlob -> CastToBlob -> CastFromBlob with hex string
	result = con.Query("SELECT '\\xAAFFAAAAFFAAAAFFAA'::BLOB::VARCHAR::BLOB::VARCHAR");
	REQUIRE(CHECK_COLUMN(result, 0, {Value("\\xAAFFAAAAFFAAAAFFAA")}));

	// CastToBlob -> CastFromBlob -> CastToBlob with hex string
	result = con.Query("SELECT '\\xAAFFAAAAFFAAAAFFAA'::VARCHAR::BLOB::VARCHAR::BLOB");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("\\xAAFFAAAAFFAAAAFFAA")}));

	REQUIRE_FAIL(con.Query("SELECT 1::BYTEA"));
	REQUIRE_FAIL(con.Query("SELECT 1.0::BYTEA"));

    // numeric -> bytea, not valid/implemented casts
	vector<string> types = {"tinyint", "smallint", "integer", "bigint", "decimal"};
	for (auto &type : types) {
		REQUIRE_FAIL(con.Query("SELECT 1::"+ type + "::BYTEA"));
	}
}

TEST_CASE("Insert BLOB values from normal strings", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BYTEA);"));
	// insert BLOB from string
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('aaaaaaaaaa')"));
	// sizes: 10, 100, 1000, 10000 -> double plus two due to hexadecimal representation
	for (idx_t i = 0; i < 3; i++) {
		// The concat function casts BLOB to VARCHAR,resulting in a hex string
		REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs SELECT b||b||b||b||b||b||b||b||b||b FROM blobs "
								  "WHERE OCTET_LENGTH(b)=(SELECT MAX(OCTET_LENGTH(b)) FROM blobs)"));
	}

	result = con.Query("SELECT OCTET_LENGTH(b) FROM blobs ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {10, 100, 1000, 10000}));
}

TEST_CASE("Insert BLOB values from hex strings and others", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BYTEA);"));

	// Insert valid hex strings
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES('\\xAAFFAA'), ('\\xAAFFAAAAFFAA'), ('\\xAAFFAAAAFFAAAAFFAA')"));
	result = con.Query("SELECT * FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("\\xAAFFAA"), Value::BLOB("\\xAAFFAAAAFFAA"), Value::BLOB("\\xAAFFAAAAFFAAAAFFAA")}));

	// Insert valid hex strings, lower case
	REQUIRE_NO_FAIL(con.Query("DELETE FROM blobs"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES('\\xaaffaa'), ('\\xaaffaaaaffaa'), ('\\xaaffaaaaffaaaaffaa')"));
	result = con.Query("SELECT * FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("\\xaaffaa"), Value::BLOB("\\xaaffaaaaffaa"), Value::BLOB("\\xaaffaaaaffaaaaffaa")}));

	// Insert valid hex strings with number and letters
	REQUIRE_NO_FAIL(con.Query("DELETE FROM blobs"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES('\\xaa1199'), ('\\xaa1199aa1199'), ('\\xaa1199aa1199aa1199')"));
	result = con.Query("SELECT * FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("\\xaa1199"), Value::BLOB("\\xaa1199aa1199"), Value::BLOB("\\xaa1199aa1199aa1199")}));

	// Insert INvalid hex strings (invalid hex chars: G, H, I)
	REQUIRE_FAIL(con.Query("INSERT INTO blobs VALUES('\\xGAFFAA'), ('\\xHAFFAAAAFFAA'), ('\\xIAFFAAAAFFAAAAFFAA')"));

	// Insert INvalid hex strings (odd # of chars)
	REQUIRE_FAIL(con.Query("INSERT INTO blobs VALUES('\\xAAAFFAA'), ('\\xAAAFFAAAAFFAA'), ('\\xAAAFFAAAAFFAAAAFFAA')"));

	// insert BLOB with “non-printable” octets
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('\153\154\155 \052\251\124'::BYTEA)"));

	// insert BLOB with “non-printable” octets, but now using VARCHAR string (should fail)
	REQUIRE_FAIL(con.Query("INSERT INTO blobs VALUES ('\153\154\155 \052\251\124'::VARCHAR)"));
	REQUIRE_FAIL(con.Query("INSERT INTO blobs VALUES ('\153\154\155 \052\251\124')"));

	// insert BLOB with “non-printable” octets, but now using string
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('\153\154\155 \052\251\124'::BLOB)"));
}

TEST_CASE("Select BLOB values", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BYTEA);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('\\xFF00AA'), ('a a'::BYTEA)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('\153\154\155 \052\251\124'::BYTEA)"));

	result = con.Query("SELECT * FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("\\xFF00AA"), Value::BLOB("a a"), Value::BLOB("\153\154\155 \052\251\124")}));

	//BLOB with “non-printable” octets
	REQUIRE_NO_FAIL(con.Query("SELECT 'abc \201'::BYTEA;"));
	result = con.Query("SELECT 'abc \201'::BYTEA;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("abc \201")}));

	REQUIRE_NO_FAIL(con.Query("SELECT 'abc \153\154\155 \052\251\124'::BYTEA;"));
	result = con.Query("SELECT 'abc \153\154\155 \052\251\124'::BYTEA;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("abc \153\154\155 \052\251\124")}));

	//now VARCHAR with “non-printable” octets, should fail
	REQUIRE_FAIL(con.Query("SELECT 'abc \201'::VARCHAR;"));
	REQUIRE_FAIL(con.Query("SELECT 'abc \153\154\155 \052\251\124'::VARCHAR;"));
}

TEST_CASE("Test BLOB with COPY INTO", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// Creating a blob buffer with almost ALL ASCII chars
	uint8_t num_chars = 256 - 5; // skipping: '\0', '\n', '\15', ',', '\32'
	unique_ptr<char[]> blob_chars(new char[num_chars + 1]);
	char ch = '\0';
	idx_t buf_idx = 0;
	for(idx_t i = 0; i < 255; ++i, ++ch) {
		// skip chars: '\0', new line, shift in, comma, and crtl+Z
		if(ch == '\0' || ch == '\n' || ch == '\15' || ch == ',' || ch == '\32') {
			continue;
		}
		blob_chars[buf_idx] = ch;
    	++buf_idx;
	}
	blob_chars[num_chars] = '\0';

	// Wrinting BLOB values to a csv file
	string blob_file_path = TestCreatePath("blob_file.csv");
    ofstream ofs_blob_file(blob_file_path, std::ofstream::out | std::ofstream::app);
    // Insert all ASCII chars from 1 to 255, skipping '\0', '\n', '\15', and ',' chars
   	ofs_blob_file << blob_chars.get();
	ofs_blob_file.close();

	// COPY INTO
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BYTEA);"));
    result = con.Query("COPY blobs FROM '" + blob_file_path + "';");
    REQUIRE(CHECK_COLUMN(result, 0, {1}));

    // Testing if the system load/store correctly the bytes
	string blob_str(blob_chars.get(), num_chars);
	result = con.Query("SELECT b FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB(blob_str)}));

	blob_chars.reset();
	TestDeleteFile(blob_file_path);
}

TEST_CASE("Test BLOB with PreparedStatement from a file", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// Creating a blob buffer with almost ALL ASCII chars
	uint8_t num_chars = 256 - 5; // skipping: '\0', '\n', '\15', ',', '\32'
	unique_ptr<char[]> blob_chars(new char[num_chars]);
	char ch = '\0';
	idx_t buf_idx = 0;
	for(idx_t i = 0; i < 255; ++i, ++ch) {
		// skip chars: '\0', new line, shift in, comma, and crtl+Z
		if(ch == '\0' || ch == '\n' || ch == '\15' || ch == ',' || ch == '\32') {
			continue;
		}
		blob_chars[buf_idx] = ch;
    	++buf_idx;
	}

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BYTEA);"));

	// Insert blob values through a PreparedStatement
	string str_blob(blob_chars.get(), num_chars);
	unique_ptr<PreparedStatement> ps = con.Prepare("INSERT INTO blobs VALUES (?::BYTEA)");
	ps->Execute(str_blob);
	REQUIRE(ps->success);
	ps.reset();

	// Testing if the bytes are stored correctly
	result = con.Query("SELECT OCTET_LENGTH(b) FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {num_chars}));

	result = con.Query("SELECT count(b) FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("SELECT b FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB(str_blob)}));

	blob_chars.reset();
}

TEST_CASE("BLOB with Functions", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BYTEA);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('a'::BYTEA)"));

	// conventional concat
	result = con.Query("SELECT b || 'ZZ'::BYTEA FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("aZZ")}));

	REQUIRE_NO_FAIL(con.Query("SELECT 'abc '::BYTEA || '\153\154\155 \052\251\124'::BYTEA"));
	result = con.Query("SELECT 'abc '::BYTEA || '\153\154\155 \052\251\124'::BYTEA");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("abc \153\154\155 \052\251\124")}));

	result = con.Query("SELECT 'abc '::BYTEA || '\153\154\155 \052\251\124'::BYTEA");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("abc \153\154\155 \052\251\124")}));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('abc \153\154\155 \052\251\124'::BYTEA)"));

	result = con.Query("SELECT COUNT(*) FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	// octet_length
	result = con.Query("SELECT OCTET_LENGTH(b) FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 11}));

	// HEX strings
	REQUIRE_NO_FAIL(con.Query("DELETE FROM blobs"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('\\xFF'::BYTEA)"));

	result = con.Query("SELECT b || 'ZZ'::BYTEA FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("\\xFF5A5A")}));

	result = con.Query("SELECT b || '\\x5A5A'::BYTEA FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("\\xFF5A5A")}));

	// BLOB || VARCHAR is not allowed, should fail
	REQUIRE_FAIL(con.Query("SELECT b || '5A5A'::VARCHAR FROM blobs"));

	// Octet Length tests
	REQUIRE_NO_FAIL(con.Query("DELETE FROM blobs"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('\\xFF'::BYTEA)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('FF'::BYTEA)"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('\\x55AAFF55AAFF55AAFF01'::BYTEA)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('55AAFF55AAFF55AAFF01'::BYTEA)"));

	result = con.Query("SELECT OCTET_LENGTH(b) FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 10, 20}));
}

TEST_CASE("Test BLOBs with various SQL operators", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BYTEA, g INTEGER);"));
    // strings: hello -> \x68656C6C6F, r -> \x72
    REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('hello', 0), ('\\xAAFFAA', 1), (NULL, 0), ('r', 1)"));

    // simple aggregates only
    result = con.Query("SELECT COUNT(*), COUNT(b), MIN(b), MAX(b) FROM blobs");
    REQUIRE(CHECK_COLUMN(result, 0, {4}));
    REQUIRE(CHECK_COLUMN(result, 1, {3}));
    REQUIRE(CHECK_COLUMN(result, 2, {"hello"}));
    REQUIRE(CHECK_COLUMN(result, 3, {Value::BLOB("\\xAAFFAA")}));

    // ORDER BY
    result = con.Query("SELECT * FROM blobs ORDER BY b");
    REQUIRE(CHECK_COLUMN(result, 0, {Value(nullptr), "hello", "r", Value::BLOB("\\xAAFFAA")}));

    // GROUP BY
    REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('hello', 3), ('\\xAAFFAA', 9), (NULL, 0), ('r', 19)"));
    result = con.Query("SELECT SUM(g) FROM blobs GROUP BY b ORDER BY b");
    REQUIRE(CHECK_COLUMN(result, 0, {Value(0.0), Value(3.0), Value(20.0), Value(10.0)}));

    // JOIN
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs2 (b BYTEA, g INTEGER);"));
    REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs2 VALUES ('hello', 0), ('\\xAAFFAA', 100), (NULL, 0), ('r', 200)"));

    // group by blobs.b, explicit JOIN
    result = con.Query("SELECT L.b, SUM(L.g) FROM blobs as L JOIN blobs2 AS R ON L.b=R.b GROUP BY L.b ORDER BY L.b");
    REQUIRE(CHECK_COLUMN(result, 0, {"hello", 	"r", 		  Value::BLOB("\\xAAFFAA")}));
    REQUIRE(CHECK_COLUMN(result, 1, {Value(3.0), Value(20.0), Value(10.0)}));

    // group by blobs2.b, implicit JOIN
    result = con.Query("SELECT R.b, SUM(R.g) FROM blobs as L, blobs2 AS R WHERE L.b=R.b GROUP BY R.b ORDER BY R.b");
    REQUIRE(CHECK_COLUMN(result, 0, {"hello", 	"r", 		   Value::BLOB("\\xAAFFAA")}));
    REQUIRE(CHECK_COLUMN(result, 1, {Value(0.0), Value(400.0), Value(200.0)}));
}
