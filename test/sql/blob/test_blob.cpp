#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/prepared_statement.hpp"

#include <fstream>

using namespace duckdb;
using namespace std;

TEST_CASE("Cast BLOB values", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// Test BLOB to VARCHAR -> CastFromBlob
	result = con.Query("SELECT 'a'::BYTEA::VARCHAR");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("\\x61")}));

	// Test VARCHAR to BLOB -> nop cast
	result = con.Query("SELECT 'a'::VARCHAR::BYTEA");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("a")}));

	REQUIRE_FAIL(con.Query("SELECT 1::BYTEA"));
	REQUIRE_FAIL(con.Query("SELECT 1.0::BYTEA"));

    // numeric -> bytea, not valid/implemented casts
	vector<string> types = {"tinyint", "smallint", "integer", "bigint", "decimal"};
	for (auto &type : types) {
		REQUIRE_FAIL(con.Query("SELECT 1::"+ type + "::BYTEA"));
	}
}

TEST_CASE("Insert BLOB values from string", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BYTEA);"));
	// insert BLOB from string
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('aaaaaaaaaa')"));
	// sizes: 22, 202, 20002, 20002 -> double plus two due to hexadecimal representation
	for (idx_t i = 0; i < 3; i++) {
		// The concat function casts BLOB to VARCHAR,resulting in a hex string
		REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs SELECT b||b||b||b||b||b||b||b||b||b FROM blobs "
								  "WHERE LENGTH(b)=(SELECT MAX(LENGTH(b)) FROM blobs)"));
	}

	result = con.Query("SELECT LENGTH(b) FROM blobs ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {10*2 + 2, 100*2 + 2, 1000*2 + 2, 10000*2 + 2}));
}

TEST_CASE("Insert BLOB values", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BYTEA);"));
	// insert BLOB
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('aaaaaaaaaa'::BYTEA)"));
	// insert BLOB with “non-printable” octets
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('\153\154\155 \052\251\124'::BYTEA)"));

	// insert BLOB with “non-printable” octets, but now using VARCHAR string (should fail)
	REQUIRE_FAIL(con.Query("INSERT INTO blobs VALUES ('\153\154\155 \052\251\124'::VARCHAR)"));

	// insert BLOB with “non-printable” octets, but now using string (should fail)
	REQUIRE_FAIL(con.Query("INSERT INTO blobs VALUES ('\153\154\155 \052\251\124')"));
}

TEST_CASE("Select BLOB values", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BYTEA);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('a a'::BYTEA)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('\153\154\155 \052\251\124'::BYTEA)"));

	result = con.Query("SELECT * FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("a a"), Value::BLOB("\153\154\155 \052\251\124")}));

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

TEST_CASE("Test BLOB with PreparedStatement from a file", "[blob]") {
	unique_ptr<QueryResult> result;
	unique_ptr<QueryResult> result_other;
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BYTEA);"));

	string blob_file_path = TestCreatePath("blob_file.txt");
    ofstream ofs_blob_file(blob_file_path, std::ofstream::out | std::ofstream::app);
    // Insert all ASCII chars from 1 to 255, avoiding only the '\0' char
	char ch = '\1';
	for(idx_t i = 0; i < 255; ++i, ++ch) {
    	ofs_blob_file << ch;
	}
	ofs_blob_file.close();

	// DuckDB readme file
	ifstream ifs(blob_file_path, ifstream::binary);
	REQUIRE(ifs.is_open());

	// get pointer to associated buffer object
	filebuf *file_buf = ifs.rdbuf();

	// get file size using buffer's members
	size_t size = file_buf->pubseekoff(0, ifs.end, ifs.in);
	file_buf->pubseekpos (0, ifs.in);

	// allocate memory to contain file data
	unique_ptr<char[]> buffer(new char[size + 1]);

	// get file data
	file_buf->sgetn (buffer.get(), size);

	ifs.close();

	string str_buffer(buffer.get(), size);

	unique_ptr<PreparedStatement> ps = con.Prepare("INSERT INTO blobs VALUES (?::BYTEA)");
	ps->Execute(str_buffer);
	REQUIRE(ps->success);
	ps.reset();

	result = con.Query("SELECT OCTET_LENGTH(b) FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {255}));

	result = con.Query("SELECT count(b) FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	ch = '\1';
	for(idx_t i = 0; i < 255; ++i, ++ch) {
    	buffer[i] = ch;
	}
	string blob_str(buffer.get(), 255);

	result = con.Query("SELECT b FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB(blob_str)}));

	TestDeleteFile(blob_file_path);
}

TEST_CASE("BLOB with Functions", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BYTEA);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('a'::BYTEA)"));

	// conventional concat, (cast BLOB to VARCHAR generating a hex string)
	result = con.Query("SELECT b || 'ZZ'::BYTEA FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("\\x615A5A")}));

	REQUIRE_NO_FAIL(con.Query("SELECT 'abc '::BYTEA || '\153\154\155 \052\251\124'::BYTEA"));
	result = con.Query("SELECT 'abc '::BYTEA || '\153\154\155 \052\251\124'::BYTEA");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("\\x616263206B6C6D202AA954")}));

	// specialized concat BLOB
	result = con.Query("SELECT concat_blob(b, 'ZZ'::BYTEA) FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("aZZ")}));

	result = con.Query("SELECT concat_blob('abc '::BYTEA, '\153\154\155 \052\251\124'::BYTEA)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("abc \153\154\155 \052\251\124")}));


	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('abc \153\154\155 \052\251\124'::BYTEA)"));

	result = con.Query("SELECT COUNT(*) FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	// octet_length
	result = con.Query("SELECT OCTET_LENGTH(b) FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 11}));

	// length
	result = con.Query("SELECT LENGTH(b) FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {1*2 + 2, 11*2 + 2}));
}
