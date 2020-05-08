#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/prepared_statement.hpp"

#include <fstream>

using namespace duckdb;
using namespace std;

TEST_CASE("Insert BLOB values from string", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BLOB);"));
	// insert BLOB from string
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('aaaaaaaaaa')"));
	// sizes: 10, 100, 1000, 10000
	for (idx_t i = 0; i < 3; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs SELECT b||b||b||b||b||b||b||b||b||b FROM blobs "
								  "WHERE LENGTH(b)=(SELECT MAX(LENGTH(b)) FROM blobs)"));
	}

	result = con.Query("SELECT LENGTH(b) FROM blobs ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {10, 100, 1000, 10000}));
}

TEST_CASE("Insert BLOB values", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BLOB);"));
	// insert BLOB
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('aaaaaaaaaa'::BLOB)"));
	// insert BLOB with “non-printable” octets
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('\153\154\155 \052\251\124'::BLOB)"));

	// insert BLOB with “non-printable” octets, but now using VARCHAR string (should fail)
	REQUIRE_FAIL(con.Query("INSERT INTO blobs VALUES ('\153\154\155 \052\251\124'::VARCHAR)"));

	// insert BLOB with “non-printable” octets, but now using string (should fail)
	REQUIRE_FAIL(con.Query("INSERT INTO blobs VALUES ('\153\154\155 \052\251\124')"));
}

TEST_CASE("Select BLOB values", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BLOB);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('aaaaaaaaaa'::BLOB)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('\153\154\155 \052\251\124'::BLOB)"));

	result = con.Query("SELECT count(*) FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	//BLOB with “non-printable” octets
	REQUIRE_NO_FAIL(con.Query("SELECT 'abc \201'::BLOB;"));
	REQUIRE_NO_FAIL(con.Query("SELECT 'abc \153\154\155 \052\251\124'::BLOB;"));

	//now VARCHAR with “non-printable” octets, should fail
	REQUIRE_FAIL(con.Query("SELECT 'abc \201'::VARCHAR;"));
	REQUIRE_FAIL(con.Query("SELECT 'abc \153\154\155 \052\251\124'::VARCHAR;"));
}

TEST_CASE("Test BLOB with PreparedStatement from a file", "[blob]") {
	unique_ptr<QueryResult> result;
	unique_ptr<QueryResult> result_other;
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BLOB);"));

	// DuckDB readme file
	ifstream ifs("README.md", ifstream::binary);
	REQUIRE(ifs.is_open());

	// get pointer to associated buffer object
	filebuf *file_buf = ifs.rdbuf();

	// get file size using buffer's members
	size_t size = file_buf->pubseekoff(0, ifs.end, ifs.in);
	file_buf->pubseekpos (0, ifs.in);

	// allocate memory to contain file data
	char* buffer=new char[size + 1];

	// get file data
	file_buf->sgetn (buffer,size);

	ifs.close();

	buffer[size] = '\0';
	string str_buffer(buffer);
	delete[] buffer;

	unique_ptr<PreparedStatement> ps = con.Prepare("INSERT INTO blobs VALUES (?::BLOB)");
	ps->Execute(str_buffer);
	REQUIRE(ps->success);
	ps.reset();
//	result = con.Query("SELECT LENGTH(b) FROM blobs");
//	REQUIRE(CHECK_COLUMN(result, 0, {size}));
	result = con.Query("SELECT count(b) FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("BLOB with Functions", "[blob]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BLOB);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('a'::BLOB)"));

	// concat BLOBs -----------------------------------------------------------------------
	//FIXME: this is failing because of UTFVerify() methods that call string_t::Verify()
//	REQUIRE_NO_FAIL(con.Query("SELECT 'abc '::BLOB || '\153\154\155 \052\251\124'::BLOB)"));

	result = con.Query("SELECT b || 'ZZ'::BLOB FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("aZZ")}));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES ('abc \153\154\155 \052\251\124'::BLOB)"));

	result = con.Query("SELECT count(*) FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	// octet_length
	result = con.Query("SELECT octet_length(b) FROM blobs");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 11}));

	//this should fail because LENGTH only supports UTF8 strings
	//FIXME: this causes a stack-buffer-overflow in duckdb/third_party/utf8proc/utf8proc.cpp:354
//	REQUIRE_FAIL(con.Query("SELECT length(T.b) FROM (SELECT 'abc \153\154\155 \052\251\124'::BLOB as b) AS T"));
//	REQUIRE_FAIL(con.Query("SELECT length(b) FROM blobs"));

}
