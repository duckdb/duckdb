#include "catch.hpp"
#include "duckdb/common/adbc/adbc.hpp"
#include "arrow/arrow_test_helper.hpp"
#include <iostream>

using namespace std;

using namespace duckdb;

bool SUCCESS(duckdb_adbc::AdbcStatusCode status) {
	return status == ADBC_STATUS_OK;
}
const char *duckdb_lib = std::getenv("DUCKDB_INSTALL_LIB");

class ADBCTestDatabase {
public:
	explicit ADBCTestDatabase(const string &path_parameter = "test.db") {
		duckdb_adbc::InitiliazeADBCError(&adbc_error);
		path = TestCreatePath(path_parameter);
		REQUIRE(duckdb_lib);
		REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
		REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", duckdb_lib, &adbc_error)));
		REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error)));
		REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", path.c_str(), &adbc_error)));

		REQUIRE(SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error)));

		REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));
		REQUIRE(SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));
		arrow_stream.release = nullptr;
	}

	~ADBCTestDatabase() {
		if (arrow_stream.release) {
			arrow_stream.release(&arrow_stream);
			arrow_stream.release = nullptr;
		}
		REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
		REQUIRE(SUCCESS(AdbcConnectionRelease(&adbc_connection, &adbc_error)));
		REQUIRE(SUCCESS(AdbcDatabaseRelease(&adbc_database, &adbc_error)));
	}

	bool QueryAndCheck(const string &query) {
		Query(query);
		DuckDB db(path);
		Connection con(db);
		return ArrowTestHelper::RunArrowComparison(con, query, arrow_stream);
	}

	ArrowArrayStream &Query(const string &query) {
		if (arrow_stream.release) {
			arrow_stream.release(&arrow_stream);
			arrow_stream.release = nullptr;
		}
		REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
		REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, query.c_str(), &adbc_error)));
		int64_t rows_affected;
		REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));
		return arrow_stream;
	}

	void CreateTable(const string &table_name, ArrowArrayStream &input_data) {
		REQUIRE(input_data.release);

		REQUIRE(SUCCESS(duckdb_adbc::StatementNew(&adbc_connection, &adbc_statement, &adbc_error)));

		REQUIRE(SUCCESS(duckdb_adbc::StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TARGET_TABLE,
		                                                table_name.c_str(), &adbc_error)));

		REQUIRE(SUCCESS(duckdb_adbc::StatementBindStream(&adbc_statement, &arrow_stream, &adbc_error)));

		REQUIRE(SUCCESS(duckdb_adbc::StatementExecuteQuery(&adbc_statement, nullptr, nullptr, &adbc_error)));
	}

	duckdb_adbc::AdbcError adbc_error;
	duckdb_adbc::AdbcDatabase adbc_database;
	duckdb_adbc::AdbcConnection adbc_connection;
	duckdb_adbc::AdbcStatement adbc_statement;
	ArrowArrayStream arrow_stream;
	std::string path;
};

TEST_CASE("ADBC - Select 42", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	ADBCTestDatabase db;

	REQUIRE(db.QueryAndCheck("SELECT 42"));
}

TEST_CASE("ADBC - Test ingestion", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	ADBCTestDatabase db;

	// Create Arrow Result
	auto input_data = db.Query("SELECT 42");

	// Create Table 'my_table' from the Arrow Result
	db.CreateTable("my_table", input_data);

	REQUIRE(db.QueryAndCheck("SELECT * FROM my_table"));
}

TEST_CASE("ADBC - Test ingestion - Lineitem", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	ADBCTestDatabase db;

	// Create Arrow Result
	auto input_data = db.Query("SELECT * FROM read_csv_auto(\'data/csv/lineitem-carriage.csv\')");

	// Create Table 'my_table' from the Arrow Result
	db.CreateTable("lineitem", input_data);

	REQUIRE(db.QueryAndCheck("SELECT l_partkey, l_comment FROM lineitem WHERE l_orderkey=1 ORDER BY l_linenumber"));
}

TEST_CASE("Test Null Error/Database", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	duckdb_adbc::AdbcStatusCode adbc_status;
	duckdb_adbc::AdbcError adbc_error;
	duckdb_adbc::InitiliazeADBCError(&adbc_error);
	duckdb_adbc::AdbcDatabase adbc_database;
	// NULL error
	adbc_status = duckdb_adbc::DatabaseInit(&adbc_database, nullptr);
	REQUIRE(adbc_status == ADBC_STATUS_INVALID_ARGUMENT);
	// NULL database
	adbc_status = duckdb_adbc::DatabaseInit(nullptr, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_INVALID_ARGUMENT);
	REQUIRE(std::strcmp(adbc_error.message, "ADBC Database has an invalid pointer") == 0);

	// We must Release the error (Malloc-ed string)
	adbc_error.release(&adbc_error);

	// Null Error and Database
	adbc_status = duckdb_adbc::DatabaseInit(nullptr, nullptr);
	REQUIRE(adbc_status == ADBC_STATUS_INVALID_ARGUMENT);
}

TEST_CASE("Test Invalid Path", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	duckdb_adbc::AdbcError adbc_error;
	duckdb_adbc::InitiliazeADBCError(&adbc_error);
	duckdb_adbc::AdbcDatabase adbc_database;

	REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", duckdb_lib, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", "/this/path/is/imaginary/hopefully/", &adbc_error)));

	REQUIRE(!SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error)));

	REQUIRE(std::strcmp(
	            adbc_error.message,
	            "IO Error: Cannot open file \"/this/path/is/imaginary/hopefully/\": No such file or directory") == 0);
}

TEST_CASE("Error Release", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	duckdb_adbc::AdbcError adbc_error;
	duckdb_adbc::InitiliazeADBCError(&adbc_error);

	duckdb_adbc::AdbcDatabase adbc_database;
	duckdb_adbc::AdbcConnection adbc_connection;
	duckdb_adbc::AdbcStatement adbc_statement;
	ArrowArrayStream arrow_stream;
	ArrowArray arrow_array;

	int64_t rows_affected;

	// Create connection - database and whatnot
	REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", duckdb_lib, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", ":memory:", &adbc_error)));

	REQUIRE(SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));

	// Run Query so we can start trying to mess around with it
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, "SELECT 42", &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));

	// Let's release the stream
	arrow_stream.release(&arrow_stream);

	// Release pointer is null
	REQUIRE(!arrow_stream.release);

	// Can't get data from release stream
	REQUIRE(arrow_stream.get_next(&arrow_stream, &arrow_array) != 0);

	// Release ADBC Statement
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));

	// Not possible to get Arrow stream with released statement
	REQUIRE(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error) ==
	        ADBC_STATUS_INVALID_STATE);

	// We can release a statement and consume the stream afterwards if we have called GetStream beforehand
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, "SELECT 42", &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));

	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));

	auto arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(arrow_array.length == 1);
	REQUIRE(arrow_status == 0);
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	// We can't run a query on a nullptr connection
	REQUIRE(AdbcStatementNew(nullptr, &adbc_statement, &adbc_error) == ADBC_STATUS_INVALID_ARGUMENT);

	// We can't run a query on a nullptr statement
	REQUIRE(AdbcStatementExecuteQuery(nullptr, &arrow_stream, &rows_affected, &adbc_error) ==
	        ADBC_STATUS_INVALID_ARGUMENT);

	// We can't run a query without a query
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	AdbcStatementSetSqlQuery(&adbc_statement, nullptr, &adbc_error);
	REQUIRE(std::strcmp(adbc_error.message, "Missing query") == 0);

	// Release the connection
	REQUIRE(SUCCESS(ConnectionRelease(&adbc_connection, &adbc_error)));
	// Release the error
	adbc_error.release(&adbc_error);

	// We can't run a query after releasing a connection
	REQUIRE(!SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	D_ASSERT(std::strcmp(adbc_error.message, "Invalid connection object") == 0);
	// Release the error
	adbc_error.release(&adbc_error);

	// We can release it multiple times
	REQUIRE(SUCCESS(ConnectionRelease(&adbc_connection, &adbc_error)));

	// We can't Init with a released connection
	REQUIRE(!SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));
	D_ASSERT(std::strcmp(adbc_error.message, "Must call AdbcConnectionNew first") == 0);
	// Release the error
	adbc_error.release(&adbc_error);

	// Shut down the database
	REQUIRE(SUCCESS(DatabaseRelease(&adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));

	REQUIRE(!SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));
	D_ASSERT(std::strcmp(adbc_error.message, "Invalid database") == 0);
	// Release the error
	adbc_error.release(&adbc_error);
}
