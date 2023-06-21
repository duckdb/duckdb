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
	REQUIRE(std::strcmp(adbc_error.message, "Invalid connection object") == 0);
	// Release the error
	adbc_error.release(&adbc_error);

	// We can release it multiple times
	REQUIRE(SUCCESS(ConnectionRelease(&adbc_connection, &adbc_error)));

	// We can't Init with a released connection
	REQUIRE(!SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));
	REQUIRE(std::strcmp(adbc_error.message, "Must call AdbcConnectionNew first") == 0);
	// Release the error
	adbc_error.release(&adbc_error);

	// Shut down the database
	REQUIRE(SUCCESS(DatabaseRelease(&adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));

	REQUIRE(!SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));
	REQUIRE(std::strcmp(adbc_error.message, "Invalid database") == 0);
	// Release the error
	adbc_error.release(&adbc_error);
}

TEST_CASE("Test Not-Implemented Partition Functions", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	duckdb_adbc::AdbcDatabase adbc_database;
	duckdb_adbc::AdbcConnection adbc_connection;

	duckdb_adbc::AdbcError adbc_error;
	duckdb_adbc::InitiliazeADBCError(&adbc_error);

	// Create connection - database and whatnot
	REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", duckdb_lib, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", ":memory:", &adbc_error)));

	REQUIRE(SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));

	auto status = AdbcConnectionReadPartition(&adbc_connection, nullptr, 0, nullptr, &adbc_error);
	REQUIRE(status == ADBC_STATUS_NOT_IMPLEMENTED);
	REQUIRE(std::strcmp(adbc_error.message, "Read Partitions are not supported in DuckDB") == 0);
	adbc_error.release(&adbc_error);

	duckdb_adbc::AdbcStatement adbc_statement;
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));

	status = AdbcStatementExecutePartitions(&adbc_statement, nullptr, nullptr, nullptr, &adbc_error);
	REQUIRE(status == ADBC_STATUS_NOT_IMPLEMENTED);
	REQUIRE(std::strcmp(adbc_error.message, "Execute Partitions are not supported in DuckDB") == 0);
	adbc_error.release(&adbc_error);
}

TEST_CASE("Test ADBC Transactions", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}

	ADBCTestDatabase db;

	// Create Arrow Result
	auto &input_data = db.Query("SELECT 42");
	string table_name = "test";
	string query = "select count(*) from test";

	duckdb_adbc::AdbcDatabase adbc_database;
	duckdb_adbc::AdbcConnection adbc_connection;

	duckdb_adbc::AdbcConnection adbc_connection_2;

	duckdb_adbc::AdbcError adbc_error;
	duckdb_adbc::InitiliazeADBCError(&adbc_error);
	ArrowArrayStream arrow_stream;
	ArrowArray arrow_array;

	// Create connection - database and whatnot
	REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", duckdb_lib, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", ":memory:", &adbc_error)));

	REQUIRE(SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionInit(&adbc_connection_2, &adbc_database, &adbc_error)));

	// Let's first insert with Auto-Commit On
	duckdb_adbc::AdbcStatement adbc_statement;

	duckdb_adbc::AdbcStatement adbc_statement_2;

	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));

	REQUIRE(SUCCESS(duckdb_adbc::StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TARGET_TABLE,
	                                                table_name.c_str(), &adbc_error)));

	REQUIRE(SUCCESS(duckdb_adbc::StatementBindStream(&adbc_statement, &input_data, &adbc_error)));

	REQUIRE(SUCCESS(duckdb_adbc::StatementExecuteQuery(&adbc_statement, nullptr, nullptr, &adbc_error)));

	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection_2, &adbc_statement_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement_2, query.c_str(), &adbc_error)));
	int64_t rows_affected;
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement_2, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((int64_t *)arrow_array.children[0]->buffers[1])[0] == 1);
	// Release the boys
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	// Now lets insert with Auto-Commit Off
	REQUIRE(SUCCESS(AdbcConnectionSetOption(&adbc_connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
	                                        ADBC_OPTION_VALUE_DISABLED, &adbc_error)));
	input_data = db.Query("SELECT 42;");

	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));

	REQUIRE(SUCCESS(duckdb_adbc::StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TARGET_TABLE,
	                                                table_name.c_str(), &adbc_error)));

	REQUIRE(SUCCESS(duckdb_adbc::StatementBindStream(&adbc_statement, &input_data, &adbc_error)));

	REQUIRE(SUCCESS(duckdb_adbc::StatementExecuteQuery(&adbc_statement, nullptr, nullptr, &adbc_error)));

	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection_2, &adbc_statement_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement_2, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement_2, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((int64_t *)arrow_array.children[0]->buffers[1])[0] == 1);
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	// If we check from con1, we should have 2
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((int64_t *)arrow_array.children[0]->buffers[1])[0] == 2);
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	// Now if we do a commit on the first connection this should be 2 on the second connection
	REQUIRE(SUCCESS(AdbcConnectionCommit(&adbc_connection, &adbc_error)));

	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection_2, &adbc_statement_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement_2, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement_2, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((int64_t *)arrow_array.children[0]->buffers[1])[0] == 2);
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	// Lets do a rollback
	input_data = db.Query("SELECT 42;");

	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));

	REQUIRE(SUCCESS(duckdb_adbc::StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TARGET_TABLE,
	                                                table_name.c_str(), &adbc_error)));

	REQUIRE(SUCCESS(duckdb_adbc::StatementBindStream(&adbc_statement, &input_data, &adbc_error)));

	REQUIRE(SUCCESS(duckdb_adbc::StatementExecuteQuery(&adbc_statement, nullptr, nullptr, &adbc_error)));

	// If we check from con1, we should have 3
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((int64_t *)arrow_array.children[0]->buffers[1])[0] == 3);
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	// If we check from con2 we should 2

	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection_2, &adbc_statement_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement_2, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement_2, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((int64_t *)arrow_array.children[0]->buffers[1])[0] == 2);
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	// If we rollback con1, we should now have two again on con1
	REQUIRE(SUCCESS(AdbcConnectionRollback(&adbc_connection, &adbc_error)));

	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((int64_t *)arrow_array.children[0]->buffers[1])[0] == 2);
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	// Let's change the Auto commit config mid-transaction
	input_data = db.Query("SELECT 42;");

	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));

	REQUIRE(SUCCESS(duckdb_adbc::StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TARGET_TABLE,
	                                                table_name.c_str(), &adbc_error)));

	REQUIRE(SUCCESS(duckdb_adbc::StatementBindStream(&adbc_statement, &input_data, &adbc_error)));

	REQUIRE(SUCCESS(duckdb_adbc::StatementExecuteQuery(&adbc_statement, nullptr, nullptr, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionSetOption(&adbc_connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
	                                        ADBC_OPTION_VALUE_ENABLED, &adbc_error)));

	// Now Both con1 and con2 should have 3
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection_2, &adbc_statement_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement_2, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement_2, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((int64_t *)arrow_array.children[0]->buffers[1])[0] == 3);
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((int64_t *)arrow_array.children[0]->buffers[1])[0] == 3);
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	input_data = db.Query("SELECT 42;");

	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));

	REQUIRE(SUCCESS(duckdb_adbc::StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TARGET_TABLE,
	                                                table_name.c_str(), &adbc_error)));

	REQUIRE(SUCCESS(duckdb_adbc::StatementBindStream(&adbc_statement, &input_data, &adbc_error)));

	REQUIRE(SUCCESS(duckdb_adbc::StatementExecuteQuery(&adbc_statement, nullptr, nullptr, &adbc_error)));

	// Auto-Commit is on, so this should just be commited
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection_2, &adbc_statement_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement_2, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement_2, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((int64_t *)arrow_array.children[0]->buffers[1])[0] == 4);
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((int64_t *)arrow_array.children[0]->buffers[1])[0] == 4);
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);
}

TEST_CASE("Test ADBC Transaction Errors", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	duckdb_adbc::AdbcDatabase adbc_database;
	duckdb_adbc::AdbcConnection adbc_connection;

	duckdb_adbc::AdbcError adbc_error;
	duckdb_adbc::InitiliazeADBCError(&adbc_error);

	REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", duckdb_lib, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", ":memory:", &adbc_error)));

	REQUIRE(SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));

	// Can't commit if there is no transaction
	REQUIRE(!SUCCESS(AdbcConnectionCommit(&adbc_connection, &adbc_error)));
	REQUIRE(std::strcmp(adbc_error.message, "No active transaction, cannot commit") == 0);
	adbc_error.release(&adbc_error);

	// Can't rollback if there is no transaction
	REQUIRE(!SUCCESS(AdbcConnectionRollback(&adbc_connection, &adbc_error)));
	REQUIRE(std::strcmp(adbc_error.message, "No active transaction, cannot rollback") == 0);
	adbc_error.release(&adbc_error);

	// Try to set Commit option to random gunk
	REQUIRE(SUCCESS(!AdbcConnectionSetOption(&adbc_connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT, "bla", &adbc_error)));

	REQUIRE(std::strcmp(adbc_error.message, "Invalid connection option value adbc.connection.autocommit=bla") == 0);
	adbc_error.release(&adbc_error);

	// Let's disable the autocommit
	REQUIRE(SUCCESS(AdbcConnectionSetOption(&adbc_connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
	                                        ADBC_OPTION_VALUE_DISABLED, &adbc_error)));

	// We should succeed on committing and rolling back.
	REQUIRE(SUCCESS(AdbcConnectionCommit(&adbc_connection, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionRollback(&adbc_connection, &adbc_error)));
}
