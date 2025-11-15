#include "arrow/arrow_test_helper.hpp"
#include "catch.hpp"
#include "duckdb/common/adbc/adbc.hpp"
#include "duckdb/common/adbc/wrappers.hpp"
#include "duckdb/common/adbc/options.h"
#include "duckdb/common/arrow/nanoarrow/nanoarrow.hpp"
#include <iostream>

namespace duckdb {

using duckdb_adbc::ConnectionRelease;
using duckdb_adbc::DatabaseInit;
using duckdb_adbc::DatabaseRelease;
using duckdb_adbc::InitializeADBCError;
using duckdb_adbc::StatementBindStream;
using duckdb_adbc::StatementExecuteQuery;
using duckdb_adbc::StatementSetOption;

bool SUCCESS(AdbcStatusCode status) {
	return status == ADBC_STATUS_OK;
}

const char *duckdb_lib = std::getenv("DUCKDB_INSTALL_LIB");

class ADBCTestDatabase {
public:
	explicit ADBCTestDatabase(const string &path_parameter = ":memory:") {
		InitializeADBCError(&adbc_error);
		if (path_parameter != ":memory:") {
			path = TestCreatePath(path_parameter);
		} else {
			path = path_parameter;
		}
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
		REQUIRE(SUCCESS(AdbcConnectionRelease(&adbc_connection, &adbc_error)));
		REQUIRE(SUCCESS(AdbcDatabaseRelease(&adbc_database, &adbc_error)));
	}

	bool QueryAndCheck(const string &query) {
		QueryArrow(query);
		auto conn_wrapper = static_cast<DuckDBAdbcConnectionWrapper *>(adbc_connection.private_data);

		auto cconn = reinterpret_cast<Connection *>(conn_wrapper->connection);
		return ArrowTestHelper::RunArrowComparison(*cconn, query, arrow_stream);
	}

	unique_ptr<MaterializedQueryResult> Query(const string &query) {
		auto conn_wrapper = static_cast<DuckDBAdbcConnectionWrapper *>(adbc_connection.private_data);
		auto cconn = reinterpret_cast<Connection *>(conn_wrapper->connection);
		return cconn->Query(query);
	}

	ArrowArrayStream &QueryArrow(const string &query) {
		if (arrow_stream.release) {
			arrow_stream.release(&arrow_stream);
			arrow_stream.release = nullptr;
		}
		AdbcStatement adbc_statement;
		REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
		REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, query.c_str(), &adbc_error)));
		int64_t rows_affected;
		REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));
		// Release the statement
		REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
		return arrow_stream;
	}

	void CreateTable(const string &table_name, ArrowArrayStream &input_data, string schema = "",
	                 bool temporary = false) {
		REQUIRE(input_data.release);
		AdbcStatement adbc_statement;
		REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
		if (!schema.empty()) {
			REQUIRE(SUCCESS(AdbcStatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TARGET_DB_SCHEMA, schema.c_str(),
			                                       &adbc_error)));
		}
		if (temporary) {
			if (!schema.empty()) {
				REQUIRE(!SUCCESS(AdbcStatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TEMPORARY,
				                                        ADBC_OPTION_VALUE_ENABLED, &adbc_error)));
				REQUIRE((std::strcmp(adbc_error.message, "Temporary option is not supported with schema") == 0));
				// We must Release the error (Malloc-ed string)
				adbc_error.release(&adbc_error);
				InitializeADBCError(&adbc_error);
				REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
				// Release the input_data stream since we didn't transfer ownership
				if (input_data.release) {
					input_data.release(&input_data);
				}
				input_data.release = nullptr;
				return;
			}
			REQUIRE(SUCCESS(AdbcStatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TEMPORARY,
			                                       ADBC_OPTION_VALUE_ENABLED, &adbc_error)));
		}
		REQUIRE(SUCCESS(
		    AdbcStatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TARGET_TABLE, table_name.c_str(), &adbc_error)));

		REQUIRE(SUCCESS(AdbcStatementBindStream(&adbc_statement, &input_data, &adbc_error)));
		REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, nullptr, nullptr, &adbc_error)));
		// Release the statement
		REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
		if (input_data.release) {
			input_data.release(&input_data);
		}
		input_data.release = nullptr;
	}

	AdbcError adbc_error;
	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;

	ArrowArrayStream arrow_stream;
	string path;
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
	auto &input_data = db.QueryArrow("SELECT 42 as value");

	// Create Table 'my_table' from the Arrow Result
	db.CreateTable("my_table", input_data);

	REQUIRE(db.QueryAndCheck("SELECT * FROM my_table"));
}

TEST_CASE("ADBC - Test ingestion - Temporary Table", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	ADBCTestDatabase db;

	// Create Arrow Result
	auto &input_data = db.QueryArrow("SELECT 42 as value");

	// Create Table 'my_table' from the Arrow Result
	db.CreateTable("my_table", input_data, "", true);

	REQUIRE(db.QueryAndCheck("SELECT * FROM my_table"));
}

TEST_CASE("ADBC - Test ingestion - Temporary Table - Schema Set", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	ADBCTestDatabase db;
	db.Query("CREATE SCHEMA my_schema;");
	// Create Arrow Result
	auto &input_data = db.QueryArrow("SELECT 42 as value");

	// Since this is temporary and has a schema, it will fail in an internal code path
	db.CreateTable("my_table", input_data, "my_schema", true);

	// We released it in the error, hence we create it again
	auto &input_data_2 = db.QueryArrow("SELECT 42 as value");

	db.CreateTable("my_table", input_data_2, "my_schema");

	// we can check it works
	REQUIRE(db.QueryAndCheck("SELECT * FROM my_schema.my_table"));
}

TEST_CASE("ADBC - Test ingestion - Quoted Table and Schema", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	ADBCTestDatabase db;
	db.Query("CREATE SCHEMA \"my_schema\";");
	// Create Arrow Result
	auto &input_data = db.QueryArrow("SELECT 42 as value");

	// Create table with name requiring quoting
	db.CreateTable("my_table", input_data, "my_schema");

	// Validate that we can get its schema
	AdbcError adbc_error;
	InitializeADBCError(&adbc_error);

	ArrowSchema arrow_schema;
	REQUIRE(SUCCESS(AdbcConnectionGetTableSchema(&db.adbc_connection, nullptr, "my_schema", "my_table", &arrow_schema,
	                                             &adbc_error)));
	REQUIRE((arrow_schema.n_children == 1));
	arrow_schema.release(&arrow_schema);
}

TEST_CASE("ADBC - Test Ingestion - Funky identifiers", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	ADBCTestDatabase db;
	std::vector<std::string> column_names = {"column.with.dots", "column\"with\"quotes", "column with whitespace"};
	std::string table_name = "my.table\"is 😵";
	std::string schema_name = "my schema\"is.😬";
	// Create a schema and allocate space for a single child
	ArrowSchema arrow_schema;
	duckdb_nanoarrow::ArrowSchemaInit(&arrow_schema, duckdb_nanoarrow::ArrowType::NANOARROW_TYPE_STRUCT);
	duckdb_nanoarrow::ArrowSchemaAllocateChildren(&arrow_schema, static_cast<int64_t>(column_names.size()));

	// Create the child schemas and build the select query to get test data
	std::string query = "SELECT ";
	for (size_t i = 0; i < column_names.size(); i++) {
		duckdb_nanoarrow::ArrowSchemaInit(arrow_schema.children[i], duckdb_nanoarrow::ArrowType::NANOARROW_TYPE_INT32);
		duckdb_nanoarrow::ArrowSchemaSetName(arrow_schema.children[i], column_names.at(i).c_str());
		if (i > 0) {
			query += ",";
		}
		query += std::to_string(i);
	}

	// Create input data
	auto &input_data = db.QueryArrow(query);
	ArrowArray prepared_array;
	input_data.get_next(&input_data, &prepared_array);

	// Create the schema
	db.Query("CREATE SCHEMA " + KeywordHelper::WriteOptionallyQuoted(schema_name));

	// Create ADBC statement that will create a table called "test"
	AdbcStatement adbc_stmt;
	AdbcError adbc_error;
	InitializeADBCError(&adbc_error);
	REQUIRE(SUCCESS(AdbcStatementNew(&db.adbc_connection, &adbc_stmt, &adbc_error)));
	REQUIRE(SUCCESS(
	    AdbcStatementSetOption(&adbc_stmt, ADBC_INGEST_OPTION_MODE, ADBC_INGEST_OPTION_MODE_CREATE, &adbc_error)));
	REQUIRE(SUCCESS(
	    AdbcStatementSetOption(&adbc_stmt, ADBC_INGEST_OPTION_TARGET_DB_SCHEMA, schema_name.c_str(), &adbc_error)));
	REQUIRE(
	    SUCCESS(AdbcStatementSetOption(&adbc_stmt, ADBC_INGEST_OPTION_TARGET_TABLE, table_name.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementBind(&adbc_stmt, &prepared_array, &arrow_schema, &adbc_error)));

	// Now execute: this should create the table and put the test data in
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_stmt, nullptr, nullptr, &adbc_error)));

	// Release everything
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_stmt, &adbc_error)));
	if (arrow_schema.release) {
		arrow_schema.release(&arrow_schema);
	}
	if (adbc_error.release) {
		adbc_error.release(&adbc_error);
	}
	if (input_data.release) {
		input_data.release(&input_data);
	}
	if (prepared_array.release) {
		prepared_array.release(&prepared_array);
	}

	// Check we can query
	auto schema_table =
	    KeywordHelper::WriteOptionallyQuoted(schema_name) + "." + KeywordHelper::WriteOptionallyQuoted(table_name);
	auto res = db.Query("select * from " + schema_table);
	for (size_t i = 0; i < column_names.size(); i++) {
		REQUIRE((res->ColumnName(i) == column_names.at(i)));
		REQUIRE((res->GetValue(i, 0) == i));
	}
}

TEST_CASE("ADBC - Test ingestion - Lineitem", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	ADBCTestDatabase db;

	// Create Arrow Result
	auto &input_data = db.QueryArrow("SELECT * FROM read_csv_auto(\'data/csv/lineitem-carriage.csv\')");

	// Create Table 'my_table' from the Arrow Result
	db.CreateTable("lineitem", input_data);

	REQUIRE(db.QueryAndCheck("SELECT l_partkey, l_comment FROM lineitem WHERE l_orderkey=1 ORDER BY l_linenumber"));
}

TEST_CASE("ADBC - Pivot", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	ADBCTestDatabase db;

	auto &input_data = db.QueryArrow("SELECT * FROM read_csv_auto(\'data/csv/flights.csv\')");

	db.CreateTable("flights", input_data);

	REQUIRE(db.QueryAndCheck("PIVOT flights ON UniqueCarrier USING COUNT(1) GROUP BY OriginCityName;"));
}

TEST_CASE("Test Null Error/Database", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	AdbcError adbc_error;
	InitializeADBCError(&adbc_error);
	AdbcDatabase adbc_database;
	// NULL error
	auto adbc_status = DatabaseInit(&adbc_database, nullptr);
	REQUIRE((adbc_status == ADBC_STATUS_INVALID_ARGUMENT));
	// NULL database
	adbc_status = DatabaseInit(nullptr, &adbc_error);
	REQUIRE((adbc_status == ADBC_STATUS_INVALID_ARGUMENT));
	REQUIRE((std::strcmp(adbc_error.message, "ADBC Database has an invalid pointer") == 0));

	// We must Release the error (Malloc-ed string)
	adbc_error.release(&adbc_error);

	// Null Error and Database
	adbc_status = DatabaseInit(nullptr, nullptr);
	REQUIRE((adbc_status == ADBC_STATUS_INVALID_ARGUMENT));
}

TEST_CASE("Test Invalid Path", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	AdbcError adbc_error;
	InitializeADBCError(&adbc_error);
	AdbcDatabase adbc_database;

	REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", duckdb_lib, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", "/this/path/is/imaginary/hopefully/", &adbc_error)));

	REQUIRE(!SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error)));

	if (!adbc_error.message) {
		REQUIRE(false);
	} else {
		REQUIRE(std::strstr(adbc_error.message, "Cannot open file"));
	}
	adbc_error.release(&adbc_error);
	InitializeADBCError(&adbc_error);
	REQUIRE(SUCCESS(AdbcDatabaseRelease(&adbc_database, &adbc_error)));
	adbc_error.release(&adbc_error);
}

TEST_CASE("ADBC - Statement reuse", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	AdbcError adbc_error;
	InitializeADBCError(&adbc_error);

	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;
	AdbcStatement adbc_statement;
	ArrowArrayStream arrow_stream;

	int64_t rows_affected;

	// Create connection - database and whatnot
	REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", duckdb_lib, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", ":memory:", &adbc_error)));

	REQUIRE(SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));

	// Insert some data
	// Create Arrow Result
	ADBCTestDatabase db;
	auto &input_data = db.QueryArrow("SELECT 42 as value");
	string table_name = "my_table";

	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));

	REQUIRE(
	    SUCCESS(StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TARGET_TABLE, table_name.c_str(), &adbc_error)));

	REQUIRE(SUCCESS(StatementBindStream(&adbc_statement, &input_data, &adbc_error)));
	REQUIRE(SUCCESS(StatementExecuteQuery(&adbc_statement, nullptr, nullptr, &adbc_error)));

	string query = "DELETE FROM my_table";
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));

	if (arrow_stream.release) {
		arrow_stream.release(&arrow_stream);
	}

	auto &input_data_2 = db.QueryArrow("SELECT 42 as value");

	StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_MODE, ADBC_INGEST_OPTION_MODE_APPEND, &adbc_error);

	REQUIRE(SUCCESS(StatementBindStream(&adbc_statement, &input_data_2, &adbc_error)));
	REQUIRE(SUCCESS(StatementExecuteQuery(&adbc_statement, nullptr, nullptr, &adbc_error)));

	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));

	if (arrow_stream.release) {
		arrow_stream.release(&arrow_stream);
	}

	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionRelease(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseRelease(&adbc_database, &adbc_error)));
}

TEST_CASE("Error Release", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	AdbcError adbc_error;
	InitializeADBCError(&adbc_error);

	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;
	AdbcStatement adbc_statement;
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
	REQUIRE((arrow_stream.get_next(&arrow_stream, &arrow_array) != 0));

	// Release ADBC Statement
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));

	// Not possible to get Arrow stream with released statement
	REQUIRE((AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error) ==
	         ADBC_STATUS_INVALID_STATE));

	// We can release a statement and consume the stream afterward if we have called GetStream beforehand
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, "SELECT 42", &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));

	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));

	auto arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE((arrow_array.length == 1));
	REQUIRE((arrow_status == 0));
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	// We can't run a query without a query
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	AdbcStatementSetSqlQuery(&adbc_statement, nullptr, &adbc_error);
	REQUIRE((std::strcmp(adbc_error.message, "Missing query") == 0));

	// Release the connection
	REQUIRE(SUCCESS(ConnectionRelease(&adbc_connection, &adbc_error)));
	// Release the error
	adbc_error.release(&adbc_error);

	// We can't run a query after releasing a connection
	REQUIRE(!SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	REQUIRE((std::strcmp(adbc_error.message, "Invalid connection object") == 0));
	// Release the error
	adbc_error.release(&adbc_error);

	// We can release it multiple times
	REQUIRE(SUCCESS(ConnectionRelease(&adbc_connection, &adbc_error)));

	// We can't Init with a released connection
	REQUIRE(!SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));
	REQUIRE((std::strcmp(adbc_error.message, "Must call AdbcConnectionNew first") == 0));
	// Release the error
	adbc_error.release(&adbc_error);

	// Shut down the database
	REQUIRE(SUCCESS(DatabaseRelease(&adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));

	REQUIRE(!SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));
	REQUIRE((std::strcmp(adbc_error.message, "Invalid database") == 0));

	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionRelease(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseRelease(&adbc_database, &adbc_error)));
	// Release the error
	adbc_error.release(&adbc_error);
}

TEST_CASE("Test Not-Implemented Partition Functions", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;

	AdbcError adbc_error;
	InitializeADBCError(&adbc_error);

	// Create connection - database and whatnot
	REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", duckdb_lib, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", ":memory:", &adbc_error)));

	REQUIRE(SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));

	auto status = AdbcConnectionReadPartition(&adbc_connection, nullptr, 0, nullptr, &adbc_error);
	REQUIRE((status == ADBC_STATUS_NOT_IMPLEMENTED));
	REQUIRE((std::strcmp(adbc_error.message, "Read Partitions are not supported in DuckDB") == 0));
	adbc_error.release(&adbc_error);

	AdbcStatement adbc_statement;
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));

	status = AdbcStatementExecutePartitions(&adbc_statement, nullptr, nullptr, nullptr, &adbc_error);
	REQUIRE((status == ADBC_STATUS_NOT_IMPLEMENTED));
	REQUIRE((std::strcmp(adbc_error.message, "Execute Partitions are not supported in DuckDB") == 0));
	adbc_error.release(&adbc_error);
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionRelease(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseRelease(&adbc_database, &adbc_error)));
}

TEST_CASE("Test ADBC ConnectionGetInfo", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;

	AdbcError adbc_error;
	InitializeADBCError(&adbc_error);

	// Create connection - database and whatnot
	REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", duckdb_lib, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", ":memory:", &adbc_error)));

	REQUIRE(SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));

	AdbcStatusCode status = ADBC_STATUS_OK;
	ArrowArrayStream out_stream;

	// ==== UNHAPPY PATH ====

	static uint32_t test_info_codes[] = {100, 4, 3};
	static constexpr size_t TEST_INFO_CODE_LENGTH = sizeof(test_info_codes) / sizeof(uint32_t);

	// No error
	status = AdbcConnectionGetInfo(&adbc_connection, test_info_codes, TEST_INFO_CODE_LENGTH, &out_stream, nullptr);
	REQUIRE((status != ADBC_STATUS_OK));

	// Invalid connection
	AdbcConnection bogus_connection;
	bogus_connection.private_data = nullptr;
	bogus_connection.private_driver = nullptr;
	status = AdbcConnectionGetInfo(&bogus_connection, test_info_codes, TEST_INFO_CODE_LENGTH, &out_stream, &adbc_error);
	REQUIRE((status != ADBC_STATUS_OK));

	// No stream
	status = AdbcConnectionGetInfo(&adbc_connection, test_info_codes, TEST_INFO_CODE_LENGTH, nullptr, &adbc_error);
	REQUIRE((status != ADBC_STATUS_OK));

	// ==== HAPPY PATH ====

	// This returns all known info codes
	status = AdbcConnectionGetInfo(&adbc_connection, nullptr, 42, &out_stream, &adbc_error);
	REQUIRE((status == ADBC_STATUS_OK));
	REQUIRE((out_stream.release != nullptr));

	out_stream.release(&out_stream);

	REQUIRE(SUCCESS(AdbcConnectionRelease(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseRelease(&adbc_database, &adbc_error)));
	adbc_error.release(&adbc_error);
}

TEST_CASE("Test ADBC Statement Bind (unhappy)", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;

	AdbcError adbc_error;
	InitializeADBCError(&adbc_error);

	string query = "select ?, ?, ?";

	// Create connection - database and whatnot
	REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", duckdb_lib, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", ":memory:", &adbc_error)));

	REQUIRE(SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));

	AdbcStatement adbc_statement;
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementPrepare(&adbc_statement, &adbc_error)));

	ADBCTestDatabase db;
	auto &input_data = db.QueryArrow("SELECT 42, true, 'this is a string'");
	ArrowArray prepared_array;
	ArrowSchema prepared_schema;
	input_data.get_next(&input_data, &prepared_array);
	input_data.get_schema(&input_data, &prepared_schema);

	AdbcStatusCode status = ADBC_STATUS_OK;
	// No error passed in
	// This is not an error, this only means we can't provide a message
	status = AdbcStatementBind(&adbc_statement, &prepared_array, &prepared_schema, nullptr);
	REQUIRE((status == ADBC_STATUS_OK));

	// No valid statement
	AdbcStatement bogus_statement;
	bogus_statement.private_data = nullptr;
	bogus_statement.private_driver = nullptr;
	status = AdbcStatementBind(&bogus_statement, &prepared_array, &prepared_schema, &adbc_error);
	REQUIRE((status != ADBC_STATUS_OK));

	// No array
	status = AdbcStatementBind(&adbc_statement, nullptr, &prepared_schema, &adbc_error);
	REQUIRE((status != ADBC_STATUS_OK));

	// No schema
	status = AdbcStatementBind(&adbc_statement, &prepared_array, nullptr, &adbc_error);
	REQUIRE((status != ADBC_STATUS_OK));

	// ---- Get Parameter Schema ----

	// No error passed in
	ArrowSchema result;
	result.release = nullptr;
	status = AdbcStatementGetParameterSchema(&adbc_statement, &result, nullptr);
	result.release(&result);
	result.release = nullptr;
	REQUIRE((status == ADBC_STATUS_OK));

	// No valid statement
	status = AdbcStatementGetParameterSchema(&bogus_statement, &result, &adbc_error);
	REQUIRE((status != ADBC_STATUS_OK));

	// No result
	status = AdbcStatementGetParameterSchema(&adbc_statement, nullptr, &adbc_error);
	REQUIRE((status != ADBC_STATUS_OK));

	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionRelease(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseRelease(&adbc_database, &adbc_error)));

	adbc_error.release(&adbc_error);
}

TEST_CASE("Test ADBC Statement Bind", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}

	ADBCTestDatabase db;

	// Create prepared parameter array
	auto &input_data = db.QueryArrow("SELECT 42, true, 'this is a string'");
	string query = "select ?, ?, ?";

	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;

	AdbcError adbc_error;
	InitializeADBCError(&adbc_error);

	// Create connection - database and whatnot
	REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", duckdb_lib, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", ":memory:", &adbc_error)));

	REQUIRE(SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));

	AdbcStatement adbc_statement;
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementPrepare(&adbc_statement, &adbc_error)));

	ArrowSchema expected_schema;
	expected_schema.release = nullptr;
	REQUIRE(SUCCESS(AdbcStatementGetParameterSchema(&adbc_statement, &expected_schema, &adbc_error)));
	REQUIRE((expected_schema.n_children == 3));
	for (int64_t i = 0; i < expected_schema.n_children; i++) {
		auto child = expected_schema.children[i];
		std::string child_name = child->name;
		std::string expected_name = StringUtil::Format("%d", i);
		REQUIRE((child_name == expected_name));
	}
	expected_schema.release(&expected_schema);

	ArrowArray prepared_array;
	ArrowSchema prepared_schema;
	input_data.get_next(&input_data, &prepared_array);
	input_data.get_schema(&input_data, &prepared_schema);
	REQUIRE(SUCCESS(AdbcStatementBind(&adbc_statement, &prepared_array, &prepared_schema, &adbc_error)));
	REQUIRE((prepared_array.release == nullptr));
	REQUIRE((prepared_schema.release == nullptr));

	int64_t rows_affected;
	ArrowArrayStream arrow_stream;
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));

	input_data.release(&input_data);

	ArrowArray result_array;
	arrow_stream.get_next(&arrow_stream, &result_array);
	REQUIRE((reinterpret_cast<const int32_t *>(result_array.children[0]->buffers[1])[0] == 42));

	result_array.release(&result_array);
	arrow_stream.release(&arrow_stream);
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionRelease(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseRelease(&adbc_database, &adbc_error)));
	adbc_error.release(&adbc_error);
}

TEST_CASE("Test ADBC Statement with Zero Parameters", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}

	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;
	AdbcError adbc_error;
	InitializeADBCError(&adbc_error);

	// Create connection
	REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", duckdb_lib, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", ":memory:", &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));

	// Test 1: Query with no parameters (CREATE TABLE)
	{
		AdbcStatement adbc_statement;
		REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
		REQUIRE(SUCCESS(
		    AdbcStatementSetSqlQuery(&adbc_statement, "CREATE TABLE test_zero_params (id INTEGER)", &adbc_error)));
		REQUIRE(SUCCESS(AdbcStatementPrepare(&adbc_statement, &adbc_error)));

		// Get parameter schema - should have 0 children
		ArrowSchema param_schema;
		param_schema.release = nullptr;
		REQUIRE(SUCCESS(AdbcStatementGetParameterSchema(&adbc_statement, &param_schema, &adbc_error)));
		REQUIRE(param_schema.n_children == 0);
		REQUIRE(param_schema.format != nullptr);
		REQUIRE(strcmp(param_schema.format, "+s") == 0); // Should be a struct
		param_schema.release(&param_schema);

		// Execute the statement without binding any parameters
		ArrowArrayStream arrow_stream;
		arrow_stream.release = nullptr;
		REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, nullptr, &adbc_error)));
		arrow_stream.release(&arrow_stream);

		REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	}

	// Test 2: Query with no parameters (SELECT constant)
	{
		AdbcStatement adbc_statement;
		REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
		REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, "SELECT 42", &adbc_error)));
		REQUIRE(SUCCESS(AdbcStatementPrepare(&adbc_statement, &adbc_error)));

		// Get parameter schema - should have 0 children
		ArrowSchema param_schema;
		param_schema.release = nullptr;
		REQUIRE(SUCCESS(AdbcStatementGetParameterSchema(&adbc_statement, &param_schema, &adbc_error)));
		REQUIRE(param_schema.n_children == 0);
		param_schema.release(&param_schema);

		// Execute and verify result
		ArrowArrayStream arrow_stream;
		arrow_stream.release = nullptr;
		REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, nullptr, &adbc_error)));

		ArrowSchema result_schema;
		arrow_stream.get_schema(&arrow_stream, &result_schema);
		REQUIRE(result_schema.n_children == 1); // One column in result
		result_schema.release(&result_schema);

		arrow_stream.release(&arrow_stream);
		REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	}

	REQUIRE(SUCCESS(AdbcConnectionRelease(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseRelease(&adbc_database, &adbc_error)));
	adbc_error.release(&adbc_error);
}

TEST_CASE("Test ADBC Transactions", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}

	ADBCTestDatabase db;

	// Create Arrow Result
	auto &input_data = db.QueryArrow("SELECT 42 as value");
	string table_name = "test";
	string query = "select count(*) from test";

	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;

	AdbcConnection adbc_connection_2;

	AdbcError adbc_error;
	InitializeADBCError(&adbc_error);
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
	AdbcStatement adbc_statement;

	AdbcStatement adbc_statement_2;

	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));

	REQUIRE(
	    SUCCESS(StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TARGET_TABLE, table_name.c_str(), &adbc_error)));

	REQUIRE(SUCCESS(StatementBindStream(&adbc_statement, &input_data, &adbc_error)));

	REQUIRE(SUCCESS(StatementExecuteQuery(&adbc_statement, nullptr, nullptr, &adbc_error)));

	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection_2, &adbc_statement_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement_2, query.c_str(), &adbc_error)));
	int64_t rows_affected;
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement_2, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((reinterpret_cast<const int64_t *>(arrow_array.children[0]->buffers[1])[0] == 1)));
	// Release the boys
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	// Now lets insert with Auto-Commit Off
	REQUIRE(SUCCESS(AdbcConnectionSetOption(&adbc_connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
	                                        ADBC_OPTION_VALUE_DISABLED, &adbc_error)));
	auto &input_data_2 = db.QueryArrow("SELECT 42;");
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));

	REQUIRE(
	    SUCCESS(StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TARGET_TABLE, table_name.c_str(), &adbc_error)));

	REQUIRE(SUCCESS(
	    StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_MODE, ADBC_INGEST_OPTION_MODE_APPEND, &adbc_error)));

	REQUIRE(SUCCESS(StatementBindStream(&adbc_statement, &input_data_2, &adbc_error)));

	REQUIRE(SUCCESS(StatementExecuteQuery(&adbc_statement, nullptr, nullptr, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection_2, &adbc_statement_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement_2, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement_2, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((reinterpret_cast<const int64_t *>(arrow_array.children[0]->buffers[1])[0] == 1)));
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	// If we check from con1, we should have 2
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((reinterpret_cast<const int64_t *>(arrow_array.children[0]->buffers[1])[0] == 2)));
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	// Now if we do a commit on the first connection this should be 2 on the second connection
	REQUIRE(SUCCESS(AdbcConnectionCommit(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection_2, &adbc_statement_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement_2, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement_2, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((reinterpret_cast<const int64_t *>(arrow_array.children[0]->buffers[1])[0] == 2)));
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	// Lets do a rollback
	auto &input_data_3 = db.QueryArrow("SELECT 42;");
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));

	REQUIRE(
	    SUCCESS(StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TARGET_TABLE, table_name.c_str(), &adbc_error)));

	REQUIRE(SUCCESS(
	    StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_MODE, ADBC_INGEST_OPTION_MODE_APPEND, &adbc_error)));

	REQUIRE(SUCCESS(StatementBindStream(&adbc_statement, &input_data_3, &adbc_error)));

	REQUIRE(SUCCESS(StatementExecuteQuery(&adbc_statement, nullptr, nullptr, &adbc_error)));

	// If we check from con1, we should have 3
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((reinterpret_cast<const int64_t *>(arrow_array.children[0]->buffers[1])[0] == 3)));
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	// If we check from con2 we should 2
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection_2, &adbc_statement_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement_2, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement_2, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((reinterpret_cast<const int64_t *>(arrow_array.children[0]->buffers[1])[0] == 2)));
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	// If we rollback con1, we should now have two again on con1
	REQUIRE(SUCCESS(AdbcConnectionRollback(&adbc_connection, &adbc_error)));

	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((reinterpret_cast<const int64_t *>(arrow_array.children[0]->buffers[1])[0] == 2)));
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	// Let's change the Auto commit config mid-transaction
	auto &input_data_4 = db.QueryArrow("SELECT 42;");
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));

	REQUIRE(
	    SUCCESS(StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TARGET_TABLE, table_name.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(
	    StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_MODE, ADBC_INGEST_OPTION_MODE_APPEND, &adbc_error)));

	REQUIRE(SUCCESS(StatementBindStream(&adbc_statement, &input_data_4, &adbc_error)));

	REQUIRE(SUCCESS(StatementExecuteQuery(&adbc_statement, nullptr, nullptr, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionSetOption(&adbc_connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
	                                        ADBC_OPTION_VALUE_ENABLED, &adbc_error)));

	// Now Both con1 and con2 should have 3
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection_2, &adbc_statement_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement_2, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement_2, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((reinterpret_cast<const int64_t *>(arrow_array.children[0]->buffers[1])[0] == 3)));
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((reinterpret_cast<const int64_t *>(arrow_array.children[0]->buffers[1])[0] == 3)));
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	auto &input_data_5 = db.QueryArrow("SELECT 42;");
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));

	REQUIRE(
	    SUCCESS(StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TARGET_TABLE, table_name.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(
	    StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_MODE, ADBC_INGEST_OPTION_MODE_APPEND, &adbc_error)));

	REQUIRE(SUCCESS(StatementBindStream(&adbc_statement, &input_data_5, &adbc_error)));

	REQUIRE(SUCCESS(StatementExecuteQuery(&adbc_statement, nullptr, nullptr, &adbc_error)));

	// Auto-Commit is on, so this should just be commited
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection_2, &adbc_statement_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement_2, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement_2, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((reinterpret_cast<const int64_t *>(arrow_array.children[0]->buffers[1])[0] == 4)));
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, query.c_str(), &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error)));

	arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(((reinterpret_cast<const int64_t *>(arrow_array.children[0]->buffers[1])[0] == 4)));
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));
	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement_2, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionRelease(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionRelease(&adbc_connection_2, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseRelease(&adbc_database, &adbc_error)));
}

TEST_CASE("Test ADBC Transaction Errors", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;

	AdbcError adbc_error;
	InitializeADBCError(&adbc_error);

	REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", duckdb_lib, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", ":memory:", &adbc_error)));

	REQUIRE(SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));

	// Can't commit if there is no transaction
	REQUIRE(!SUCCESS(AdbcConnectionCommit(&adbc_connection, &adbc_error)));
	REQUIRE((std::strcmp(adbc_error.message, "No active transaction, cannot commit") == 0));
	adbc_error.release(&adbc_error);

	// Can't rollback if there is no transaction
	REQUIRE(!SUCCESS(AdbcConnectionRollback(&adbc_connection, &adbc_error)));
	REQUIRE((std::strcmp(adbc_error.message, "No active transaction, cannot rollback") == 0));
	adbc_error.release(&adbc_error);

	// Try to set Commit option to random gunk
	REQUIRE(SUCCESS(!AdbcConnectionSetOption(&adbc_connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT, "bla", &adbc_error)));

	REQUIRE((std::strcmp(adbc_error.message, "Invalid connection option value adbc.connection.autocommit=bla") == 0));
	adbc_error.release(&adbc_error);

	// Let's disable the autocommit
	REQUIRE(SUCCESS(AdbcConnectionSetOption(&adbc_connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
	                                        ADBC_OPTION_VALUE_DISABLED, &adbc_error)));

	// We should succeed on committing and rolling back.
	REQUIRE(SUCCESS(AdbcConnectionCommit(&adbc_connection, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionRollback(&adbc_connection, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionRelease(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseRelease(&adbc_database, &adbc_error)));
}

TEST_CASE("Test ADBC ConnectionGetTableSchema", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;

	AdbcError adbc_error;
	InitializeADBCError(&adbc_error);

	ArrowSchema arrow_schema;
	REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", duckdb_lib, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", ":memory:", &adbc_error)));

	REQUIRE(SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));

	// Test successful schema return
	REQUIRE(SUCCESS(
	    AdbcConnectionGetTableSchema(&adbc_connection, nullptr, "main", "duckdb_indexes", &arrow_schema, &adbc_error)));
	REQUIRE((arrow_schema.n_children == 14));
	arrow_schema.release(&arrow_schema);

	// Test Catalog Name
	REQUIRE(!SUCCESS(
	    AdbcConnectionGetTableSchema(&adbc_connection, "bla", "main", "duckdb_indexes", &arrow_schema, &adbc_error)));
	REQUIRE(StringUtil::Contains(adbc_error.message, "Catalog \"bla\" does not exist"));
	adbc_error.release(&adbc_error);

	REQUIRE(SUCCESS(AdbcConnectionGetTableSchema(&adbc_connection, "system", "main", "duckdb_indexes", &arrow_schema,
	                                             &adbc_error)));
	REQUIRE((arrow_schema.n_children == 14));
	arrow_schema.release(&arrow_schema);

	// Empty schema should be fine
	REQUIRE(SUCCESS(
	    AdbcConnectionGetTableSchema(&adbc_connection, nullptr, "", "duckdb_indexes", &arrow_schema, &adbc_error)));
	REQUIRE((arrow_schema.n_children == 14));
	arrow_schema.release(&arrow_schema);

	// Test null and empty table name
	REQUIRE(!SUCCESS(AdbcConnectionGetTableSchema(&adbc_connection, nullptr, "", nullptr, &arrow_schema, &adbc_error)));
	REQUIRE((std::strcmp(adbc_error.message, "AdbcConnectionGetTableSchema: must provide table_name") == 0));
	adbc_error.release(&adbc_error);

	REQUIRE(!SUCCESS(AdbcConnectionGetTableSchema(&adbc_connection, nullptr, "", "", &arrow_schema, &adbc_error)));
	REQUIRE((std::strcmp(adbc_error.message, "AdbcConnectionGetTableSchema: must provide table_name") == 0));
	adbc_error.release(&adbc_error);

	// Test invalid schema

	REQUIRE(!SUCCESS(
	    AdbcConnectionGetTableSchema(&adbc_connection, nullptr, "b", "duckdb_indexes", &arrow_schema, &adbc_error)));
	REQUIRE((std::strstr(adbc_error.message, "Catalog Error") != nullptr));
	adbc_error.release(&adbc_error);

	// Test invalid table
	REQUIRE(!SUCCESS(
	    AdbcConnectionGetTableSchema(&adbc_connection, nullptr, "", "duckdb_indexeeees", &arrow_schema, &adbc_error)));
	REQUIRE((std::strstr(adbc_error.message, "Catalog Error") != nullptr));
	REQUIRE(SUCCESS(AdbcConnectionRelease(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseRelease(&adbc_database, &adbc_error)));
	adbc_error.release(&adbc_error);
}

TEST_CASE("Test ADBC Prepared Statement - Prepare nop", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;

	AdbcError adbc_error;
	InitializeADBCError(&adbc_error);

	AdbcStatement adbc_statement;

	REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", duckdb_lib, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", ":memory:", &adbc_error)));

	REQUIRE(SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error)));

	// Statement Prepare is a nop for us, so it should just work, although it just does some error checking.
	REQUIRE(SUCCESS(AdbcStatementPrepare(&adbc_statement, &adbc_error)));

	REQUIRE(SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error)));

	REQUIRE(!SUCCESS(AdbcStatementPrepare(&adbc_statement, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionRelease(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseRelease(&adbc_database, &adbc_error)));
}

TEST_CASE("Test AdbcConnectionGetTableTypes", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}

	ADBCTestDatabase db("AdbcConnectionGetTableTypes.db");

	// Create Arrow Result
	auto &input_data = db.QueryArrow("SELECT 42 as value");
	// Create Table 'my_table' from the Arrow Result
	db.CreateTable("my_table", input_data);
	ArrowArrayStream arrow_stream;
	AdbcError adbc_error;
	InitializeADBCError(&adbc_error);
	REQUIRE(SUCCESS(AdbcConnectionGetTableTypes(&db.adbc_connection, &arrow_stream, &adbc_error)));
	db.CreateTable("result", arrow_stream);

	auto res = db.Query("Select * from result");
	REQUIRE((res->ColumnCount() == 1));
	REQUIRE((res->GetValue(0, 0).ToString() == "BASE TABLE"));
	adbc_error.release(&adbc_error);
}

TEST_CASE("Test Segfault Option Set", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}

	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;

	AdbcError adbc_error;
	InitializeADBCError(&adbc_error);

	REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", duckdb_lib, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", ":memory:", &adbc_error)));

	REQUIRE(SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionSetOption(&adbc_connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
	                                        ADBC_OPTION_VALUE_DISABLED, &adbc_error)));

	REQUIRE(SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error)));

	auto conn_wrapper = static_cast<DuckDBAdbcConnectionWrapper *>(adbc_connection.private_data);
	auto cconn = reinterpret_cast<Connection *>(conn_wrapper->connection);

	REQUIRE(!cconn->IsAutoCommit());

	REQUIRE(SUCCESS(AdbcConnectionRelease(&adbc_connection, &adbc_error)));
	REQUIRE(SUCCESS(AdbcDatabaseRelease(&adbc_database, &adbc_error)));
	adbc_error.release(&adbc_error);
}

TEST_CASE("Test AdbcConnectionGetObjects", "[adbc]") {
	if (!duckdb_lib) {
		return;
	}
	// Let's first try what works
	// 1. Test ADBC_OBJECT_DEPTH_CATALOGS
	{
		ADBCTestDatabase db("test_catalog_depth");
		// Create Arrow Result
		auto &input_data = db.QueryArrow("SELECT 42 as value");
		// Create Table 'my_table' from the Arrow Result
		db.CreateTable("my_table", input_data);

		AdbcError adbc_error;
		InitializeADBCError(&adbc_error);
		ArrowArrayStream arrow_stream;
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_CATALOGS, nullptr, nullptr,
		                                         nullptr, nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		auto res = db.Query("Select * from result order by catalog_name asc");
		REQUIRE((res->ColumnCount() == 2));
		REQUIRE((res->RowCount() == 3));
		REQUIRE((res->GetValue(0, 0).ToString() == "system"));
		REQUIRE((res->GetValue(0, 1).ToString() == "temp"));
		REQUIRE((res->GetValue(0, 2).ToString() == "test_catalog_depth"));
		REQUIRE((res->GetValue(1, 0).ToString() == "[]"));
		REQUIRE((res->GetValue(1, 1).ToString() == "[]"));
		REQUIRE((res->GetValue(1, 2).ToString() == "[]"));
		db.Query("Drop table result;");

		// Test Filters
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_CATALOGS, "bla", nullptr,
		                                         nullptr, nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		res = db.Query("Select * from result order by catalog_name asc");
		REQUIRE((res->ColumnCount() == 2));
		REQUIRE((res->RowCount() == 0));
		db.Query("Drop table result;");
	}
	// 2. Test ADBC_OBJECT_DEPTH_DB_SCHEMAS
	{
		ADBCTestDatabase db("ADBC_OBJECT_DEPTH_DB_SCHEMAS.db");
		// Create Arrow Result
		auto &input_data = db.QueryArrow("SELECT 42 as value");
		// Create Table 'my_table' from the Arrow Result
		db.CreateTable("my_table", input_data);

		AdbcError adbc_error;
		InitializeADBCError(&adbc_error);
		ArrowArrayStream arrow_stream;

		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_DB_SCHEMAS, nullptr, nullptr,
		                                         nullptr, nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		auto res = db.Query("Select * from result order by catalog_name asc");
		REQUIRE((res->ColumnCount() == 2));
		REQUIRE((res->RowCount() == 3));
		REQUIRE((res->GetValue(0, 0).ToString() == "ADBC_OBJECT_DEPTH_DB_SCHEMAS"));
		REQUIRE((res->GetValue(0, 1).ToString() == "system"));
		REQUIRE((res->GetValue(0, 2).ToString() == "temp"));
		string expected = R"([
		    {
		        'db_schema_name': information_schema,
		        'db_schema_tables': []
		    },
		    {
		        'db_schema_name': main,
		        'db_schema_tables': []
		    },
		    {
		        'db_schema_name': pg_catalog,
		        'db_schema_tables': []
		    }
		])";
		REQUIRE(res->GetValue(1, 0).ToString() == "[{'db_schema_name': main, 'db_schema_tables': []}]");
		REQUIRE((StringUtil::Replace(res->GetValue(1, 1).ToString(), " ", "") ==
		         StringUtil::Replace(StringUtil::Replace(StringUtil::Replace(expected, "\n", ""), "\t", ""), " ", "")));
		REQUIRE(res->GetValue(1, 2).ToString() == "[{'db_schema_name': main, 'db_schema_tables': []}]");
		db.Query("Drop table result;");

		// Test Filters
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_DB_SCHEMAS, "bla", nullptr,
		                                         nullptr, nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		res = db.Query("Select * from result order by catalog_name asc");
		REQUIRE((res->ColumnCount() == 2));
		REQUIRE((res->RowCount() == 0));
		db.Query("Drop table result;");

		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_DB_SCHEMAS, nullptr, "bla",
		                                         nullptr, nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		res = db.Query("Select * from result order by catalog_name asc");
		REQUIRE((res->ColumnCount() == 2));
		REQUIRE((res->RowCount() == 3));
		REQUIRE((res->GetValue(1, 0).ToString() == "NULL"));
		REQUIRE((res->GetValue(1, 1).ToString() == "NULL"));
		REQUIRE((res->GetValue(1, 2).ToString() == "NULL"));
		db.Query("Drop table result;");
	}
	// 3. Test ADBC_OBJECT_DEPTH_TABLES
	{
		ADBCTestDatabase db("test_table_depth");
		// Create Arrow Result
		auto &input_data = db.QueryArrow("SELECT 42 as value");
		// Create Table 'my_table' from the Arrow Result
		db.CreateTable("my_table", input_data);
		// Create View 'my_view'
		db.Query("Create view my_view as from my_table;");

		AdbcError adbc_error;
		InitializeADBCError(&adbc_error);
		ArrowArrayStream arrow_stream;
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_TABLES, nullptr, nullptr,
		                                         nullptr, nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		auto res = db.Query(R"(
			SELECT
				catalog_name,
				list_sort(
					list_transform(
						catalog_db_schemas,
						lambda catalog_db_schema: {
							db_schema_name: catalog_db_schema.db_schema_name,
							db_schema_tables: list_sort(catalog_db_schema.db_schema_tables),
						}
					)
				)
			FROM result
			ORDER BY catalog_name ASC
		)");
		REQUIRE((res->ColumnCount() == 2));
		REQUIRE((res->RowCount() == 3));
		REQUIRE((res->GetValue(0, 0).ToString() == "system"));
		REQUIRE((res->GetValue(0, 1).ToString() == "temp"));
		REQUIRE((res->GetValue(0, 2).ToString() == "test_table_depth"));
		string expected_result_1 = R"({
                'db_schema_name': information_schema,
                'db_schema_tables': NULL
            })";
		string expected_result_2 = R"({
                'db_schema_name': main,
                'db_schema_tables': NULL
            })";
		string expected_result_3 = R"({
                'db_schema_name': main,
                'db_schema_tables': [
                    {
                        'table_name': my_table,
                        'table_type': BASE TABLE,
                        'table_columns': [],
                        'table_constraints': []
                    },
                    {
                        'table_name': my_view,
                        'table_type': VIEW,
                        'table_columns': [],
                        'table_constraints': []
                    }
                ]
            })";
		string expected_1_clean = StringUtil::Replace(
		    StringUtil::Replace(StringUtil::Replace(expected_result_1, "\n", ""), "\t", ""), " ", "");
		string expected_2_clean = StringUtil::Replace(
		    StringUtil::Replace(StringUtil::Replace(expected_result_2, "\n", ""), "\t", ""), " ", "");
		string expected_3_clean = StringUtil::Replace(
		    StringUtil::Replace(StringUtil::Replace(expected_result_3, "\n", ""), "\t", ""), " ", "");
		REQUIRE((StringUtil::Replace(res->GetValue(1, 0).ToString(), " ", "").find(expected_1_clean) != string::npos));
		REQUIRE((StringUtil::Replace(res->GetValue(1, 1).ToString(), " ", "").find(expected_2_clean) != string::npos));
		REQUIRE((StringUtil::Replace(res->GetValue(1, 2).ToString(), " ", "").find(expected_3_clean) != string::npos));
		db.Query("Drop table result;");

		// Test Filters

		// catalog
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_TABLES, "bla", nullptr, nullptr,
		                                         nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		res = db.Query("Select * from result order by catalog_name asc");
		REQUIRE((res->ColumnCount() == 2));
		REQUIRE((res->RowCount() == 0));
		db.Query("Drop table result;");

		// db_schema
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_TABLES, nullptr, "bla", nullptr,
		                                         nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		res = db.Query("Select * from result order by catalog_name asc");
		REQUIRE((res->ColumnCount() == 2));
		REQUIRE((res->RowCount() == 3));
		REQUIRE((res->GetValue(1, 0).ToString() == "NULL"));
		REQUIRE((res->GetValue(1, 1).ToString() == "NULL"));
		REQUIRE((res->GetValue(1, 2).ToString() == "NULL"));
		db.Query("Drop table result;");

		// table_name
		string select_catalog_db_schemas = R"(
			SELECT
				list_sort(
					list_transform(
						catalog_db_schemas,
						lambda catalog_db_schema: {
							db_schema_name: catalog_db_schema.db_schema_name,
							db_schema_tables: list_sort(catalog_db_schema.db_schema_tables),
						}
					)
				) as catalog_db_schemas
			FROM result
			WHERE catalog_name == 'test_table_depth'
        )";
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_TABLES, nullptr, nullptr, "bla",
		                                         nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		res = db.Query(select_catalog_db_schemas);
		REQUIRE((res->ColumnCount() == 1));
		REQUIRE((res->RowCount() == 1));
		string expected = "[{'db_schema_name': main, 'db_schema_tables': NULL}]";
		REQUIRE((res->GetValue(0, 0).ToString() == expected));
		db.Query("Drop table result;");

		{
			// table_type: Empty table_type returns all tables and views
			std::vector<const char *> table_type = {nullptr};
			REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_TABLES, nullptr, nullptr,
			                                         nullptr, table_type.data(), nullptr, &arrow_stream, &adbc_error)));
			db.CreateTable("result", arrow_stream);
			res = db.Query(select_catalog_db_schemas);
			REQUIRE((res->ColumnCount() == 1));
			REQUIRE((res->RowCount() == 1));
			REQUIRE(
			    (StringUtil::Replace(res->GetValue(0, 0).ToString(), " ", "").find(expected_3_clean) != string::npos));
			db.Query("Drop table result;");
		}

		{
			// table_type: ["BASE TABLE", "VIEW"] returns all tables and views
			std::vector<const char *> table_type = {"BASE TABLE", "VIEW", nullptr};
			REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_TABLES, nullptr, nullptr,
			                                         nullptr, table_type.data(), nullptr, &arrow_stream, &adbc_error)));
			db.CreateTable("result", arrow_stream);
			res = db.Query(select_catalog_db_schemas);
			REQUIRE((res->ColumnCount() == 1));
			REQUIRE((res->RowCount() == 1));
			REQUIRE(
			    (StringUtil::Replace(res->GetValue(0, 0).ToString(), " ", "").find(expected_3_clean) != string::npos));
			db.Query("Drop table result;");
		}

		{
			// table_type: BASE TABLE returns all tables
			std::vector<const char *> table_type = {"BASE TABLE", nullptr};
			REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_TABLES, nullptr, nullptr,
			                                         nullptr, table_type.data(), nullptr, &arrow_stream, &adbc_error)));
			db.CreateTable("result", arrow_stream);
			res = db.Query(select_catalog_db_schemas);
			REQUIRE((res->ColumnCount() == 1));
			REQUIRE((res->RowCount() == 1));
			string expected_result = R"([{
                'db_schema_name': main,
                'db_schema_tables': [
                    {
                        'table_name': my_table,
                        'table_type': BASE TABLE,
                        'table_columns': [],
                        'table_constraints': []
                    }
                ]
            }])";
			string expected_clean = StringUtil::Replace(
			    StringUtil::Replace(StringUtil::Replace(expected_result, "\n", ""), "\t", ""), " ", "");
			REQUIRE((StringUtil::Replace(res->GetValue(0, 0).ToString(), " ", "") == expected_clean));
			db.Query("Drop table result;");
		}

		{
			// table_type: VIEW returns all views
			std::vector<const char *> table_type = {"VIEW", nullptr};
			REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_TABLES, nullptr, nullptr,
			                                         nullptr, table_type.data(), nullptr, &arrow_stream, &adbc_error)));
			db.CreateTable("result", arrow_stream);
			res = db.Query(select_catalog_db_schemas);
			REQUIRE((res->ColumnCount() == 1));
			REQUIRE((res->RowCount() == 1));
			string expected_result = R"([{
                'db_schema_name': main,
                'db_schema_tables': [
                    {
                        'table_name': my_view,
                        'table_type': VIEW,
                        'table_columns': [],
                        'table_constraints': []
                    }
                ]
            }])";
			string expected_clean = StringUtil::Replace(
			    StringUtil::Replace(StringUtil::Replace(expected_result, "\n", ""), "\t", ""), " ", "");
			REQUIRE((StringUtil::Replace(res->GetValue(0, 0).ToString(), " ", "") == expected_clean));
			db.Query("Drop table result;");
		}
	}
	// 4. Test ADBC_OBJECT_DEPTH_COLUMNS
	{
		ADBCTestDatabase db("test_column_depth");
		// Create Arrow Result
		auto &input_data = db.QueryArrow("SELECT 42 as value");
		// Create Table 'my_table' from the Arrow Result
		db.CreateTable("my_table", input_data);
		// Create View 'my_view'
		db.Query("Create view my_view as from my_table;");

		AdbcError adbc_error;
		InitializeADBCError(&adbc_error);
		ArrowArrayStream arrow_stream;
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, nullptr,
		                                         nullptr, nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		auto res = db.Query(R"(
			SELECT
				catalog_name,
				list_sort(
					list_transform(
						catalog_db_schemas,
						lambda catalog_db_schema: {
							db_schema_name: catalog_db_schema.db_schema_name,
							db_schema_tables: list_sort(catalog_db_schema.db_schema_tables),
						}
					)
				)
			FROM result
			ORDER BY catalog_name ASC
		)");
		REQUIRE((res->ColumnCount() == 2));
		REQUIRE((res->RowCount() == 3));
		REQUIRE((res->GetValue(0, 0).ToString() == "system"));
		REQUIRE((res->GetValue(0, 1).ToString() == "temp"));
		REQUIRE((res->GetValue(0, 2).ToString() == "test_column_depth"));
		string expected_1 = R"(
            {
                'db_schema_name': information_schema,
                'db_schema_tables': NULL
            })";
		string expected_2 = R"(
            {
                'db_schema_name': main,
                'db_schema_tables': NULL
            })";
		string expected_3 = R"(
            {
                'db_schema_name': main,
                'db_schema_tables': [
                    {
                        'table_name': my_table,
                        'table_type': BASE TABLE,
                        'table_columns': [
                            {
                                'column_name': value,
                                'ordinal_position': 1,
                                'remarks': '',
                                'xdbc_data_type': NULL,
                                'xdbc_type_name': NULL,
                                'xdbc_column_size': NULL,
                                'xdbc_decimal_digits': NULL,
                                'xdbc_num_prec_radix': NULL,
                                'xdbc_nullable': NULL,
                                'xdbc_column_def': NULL,
                                'xdbc_sql_data_type': NULL,
                                'xdbc_datetime_sub': NULL,
                                'xdbc_char_octet_length': NULL,
                                'xdbc_is_nullable': NULL,
                                'xdbc_scope_catalog': NULL,
                                'xdbc_scope_schema': NULL,
                                'xdbc_scope_table': NULL,
                                'xdbc_is_autoincrement': NULL,
                                'xdbc_is_generatedcolumn': NULL
                            }
                        ],
                        'table_constraints': NULL
                    },
                    {
                        'table_name': my_view,
                        'table_type': VIEW,
                        'table_columns': [
                            {
                                'column_name': value,
                                'ordinal_position': 1,
                                'remarks': '',
                                'xdbc_data_type': NULL,
                                'xdbc_type_name': NULL,
                                'xdbc_column_size': NULL,
                                'xdbc_decimal_digits': NULL,
                                'xdbc_num_prec_radix': NULL,
                                'xdbc_nullable': NULL,
                                'xdbc_column_def': NULL,
                                'xdbc_sql_data_type': NULL,
                                'xdbc_datetime_sub': NULL,
                                'xdbc_char_octet_length': NULL,
                                'xdbc_is_nullable': NULL,
                                'xdbc_scope_catalog': NULL,
                                'xdbc_scope_schema': NULL,
                                'xdbc_scope_table': NULL,
                                'xdbc_is_autoincrement': NULL,
                                'xdbc_is_generatedcolumn': NULL
                            }
                        ],
                        'table_constraints': NULL
                    }
                ]
            })";
		string expected[3];
		expected[0] =
		    StringUtil::Replace(StringUtil::Replace(StringUtil::Replace(expected_1, "\n", ""), "\t", ""), " ", "");
		expected[1] =
		    StringUtil::Replace(StringUtil::Replace(StringUtil::Replace(expected_2, "\n", ""), "\t", ""), " ", "");
		expected[2] =
		    StringUtil::Replace(StringUtil::Replace(StringUtil::Replace(expected_3, "\n", ""), "\t", ""), " ", "");
		REQUIRE((StringUtil::Replace(res->GetValue(1, 0).ToString(), " ", "").find(expected[0]) != string::npos));
		REQUIRE((StringUtil::Replace(res->GetValue(1, 1).ToString(), " ", "").find(expected[1]) != string::npos));
		REQUIRE((StringUtil::Replace(res->GetValue(1, 2).ToString(), " ", "").find(expected[2]) != string::npos));
		db.Query("Drop table result;");

		// Test Filters

		// catalog
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, "bla", nullptr,
		                                         nullptr, nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		res = db.Query("Select * from result order by catalog_name asc");
		REQUIRE((res->ColumnCount() == 2));
		REQUIRE((res->RowCount() == 0));
		db.Query("Drop table result;");

		// db_schema
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, "bla",
		                                         nullptr, nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		res = db.Query("Select * from result order by catalog_name asc");
		REQUIRE((res->ColumnCount() == 2));
		REQUIRE((res->RowCount() == 3));
		REQUIRE((res->GetValue(1, 0).ToString() == "NULL"));
		REQUIRE((res->GetValue(1, 1).ToString() == "NULL"));
		REQUIRE((res->GetValue(1, 2).ToString() == "NULL"));
		db.Query("Drop table result;");

		// table_name
		string select_catalog_db_schemas = R"(
			SELECT
				list_sort(
					list_transform(
						catalog_db_schemas,
						lambda catalog_db_schema: {
							db_schema_name: catalog_db_schema.db_schema_name,
							db_schema_tables: list_sort(catalog_db_schema.db_schema_tables),
						}
					)
				) as catalog_db_schemas
			FROM result
			WHERE catalog_name == 'test_column_depth'
        )";
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, nullptr,
		                                         "bla", nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		res = db.Query(select_catalog_db_schemas);
		REQUIRE((res->ColumnCount() == 1));
		REQUIRE((res->RowCount() == 1));
		REQUIRE((res->GetValue(0, 0).ToString() == "[{'db_schema_name': main, 'db_schema_tables': NULL}]"));
		db.Query("Drop table result;");

		// column_name
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, nullptr,
		                                         nullptr, nullptr, "bla", &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		res = db.Query(select_catalog_db_schemas);
		auto expected_value = "[{'db_schema_name': main, "
		                      "'db_schema_tables': [{'table_name': my_table, 'table_type': BASE TABLE, "
		                      "'table_columns': NULL, 'table_constraints': NULL}, "
		                      "{'table_name': my_view, 'table_type': VIEW, "
		                      "'table_columns': NULL, 'table_constraints': NULL}]}]";
		REQUIRE((res->GetValue(0, 0).ToString() == expected_value));
		db.Query("Drop table result;");

		{
			// table_type: Empty table_type returns all tables and views
			std::vector<const char *> table_type = {nullptr};
			REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, nullptr,
			                                         nullptr, table_type.data(), nullptr, &arrow_stream, &adbc_error)));
			db.CreateTable("result", arrow_stream);
			res = db.Query(select_catalog_db_schemas);
			REQUIRE((res->ColumnCount() == 1));
			REQUIRE((res->RowCount() == 1));
			REQUIRE((StringUtil::Replace(res->GetValue(0, 0).ToString(), " ", "").find(expected[2]) != string::npos));
			db.Query("Drop table result;");
		}

		{
			// table_type: ["BASE TABLE", "VIEW"] returns all tables and views
			std::vector<const char *> table_type = {"BASE TABLE", "VIEW", nullptr};
			REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, nullptr,
			                                         nullptr, table_type.data(), nullptr, &arrow_stream, &adbc_error)));
			db.CreateTable("result", arrow_stream);
			res = db.Query(select_catalog_db_schemas);
			REQUIRE((res->ColumnCount() == 1));
			REQUIRE((res->RowCount() == 1));
			REQUIRE((StringUtil::Replace(res->GetValue(0, 0).ToString(), " ", "").find(expected[2]) != string::npos));
			db.Query("Drop table result;");
		}

		{
			// table_type: BASE TABLE returns all tables
			std::vector<const char *> table_type = {"BASE TABLE", nullptr};
			REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, nullptr,
			                                         nullptr, table_type.data(), nullptr, &arrow_stream, &adbc_error)));
			db.CreateTable("result", arrow_stream);
			res = db.Query(select_catalog_db_schemas);
			REQUIRE((res->ColumnCount() == 1));
			REQUIRE((res->RowCount() == 1));
			string expected_result = R"({
                'db_schema_name': main,
                'db_schema_tables': [
                    {
                        'table_name': my_table,
                        'table_type': BASE TABLE,
                        'table_columns': [
                            {
                                'column_name': value,
                                'ordinal_position': 1,
                                'remarks': '',
                                'xdbc_data_type': NULL,
                                'xdbc_type_name': NULL,
                                'xdbc_column_size': NULL,
                                'xdbc_decimal_digits': NULL,
                                'xdbc_num_prec_radix': NULL,
                                'xdbc_nullable': NULL,
                                'xdbc_column_def': NULL,
                                'xdbc_sql_data_type': NULL,
                                'xdbc_datetime_sub': NULL,
                                'xdbc_char_octet_length': NULL,
                                'xdbc_is_nullable': NULL,
                                'xdbc_scope_catalog': NULL,
                                'xdbc_scope_schema': NULL,
                                'xdbc_scope_table': NULL,
                                'xdbc_is_autoincrement': NULL,
                                'xdbc_is_generatedcolumn': NULL
                            }
                        ],
                        'table_constraints': NULL
                    }
                ]
            })";
			string expected_clean = StringUtil::Replace(
			    StringUtil::Replace(StringUtil::Replace(expected_result, "\n", ""), "\t", ""), " ", "");
			REQUIRE(
			    (StringUtil::Replace(res->GetValue(0, 0).ToString(), " ", "").find(expected_clean) != string::npos));
			db.Query("Drop table result;");
		}

		{
			// table_type: VIEW returns all views
			std::vector<const char *> table_type = {"VIEW", nullptr};
			REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, nullptr,
			                                         nullptr, table_type.data(), nullptr, &arrow_stream, &adbc_error)));
			db.CreateTable("result", arrow_stream);
			res = db.Query(select_catalog_db_schemas);
			REQUIRE((res->ColumnCount() == 1));
			REQUIRE((res->RowCount() == 1));
			string expected_result = R"({
                'db_schema_name': main,
                'db_schema_tables': [
                    {
                        'table_name': my_view,
                        'table_type': VIEW,
                        'table_columns': [
                            {
                                'column_name': value,
                                'ordinal_position': 1,
                                'remarks': '',
                                'xdbc_data_type': NULL,
                                'xdbc_type_name': NULL,
                                'xdbc_column_size': NULL,
                                'xdbc_decimal_digits': NULL,
                                'xdbc_num_prec_radix': NULL,
                                'xdbc_nullable': NULL,
                                'xdbc_column_def': NULL,
                                'xdbc_sql_data_type': NULL,
                                'xdbc_datetime_sub': NULL,
                                'xdbc_char_octet_length': NULL,
                                'xdbc_is_nullable': NULL,
                                'xdbc_scope_catalog': NULL,
                                'xdbc_scope_schema': NULL,
                                'xdbc_scope_table': NULL,
                                'xdbc_is_autoincrement': NULL,
                                'xdbc_is_generatedcolumn': NULL
                            }
                        ],
                        'table_constraints': NULL
                    }
                ]
            })";
			string expected_clean = StringUtil::Replace(
			    StringUtil::Replace(StringUtil::Replace(expected_result, "\n", ""), "\t", ""), " ", "");
			REQUIRE(
			    (StringUtil::Replace(res->GetValue(0, 0).ToString(), " ", "").find(expected_clean) != string::npos));
			db.Query("Drop table result;");
		}
	}
	// 5. Test ADBC_OBJECT_DEPTH_ALL
	{
		ADBCTestDatabase db("test_all_depth");
		// Create Table 'my_table' with primary key
		db.Query("Create table my_table (a int, b int, primary key (a, b))");
		// Create View 'my_view'
		db.Query("Create view my_view as from my_table;");

		AdbcError adbc_error;
		InitializeADBCError(&adbc_error);
		ArrowArrayStream arrow_stream;
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_ALL, nullptr, nullptr, nullptr,
		                                         nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		auto res = db.Query(R"(
			SELECT
				catalog_name,
				list_sort(
					list_transform(
						catalog_db_schemas,
						lambda catalog_db_schema: {
							db_schema_name: catalog_db_schema.db_schema_name,
							db_schema_tables: list_sort(catalog_db_schema.db_schema_tables),
						}
					)
				)
			FROM result
			ORDER BY catalog_name ASC
		)");
		REQUIRE((res->RowCount() == 3));
		REQUIRE((res->GetValue(0, 0).ToString() == "system"));
		REQUIRE((res->GetValue(0, 1).ToString() == "temp"));
		REQUIRE((res->GetValue(0, 2).ToString() == "test_all_depth"));
		string expected_1 = R"(
            {
                'db_schema_name': information_schema,
                'db_schema_tables': NULL
            })";
		string expected_2 = R"(
            {
                'db_schema_name': main,
                'db_schema_tables': NULL
            })";
		string expected_3 = R"(
            {
                'db_schema_name': main,
                'db_schema_tables': [
                    {
                        'table_name': my_table,
                        'table_type': BASE TABLE,
                        'table_columns': [
                            {
                                'column_name': a,
                                'ordinal_position': 1,
                                'remarks': '',
                                'xdbc_data_type': NULL,
                                'xdbc_type_name': NULL,
                                'xdbc_column_size': NULL,
                                'xdbc_decimal_digits': NULL,
                                'xdbc_num_prec_radix': NULL,
                                'xdbc_nullable': NULL,
                                'xdbc_column_def': NULL,
                                'xdbc_sql_data_type': NULL,
                                'xdbc_datetime_sub': NULL,
                                'xdbc_char_octet_length': NULL,
                                'xdbc_is_nullable': NULL,
                                'xdbc_scope_catalog': NULL,
                                'xdbc_scope_schema': NULL,
                                'xdbc_scope_table': NULL,
                                'xdbc_is_autoincrement': NULL,
                                'xdbc_is_generatedcolumn': NULL
                            },
                            {
                                'column_name': b,
                                'ordinal_position': 2,
                                'remarks': '',
                                'xdbc_data_type': NULL,
                                'xdbc_type_name': NULL,
                                'xdbc_column_size': NULL,
                                'xdbc_decimal_digits': NULL,
                                'xdbc_num_prec_radix': NULL,
                                'xdbc_nullable': NULL,
                                'xdbc_column_def': NULL,
                                'xdbc_sql_data_type': NULL,
                                'xdbc_datetime_sub': NULL,
                                'xdbc_char_octet_length': NULL,
                                'xdbc_is_nullable': NULL,
                                'xdbc_scope_catalog': NULL,
                                'xdbc_scope_schema': NULL,
                                'xdbc_scope_table': NULL,
                                'xdbc_is_autoincrement': NULL,
                                'xdbc_is_generatedcolumn': NULL
                            }
                        ],
                        'table_constraints': [
                            {
                                'constraint_name': my_table_a_b_pkey,
                                'constraint_type': PRIMARY KEY,
                                'constraint_column_names': [a, b],
                                'constraint_column_usage': []
                            }
                        ]
                    },
                    {
                        'table_name': my_view,
                        'table_type': VIEW,
                        'table_columns': [
                            {
                                'column_name': a,
                                'ordinal_position': 1,
                                'remarks': '',
                                'xdbc_data_type': NULL,
                                'xdbc_type_name': NULL,
                                'xdbc_column_size': NULL,
                                'xdbc_decimal_digits': NULL,
                                'xdbc_num_prec_radix': NULL,
                                'xdbc_nullable': NULL,
                                'xdbc_column_def': NULL,
                                'xdbc_sql_data_type': NULL,
                                'xdbc_datetime_sub': NULL,
                                'xdbc_char_octet_length': NULL,
                                'xdbc_is_nullable': NULL,
                                'xdbc_scope_catalog': NULL,
                                'xdbc_scope_schema': NULL,
                                'xdbc_scope_table': NULL,
                                'xdbc_is_autoincrement': NULL,
                                'xdbc_is_generatedcolumn': NULL
                            },
                            {
                                'column_name': b,
                                'ordinal_position': 2,
                                'remarks': '',
                                'xdbc_data_type': NULL,
                                'xdbc_type_name': NULL,
                                'xdbc_column_size': NULL,
                                'xdbc_decimal_digits': NULL,
                                'xdbc_num_prec_radix': NULL,
                                'xdbc_nullable': NULL,
                                'xdbc_column_def': NULL,
                                'xdbc_sql_data_type': NULL,
                                'xdbc_datetime_sub': NULL,
                                'xdbc_char_octet_length': NULL,
                                'xdbc_is_nullable': NULL,
                                'xdbc_scope_catalog': NULL,
                                'xdbc_scope_schema': NULL,
                                'xdbc_scope_table': NULL,
                                'xdbc_is_autoincrement': NULL,
                                'xdbc_is_generatedcolumn': NULL
                            }
                        ],
                        'table_constraints': NULL
                    }
                ]
            })";
		string expected_1_clean =
		    StringUtil::Replace(StringUtil::Replace(StringUtil::Replace(expected_1, "\n", ""), "\t", ""), " ", "");
		string expected_2_clean =
		    StringUtil::Replace(StringUtil::Replace(StringUtil::Replace(expected_2, "\n", ""), "\t", ""), " ", "");
		string expected_3_clean =
		    StringUtil::Replace(StringUtil::Replace(StringUtil::Replace(expected_3, "\n", ""), "\t", ""), " ", "");
		REQUIRE((StringUtil::Replace(res->GetValue(1, 0).ToString(), " ", "").find(expected_1_clean) != string::npos));
		REQUIRE((StringUtil::Replace(res->GetValue(1, 1).ToString(), " ", "").find(expected_2_clean) != string::npos));
		REQUIRE((StringUtil::Replace(res->GetValue(1, 2).ToString(), " ", "").find(expected_3_clean) != string::npos));
		db.Query("Drop table result;");

		// Test Filters

		// catalog
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, "bla", nullptr,
		                                         nullptr, nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		res = db.Query("Select * from result order by catalog_name asc");
		REQUIRE((res->ColumnCount() == 2));
		REQUIRE((res->RowCount() == 0));
		db.Query("Drop table result;");

		// db_schema
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, "bla",
		                                         nullptr, nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		res = db.Query("Select * from result order by catalog_name asc");
		REQUIRE((res->ColumnCount() == 2));
		REQUIRE((res->RowCount() == 3));
		REQUIRE((res->GetValue(1, 0).ToString() == "NULL"));
		REQUIRE((res->GetValue(1, 1).ToString() == "NULL"));
		REQUIRE((res->GetValue(1, 2).ToString() == "NULL"));
		db.Query("Drop table result;");

		// table_name
		string select_catalog_db_schemas = R"(
			SELECT
				list_sort(
					list_transform(
						catalog_db_schemas,
						lambda catalog_db_schema: {
							db_schema_name: catalog_db_schema.db_schema_name,
							db_schema_tables: list_sort(catalog_db_schema.db_schema_tables),
						}
					)
				) as catalog_db_schemas
			FROM result
			WHERE catalog_name == 'test_all_depth'
        )";
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, nullptr,
		                                         "bla", nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		res = db.Query(select_catalog_db_schemas);
		REQUIRE((res->RowCount() == 1));
		string expected = "[{'db_schema_name': main, 'db_schema_tables': NULL}]";
		REQUIRE((res->GetValue(0, 0).ToString() == expected));
		db.Query("Drop table result;");

		// column_name
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, nullptr,
		                                         nullptr, nullptr, "bla", &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		res = db.Query(select_catalog_db_schemas);
		REQUIRE((res->RowCount() == 1));
		// clang-format off
		expected = "["
			"{"
				"'db_schema_name': main, "
				"'db_schema_tables': ["
					"{"
						"'table_name': my_table, "
						"'table_type': BASE TABLE, "
						"'table_columns': NULL, "
						"'table_constraints': NULL"
					"}, "
					"{"
						"'table_name': my_view, "
						"'table_type': VIEW, "
						"'table_columns': NULL, "
						"'table_constraints': NULL"
					"}"
				"]"
			"}"
		"]";
		// clang-format on
		REQUIRE((res->GetValue(0, 0).ToString() == expected));
		db.Query("Drop table result;");

		{
			// table_type: Empty table_type returns all tables and views
			std::vector<const char *> table_type = {nullptr};
			REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, nullptr,
			                                         nullptr, table_type.data(), nullptr, &arrow_stream, &adbc_error)));
			db.CreateTable("result", arrow_stream);
			res = db.Query(select_catalog_db_schemas);
			REQUIRE((res->ColumnCount() == 1));
			REQUIRE((res->RowCount() == 1));
			REQUIRE((StringUtil::Replace(res->GetValue(0, 0).ToString(), " ", "").find(expected[2]) != string::npos));
			db.Query("Drop table result;");
		}

		{
			// table_type: ["BASE TABLE", "VIEW"] returns all tables and views
			std::vector<const char *> table_type = {"BASE TABLE", "VIEW", nullptr};
			REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, nullptr,
			                                         nullptr, table_type.data(), nullptr, &arrow_stream, &adbc_error)));
			db.CreateTable("result", arrow_stream);
			res = db.Query(select_catalog_db_schemas);
			REQUIRE((res->ColumnCount() == 1));
			REQUIRE((res->RowCount() == 1));
			REQUIRE((StringUtil::Replace(res->GetValue(0, 0).ToString(), " ", "").find(expected[2]) != string::npos));
			db.Query("Drop table result;");
		}

		{
			// table_type: BASE TABLE returns all tables
			std::vector<const char *> table_type = {"BASE TABLE", nullptr};
			REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, nullptr,
			                                         nullptr, table_type.data(), nullptr, &arrow_stream, &adbc_error)));
			db.CreateTable("result", arrow_stream);
			res = db.Query(select_catalog_db_schemas);
			REQUIRE((res->ColumnCount() == 1));
			REQUIRE((res->RowCount() == 1));
			string expected_result = R"([{
                'db_schema_name': main,
                'db_schema_tables': [
                    {
                        'table_name': my_table,
                        'table_type': BASE TABLE,
                        'table_columns': [
                            {
                                'column_name': a,
                                'ordinal_position': 1,
                                'remarks': '',
                                'xdbc_data_type': NULL,
                                'xdbc_type_name': NULL,
                                'xdbc_column_size': NULL,
                                'xdbc_decimal_digits': NULL,
                                'xdbc_num_prec_radix': NULL,
                                'xdbc_nullable': NULL,
                                'xdbc_column_def': NULL,
                                'xdbc_sql_data_type': NULL,
                                'xdbc_datetime_sub': NULL,
                                'xdbc_char_octet_length': NULL,
                                'xdbc_is_nullable': NULL,
                                'xdbc_scope_catalog': NULL,
                                'xdbc_scope_schema': NULL,
                                'xdbc_scope_table': NULL,
                                'xdbc_is_autoincrement': NULL,
                                'xdbc_is_generatedcolumn': NULL
                            },
                            {
                                'column_name': b,
                                'ordinal_position': 2,
                                'remarks': '',
                                'xdbc_data_type': NULL,
                                'xdbc_type_name': NULL,
                                'xdbc_column_size': NULL,
                                'xdbc_decimal_digits': NULL,
                                'xdbc_num_prec_radix': NULL,
                                'xdbc_nullable': NULL,
                                'xdbc_column_def': NULL,
                                'xdbc_sql_data_type': NULL,
                                'xdbc_datetime_sub': NULL,
                                'xdbc_char_octet_length': NULL,
                                'xdbc_is_nullable': NULL,
                                'xdbc_scope_catalog': NULL,
                                'xdbc_scope_schema': NULL,
                                'xdbc_scope_table': NULL,
                                'xdbc_is_autoincrement': NULL,
                                'xdbc_is_generatedcolumn': NULL
                            }
                        ],
                        'table_constraints': [
                            {
                                'constraint_name': my_table_a_b_pkey,
                                'constraint_type': PRIMARY KEY,
                                'constraint_column_names': [a, b],
                                'constraint_column_usage': []
                            }
                        ]
                    }
                ]
            }])";
			string expected_clean = StringUtil::Replace(
			    StringUtil::Replace(StringUtil::Replace(expected_result, "\n", ""), "\t", ""), " ", "");
			REQUIRE((StringUtil::Replace(res->GetValue(0, 0).ToString(), " ", "") == expected_clean));
			db.Query("Drop table result;");
		}

		{
			// table_type: VIEW returns all views
			std::vector<const char *> table_type = {"VIEW", nullptr};
			REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, nullptr,
			                                         nullptr, table_type.data(), nullptr, &arrow_stream, &adbc_error)));
			db.CreateTable("result", arrow_stream);
			res = db.Query(select_catalog_db_schemas);
			REQUIRE((res->ColumnCount() == 1));
			REQUIRE((res->RowCount() == 1));
			string expected_result = R"([{
                'db_schema_name': main,
                'db_schema_tables': [
                    {
                        'table_name': my_view,
                        'table_type': VIEW,
                        'table_columns': [
                            {
                                'column_name': a,
                                'ordinal_position': 1,
                                'remarks': '',
                                'xdbc_data_type': NULL,
                                'xdbc_type_name': NULL,
                                'xdbc_column_size': NULL,
                                'xdbc_decimal_digits': NULL,
                                'xdbc_num_prec_radix': NULL,
                                'xdbc_nullable': NULL,
                                'xdbc_column_def': NULL,
                                'xdbc_sql_data_type': NULL,
                                'xdbc_datetime_sub': NULL,
                                'xdbc_char_octet_length': NULL,
                                'xdbc_is_nullable': NULL,
                                'xdbc_scope_catalog': NULL,
                                'xdbc_scope_schema': NULL,
                                'xdbc_scope_table': NULL,
                                'xdbc_is_autoincrement': NULL,
                                'xdbc_is_generatedcolumn': NULL
                            },
                            {
                                'column_name': b,
                                'ordinal_position': 2,
                                'remarks': '',
                                'xdbc_data_type': NULL,
                                'xdbc_type_name': NULL,
                                'xdbc_column_size': NULL,
                                'xdbc_decimal_digits': NULL,
                                'xdbc_num_prec_radix': NULL,
                                'xdbc_nullable': NULL,
                                'xdbc_column_def': NULL,
                                'xdbc_sql_data_type': NULL,
                                'xdbc_datetime_sub': NULL,
                                'xdbc_char_octet_length': NULL,
                                'xdbc_is_nullable': NULL,
                                'xdbc_scope_catalog': NULL,
                                'xdbc_scope_schema': NULL,
                                'xdbc_scope_table': NULL,
                                'xdbc_is_autoincrement': NULL,
                                'xdbc_is_generatedcolumn': NULL
                            }
                        ],
                        'table_constraints': NULL
                    }
                ]
            }])";
			string expected_clean = StringUtil::Replace(
			    StringUtil::Replace(StringUtil::Replace(expected_result, "\n", ""), "\t", ""), " ", "");
			REQUIRE((StringUtil::Replace(res->GetValue(0, 0).ToString(), " ", "") == expected_clean));
			db.Query("Drop table result;");
		}
	}
	// 6. Test constraints
	//
	// Available constraints: https://duckdb.org/docs/stable/sql/constraints.html
	{
		ADBCTestDatabase db("test_constraints");
		// Create table 'foreign_table'
		db.Query("CREATE TABLE foreign_table (id INTEGER PRIMARY KEY)");
		// Create table 'my_table' with constraints
		db.Query(R"(
			CREATE TABLE my_table (
				primary_key INTEGER PRIMARY KEY,
				not_null INTEGER NOT NULL,
				custom_check INTEGER CHECK (custom_check > 0),
				unique_single INTEGER UNIQUE,
				unique_multi1 INTEGER,
				unique_multi2 INTEGER,
				UNIQUE (unique_multi2, unique_multi1),
				foreign_id INTEGER REFERENCES foreign_table(id)
			)
		)");

		AdbcError adbc_error;
		InitializeADBCError(&adbc_error);
		ArrowArrayStream arrow_stream;
		REQUIRE(SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, nullptr,
		                                         "my_table", nullptr, nullptr, &arrow_stream, &adbc_error)));
		db.CreateTable("result", arrow_stream);
		string select_catalog_db_schemas = R"(
			SELECT
				list_sort(
					list_transform(
						catalog_db_schemas,
						lambda catalog_db_schema: {
							db_schema_tables: list_sort(
								list_transform(
									catalog_db_schema.db_schema_tables,
									lambda db_schema_table: {
										table_name: db_schema_table.table_name,
										table_constraints: list_sort(db_schema_table.table_constraints)
									}
								)
							)
						}
					)
				) as catalog_db_schemas
			FROM result
			WHERE catalog_name == 'test_constraints'
		)";
		auto res = db.Query(select_catalog_db_schemas);
		REQUIRE((res->RowCount() == 1));
		// clang-format off
		std::string expected = "["
			"{"
				"'db_schema_tables': ["
					"{"
						"'table_name': my_table, "
						"'table_constraints': [{"
							"'constraint_name': my_table_custom_check_check, "
							"'constraint_type': CHECK, "
							"'constraint_column_names': [custom_check], "
							"'constraint_column_usage': []"
						"}, {"
							"'constraint_name': my_table_foreign_id_id_fkey, "
							"'constraint_type': FOREIGN KEY, "
							"'constraint_column_names': [foreign_id], "
							"'constraint_column_usage': [{"
								"'fk_catalog': test_constraints, "
								"'fk_db_schema': main, "
								"'fk_table': foreign_table, "
								"'fk_column_name': id"
							"}]"
						"}, {"
							"'constraint_name': my_table_primary_key_pkey, "
							"'constraint_type': PRIMARY KEY, "
							"'constraint_column_names': [primary_key], "
							"'constraint_column_usage': []"
						"}, {"
							"'constraint_name': my_table_unique_multi2_unique_multi1_key, "
							"'constraint_type': UNIQUE, "
							"'constraint_column_names': [unique_multi2, unique_multi1], "
							"'constraint_column_usage': []"
						"}, {"
							"'constraint_name': my_table_unique_single_key, "
							"'constraint_type': UNIQUE, "
							"'constraint_column_names': [unique_single], "
							"'constraint_column_usage': []"
						"}]"
					"}"
				"]"
			"}"
		"]";
		// clang-format on
		REQUIRE((res->GetValue(0, 0).ToString() == expected));
		db.Query("DROP TABLE result;");
	}
	// Now lets test some errors
	{
		ADBCTestDatabase db("test_errors");
		// Create Arrow Result
		auto &input_data = db.QueryArrow("SELECT 42 as value");
		// Create Table 'my_table' from the Arrow Result
		db.CreateTable("my_table", input_data);

		AdbcError adbc_error;
		InitializeADBCError(&adbc_error);
		ArrowArrayStream arrow_stream;

		REQUIRE(!SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, 42, nullptr, nullptr, nullptr, nullptr, nullptr,
		                                          &arrow_stream, &adbc_error)));
		REQUIRE((std::strcmp(adbc_error.message, "Invalid value of Depth") == 0));
		adbc_error.release(&adbc_error);

		// Invalid table type
		std::vector<const char *> table_type = {"INVALID", nullptr};
		REQUIRE(!SUCCESS(AdbcConnectionGetObjects(&db.adbc_connection, ADBC_OBJECT_DEPTH_ALL, nullptr, nullptr, nullptr,
		                                          table_type.data(), nullptr, &arrow_stream, &adbc_error)));
		REQUIRE((strcmp(adbc_error.message,
		                "Table type must be \"LOCAL TABLE\", \"BASE TABLE\" or \"VIEW\": \"INVALID\"") == 0));
		adbc_error.release(&adbc_error);
	}
}
} // namespace duckdb
