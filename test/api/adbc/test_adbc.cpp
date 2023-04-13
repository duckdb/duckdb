#include "catch.hpp"
#include "duckdb/common/adbc/adbc.hpp"
#include "arrow/arrow_test_helper.hpp"
#include <iostream>

using namespace std;

using namespace duckdb;

bool SUCCESS(AdbcStatusCode status) {
	return status == ADBC_STATUS_OK;
}
const char *DUCKDB_LIB = std::getenv("DUCKDB_INSTALL_LIB");

class ADBCTestDatabase {
public:
	explicit ADBCTestDatabase(const string &path_parameter = "test.db") {
		path = TestCreatePath(path_parameter);
		REQUIRE(DUCKDB_LIB);
		REQUIRE(SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error)));
		REQUIRE(SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", DUCKDB_LIB, &adbc_error)));
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

		REQUIRE(SUCCESS(adbc::StatementNew(&adbc_connection, &adbc_statement, &adbc_error)));

		REQUIRE(SUCCESS(adbc::StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TARGET_TABLE, table_name.c_str(),
		                                         &adbc_error)));

		REQUIRE(SUCCESS(adbc::StatementBindStream(&adbc_statement, &arrow_stream, &adbc_error)));

		REQUIRE(SUCCESS(adbc::StatementExecuteQuery(&adbc_statement, nullptr, nullptr, &adbc_error)));
	}

	AdbcError adbc_error;
	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;
	AdbcStatement adbc_statement;
	ArrowArrayStream arrow_stream;
	std::string path;
};

TEST_CASE("ADBC - Select 42", "[adbc]") {
	if (!DUCKDB_LIB) {
		return;
	}
	ADBCTestDatabase db;

	REQUIRE(db.QueryAndCheck("SELECT 42"));
}

TEST_CASE("ADBC - Test ingestion", "[adbc]") {
	if (!DUCKDB_LIB) {
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
	if (!DUCKDB_LIB) {
		return;
	}
	ADBCTestDatabase db;

	// Create Arrow Result
	auto input_data = db.Query("SELECT * FROM read_csv_auto(\'data/csv/lineitem-carriage.csv\')");

	// Create Table 'my_table' from the Arrow Result
	db.CreateTable("lineitem", input_data);

	REQUIRE(db.QueryAndCheck("SELECT l_partkey, l_comment FROM lineitem WHERE l_orderkey=1 ORDER BY l_linenumber"));
}
