#include "catch.hpp"
#include "duckdb/common/adbc/adbc.hpp"
#include <iostream>

using namespace std;

using namespace duckdb;

void SUCCESS(AdbcStatusCode status) {
	REQUIRE(status == ADBC_STATUS_OK);
}

class ADBCTestDatabase {
public:
	ADBCTestDatabase() {
		SUCCESS(AdbcDatabaseNew(&adbc_database, &adbc_error));
		SUCCESS(AdbcDatabaseSetOption(&adbc_database, "driver", DUCKDB_INSTALL_LIB, &adbc_error));
		SUCCESS(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error));
//		SUCCESS(AdbcDatabaseSetOption(&adbc_database, "path", ":memory:", &adbc_error));

		SUCCESS(AdbcDatabaseInit(&adbc_database, &adbc_error));

		SUCCESS(AdbcConnectionNew(&adbc_connection, &adbc_error));
		SUCCESS(AdbcConnectionInit(&adbc_connection, &adbc_database, &adbc_error));
		arrow_stream.release = nullptr;
	}

	~ADBCTestDatabase() {
		if (arrow_stream.release) {
			arrow_stream.release(&arrow_stream);
			arrow_stream.release = nullptr;
		}
		SUCCESS(AdbcStatementRelease(&adbc_statement, &adbc_error));
		SUCCESS(AdbcConnectionRelease(&adbc_connection, &adbc_error));
		SUCCESS(AdbcDatabaseRelease(&adbc_database, &adbc_error));
	}

	ArrowArrayStream &Query(string query) {
		if (arrow_stream.release) {
			arrow_stream.release(&arrow_stream);
			arrow_stream.release = nullptr;
		}
		SUCCESS(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error));
		SUCCESS(AdbcStatementSetSqlQuery(&adbc_statement, query.c_str(), &adbc_error));
		int64_t rows_affected;
		SUCCESS(AdbcStatementExecuteQuery(&adbc_statement, &arrow_stream, &rows_affected, &adbc_error));
		return arrow_stream;
	}

	AdbcError adbc_error;
	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;
	AdbcStatement adbc_statement;
	ArrowArrayStream arrow_stream;
};

TEST_CASE("ADBC - Select 42", "[adbc]") {
	ADBCTestDatabase db;

	auto result = db.Query("SELECT 42");

	ArrowArray arrow_array;
	REQUIRE(result.get_next(&result, &arrow_array) == 0);
	// This should be 42
	REQUIRE(((int *)arrow_array.children[0]->buffers[1])[0] == 42);
	arrow_array.release(&arrow_array);
}

TEST_CASE("ADBC - Test ingestion", "[adbc]") {

	ADBCTestDatabase db;
	// insert some data
	SUCCESS(adbc::StatementNew(&db.adbc_connection, &db.adbc_statement, &db.adbc_error));

//	adbc_status = adbc::StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TARGET_TABLE, "my_table", &adbc_error);
//	REQUIRE(adbc_status == ADBC_STATUS_OK);
//
//	adbc_status = adbc::StatementBindStream(&adbc_statement, &arrow_stream, &adbc_error);
//	REQUIRE(adbc_status == ADBC_STATUS_OK);
//
//	adbc_status = adbc::StatementExecuteQuery(&adbc_statement, NULL, NULL, &adbc_error);
//	REQUIRE(adbc_status == ADBC_STATUS_OK);
//
//	// see if we have anything
//
//	adbc_status = adbc::StatementNew(&adbc_connection, &adbc_statement, &adbc_error);
//	REQUIRE(adbc_status == ADBC_STATUS_OK);
//
//	adbc_status = adbc::StatementSetSqlQuery(&adbc_statement, "SELECT * FROM my_table", &adbc_error);
//	REQUIRE(adbc_status == ADBC_STATUS_OK);
//
//	adbc_status = adbc::StatementExecuteQuery(&adbc_statement, &arrow_stream, NULL, &adbc_error);
//	REQUIRE(adbc_status == ADBC_STATUS_OK);
//
//	arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
//	REQUIRE(arrow_status == 0);
//	REQUIRE(((int *)arrow_array.children[0]->buffers[1])[0] == 42);
//	arrow_array.release(&arrow_array);
//	arrow_stream.release(&arrow_stream);
//
//	adbc_status = adbc::StatementRelease(&adbc_statement, &adbc_error);
//	REQUIRE(adbc_status == ADBC_STATUS_OK);
//
//	// shut down the connection again
//	adbc_status = adbc::ConnectionRelease(&adbc_connection, &adbc_error);
//	REQUIRE(adbc_status == ADBC_STATUS_OK);
//
//	// shut down the database again
//	adbc_status = adbc::DatabaseRelease(&adbc_database, &adbc_error);
//	REQUIRE(adbc_status == ADBC_STATUS_OK);
}