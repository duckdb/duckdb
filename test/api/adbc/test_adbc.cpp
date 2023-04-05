#include "catch.hpp"
#include "duckdb/common/adbc/adbc.hpp"

using namespace std;

using namespace duckdb;

TEST_CASE("Happy path", "[adbc]") {
	AdbcStatusCode adbc_status;
	AdbcError adbc_error;
	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;
	AdbcStatement adbc_statement;
	// ArrowArrayStream arrow_stream;
	// ArrowArray arrow_array;
	// int arrow_status;

	adbc_status = adbc::DatabaseNew(&adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);
	adbc_status = adbc::DatabaseInit(&adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// connect!
	adbc_status = adbc::ConnectionNew(&adbc_connection, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);
	adbc_status = adbc::ConnectionInit(&adbc_connection, &adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);
	//
	//	// we can query catalogs
	//	adbc_status = adbc::ConnectionGetCatalogs(&adbc_connection, &adbc_statement, &adbc_error);
	//	REQUIRE(adbc_status == ADBC_STATUS_OK);
	//
	//	adbc_status = adbc::StatementGetStream(&adbc_statement, &arrow_stream, &adbc_error);
	//	REQUIRE(adbc_status == ADBC_STATUS_OK);
	//
	//	arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
	//	REQUIRE(arrow_status == 0);
	//	REQUIRE((arrow_array.n_children == 1 && arrow_array.children[0]->length == 1));
	//
	//	arrow_array.release(&arrow_array);
	//	arrow_stream.release(&arrow_stream);
	//
	//	adbc_status = adbc::StatementRelease(&adbc_statement, &adbc_error);
	//	REQUIRE(adbc_status == ADBC_STATUS_OK);
	//
	//	// we can release again
	//	adbc_status = adbc::StatementRelease(&adbc_statement, &adbc_error);
	//	REQUIRE(adbc_status == ADBC_STATUS_OK);
	//
	//	// we can release a nullptr
	//	adbc_status = adbc::StatementRelease(nullptr, &adbc_error);
	//	REQUIRE(adbc_status == ADBC_STATUS_OK);
	//
	//	// we can query schemata
	//	adbc_status = adbc::ConnectionGetDbSchemas(&adbc_connection, &adbc_statement, &adbc_error);
	//	REQUIRE(adbc_status == ADBC_STATUS_OK);
	//
	//	adbc_status = adbc::StatementGetStream(&adbc_statement, &arrow_stream, &adbc_error);
	//	REQUIRE(adbc_status == ADBC_STATUS_OK);
	//
	//	arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
	//	REQUIRE(arrow_status == 0);
	//	REQUIRE((arrow_array.n_children == 2 && arrow_array.children[0]->length > 0));
	//
	//	arrow_array.release(&arrow_array);
	//	arrow_stream.release(&arrow_stream);
	//
	//	adbc_status = adbc::StatementRelease(&adbc_statement, &adbc_error);
	//	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::StatementNew(&adbc_connection, &adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::StatementSetSqlQuery(&adbc_statement, "CREATE TABLE dummy(a INTEGER)", &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// create a dummy table
	adbc_status = adbc::StatementExecuteQuery(&adbc_statement, NULL, NULL, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);
	adbc_status = adbc::StatementRelease(&adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);
	//
	//	// we can query tables
	//	adbc_status =
	//	    AdbcConnectionGetTables(&adbc_connection, nullptr, nullptr, nullptr, nullptr, &adbc_statement, &adbc_error);
	//	REQUIRE(adbc_status == ADBC_STATUS_OK);
	//
	//	adbc_status = adbc::StatementGetStream(&adbc_statement, &arrow_stream, &adbc_error);
	//	REQUIRE(adbc_status == ADBC_STATUS_OK);
	//
	//	arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
	//	REQUIRE(arrow_status == 0);
	//	REQUIRE((arrow_array.n_children == 4 && arrow_array.children[0]->length == 1));
	//
	//	arrow_array.release(&arrow_array);
	//	arrow_stream.release(&arrow_stream);
	//
	//	adbc_status = adbc::StatementRelease(&adbc_statement, &adbc_error);
	//	REQUIRE(adbc_status == ADBC_STATUS_OK);
	//
	//	// we can query tables using specific schema names and prefixes
	//	adbc_status =
	//	    AdbcConnectionGetTables(&adbc_connection, nullptr, "main", "dum%", nullptr, &adbc_statement, &adbc_error);
	//	REQUIRE(adbc_status == ADBC_STATUS_OK);
	//
	//	adbc_status = adbc::StatementGetStream(&adbc_statement, &arrow_stream, &adbc_error);
	//	REQUIRE(adbc_status == ADBC_STATUS_OK);
	//
	//	arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
	//	REQUIRE(arrow_status == 0);
	//	REQUIRE((arrow_array.n_children == 4 && arrow_array.children[0]->length == 1));
	//
	//	arrow_array.release(&arrow_array);
	//	arrow_stream.release(&arrow_stream);
	//
	//	adbc_status = adbc::StatementRelease(&adbc_statement, &adbc_error);
	//	REQUIRE(adbc_status == ADBC_STATUS_OK);
	//
	//	// we can release again
	//	adbc_status = adbc::StatementRelease(&adbc_statement, &adbc_error);
	//	REQUIRE(adbc_status == ADBC_STATUS_OK);
	//
	//	// we can release a nullptr
	//	adbc_status = adbc::StatementRelease(nullptr, &adbc_error);
	//	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// tear down connection and database again
	adbc_status = adbc::ConnectionRelease(&adbc_connection, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// we can release again
	adbc_status = adbc::ConnectionRelease(&adbc_connection, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// we can also release a nullptr
	adbc_status = adbc::ConnectionRelease(nullptr, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::DatabaseRelease(&adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// can release twice no problem
	adbc_status = adbc::DatabaseRelease(&adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// can release a nullptr
	adbc_status = adbc::DatabaseRelease(nullptr, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);
}

TEST_CASE("Bad query", "[adbc]") {
	AdbcStatusCode adbc_status;
	AdbcError adbc_error;
	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;
	AdbcStatement adbc_statement;
	// ArrowArrayStream arrow_stream;
	// ArrowArray arrow_array;
	// int arrow_status;

	adbc_status = adbc::DatabaseNew(&adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);
	adbc_status = adbc::DatabaseInit(&adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// connect!
	adbc_status = adbc::ConnectionNew(&adbc_connection, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);
	adbc_status = adbc::ConnectionInit(&adbc_connection, &adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// create a dummy table
	adbc_status = adbc::StatementNew(&adbc_connection, &adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::StatementSetSqlQuery(&adbc_statement, "CREATE TABLE dummy(a INTEGER, b INTEGER)", &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::StatementExecuteQuery(&adbc_statement, NULL, NULL, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::StatementRelease(&adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// execute statement that requires parameters
	adbc_status = adbc::StatementNew(&adbc_connection, &adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::StatementSetSqlQuery(&adbc_statement, "INSERT INTO dummy(a, b) VALUES (?, ?)", &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::StatementExecuteQuery(&adbc_statement, NULL, NULL, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::StatementRelease(&adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// tear down connection and database again
	adbc_status = adbc::ConnectionRelease(&adbc_connection, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::DatabaseRelease(&adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);
}

// TEST_CASE("ADBC Main Test", "[adbc]") {
//	AdbcError adbc_error;
//	AdbcStatusCode adbc_status;
//	AdbcDatabase adbc_database;
//	AdbcConnection adbc_connection;
//	AdbcStatement adbc_statement;
//	ArrowArrayStream arrow_stream;
//
//	REQUIRE(AdbcDatabaseNew(&adbc_database, &adbc_error));
//	REQUIRE(AdbcDatabaseInit(&adbc_database, &adbc_error));
//
//	REQUIRE(AdbcConnectionNew(&adbc_database, &adbc_connection, &adbc_error) == ADBC_STATUS_OK);
//	REQUIRE(AdbcConnectionInit(&adbc_connection, &adbc_error) == ADBC_STATUS_OK);
//
//	REQUIRE(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error) == ADBC_STATUS_OK);
//	REQUIRE(AdbcStatementSetSqlQuery(&adbc_statement, "SELECT 42", &adbc_error) == ADBC_STATUS_OK);
//	REQUIRE(AdbcStatementExecute(&adbc_statement, &adbc_error) == ADBC_STATUS_OK);
//	REQUIRE(AdbcStatementGetStream(&adbc_statement, &arrow_stream, &adbc_error) == ADBC_STATUS_OK);
//
//	ArrowArray arrow_array;
//	int arrow_status;
//	arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
//
//	printf("should be 42: %d\n", ((int *)arrow_array.children[0]->buffers[1])[0]);
//
//	arrow_array.release(&arrow_array);
//	arrow_stream.release(&arrow_stream);
//
//	REQUIRE(AdbcStatementRelease(&adbc_statement, &adbc_error));
//	REQUIRE(AdbcConnectionRelease(&adbc_connection, &adbc_error));
//	REQUIRE(AdbcDatabaseRelease(&adbc_database, &adbc_error));
//}

// TEST_CASE("ADBC Main Driver Manager", "[adbc]") {
//	AdbcError adbc_error;
//	AdbcStatusCode adbc_status;
//	AdbcDatabase adbc_database;
//	AdbcConnection adbc_connection;
//	AdbcStatement adbc_statement;
//	ArrowArrayStream arrow_stream;
//
//	REQUIRE(AdbcDatabaseNew(&adbc_database, &adbc_error) == ADBC_STATUS_OK);
//	REQUIRE(AdbcDatabaseSetOption(&adbc_database, "driver", "../../build/debug/src/libduckdb.dylib", &adbc_error) ==
//ADBC_STATUS_OK); 	REQUIRE(AdbcDatabaseSetOption(&adbc_database, "entrypoint", "duckdb_adbc_init", &adbc_error) ==
//ADBC_STATUS_OK); 	REQUIRE(AdbcDatabaseInit(&adbc_database, &adbc_error) == ADBC_STATUS_OK);
//
//	REQUIRE(AdbcConnectionNew(&adbc_database, &adbc_connection, &adbc_error) == ADBC_STATUS_OK );
//	REQUIRE(AdbcConnectionInit(&adbc_connection, &adbc_error) == ADBC_STATUS_OK );
//
//	REQUIRE(AdbcStatementNew(&adbc_connection, &adbc_statement, &adbc_error) == ADBC_STATUS_OK);
//	REQUIRE(AdbcStatementSetSqlQuery(&adbc_statement, "SELECT 42", &adbc_error) == ADBC_STATUS_OK);
//	REQUIRE(AdbcStatementExecute(&adbc_statement, &adbc_error) == ADBC_STATUS_OK);
//	REQUIRE(AdbcStatementGetStream(&adbc_statement, &arrow_stream, &adbc_error) == ADBC_STATUS_OK);
//
//	ArrowArray arrow_array;
//	int arrow_status;
//	arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
//
//	printf("should be 42: %d\n", ((int *)arrow_array.children[0]->buffers[1])[0]);
//
//	arrow_array.release(&arrow_array);
//	arrow_stream.release(&arrow_stream);
//
//	REQUIRE(AdbcStatementRelease(&adbc_statement, &adbc_error));
//	REQUIRE(AdbcConnectionRelease(&adbc_connection, &adbc_error));
//	REQUIRE(AdbcDatabaseRelease(&adbc_database, &adbc_error));
//}
//

//}

/*
TEST_CASE("Error conditions", "[adbc]") {
    AdbcStatusCode adbc_status;
    AdbcError adbc_error;
    AdbcDatabaseOptions adbc_database_options;
    AdbcDatabase adbc_database;
    AdbcConnection adbc_connection;
    AdbcConnectionOptions adbc_connection_options;
    AdbcStatement adbc_statement;
    ArrowArrayStream arrow_stream;
    ArrowArray arrow_array;
    int arrow_status;
    // NULL options
    adbc_status = adbc::DatabaseInit(nullptr, &adbc_database, &adbc_error);
    REQUIRE(adbc_status != ADBC_STATUS_OK);
    REQUIRE((adbc_error.message && strlen(adbc_error.message) > 0));
    AdbcErrorRelease(&adbc_error);
    // NULL database
    adbc_status = adbc::DatabaseInit(&adbc_database_options, nullptr, &adbc_error);
    REQUIRE(adbc_status != ADBC_STATUS_OK);
    REQUIRE((adbc_error.message && strlen(adbc_error.message) > 0));
    AdbcErrorRelease(&adbc_error);
    // null error
    adbc_status = adbc::DatabaseInit(&adbc_database_options, nullptr, nullptr);
    REQUIRE(adbc_status != ADBC_STATUS_OK);
    // non-writeable path
    adbc_database_options.target = "/cant/write/this";
    adbc_status = adbc::DatabaseInit(&adbc_database_options, &adbc_database, &adbc_error);
    REQUIRE(adbc_status != ADBC_STATUS_OK);
    REQUIRE((adbc_error.message && strlen(adbc_error.message) > 0));
    AdbcErrorRelease(&adbc_error);
    // also, we can release an error again
    AdbcErrorRelease(&adbc_error);
    // and we can release a nullptr
    AdbcErrorRelease(nullptr);
    // so now lets actually make a connection so we can mess with it
    adbc_database_options.target = ":memory:";
    adbc_status = adbc::DatabaseInit(&adbc_database_options, &adbc_database, &adbc_error);
    REQUIRE(adbc_status == ADBC_STATUS_OK);
    adbc_connection_options.database = &adbc_database;
    adbc_status = adbc::ConnectionInit(&adbc_connection_options, &adbc_connection, &adbc_error);
    REQUIRE(adbc_status == ADBC_STATUS_OK);
    // run a real (TM) query
    adbc_status = adbc::ConnectionSqlExecute(&adbc_connection, "SELECT 42", &adbc_statement, &adbc_error);
    REQUIRE(adbc_status == ADBC_STATUS_OK);
    adbc_status = adbc::StatementGetStream(&adbc_statement, &arrow_stream, &adbc_error);
    REQUIRE(adbc_status == ADBC_STATUS_OK);
    arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
    REQUIRE(arrow_status == 0);
    arrow_array.release(&arrow_array);
    // we can release again
    arrow_stream.release(&arrow_stream);
    // can't get an array from a released stream
    arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
    REQUIRE(arrow_status);
    // clean query result up
    adbc_status = adbc::StatementRelease(&adbc_statement, &adbc_error);
    REQUIRE(adbc_status == ADBC_STATUS_OK);
    // can't get an arrow stream if the statement is released (insert appropriate meme)
    adbc_status = adbc::StatementGetStream(&adbc_statement, &arrow_stream, &adbc_error);
    REQUIRE(adbc_status != ADBC_STATUS_OK);
    REQUIRE((adbc_error.message && strlen(adbc_error.message) > 0));
    AdbcErrorRelease(&adbc_error);
    // we can release a statement and still consume the stream afterwards if we have called GetStream beforehand
    adbc_status = adbc::ConnectionSqlExecute(&adbc_connection, "SELECT 42", &adbc_statement, &adbc_error);
    REQUIRE(adbc_status == ADBC_STATUS_OK);
    adbc_status = adbc::StatementGetStream(&adbc_statement, &arrow_stream, &adbc_error);
    REQUIRE(adbc_status == ADBC_STATUS_OK);
    adbc_status = adbc::StatementRelease(&adbc_statement, &adbc_error);
    REQUIRE(adbc_status == ADBC_STATUS_OK);
    arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
    REQUIRE(arrow_status == 0);
    arrow_array.release(&arrow_array);
    // can't run a query on a nullptr connection
    adbc_status = adbc::ConnectionSqlExecute(nullptr, "SELECT 42", &adbc_statement, &adbc_error);
    REQUIRE(adbc_status != ADBC_STATUS_OK);
    REQUIRE((adbc_error.message && strlen(adbc_error.message) > 0));
    AdbcErrorRelease(&adbc_error);
    // can't run a query on a nullptr connection
    adbc_status = adbc::ConnectionSqlExecute(&adbc_connection, "SELECT 42", nullptr, &adbc_error);
    REQUIRE(adbc_status != ADBC_STATUS_OK);
    REQUIRE((adbc_error.message && strlen(adbc_error.message) > 0));
    AdbcErrorRelease(&adbc_error);
    // can't run a query without a query (doh)
    adbc_status = adbc::ConnectionSqlExecute(&adbc_connection, nullptr, &adbc_statement, &adbc_error);
    REQUIRE(adbc_status != ADBC_STATUS_OK);
    REQUIRE((adbc_error.message && strlen(adbc_error.message) > 0));
    AdbcErrorRelease(&adbc_error);
    // shut down the connection again
    adbc_status = adbc::ConnectionRelease(&adbc_connection, &adbc_error);
    REQUIRE(adbc_status == ADBC_STATUS_OK);
    // can't run a query after releasing a connection
    adbc_status = adbc::ConnectionSqlExecute(&adbc_connection, "SELECT 42", &adbc_statement, &adbc_error);
    REQUIRE(adbc_status != ADBC_STATUS_OK);
    REQUIRE((adbc_error.message && strlen(adbc_error.message) > 0));
    AdbcErrorRelease(&adbc_error);
    // shut down the database again
    adbc_status = adbc::DatabaseRelease(&adbc_database, &adbc_error);
    REQUIRE(adbc_status == ADBC_STATUS_OK);
    // can't connect after releasing db
    adbc_connection_options.database = &adbc_database;
    adbc_status = adbc::ConnectionInit(&adbc_connection_options, &adbc_connection, &adbc_error);
    REQUIRE(adbc_status != ADBC_STATUS_OK);
    REQUIRE((adbc_error.message && strlen(adbc_error.message) > 0));
    AdbcErrorRelease(&adbc_error);
    // can't connect without specifying a db
    adbc_connection_options.database = nullptr;
    adbc_status = adbc::ConnectionInit(&adbc_connection_options, &adbc_connection, &adbc_error);
    REQUIRE(adbc_status != ADBC_STATUS_OK);
    REQUIRE((adbc_error.message && strlen(adbc_error.message) > 0));
    AdbcErrorRelease(&adbc_error);
    // we can release the error twice
    AdbcErrorRelease(&adbc_error);
    // we can also release a nullptr error
    AdbcErrorRelease(nullptr);
}
*/
TEST_CASE("Test ingestion", "[adbc]") {
	AdbcStatusCode adbc_status;
	AdbcError adbc_error;
	AdbcDatabase adbc_database;
	AdbcConnection adbc_connection;
	AdbcStatement adbc_statement;

	ArrowArrayStream arrow_stream;
	ArrowArray arrow_array;
	int arrow_status;

	// so now lets actually make a connection so we can mess with it

	adbc_status = adbc::DatabaseNew(&adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::DatabaseInit(&adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::ConnectionNew(&adbc_connection, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::ConnectionInit(&adbc_connection, &adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::StatementNew(&adbc_connection, &adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::StatementSetSqlQuery(&adbc_statement, "SELECT 42", &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// run a real (TM) query
	adbc_status = adbc::StatementExecuteQuery(&adbc_statement, &arrow_stream, NULL, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(arrow_status == 0);

	adbc_status = adbc::StatementRelease(&adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// actual ingest

	// insert some data
	adbc_status = adbc::StatementNew(&adbc_connection, &adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::StatementSetOption(&adbc_statement, ADBC_INGEST_OPTION_TARGET_TABLE, "my_table", &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::StatementBindStream(&adbc_statement, &arrow_stream, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::StatementExecuteQuery(&adbc_statement, NULL, NULL, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// see if we have anything

	adbc_status = adbc::StatementNew(&adbc_connection, &adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::StatementSetSqlQuery(&adbc_statement, "SELECT * FROM my_table", &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	adbc_status = adbc::StatementExecuteQuery(&adbc_statement, &arrow_stream, NULL, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	arrow_status = arrow_stream.get_next(&arrow_stream, &arrow_array);
	REQUIRE(arrow_status == 0);
	REQUIRE(((int *)arrow_array.children[0]->buffers[1])[0] == 42);
	arrow_array.release(&arrow_array);
	arrow_stream.release(&arrow_stream);

	adbc_status = adbc::StatementRelease(&adbc_statement, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// shut down the connection again
	adbc_status = adbc::ConnectionRelease(&adbc_connection, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);

	// shut down the database again
	adbc_status = adbc::DatabaseRelease(&adbc_database, &adbc_error);
	REQUIRE(adbc_status == ADBC_STATUS_OK);
}
