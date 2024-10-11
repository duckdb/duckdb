#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/main/connection_manager.hpp"

#include <chrono>
#include <thread>

using namespace duckdb;
using namespace std;

TEST_CASE("Test comment in CPP API", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	con.SendQuery("--ups");
	//! Should not crash
	REQUIRE(1);
}

TEST_CASE("Test using connection after database is gone", "[api]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto conn = make_uniq<Connection>(*db);
	// check that the connection works
	auto result = conn->Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	// destroy the database
	db.reset();
	// try to use the connection
	// it still works: the database remains until all connections are destroyed
	REQUIRE_NO_FAIL(conn->Query("SELECT 42"));

	// now try it with an open transaction
	db = make_uniq<DuckDB>(nullptr);
	conn = make_uniq<Connection>(*db);

	REQUIRE_NO_FAIL(conn->Query("BEGIN TRANSACTION"));
	result = conn->Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	db.reset();

	REQUIRE_NO_FAIL(conn->Query("SELECT 42"));
}

TEST_CASE("Test destroying connections with open transactions", "[api]") {
	auto db = make_uniq<DuckDB>(nullptr);
	{
		Connection con(*db);
		con.Query("BEGIN TRANSACTION");
		con.Query("CREATE TABLE test(i INTEGER);");
	}

	auto conn = make_uniq<Connection>(*db);
	REQUIRE_NO_FAIL(conn->Query("CREATE TABLE test(i INTEGER)"));
}

static void long_running_query(Connection *conn, bool *correct) {
	*correct = true;
	auto result = conn->Query("SELECT i1.i FROM integers i1, integers i2, integers i3, integers i4, integers i5, "
	                          "integers i6, integers i7, integers i8, integers i9, integers i10,"
	                          "integers i11, integers i12, integers i13");
	// the query should fail
	*correct = result->HasError();
}

TEST_CASE("Test closing database during long running query", "[api]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto conn = make_uniq<Connection>(*db);
	// create the database
	REQUIRE_NO_FAIL(conn->Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(conn->Query("INSERT INTO integers FROM range(10000)"));
	conn->DisableProfiling();
	// perform a long running query in the background (many cross products)
	bool correct = true;
	auto background_thread = thread(long_running_query, conn.get(), &correct);
	// wait a little bit
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	// destroy the database
	conn->Interrupt();
	db.reset();
	// wait for the thread
	background_thread.join();
	REQUIRE(correct);
	// try to use the connection
	REQUIRE_NO_FAIL(conn->Query("SELECT 42"));
}

TEST_CASE("Test closing result after database is gone", "[api]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto conn = make_uniq<Connection>(*db);
	// check that the connection works
	auto result = conn->Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	// destroy the database
	db.reset();
	conn.reset();
	result.reset();

	// now the streaming result
	db = make_uniq<DuckDB>(nullptr);
	conn = make_uniq<Connection>(*db);
	// check that the connection works
	auto streaming_result = conn->SendQuery("SELECT 42");
	// destroy the database
	db.reset();
	conn.reset();
	REQUIRE(CHECK_COLUMN(streaming_result, 0, {42}));
	streaming_result.reset();
}

TEST_CASE("Test closing database with open prepared statements", "[api]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto conn = make_uniq<Connection>(*db);

	auto p1 = conn->Prepare("CREATE TABLE a (i INTEGER)");
	REQUIRE_NO_FAIL(p1->Execute());
	auto p2 = conn->Prepare("INSERT INTO a VALUES (42)");
	REQUIRE_NO_FAIL(p2->Execute());

	db.reset();
	conn.reset();

	// the prepared statements are still valid
	// the database is only destroyed when the prepared statements are destroyed
	REQUIRE_NO_FAIL(p2->Execute());
	p1.reset();
	p2.reset();
}

static void parallel_query(Connection *conn, bool *correct, size_t threadnr) {
	correct[threadnr] = true;
	for (size_t i = 0; i < 100; i++) {
		auto result = conn->Query("SELECT * FROM integers ORDER BY i");
		if (!CHECK_COLUMN(result, 0, {1, 2, 3, Value()})) {
			correct[threadnr] = false;
		}
	}
}

TEST_CASE("Test temp_directory defaults", "[api][.]") {
	const char *db_paths[] = {nullptr, "", ":memory:"};
	for (auto &path : db_paths) {
		auto db = make_uniq<DuckDB>(path);
		auto conn = make_uniq<Connection>(*db);

		REQUIRE(db->instance->config.options.temporary_directory == ".tmp");
	}
}

TEST_CASE("Test parallel usage of single client", "[api][.]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto conn = make_uniq<Connection>(*db);

	REQUIRE_NO_FAIL(conn->Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(conn->Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	bool correct[20];
	thread threads[20];
	for (size_t i = 0; i < 20; i++) {
		threads[i] = thread(parallel_query, conn.get(), correct, i);
	}
	for (size_t i = 0; i < 20; i++) {
		threads[i].join();
		REQUIRE(correct[i]);
	}
}

static void parallel_query_with_new_connection(DuckDB *db, bool *correct, size_t threadnr) {
	correct[threadnr] = true;
	for (size_t i = 0; i < 100; i++) {
		auto conn = make_uniq<Connection>(*db);
		auto result = conn->Query("SELECT * FROM integers ORDER BY i");
		if (!CHECK_COLUMN(result, 0, {1, 2, 3, Value()})) {
			correct[threadnr] = false;
		}
	}
}

TEST_CASE("Test making and dropping connections in parallel to a single database", "[api][.]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto conn = make_uniq<Connection>(*db);

	REQUIRE_NO_FAIL(conn->Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(conn->Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	bool correct[20];
	thread threads[20];
	for (size_t i = 0; i < 20; i++) {
		threads[i] = thread(parallel_query_with_new_connection, db.get(), correct, i);
	}
	for (size_t i = 0; i < 100; i++) {
		auto result = conn->Query("SELECT * FROM integers ORDER BY i");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, Value()}));
	}
	for (size_t i = 0; i < 20; i++) {
		threads[i].join();
		REQUIRE(correct[i]);
	}
	auto result = conn->Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, Value()}));
}

TEST_CASE("Test multiple result sets", "[api]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	con.DisableQueryVerification();
	con.EnableQueryVerification();

	con.ForceParallelism();

	result = con.Query("SELECT 42; SELECT 84");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	result = std::move(result->next);
	REQUIRE(CHECK_COLUMN(result, 0, {84}));
	REQUIRE(!result->next);

	// also with stream api
	result = con.SendQuery("SELECT 42; SELECT 84");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	result = std::move(result->next);
	REQUIRE(CHECK_COLUMN(result, 0, {84}));
	REQUIRE(!result->next);
}

TEST_CASE("Test streaming API errors", "[api]") {
	duckdb::unique_ptr<QueryResult> result, result2;
	DuckDB db(nullptr);
	Connection con(db);

	// multiple streaming result
	result = con.SendQuery("SELECT 42;");
	result2 = con.SendQuery("SELECT 42;");
	// "result" is invalidated
	REQUIRE_THROWS(CHECK_COLUMN(result, 0, {42}));
	// "result2" we can read
	REQUIRE(CHECK_COLUMN(result2, 0, {42}));

	// streaming result followed by non-streaming result
	result = con.SendQuery("SELECT 42;");
	result2 = con.Query("SELECT 42;");
	// "result" is invalidated
	REQUIRE_THROWS(CHECK_COLUMN(result, 0, {42}));
	// "result2" we can read
	REQUIRE(CHECK_COLUMN(result2, 0, {42}));

	// error in binding
	result = con.SendQuery("SELECT * FROM nonexistanttable");
	REQUIRE(!result->ToString().empty());
	REQUIRE(result->type == QueryResultType::MATERIALIZED_RESULT);
	REQUIRE_FAIL(result);

	// error in stream that only happens after fetching
	result = con.SendQuery(
	    "SELECT x::INT FROM (SELECT x::VARCHAR x FROM range(10) tbl(x) UNION ALL SELECT 'hello' x) tbl(x);");
	while (!result->HasError()) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
	}
	REQUIRE(!result->ToString().empty());
	REQUIRE_FAIL(result);

	// same query but call Materialize
	result = con.SendQuery(
	    "SELECT x::INT FROM (SELECT x::VARCHAR x FROM range(10) tbl(x) UNION ALL SELECT 'hello' x) tbl(x);");
	REQUIRE(!result->ToString().empty());
	REQUIRE(result->type == QueryResultType::STREAM_RESULT);
	result = ((StreamQueryResult &)*result).Materialize();
	REQUIRE_FAIL(result);

	// same query but call materialize after fetching
	result = con.SendQuery(
	    "SELECT x::INT FROM (SELECT x::VARCHAR x FROM range(10) tbl(x) UNION ALL SELECT 'hello' x) tbl(x);");
	while (!result->HasError()) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
	}
	REQUIRE(!result->ToString().empty());
	REQUIRE(result->type == QueryResultType::STREAM_RESULT);
	result = ((StreamQueryResult &)*result).Materialize();
	REQUIRE_FAIL(result);
}

TEST_CASE("Test fetch API", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	duckdb::unique_ptr<QueryResult> result;

	// fetch from an error
	result = con.Query("SELECT 'hello'::INT");
	REQUIRE_THROWS(result->Fetch());

	result = con.SendQuery("CREATE TABLE test (a INTEGER);");

	result = con.Query("select a from test where 1 <> 1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	result = con.SendQuery("INSERT INTO test VALUES (42)");
	result = con.SendQuery("SELECT a from test");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	auto materialized_result = con.Query("select a from test");
	REQUIRE(CHECK_COLUMN(materialized_result, 0, {42}));

	// override fetch result
	result = con.SendQuery("SELECT a from test");
	result = con.SendQuery("SELECT a from test");
	result = con.SendQuery("SELECT a from test");
	result = con.SendQuery("SELECT a from test");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}

TEST_CASE("Test fetch API not to completion", "[api]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto conn = make_uniq<Connection>(*db);
	// remove connection with active stream result
	auto result = conn->SendQuery("SELECT 42");
	// close the connection
	conn.reset();
	// now try to fetch a chunk, this should not return a nullptr
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	// Only if we would call Fetch again would we Close the QueryResult
	// this is testing that it can get cleaned up without this.

	db.reset();
}

TEST_CASE("Test fetch API robustness", "[api]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto conn = make_uniq<Connection>(*db);

	// remove connection with active stream result
	auto result = conn->SendQuery("SELECT 42");
	// close the connection
	conn.reset();
	// now try to fetch a chunk, this should not return a nullptr
	auto chunk = result->Fetch();
	REQUIRE(chunk);

	// now close the entire database
	conn = make_uniq<Connection>(*db);
	result = conn->SendQuery("SELECT 42");

	db.reset();
	// fetch should not fail
	chunk = result->Fetch();
	REQUIRE(chunk);
	// new queries on the connection should not fail either
	REQUIRE_NO_FAIL(conn->SendQuery("SELECT 42"));

	// override fetch result
	db = make_uniq<DuckDB>(nullptr);
	conn = make_uniq<Connection>(*db);
	auto result1 = conn->SendQuery("SELECT 42");
	auto result2 = conn->SendQuery("SELECT 84");
	REQUIRE_NO_FAIL(*result1);
	REQUIRE_NO_FAIL(*result2);

	// result1 should be closed now
	REQUIRE_THROWS(result1->Fetch());
	// result2 should work
	REQUIRE(result2->Fetch());

	// test materialize
	result1 = conn->SendQuery("SELECT 42");
	REQUIRE(result1->type == QueryResultType::STREAM_RESULT);
	auto materialized = ((StreamQueryResult &)*result1).Materialize();
	result2 = conn->SendQuery("SELECT 84");

	// we can read materialized still, even after opening a new result
	REQUIRE(CHECK_COLUMN(materialized, 0, {42}));
	REQUIRE(CHECK_COLUMN(result2, 0, {84}));
}

static void VerifyStreamResult(duckdb::unique_ptr<QueryResult> result) {
	REQUIRE(result->types[0] == LogicalType::INTEGER);
	size_t current_row = 0;
	int current_expected_value = 0;
	size_t expected_rows = 500 * 5;
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		auto col1_data = FlatVector::GetData<int>(chunk->data[0]);
		for (size_t k = 0; k < chunk->size(); k++) {
			if (current_row % 500 == 0) {
				current_expected_value++;
			}
			REQUIRE(col1_data[k] == current_expected_value);
			current_row++;
		}
	}
	REQUIRE(current_row == expected_rows);
}

TEST_CASE("Test fetch API with big results", "[api][.]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// create table that consists of multiple chunks
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(a INTEGER)"));
	for (size_t i = 0; i < 500; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1); INSERT INTO test VALUES (2); INSERT INTO test VALUES "
		                          "(3); INSERT INTO test VALUES (4); INSERT INTO test VALUES (5);"));
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	// stream the results using the Fetch() API
	auto result = con.SendQuery("SELECT CAST(a AS INTEGER) FROM test ORDER BY a");
	VerifyStreamResult(std::move(result));
	// we can also stream a materialized result
	auto materialized = con.Query("SELECT CAST(a AS INTEGER) FROM test ORDER BY a");
	VerifyStreamResult(std::move(materialized));
	// return multiple results using the stream API
	result = con.SendQuery("SELECT CAST(a AS INTEGER) FROM test ORDER BY a; SELECT CAST(a AS INTEGER) FROM test ORDER "
	                       "BY a; SELECT CAST(a AS INTEGER) FROM test ORDER BY a;");
	auto next = std::move(result->next);
	while (next) {
		auto nextnext = std::move(next->next);
		VerifyStreamResult(std::move(nextnext));
		next = std::move(nextnext);
	}
	VerifyStreamResult(std::move(result));
}

TEST_CASE("Test streaming query during stack unwinding", "[api]") {
	DuckDB db;
	Connection con(db);

	try {
		auto result = con.SendQuery("SELECT * FROM range(1000000)");

		throw std::runtime_error("hello");
	} catch (...) {
	}
}

TEST_CASE("Test prepare dependencies with multiple connections", "[catalog]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	auto con = make_uniq<Connection>(db);
	auto con2 = make_uniq<Connection>(db);
	auto con3 = make_uniq<Connection>(db);

	// simple prepare: begin transaction before the second client calls PREPARE
	REQUIRE_NO_FAIL(con->Query("CREATE TABLE integers(i INTEGER)"));
	// open a transaction in con2, this forces the prepared statement to be kept around until this transaction is closed
	REQUIRE_NO_FAIL(con2->Query("BEGIN TRANSACTION"));
	// we prepare a statement in con
	REQUIRE_NO_FAIL(con->Query("PREPARE s1 AS SELECT * FROM integers"));
	// now we drop con while the second client still has an active transaction
	con.reset();
	// now commit the transaction in the second client
	REQUIRE_NO_FAIL(con2->Query("COMMIT"));

	con = make_uniq<Connection>(db);
	// three transactions
	// open a transaction in con2, this forces the prepared statement to be kept around until this transaction is closed
	REQUIRE_NO_FAIL(con2->Query("BEGIN TRANSACTION"));
	// create a prepare, this creates a dependency from s1 -> integers
	REQUIRE_NO_FAIL(con->Query("PREPARE s1 AS SELECT * FROM integers"));
	// drop the client
	con.reset();
	// now begin a transaction in con3
	REQUIRE_NO_FAIL(con3->Query("BEGIN TRANSACTION"));
	// drop the table integers with cascade, this should drop s1 as well
	REQUIRE_NO_FAIL(con3->Query("DROP TABLE integers CASCADE"));
	REQUIRE_NO_FAIL(con2->Query("COMMIT"));
	REQUIRE_NO_FAIL(con3->Query("COMMIT"));
}

TEST_CASE("Test connection API", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// extract a plan node
	REQUIRE_NOTHROW(con.ExtractPlan("SELECT 42"));
	// can only extract one statement at a time
	REQUIRE_THROWS(con.ExtractPlan("SELECT 42; SELECT 84"));

	// append to a table
	con.Query("CREATE TABLE integers(i integer);");
	auto table_info = con.TableInfo("integers");

	DataChunk chunk;
	REQUIRE_NOTHROW(con.Append(*table_info, chunk));

	// no transaction active
	REQUIRE_THROWS(con.Commit());
	REQUIRE_THROWS(con.Rollback());

	// cannot start a transaction within a transaction
	REQUIRE_NOTHROW(con.BeginTransaction());
	REQUIRE_THROWS(con.BeginTransaction());

	con.SetAutoCommit(false);
	REQUIRE(!con.IsAutoCommit());

	con.SetAutoCommit(true);
	REQUIRE(con.IsAutoCommit());
}

TEST_CASE("Test parser tokenize", "[api]") {
	Parser parser;
	REQUIRE_NOTHROW(parser.Tokenize("SELECT * FROM table WHERE i+1=3 AND j='hello'; --tokenize example query"));
}

TEST_CASE("Test opening an invalid database file", "[api]") {
	duckdb::unique_ptr<DuckDB> db;
	bool success = false;
	try {
		db = make_uniq<DuckDB>("data/parquet-testing/blob.parquet");
		success = true;
	} catch (std::exception &ex) {
		REQUIRE(StringUtil::Contains(ex.what(), "DuckDB"));
	}
	REQUIRE(!success);
	try {
		db = make_uniq<DuckDB>("data/parquet-testing/h2oai/h2oai_group_small.parquet");
		success = true;
	} catch (std::exception &ex) {
		REQUIRE(StringUtil::Contains(ex.what(), "DuckDB"));
	}
	REQUIRE(!success);
}

TEST_CASE("Test large number of connections to a single database", "[api]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto context = make_uniq<ClientContext>((*db).instance);
	auto &connection_manager = ConnectionManager::Get(*context);

	duckdb::vector<duckdb::unique_ptr<Connection>> connections;
	size_t createdConnections = 5000;
	size_t remainingConnections = 500;
	size_t toRemove = createdConnections - remainingConnections;

	for (size_t i = 0; i < createdConnections; i++) {
		auto conn = make_uniq<Connection>(*db);
		connections.push_back(std::move(conn));
	}

	REQUIRE(connection_manager.GetConnectionCount() == createdConnections);

	for (size_t i = 0; i < toRemove; i++) {
		connections.erase(connections.begin());
	}

	REQUIRE(connection_manager.GetConnectionCount() == remainingConnections);
}

TEST_CASE("Issue #4583: Catch Insert/Update/Delete errors", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	duckdb::unique_ptr<QueryResult> result;

	con.EnableQueryVerification();
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0 (c0 int);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES (1);"));

	result = con.SendQuery(
	    "INSERT INTO t0(VALUES('\\x15\\x00\\x00\\x00\\x00@\\x01\\x0A\\x27:!\\x0A\\x00\\x00x12e\"\\x00'::BLOB));");
	//! Should not terminate the process
	REQUIRE_FAIL(result);

	result = con.SendQuery("SELECT MIN(c0) FROM t0;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Issue #14130: InsertStatement::ToString causes InternalException later on", "[api][.]") {
	auto db = DuckDB(nullptr);
	auto conn = Connection(db);

	conn.Query("CREATE TABLE foo(a int, b varchar, c int)");

	auto query = "INSERT INTO Foo values (1, 'qwerty', 42)";

	auto stmts = conn.ExtractStatements(query);
	auto &stmt = stmts[0];

	// Issue was here: calling ToString destroyed the 'alias' of the ValuesList
	stmt->ToString();
	// Which caused an 'InternalException: expected non-empty binding_name' here
	auto prepared_stmt = conn.Prepare(std::move(stmt));
	REQUIRE(!prepared_stmt->HasError());
	REQUIRE_NO_FAIL(prepared_stmt->Execute());
}

TEST_CASE("Issue #6284: CachingPhysicalOperator in pull causes issues", "[api][.]") {

	DBConfig config;
	config.options.maximum_threads = 8;
	DuckDB db(nullptr, &config);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("select setseed(0.1); CREATE TABLE T0 AS SELECT DISTINCT (RANDOM()*9999999)::BIGINT "
	                          "record_nb, 0.0 x_0, 1.0 y_0 FROM range(1000000) tbl"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE T1 AS SELECT record_nb, 0.0 x_1, 1.0 y_1 FROM T0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE T2 AS SELECT record_nb, 0.0 x_2, 1.0 y_2 FROM T0"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE T3 AS SELECT record_nb, 0.0 x_3, 1.0 y_3 FROM T0"));
	auto result = con.SendQuery(R"(
        SELECT T0.record_nb,
            T1.x_1 x_1,
            T1.y_1 y_1,
            T2.x_2 x_2,
            T2.y_2 y_2,
            T3.x_3 x_3,
            T3.y_3 y_3
         FROM T0
           INNER JOIN T1 on T0.record_nb = T1.record_nb
           INNER JOIN T2 on T0.record_nb = T2.record_nb
           INNER JOIN T3 on T0.record_nb = T3.record_nb
    )");

	idx_t count = 0;
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk) {
			break;
		}
		if (chunk->size() == 0) {
			break;
		}
		count += chunk->size();
	}

	REQUIRE(951468 - count == 0);
}

TEST_CASE("Fuzzer 50 - Alter table heap-use-after-free", "[api]") {
	// FIXME: not fixed yet
	return;
	DuckDB db(nullptr);
	Connection con(db);

	con.SendQuery("CREATE TABLE t0(c0 INT);");
	con.SendQuery("ALTER TABLE t0 ADD c1 TIMESTAMP_SEC;");
}

TEST_CASE("Test loading database with enable_external_access set to false", "[api]") {
	DBConfig config;
	config.options.enable_external_access = false;
	auto path = TestCreatePath("external_access_test");
	DuckDB db(path, &config);
	Connection con(db);

	REQUIRE_FAIL(con.Query("ATTACH 'mydb.db' AS external_access_test"));
}

TEST_CASE("Test insert returning in CPP API", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE test(val VARCHAR);");

	con.Query("INSERT INTO test(val) VALUES ('query_1')");
	auto res = con.Query("INSERT INTO test(val) VALUES ('query_2') returning *");
	REQUIRE(CHECK_COLUMN(res, 0, {"query_2"}));

	con.Query("INSERT INTO test(val) VALUES (?);", "query_arg_1");
	auto returning_args = con.Query("INSERT INTO test(val) VALUES (?) RETURNING *;", "query_arg_2");
	REQUIRE(CHECK_COLUMN(returning_args, 0, {"query_arg_2"}));

	con.Prepare("INSERT INTO test(val) VALUES (?);")->Execute("prepared_arg_1");
	auto prepared_returning_args =
	    con.Prepare("INSERT INTO test(val) VALUES (?) returning *;")->Execute("prepared_arg_2");
	REQUIRE(CHECK_COLUMN(prepared_returning_args, 0, {"prepared_arg_2"}));

	// make sure all inserts actually inserted
	auto result = con.Query("SELECT * from test;");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {"query_1", "query_2", "query_arg_1", "query_arg_2", "prepared_arg_1", "prepared_arg_2"}));
}

TEST_CASE("Test a logical execute still has types after an optimization pass", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("PREPARE test AS SELECT 42::INTEGER;");
	const auto query_plan = con.ExtractPlan("EXECUTE test");
	REQUIRE((query_plan->type == LogicalOperatorType::LOGICAL_EXECUTE));
	REQUIRE((query_plan->types.size() == 1));
	REQUIRE((query_plan->types[0].id() == LogicalTypeId::INTEGER));
}
