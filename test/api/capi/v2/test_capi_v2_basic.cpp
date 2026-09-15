#include "test_capi_v2.hpp"

namespace test_capi_v2 {
//----------------------------------------------------------------------------------------------------------------------
// Basic tests for the environment / database / connection lifecycle: create, open, close, destroy.
//----------------------------------------------------------------------------------------------------------------------

namespace {

// Whether `sql` runs to completion on `conn`; the error, if any, is destroyed.
bool Runs(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_result_handle r = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	if (Query(conn, sql, &r, &err) != DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_str msg = {nullptr, 0};
		duckdb_v2_error_info_get_text(err, &msg);
		UNSCOPED_INFO(std::string(sql) + " -> " + Convert(msg));
		duckdb_v2_error_info_destroy(&err);
		return false;
	}
	(void)DrainRowCount(r);
	duckdb_v2_result_destroy(&r);
	return true;
}

} // namespace

TEST_CASE("V2: env create / destroy", "[capi_v2][env]") {
	duckdb_v2_environment_handle env = nullptr;
	REQUIRE(duckdb_v2_environment_create(&env, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(env != nullptr);
	idx_t count = 99;
	REQUIRE(duckdb_v2_environment_get_database_count(env, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 0);
	REQUIRE(duckdb_v2_environment_destroy(&env) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(env == nullptr);
}

TEST_CASE("V2: database create / open / destroy", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);

	duckdb_v2_database_handle db = nullptr;
	REQUIRE(duckdb_v2_database_create(env, &db, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(db != nullptr);

	// The handle counts from creation, opened or not.
	idx_t count = 0;
	duckdb_v2_environment_get_database_count(env, &count, nullptr);
	REQUIRE(count == 1);

	REQUIRE(duckdb_v2_database_open(db, duckdb_v2_str {nullptr, 0}, nullptr) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_database_destroy(&db) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(db == nullptr);
	REQUIRE(duckdb_v2_database_destroy(&db) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_environment_get_database_count(env, &count, nullptr);
	REQUIRE(count == 0);

	duckdb_v2_environment_destroy(&env);
}

TEST_CASE("V2: environment_destroy refuses while database handles are alive", "[capi_v2][env]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);

	// A handle that never opened anything still pins the environment.
	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_database_create(env, &db, nullptr);

	REQUIRE(duckdb_v2_environment_destroy(&env) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(env != nullptr);

	duckdb_v2_database_destroy(&db);
	REQUIRE(duckdb_v2_environment_destroy(&env) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: a connection works before any database is open", "[capi_v2][db][conn]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);
	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_database_create(env, &db, nullptr);

	// connection_create starts the instance; only the system and temp catalogs exist.
	duckdb_v2_connection_handle conn = nullptr;
	REQUIRE(duckdb_v2_connection_create(db, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Runs(conn, "SELECT 1 + 1"));
	REQUIRE(Runs(conn, "CREATE TEMP TABLE tmp AS SELECT 42 AS i"));
	REQUIRE(Runs(conn, "SELECT i FROM tmp"));

	// Anything that needs a default database fails cleanly rather than crashing the instance.
	duckdb_v2_result_handle r = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(Query(conn, "CREATE TABLE t(i INTEGER)", &r, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(err != nullptr);
	duckdb_v2_str msg = {nullptr, 0};
	duckdb_v2_error_info_get_text(err, &msg);
	REQUIRE(Convert(msg).find("No database is attached") != std::string::npos);
	duckdb_v2_error_info_destroy(&err);
	REQUIRE(Runs(conn, "SELECT 1"));

	// Opening a database afterwards makes it the default for the existing connection.
	REQUIRE(duckdb_v2_database_open(db, Convert(":memory:"), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Runs(conn, "CREATE TABLE t(i INTEGER)"));
	REQUIRE(Runs(conn, "SELECT i FROM memory.t"));

	duckdb_v2_connection_destroy(&conn);
	duckdb_v2_database_destroy(&db);
	duckdb_v2_environment_destroy(&env);
}

TEST_CASE("V2: file-based open rejects a second open of the same file", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);

	auto path = duckdb::TestCreatePath("v2_test_open.db");
	duckdb::DeleteDatabase(path);

	duckdb_v2_database_handle db_a = nullptr;
	REQUIRE(OpenDatabase(env, Convert(path), &db_a, nullptr) == DUCKDB_V2_ERROR_NONE);

	SECTION("from another database handle of the same environment") {
		duckdb_v2_database_handle db_b = nullptr;
		duckdb_v2_database_create(env, &db_b, nullptr);
		duckdb_v2_error_info_handle err = nullptr;
		// TODO: Fix this, windows reports another error!
		auto open_error = duckdb_v2_database_open(db_b, Convert(path), &err);
		REQUIRE(((open_error == DUCKDB_V2_ERROR_RESOURCE_IN_USE) || (open_error == DUCKDB_V2_ERROR_IO_GENERAL)));
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);

		// The handle survives a failed open.
		duckdb_v2_database_destroy(&db_a);
		REQUIRE(duckdb_v2_database_open(db_b, Convert(path), nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_database_destroy(&db_b);
	}

	SECTION("from the same database handle") {
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_database_open(db_a, Convert(path), &err) != DUCKDB_V2_ERROR_NONE);
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);

		// Closing it frees the slot for a reopen on the same handle.
		REQUIRE(duckdb_v2_database_close(db_a, Convert(path), nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_database_open(db_a, Convert(path), nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_database_destroy(&db_a);
	}

	duckdb_v2_environment_destroy(&env);
	duckdb::DeleteDatabase(path);
}

TEST_CASE("V2: several databases on one handle, closed by path", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);
	auto path_a = duckdb::TestCreatePath("v2_multi_a.db");
	auto path_b = duckdb::TestCreatePath("v2_multi_b.db");
	duckdb::DeleteDatabase(path_a);
	duckdb::DeleteDatabase(path_b);

	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_database_create(env, &db, nullptr);
	REQUIRE(duckdb_v2_database_open(db, Convert(path_a), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_database_open(db, Convert(path_b), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_database_open(db, Convert(":memory:"), nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_connection_handle conn = nullptr;
	duckdb_v2_connection_create(db, &conn, nullptr);

	// The first database opened is the default; the others are attached under their base names.
	REQUIRE(Runs(conn, "CREATE TABLE t(i INTEGER)"));
	REQUIRE(Runs(conn, "SELECT * FROM v2_multi_a.t"));
	REQUIRE(!Runs(conn, "SELECT * FROM v2_multi_b.t"));
	REQUIRE(Runs(conn, "CREATE TABLE v2_multi_b.u(i INTEGER)"));
	REQUIRE(Runs(conn, "CREATE TABLE memory.m(i INTEGER)"));

	// Closing the default database is allowed here: the next oldest takes over.
	REQUIRE(duckdb_v2_database_close(db, Convert(path_a), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(!Runs(conn, "SELECT * FROM v2_multi_a.t"));
	REQUIRE(Runs(conn, "CREATE TABLE v(i INTEGER)"));
	REQUIRE(Runs(conn, "SELECT * FROM v2_multi_b.v"));

	// A path that is not open is an error, and closing twice is too.
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_database_close(db, Convert(path_a), &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	REQUIRE(duckdb_v2_database_close(db, Convert(":memory:"), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(!Runs(conn, "SELECT * FROM memory.m"));

	// Closing the last one leaves a running instance with nothing attached.
	REQUIRE(duckdb_v2_database_close(db, Convert(path_b), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Runs(conn, "SELECT 1"));
	REQUIRE(!Runs(conn, "CREATE TABLE w(i INTEGER)"));

	// The file was checkpointed on close and reopens with its data.
	REQUIRE(duckdb_v2_database_open(db, Convert(path_b), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Runs(conn, "SELECT * FROM v2_multi_b.u"));

	duckdb_v2_connection_destroy(&conn);
	duckdb_v2_database_destroy(&db);
	duckdb_v2_environment_destroy(&env);
	duckdb::DeleteDatabase(path_a);
	duckdb::DeleteDatabase(path_b);
}

TEST_CASE("V2: close on a handle that never started", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);
	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_database_create(env, &db, nullptr);

	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_database_close(db, Convert(":memory:"), &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	duckdb_v2_database_destroy(&db);
	duckdb_v2_environment_destroy(&env);
}

TEST_CASE("V2: connection create / destroy", "[capi_v2][conn]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);

	duckdb_v2_database_handle db = nullptr;
	OpenDatabase(env, duckdb_v2_str {nullptr, 0}, &db, nullptr);

	duckdb_v2_connection_handle conn = nullptr;
	REQUIRE(duckdb_v2_connection_create(db, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(conn != nullptr);
	REQUIRE(duckdb_v2_connection_destroy(&conn) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(conn == nullptr);
	REQUIRE(duckdb_v2_connection_destroy(&conn) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_database_destroy(&db);
	duckdb_v2_environment_destroy(&env);
}

TEST_CASE("V2: a connection keeps the instance alive after the handle is destroyed", "[capi_v2][db][conn]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);
	duckdb_v2_database_handle db = nullptr;
	OpenDatabase(env, duckdb_v2_str {nullptr, 0}, &db, nullptr);
	duckdb_v2_connection_handle conn = nullptr;
	duckdb_v2_connection_create(db, &conn, nullptr);
	REQUIRE(Runs(conn, "CREATE TABLE t AS SELECT 1 AS i"));

	duckdb_v2_database_destroy(&db);
	REQUIRE(Runs(conn, "SELECT i FROM t"));

	duckdb_v2_connection_destroy(&conn);
	duckdb_v2_environment_destroy(&env);
}

TEST_CASE("V2: null-arg validation on env / db / conn entrypoints", "[capi_v2][env][db][conn]") {
	SECTION("environment_create rejects null out_env") {
		REQUIRE(duckdb_v2_environment_create(nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("environment_destroy with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_environment_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	SECTION("database_create rejects null env") {
		duckdb_v2_database_handle db = nullptr;
		REQUIRE(duckdb_v2_database_create(nullptr, &db, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("database_create rejects null out_db") {
		duckdb_v2_environment_handle env = nullptr;
		duckdb_v2_environment_create(&env, nullptr);
		REQUIRE(duckdb_v2_database_create(env, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_environment_destroy(&env);
	}
	SECTION("database_open / database_close reject a null handle and a malformed path") {
		duckdb_v2_environment_handle env = nullptr;
		duckdb_v2_environment_create(&env, nullptr);
		duckdb_v2_database_handle db = nullptr;
		duckdb_v2_database_create(env, &db, nullptr);
		REQUIRE(duckdb_v2_database_open(nullptr, Convert(":memory:"), nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_database_open(db, duckdb_v2_str {nullptr, 3}, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_database_close(nullptr, Convert(":memory:"), nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_database_close(db, duckdb_v2_str {nullptr, 3}, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_database_destroy(&db);
		duckdb_v2_environment_destroy(&env);
	}
	SECTION("database_destroy with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_database_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	SECTION("connection_create rejects null db") {
		duckdb_v2_connection_handle conn = nullptr;
		REQUIRE(duckdb_v2_connection_create(nullptr, &conn, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("connection_destroy with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_connection_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	}
}

} // namespace test_capi_v2
