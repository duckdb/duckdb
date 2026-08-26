#include "test_capi_v2.hpp"

namespace test_capi_v2 {
//----------------------------------------------------------------------------------------------------------------------
// Basic tests for creating/opening/closing/destroying databases and connections
//----------------------------------------------------------------------------------------------------------------------

TEST_CASE("V2: env create / destroy", "[capi_v2][env]") {
	duckdb_v2_environment_handle env = nullptr;
	REQUIRE(duckdb_v2_create_environment(&env, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(env != nullptr);
	idx_t count = 99;
	REQUIRE(duckdb_v2_environment_database_count(env, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 0);
	REQUIRE(duckdb_v2_destroy_environment(&env) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(env == nullptr);
}

TEST_CASE("V2: open / close in-memory database", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_database_handle db = nullptr;
	REQUIRE(duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(db != nullptr);

	idx_t count = 0;
	duckdb_v2_environment_database_count(env, &count, nullptr);
	REQUIRE(count == 1);

	REQUIRE(duckdb_v2_close(&db) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(db == nullptr);

	duckdb_v2_environment_database_count(env, &count, nullptr);
	REQUIRE(count == 0);

	duckdb_v2_destroy_environment(&env);
}

TEST_CASE("V2: destroy_environment refuses while databases are open", "[capi_v2][env]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr);

	REQUIRE(duckdb_v2_destroy_environment(&env) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(env != nullptr); // refusal leaves env intact

	duckdb_v2_close(&db);
	REQUIRE(duckdb_v2_destroy_environment(&env) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: open with pre-open option handles", "[capi_v2][db][option]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_option_handle opt = nullptr;
	duckdb_v2_option_create(Convert("memory_limit"), Convert("1GB"), &opt, nullptr);
	duckdb_v2_option_handle opts[] = {opt};

	duckdb_v2_database_handle db = nullptr;
	REQUIRE(duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, opts, 1, &db, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_close(&db);
	duckdb_v2_option_destroy(&opt);
	duckdb_v2_destroy_environment(&env);
}

TEST_CASE("V2: file-based open rejects second open of same file", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	auto path = duckdb::TestCreatePath("v2_test_open.db");

	duckdb_v2_database_handle db_a = nullptr;
	REQUIRE(duckdb_v2_open(env, Convert(path), nullptr, 0, &db_a, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_database_handle db_b = nullptr;
	duckdb_v2_error_info_handle err = nullptr;

	// TODO: Fix this, windows reports another error!
	auto open_error = duckdb_v2_open(env, Convert(path), nullptr, 0, &db_b, &err);
	REQUIRE(((open_error == DUCKDB_V2_ERROR_RESOURCE_IN_USE) || (open_error == DUCKDB_V2_ERROR_IO_GENERAL)));

	REQUIRE(db_b == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	duckdb_v2_close(&db_a);

	// After close, reopen succeeds (the path slot is freed).
	REQUIRE(duckdb_v2_open(env, Convert(path), nullptr, 0, &db_b, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_close(&db_b);

	duckdb_v2_destroy_environment(&env);
	duckdb::DeleteDatabase(path);
}

TEST_CASE("V2: connect / disconnect", "[capi_v2][conn]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr);

	duckdb_v2_connection_handle conn = nullptr;
	REQUIRE(duckdb_v2_connect(db, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(conn != nullptr);
	REQUIRE(duckdb_v2_disconnect(&conn) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(conn == nullptr);

	duckdb_v2_close(&db);
	duckdb_v2_destroy_environment(&env);
}

TEST_CASE("V2: null-arg validation on env / db / conn entrypoints", "[capi_v2][env][db][conn]") {
	SECTION("create_environment rejects null out_env") {
		REQUIRE(duckdb_v2_create_environment(nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("destroy_environment with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_destroy_environment(nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	SECTION("open rejects null env") {
		duckdb_v2_database_handle db = nullptr;
		REQUIRE(duckdb_v2_open(nullptr, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("open rejects null out_db") {
		duckdb_v2_environment_handle env = nullptr;
		duckdb_v2_create_environment(&env, nullptr);
		REQUIRE(duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, nullptr, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_destroy_environment(&env);
	}
	SECTION("open rejects option_count > 0 with null options") {
		duckdb_v2_environment_handle env = nullptr;
		duckdb_v2_create_environment(&env, nullptr);
		duckdb_v2_database_handle db = nullptr;
		REQUIRE(duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 1, &db, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_destroy_environment(&env);
	}
	SECTION("close with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_close(nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	SECTION("connect rejects null db") {
		duckdb_v2_connection_handle conn = nullptr;
		REQUIRE(duckdb_v2_connect(nullptr, &conn, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("disconnect with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_disconnect(nullptr) == DUCKDB_V2_ERROR_NONE);
	}
}

} // namespace test_capi_v2
