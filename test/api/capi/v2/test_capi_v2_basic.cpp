#include "test_capi_v2.hpp"

// duckdb.h, included first, defines DUCKDB_API_ALLOW_UNSTABLE as 0 by default, so v2 must not read it as an opt-in.
static_assert(DUCKDB_API_ALLOW_UNSTABLE == 0,
              "duckdb.h must precede duckdb_v2.h for the next assertion to mean anything");
static_assert(DUCKDB_V2_API_ALLOW_UNSTABLE == 0, "the v2 unstable surface must be off unless opted into");

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
	REQUIRE(duckdb_v2_environment_get_instance_count(env, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 0);
	REQUIRE(duckdb_v2_environment_destroy(&env) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(env == nullptr);
}

TEST_CASE("V2: instance create / attach / destroy", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);

	duckdb_v2_instance_handle instance = nullptr;
	REQUIRE(duckdb_v2_instance_create(env, &instance, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(instance != nullptr);

	// The handle counts from creation, opened or not.
	idx_t count = 0;
	duckdb_v2_environment_get_instance_count(env, &count, nullptr);
	REQUIRE(count == 1);

	REQUIRE(duckdb_v2_instance_attach(instance, duckdb_v2_str {nullptr, 0}, nullptr, nullptr, false, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_instance_destroy(&instance) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(instance == nullptr);
	REQUIRE(duckdb_v2_instance_destroy(&instance) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_environment_get_instance_count(env, &count, nullptr);
	REQUIRE(count == 0);

	duckdb_v2_environment_destroy(&env);
}

TEST_CASE("V2: environment_destroy refuses while instance handles are alive", "[capi_v2][env]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);

	// A handle that never opened anything still pins the environment.
	duckdb_v2_instance_handle instance = nullptr;
	duckdb_v2_instance_create(env, &instance, nullptr);

	REQUIRE(duckdb_v2_environment_destroy(&env) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(env != nullptr);

	duckdb_v2_instance_destroy(&instance);
	REQUIRE(duckdb_v2_environment_destroy(&env) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: a connection works before any database is open", "[capi_v2][db][conn]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);
	duckdb_v2_instance_handle instance = nullptr;
	duckdb_v2_instance_create(env, &instance, nullptr);

	// connection_create starts the instance; only the system and temp catalogs exist.
	duckdb_v2_connection_handle conn = nullptr;
	REQUIRE(duckdb_v2_connection_create(instance, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);
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

	// Attaching afterwards makes the database reachable by name, but nothing is the default until set_default.
	REQUIRE(duckdb_v2_instance_attach(instance, Convert(":memory:"), nullptr, nullptr, false, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(Runs(conn, "CREATE TABLE memory.q(i INTEGER)"));
	REQUIRE(!Runs(conn, "CREATE TABLE t(i INTEGER)"));

	// A connection binds to the default when it is created: the existing one keeps having none.
	REQUIRE(duckdb_v2_instance_set_default(instance, Convert(":memory:"), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(!Runs(conn, "CREATE TABLE t(i INTEGER)"));
	REQUIRE(Runs(conn, "USE memory"));
	REQUIRE(Runs(conn, "CREATE TABLE t(i INTEGER)"));
	duckdb_v2_connection_handle later = nullptr;
	REQUIRE(duckdb_v2_connection_create(instance, &later, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Runs(later, "SELECT i FROM t"));
	REQUIRE(Runs(later, "CREATE TABLE t2(i INTEGER)"));
	REQUIRE(Runs(later, "SELECT i FROM memory.t2"));

	duckdb_v2_connection_destroy(&later);
	duckdb_v2_connection_destroy(&conn);
	duckdb_v2_instance_destroy(&instance);
	duckdb_v2_environment_destroy(&env);
}

TEST_CASE("V2: attaching a file twice is rejected", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);

	auto path = duckdb::TestCreatePath("v2_test_open.db");
	duckdb::DeleteDatabase(path);

	duckdb_v2_instance_handle instance_a = nullptr;
	REQUIRE(OpenInstance(env, Convert(path), &instance_a, nullptr) == DUCKDB_V2_ERROR_NONE);

	SECTION("from another instance handle of the same environment") {
		duckdb_v2_instance_handle instance_b = nullptr;
		duckdb_v2_instance_create(env, &instance_b, nullptr);
		duckdb_v2_error_info_handle err = nullptr;
		// TODO: Fix this, windows reports another error!
		auto open_error = duckdb_v2_instance_attach(instance_b, Convert(path), nullptr, nullptr, false, &err);
		REQUIRE(((open_error == DUCKDB_V2_ERROR_RESOURCE_IN_USE) || (open_error == DUCKDB_V2_ERROR_IO_GENERAL)));
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);

		// The handle survives a failed attach.
		duckdb_v2_instance_destroy(&instance_a);
		REQUIRE(duckdb_v2_instance_attach(instance_b, Convert(path), nullptr, nullptr, false, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		duckdb_v2_instance_destroy(&instance_b);
	}

	SECTION("from the same instance handle") {
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_instance_attach(instance_a, Convert(path), nullptr, nullptr, false, &err) !=
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);

		// Detaching frees the slot for a re-attach on the same handle.
		REQUIRE(duckdb_v2_instance_detach(instance_a, Convert(path), nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_instance_attach(instance_a, Convert(path), nullptr, nullptr, false, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		duckdb_v2_instance_destroy(&instance_a);
	}

	duckdb_v2_environment_destroy(&env);
	duckdb::DeleteDatabase(path);
}

TEST_CASE("V2: several databases on one handle, with an explicit default", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);
	auto path_a = duckdb::TestCreatePath("v2_multi_a.db");
	auto path_b = duckdb::TestCreatePath("v2_multi_b.db");
	duckdb::DeleteDatabase(path_a);
	duckdb::DeleteDatabase(path_b);

	duckdb_v2_instance_handle instance = nullptr;
	duckdb_v2_instance_create(env, &instance, nullptr);
	REQUIRE(duckdb_v2_instance_attach(instance, Convert(path_a), nullptr, nullptr, false, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_instance_attach(instance, Convert(path_b), nullptr, nullptr, false, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_instance_attach(instance, Convert(":memory:"), nullptr, nullptr, false, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	// Every database is reachable under its base name; none is the default until set_default says so.
	duckdb_v2_connection_handle conn = nullptr;
	duckdb_v2_connection_create(instance, &conn, nullptr);
	REQUIRE(!Runs(conn, "CREATE TABLE t(i INTEGER)"));
	REQUIRE(Runs(conn, "CREATE TABLE v2_multi_b.u(i INTEGER)"));
	REQUIRE(Runs(conn, "CREATE TABLE memory.m(i INTEGER)"));
	duckdb_v2_connection_destroy(&conn);

	// Connections created after set_default bind to it.
	REQUIRE(duckdb_v2_instance_set_default(instance, Convert(path_a), nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_connection_create(instance, &conn, nullptr);
	REQUIRE(Runs(conn, "CREATE TABLE t(i INTEGER)"));
	REQUIRE(Runs(conn, "SELECT * FROM v2_multi_a.t"));
	REQUIRE(!Runs(conn, "SELECT * FROM v2_multi_b.t"));

	// Moving the default affects new connections only; a connection's own USE always wins.
	REQUIRE(duckdb_v2_instance_set_default(instance, Convert(path_b), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Runs(conn, "CREATE TABLE v(i INTEGER)"));
	REQUIRE(Runs(conn, "SELECT * FROM v2_multi_a.v"));
	duckdb_v2_connection_handle on_b = nullptr;
	duckdb_v2_connection_create(instance, &on_b, nullptr);
	REQUIRE(Runs(on_b, "CREATE TABLE v(i INTEGER)"));
	REQUIRE(Runs(on_b, "SELECT * FROM v2_multi_b.v"));
	REQUIRE(Runs(on_b, "USE memory"));
	REQUIRE(Runs(on_b, "CREATE TABLE w(i INTEGER)"));
	REQUIRE(Runs(on_b, "SELECT * FROM memory.w"));
	REQUIRE(Runs(on_b, "RESET search_path"));
	REQUIRE(Runs(on_b, "CREATE TABLE w2(i INTEGER)"));
	REQUIRE(Runs(on_b, "SELECT * FROM v2_multi_b.w2"));

	// Detaching a connection's default is allowed; lookups then skip it, and unqualified DDL says why it fails.
	REQUIRE(duckdb_v2_instance_detach(instance, Convert(path_b), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(!Runs(on_b, "SELECT * FROM v2_multi_b.u"));
	REQUIRE(Runs(on_b, "SELECT abs(1)"));
	duckdb_v2_result_handle r = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(Query(on_b, "CREATE TABLE x(i INTEGER)", &r, &err) != DUCKDB_V2_ERROR_NONE);
	duckdb_v2_str msg = {nullptr, 0};
	duckdb_v2_error_info_get_text(err, &msg);
	REQUIRE(Convert(msg).find("has been detached") != std::string::npos);
	REQUIRE(Convert(msg).find("v2_multi_b") != std::string::npos);
	duckdb_v2_error_info_destroy(&err);
	REQUIRE(Runs(on_b, "USE v2_multi_a"));
	REQUIRE(Runs(on_b, "CREATE TABLE x(i INTEGER)"));
	REQUIRE(Runs(on_b, "SELECT * FROM v2_multi_a.x"));
	duckdb_v2_connection_destroy(&on_b);

	// A path that is not attached is an error for detach and set_default alike.
	REQUIRE(duckdb_v2_instance_detach(instance, Convert(path_b), &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
	REQUIRE(duckdb_v2_instance_set_default(instance, Convert(path_b), &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	REQUIRE(duckdb_v2_instance_detach(instance, Convert(":memory:"), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(!Runs(conn, "SELECT * FROM memory.m"));

	// Detaching the last one leaves a running instance with nothing attached.
	REQUIRE(duckdb_v2_instance_detach(instance, Convert(path_a), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Runs(conn, "SELECT 1"));
	REQUIRE(!Runs(conn, "CREATE TABLE y(i INTEGER)"));

	// The files were checkpointed on detach and reopen with their data.
	REQUIRE(duckdb_v2_instance_attach(instance, Convert(path_b), nullptr, nullptr, false, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(Runs(conn, "SELECT * FROM v2_multi_b.u"));
	REQUIRE(Runs(conn, "SELECT * FROM v2_multi_b.v"));

	duckdb_v2_connection_destroy(&conn);
	duckdb_v2_instance_destroy(&instance);
	duckdb_v2_environment_destroy(&env);
	duckdb::DeleteDatabase(path_a);
	duckdb::DeleteDatabase(path_b);
}

TEST_CASE("V2: attach options give a name and per-database options", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);
	auto fs = duckdb::FileSystem::CreateLocal();
	auto dir_a = duckdb::TestCreatePath("v2_attach_opts_a");
	auto dir_b = duckdb::TestCreatePath("v2_attach_opts_b");
	auto dir_c = duckdb::TestCreatePath("v2_attach_opts_c");
	for (auto &dir : {dir_a, dir_b, dir_c}) {
		if (!fs->DirectoryExists(dir)) {
			fs->CreateDirectory(dir);
		}
	}
	// Three files with the same base name.
	auto path_a = dir_a + "/same.db";
	auto path_b = dir_b + "/same.db";
	auto path_c = dir_c + "/same.db";
	duckdb::DeleteDatabase(path_a);
	duckdb::DeleteDatabase(path_b);
	duckdb::DeleteDatabase(path_c);

	duckdb_v2_instance_handle instance = nullptr;
	duckdb_v2_instance_create(env, &instance, nullptr);
	REQUIRE(duckdb_v2_instance_attach(instance, Convert(path_a), nullptr, nullptr, false, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	// Without a name the base name collides; with a name and options it attaches.
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_instance_attach(instance, Convert(path_c), nullptr, nullptr, false, &err) !=
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_error_info_destroy(&err);

	duckdb_v2_attach_options_handle opts = nullptr;
	REQUIRE(duckdb_v2_attach_options_create(instance, &opts, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_attach_options_set(opts, Convert("BLOCK_SIZE"), Convert("16384"), nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	auto other_name = Convert("other");
	REQUIRE(duckdb_v2_instance_attach(instance, Convert(path_b), &other_name, opts, false, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	duckdb_v2_connection_handle conn = nullptr;
	duckdb_v2_connection_create(instance, &conn, nullptr);
	REQUIRE(Runs(conn, "CREATE TABLE same.t(i INTEGER)"));
	REQUIRE(Runs(conn, "CREATE TABLE other.t(i INTEGER)"));
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(conn, "SELECT * FROM pragma_database_size() WHERE database_name = 'other' AND block_size = 16384", &r,
	              nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(r) == 1);
	duckdb_v2_result_destroy(&r);

	// Name and path both address the database for set_default and detach; the name wins.
	REQUIRE(duckdb_v2_instance_set_default(instance, Convert("other"), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_instance_detach(instance, Convert("other"), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(!Runs(conn, "SELECT * FROM other.t"));

	// Reusing the options re-attaches it read-only; the setting is text, cast like a quoted SQL literal.
	REQUIRE(duckdb_v2_attach_options_set(opts, Convert("read_only"), Convert("true"), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_instance_attach(instance, Convert(path_b), &other_name, opts, true, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(Runs(conn, "SELECT * FROM other.t"));
	REQUIRE(!Runs(conn, "INSERT INTO other.t VALUES (1)"));
	REQUIRE(duckdb_v2_instance_detach(instance, Convert(path_b), nullptr) == DUCKDB_V2_ERROR_NONE);

	// Options belong to the handle they were created from, and an unknown option fails the attach, not the set.
	duckdb_v2_instance_handle other_instance = nullptr;
	duckdb_v2_instance_create(env, &other_instance, nullptr);
	REQUIRE(duckdb_v2_instance_attach(other_instance, Convert(":memory:"), nullptr, opts, false, &err) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_instance_destroy(&other_instance);
	REQUIRE(duckdb_v2_attach_options_set(opts, Convert("no_such_attach_option"), Convert("1"), nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_instance_attach(instance, Convert(path_b), nullptr, opts, false, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	// Null-arg validation and null-safe destroy.
	REQUIRE(duckdb_v2_attach_options_create(nullptr, &opts, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_attach_options_create(instance, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_identifier_t malformed_name = {nullptr, 1};
	REQUIRE(duckdb_v2_instance_attach(instance, Convert(path_b), &malformed_name, nullptr, false, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_attach_options_set(opts, duckdb_v2_str {nullptr, 1}, Convert("1"), nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_attach_options_destroy(&opts) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(opts == nullptr);
	REQUIRE(duckdb_v2_attach_options_destroy(&opts) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_attach_options_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_connection_destroy(&conn);
	duckdb_v2_instance_destroy(&instance);
	duckdb_v2_environment_destroy(&env);
	duckdb::DeleteDatabase(path_a);
	duckdb::DeleteDatabase(path_b);
	duckdb::DeleteDatabase(path_c);
}

TEST_CASE("V2: detach and set_default on a handle that never started", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);
	duckdb_v2_instance_handle instance = nullptr;
	duckdb_v2_instance_create(env, &instance, nullptr);

	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_instance_detach(instance, Convert(":memory:"), &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
	REQUIRE(duckdb_v2_instance_set_default(instance, Convert(":memory:"), &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	duckdb_v2_instance_destroy(&instance);
	duckdb_v2_environment_destroy(&env);
}

TEST_CASE("V2: connection create / destroy", "[capi_v2][conn]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);

	duckdb_v2_instance_handle instance = nullptr;
	OpenInstance(env, duckdb_v2_str {nullptr, 0}, &instance, nullptr);

	duckdb_v2_connection_handle conn = nullptr;
	REQUIRE(duckdb_v2_connection_create(instance, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(conn != nullptr);
	REQUIRE(duckdb_v2_connection_destroy(&conn) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(conn == nullptr);
	REQUIRE(duckdb_v2_connection_destroy(&conn) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_instance_destroy(&instance);
	duckdb_v2_environment_destroy(&env);
}

TEST_CASE("V2: a connection keeps the instance alive after the handle is destroyed", "[capi_v2][db][conn]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);
	duckdb_v2_instance_handle instance = nullptr;
	OpenInstance(env, duckdb_v2_str {nullptr, 0}, &instance, nullptr);
	duckdb_v2_connection_handle conn = nullptr;
	duckdb_v2_connection_create(instance, &conn, nullptr);
	REQUIRE(Runs(conn, "CREATE TABLE t AS SELECT 1 AS i"));

	duckdb_v2_instance_destroy(&instance);
	REQUIRE(Runs(conn, "SELECT i FROM t"));

	duckdb_v2_connection_destroy(&conn);
	duckdb_v2_environment_destroy(&env);
}

TEST_CASE("V2: null-arg validation on env / instance / conn entrypoints", "[capi_v2][env][db][conn]") {
	SECTION("environment_create rejects null out_env") {
		REQUIRE(duckdb_v2_environment_create(nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("environment_destroy with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_environment_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	SECTION("instance_create rejects null env") {
		duckdb_v2_instance_handle instance = nullptr;
		REQUIRE(duckdb_v2_instance_create(nullptr, &instance, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("instance_create rejects null out_instance") {
		duckdb_v2_environment_handle env = nullptr;
		duckdb_v2_environment_create(&env, nullptr);
		REQUIRE(duckdb_v2_instance_create(env, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_environment_destroy(&env);
	}
	SECTION("instance_attach / detach / set_default reject a null handle and a malformed path") {
		duckdb_v2_environment_handle env = nullptr;
		duckdb_v2_environment_create(&env, nullptr);
		duckdb_v2_instance_handle instance = nullptr;
		duckdb_v2_instance_create(env, &instance, nullptr);
		REQUIRE(duckdb_v2_instance_attach(nullptr, Convert(":memory:"), nullptr, nullptr, false, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_instance_attach(instance, duckdb_v2_str {nullptr, 3}, nullptr, nullptr, false, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_instance_detach(nullptr, Convert(":memory:"), nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_instance_detach(instance, duckdb_v2_str {nullptr, 3}, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_instance_set_default(nullptr, Convert(":memory:"), nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_instance_set_default(instance, duckdb_v2_str {nullptr, 3}, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_instance_destroy(&instance);
		duckdb_v2_environment_destroy(&env);
	}
	SECTION("instance_destroy with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_instance_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	SECTION("connection_create rejects null instance") {
		duckdb_v2_connection_handle conn = nullptr;
		REQUIRE(duckdb_v2_connection_create(nullptr, &conn, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("connection_destroy with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_connection_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	}
}

} // namespace test_capi_v2
