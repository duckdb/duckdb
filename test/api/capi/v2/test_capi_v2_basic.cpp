#include "test_capi_v2.hpp"

#include <thread>

namespace test_capi_v2 {
//----------------------------------------------------------------------------------------------------------------------
// Basic tests for the environment / database / connection lifecycle: create, open, close, destroy.
//----------------------------------------------------------------------------------------------------------------------

namespace {

// Whether `sql` runs to completion on `conn`; the error, if any, is destroyed.
bool Runs(duckdb_v2_connection_handle conn, std::string_view sql) {
	duckdb_v2_result_handle r = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	if (Query(conn, sql, &r, &err) != DUCKDB_V2_ERROR_NONE) {
		const char *msg = nullptr;
		idx_t msg_length = 0;
		duckdb_v2_error_info_get_text(err, &msg, &msg_length);
		UNSCOPED_INFO(std::string(sql) + " -> " + std::string(msg, msg_length));
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

	REQUIRE(duckdb_v2_instance_attach(instance, nullptr, 0, nullptr, 0, nullptr, false, nullptr) ==
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
	const char *msg = nullptr;
	idx_t msg_length = 0;
	duckdb_v2_error_info_get_text(err, &msg, &msg_length);
	REQUIRE(std::string(msg, msg_length).find("No database is attached") != std::string::npos);
	duckdb_v2_error_info_destroy(&err);
	REQUIRE(Runs(conn, "SELECT 1"));

	// Attaching afterwards makes the database reachable by name, but nothing is the default until set_default.
	std::string_view path_str = ":memory:";
	REQUIRE(duckdb_v2_instance_attach(instance, nullptr, 0, path_str.data(), path_str.size(), nullptr, false,
	                                  nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Runs(conn, "CREATE TABLE memory.q(i INTEGER)"));
	REQUIRE(!Runs(conn, "CREATE TABLE t(i INTEGER)"));

	// A connection binds to the default when it is created: the existing one keeps having none.
	std::string_view memory_name = "memory";
	REQUIRE(duckdb_v2_instance_set_default(instance, memory_name.data(), memory_name.size(), nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
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
	REQUIRE(OpenInstance(env, std::string_view(path), &instance_a, nullptr) == DUCKDB_V2_ERROR_NONE);

	SECTION("from another instance handle of the same environment") {
		duckdb_v2_instance_handle instance_b = nullptr;
		duckdb_v2_instance_create(env, &instance_b, nullptr);
		duckdb_v2_error_info_handle err = nullptr;
		// TODO: Fix this, windows reports another error!
		auto path_str = std::string_view(path);
		auto open_error =
		    duckdb_v2_instance_attach(instance_b, nullptr, 0, path_str.data(), path_str.size(), nullptr, false, &err);
		REQUIRE(((open_error == DUCKDB_V2_ERROR_RESOURCE_IN_USE) || (open_error == DUCKDB_V2_ERROR_IO_GENERAL)));
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);

		// The handle survives a failed attach.
		duckdb_v2_instance_destroy(&instance_a);
		REQUIRE(duckdb_v2_instance_attach(instance_b, nullptr, 0, path_str.data(), path_str.size(), nullptr, false,
		                                  nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_instance_destroy(&instance_b);
	}

	SECTION("from the same instance handle") {
		duckdb_v2_error_info_handle err = nullptr;
		auto path_str2 = std::string_view(path);
		REQUIRE(duckdb_v2_instance_attach(instance_a, nullptr, 0, path_str2.data(), path_str2.size(), nullptr, false,
		                                  &err) != DUCKDB_V2_ERROR_NONE);
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);

		// Detaching frees the slot for a re-attach on the same handle.
		std::string_view name_str = "v2_test_open";
		REQUIRE(duckdb_v2_instance_detach(instance_a, name_str.data(), name_str.size(), nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_instance_attach(instance_a, nullptr, 0, path_str2.data(), path_str2.size(), nullptr, false,
		                                  nullptr) == DUCKDB_V2_ERROR_NONE);
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
	auto path_str = std::string_view(path_a);
	REQUIRE(duckdb_v2_instance_attach(instance, nullptr, 0, path_str.data(), path_str.size(), nullptr, false,
	                                  nullptr) == DUCKDB_V2_ERROR_NONE);
	auto path_str2 = std::string_view(path_b);
	REQUIRE(duckdb_v2_instance_attach(instance, nullptr, 0, path_str2.data(), path_str2.size(), nullptr, false,
	                                  nullptr) == DUCKDB_V2_ERROR_NONE);
	std::string_view path_str3 = ":memory:";
	REQUIRE(duckdb_v2_instance_attach(instance, nullptr, 0, path_str3.data(), path_str3.size(), nullptr, false,
	                                  nullptr) == DUCKDB_V2_ERROR_NONE);
	std::string_view name_a = "v2_multi_a";
	std::string_view name_b = "v2_multi_b";
	std::string_view name_memory = "memory";

	// Every database is reachable under its base name; none is the default until set_default says so.
	duckdb_v2_connection_handle conn = nullptr;
	duckdb_v2_connection_create(instance, &conn, nullptr);
	REQUIRE(!Runs(conn, "CREATE TABLE t(i INTEGER)"));
	REQUIRE(Runs(conn, "CREATE TABLE v2_multi_b.u(i INTEGER)"));
	REQUIRE(Runs(conn, "CREATE TABLE memory.m(i INTEGER)"));
	duckdb_v2_connection_destroy(&conn);

	// Connections created after set_default bind to it.
	REQUIRE(duckdb_v2_instance_set_default(instance, name_a.data(), name_a.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_connection_create(instance, &conn, nullptr);
	REQUIRE(Runs(conn, "CREATE TABLE t(i INTEGER)"));
	REQUIRE(Runs(conn, "SELECT * FROM v2_multi_a.t"));
	REQUIRE(!Runs(conn, "SELECT * FROM v2_multi_b.t"));

	// Moving the default affects new connections only; a connection's own USE always wins.
	REQUIRE(duckdb_v2_instance_set_default(instance, name_b.data(), name_b.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
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
	REQUIRE(duckdb_v2_instance_detach(instance, name_b.data(), name_b.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(!Runs(on_b, "SELECT * FROM v2_multi_b.u"));
	REQUIRE(Runs(on_b, "SELECT abs(1)"));
	duckdb_v2_result_handle r = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(Query(on_b, "CREATE TABLE x(i INTEGER)", &r, &err) != DUCKDB_V2_ERROR_NONE);
	const char *msg = nullptr;
	idx_t msg_length = 0;
	duckdb_v2_error_info_get_text(err, &msg, &msg_length);
	REQUIRE(std::string(msg, msg_length).find("has been detached") != std::string::npos);
	REQUIRE(std::string(msg, msg_length).find("v2_multi_b") != std::string::npos);
	duckdb_v2_error_info_destroy(&err);
	REQUIRE(Runs(on_b, "USE v2_multi_a"));
	REQUIRE(Runs(on_b, "CREATE TABLE x(i INTEGER)"));
	REQUIRE(Runs(on_b, "SELECT * FROM v2_multi_a.x"));
	duckdb_v2_connection_destroy(&on_b);

	// A name that is not attached, or the system or temp catalog, is an error for detach and set_default alike.
	std::string_view name_system = "system";
	std::string_view name_temp = "temp";
	for (auto *missing : {&name_b, &name_system, &name_temp}) {
		REQUIRE(duckdb_v2_instance_detach(instance, missing->data(), missing->size(), &err) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);
		REQUIRE(duckdb_v2_instance_set_default(instance, missing->data(), missing->size(), &err) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);
	}
	// A path is not a name.
	REQUIRE(duckdb_v2_instance_detach(instance, path_str.data(), path_str.size(), nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);

	REQUIRE(duckdb_v2_instance_detach(instance, name_memory.data(), name_memory.size(), nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(!Runs(conn, "SELECT * FROM memory.m"));

	// Detaching the last one leaves a running instance with nothing attached.
	REQUIRE(duckdb_v2_instance_detach(instance, name_a.data(), name_a.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Runs(conn, "SELECT 1"));
	REQUIRE(!Runs(conn, "CREATE TABLE y(i INTEGER)"));

	// The files were checkpointed on detach and reopen with their data.
	REQUIRE(duckdb_v2_instance_attach(instance, nullptr, 0, path_str2.data(), path_str2.size(), nullptr, false,
	                                  nullptr) == DUCKDB_V2_ERROR_NONE);
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
	auto path_str = std::string_view(path_a);
	REQUIRE(duckdb_v2_instance_attach(instance, nullptr, 0, path_str.data(), path_str.size(), nullptr, false,
	                                  nullptr) == DUCKDB_V2_ERROR_NONE);

	// Without a name the base name collides; with a name and options it attaches.
	duckdb_v2_error_info_handle err = nullptr;
	auto path_str2 = std::string_view(path_c);
	REQUIRE(duckdb_v2_instance_attach(instance, nullptr, 0, path_str2.data(), path_str2.size(), nullptr, false, &err) !=
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_error_info_destroy(&err);

	duckdb_v2_attach_options_handle opts = nullptr;
	REQUIRE(duckdb_v2_attach_options_create(instance, &opts, nullptr) == DUCKDB_V2_ERROR_NONE);
	std::string_view key_str = "BLOCK_SIZE";
	std::string_view setting_str = "16384";
	REQUIRE(duckdb_v2_attach_options_set(opts, key_str.data(), key_str.size(), setting_str.data(), setting_str.size(),
	                                     nullptr) == DUCKDB_V2_ERROR_NONE);
	std::string_view other_name = "other";
	auto path_str3 = std::string_view(path_b);
	REQUIRE(duckdb_v2_instance_attach(instance, other_name.data(), other_name.size(), path_str3.data(),
	                                  path_str3.size(), opts, false, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_connection_handle conn = nullptr;
	duckdb_v2_connection_create(instance, &conn, nullptr);
	REQUIRE(Runs(conn, "CREATE TABLE same.t(i INTEGER)"));
	REQUIRE(Runs(conn, "CREATE TABLE other.t(i INTEGER)"));
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(conn, "SELECT * FROM pragma_database_size() WHERE database_name = 'other' AND block_size = 16384", &r,
	              nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(r) == 1);
	duckdb_v2_result_destroy(&r);

	// set_default and detach address the database by the name it was attached under.
	std::string_view path_str4 = "other";
	REQUIRE(duckdb_v2_instance_set_default(instance, path_str4.data(), path_str4.size(), nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_instance_detach(instance, path_str4.data(), path_str4.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(!Runs(conn, "SELECT * FROM other.t"));

	// Reusing the options re-attaches it read-only; the setting is text, cast like a quoted SQL literal.
	std::string_view key_str2 = "read_only";
	std::string_view setting_str2 = "true";
	REQUIRE(duckdb_v2_attach_options_set(opts, key_str2.data(), key_str2.size(), setting_str2.data(),
	                                     setting_str2.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_instance_attach(instance, other_name.data(), other_name.size(), path_str3.data(),
	                                  path_str3.size(), opts, true, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Runs(conn, "SELECT * FROM other.t"));
	REQUIRE(!Runs(conn, "INSERT INTO other.t VALUES (1)"));
	REQUIRE(duckdb_v2_instance_detach(instance, other_name.data(), other_name.size(), nullptr) == DUCKDB_V2_ERROR_NONE);

	// Options belong to the handle they were created from, and an unknown option fails the attach, not the set.
	duckdb_v2_instance_handle other_instance = nullptr;
	duckdb_v2_instance_create(env, &other_instance, nullptr);
	std::string_view path_str5 = ":memory:";
	REQUIRE(duckdb_v2_instance_attach(other_instance, nullptr, 0, path_str5.data(), path_str5.size(), opts, false,
	                                  &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_instance_destroy(&other_instance);
	std::string_view key_str3 = "no_such_attach_option";
	std::string_view setting_str3 = "1";
	REQUIRE(duckdb_v2_attach_options_set(opts, key_str3.data(), key_str3.size(), setting_str3.data(),
	                                     setting_str3.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_instance_attach(instance, nullptr, 0, path_str3.data(), path_str3.size(), opts, false, &err) !=
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	// Null-arg validation and null-safe destroy.
	REQUIRE(duckdb_v2_attach_options_create(nullptr, &opts, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_attach_options_create(instance, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_instance_attach(instance, nullptr, 1, path_str3.data(), path_str3.size(), nullptr, false,
	                                  nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_attach_options_set(opts, nullptr, 1, setting_str3.data(), setting_str3.size(), nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	std::string_view key_str5 = "\x80";
	REQUIRE(duckdb_v2_attach_options_set(opts, key_str5.data(), key_str5.size(), setting_str3.data(),
	                                     setting_str3.size(), nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
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

TEST_CASE("V2: detach and set_default on an instance with nothing attached", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);
	duckdb_v2_instance_handle instance = nullptr;
	duckdb_v2_instance_create(env, &instance, nullptr);

	duckdb_v2_error_info_handle err = nullptr;
	std::string_view name_str = "memory";
	REQUIRE(duckdb_v2_instance_detach(instance, name_str.data(), name_str.size(), &err) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
	REQUIRE(duckdb_v2_instance_set_default(instance, name_str.data(), name_str.size(), &err) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	duckdb_v2_instance_destroy(&instance);
	duckdb_v2_environment_destroy(&env);
}

TEST_CASE("V2: connection create / destroy", "[capi_v2][conn]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);

	duckdb_v2_instance_handle instance = nullptr;
	OpenInstance(env, std::string_view(), &instance, nullptr);

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
	OpenInstance(env, std::string_view(), &instance, nullptr);
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
	SECTION("instance_attach / detach / set_default reject a null handle and a malformed path or name") {
		duckdb_v2_environment_handle env = nullptr;
		duckdb_v2_environment_create(&env, nullptr);
		duckdb_v2_instance_handle instance = nullptr;
		duckdb_v2_instance_create(env, &instance, nullptr);
		std::string_view path_str = ":memory:";
		REQUIRE(duckdb_v2_instance_attach(nullptr, nullptr, 0, path_str.data(), path_str.size(), nullptr, false,
		                                  nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_instance_attach(instance, nullptr, 0, nullptr, 3, nullptr, false, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_instance_detach(nullptr, path_str.data(), path_str.size(), nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_instance_detach(instance, nullptr, 3, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_instance_set_default(nullptr, path_str.data(), path_str.size(), nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_instance_set_default(instance, nullptr, 3, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
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

namespace {

//! Opens `path` shared through `env` with the startup options `names` = `values`.
DUCKDB_V2_ERROR OpenShared(duckdb_v2_environment_handle env, std::string_view path,
                           std::initializer_list<std::string_view> names,
                           std::initializer_list<std::string_view> values, duckdb_v2_instance_handle *out_instance,
                           duckdb_v2_error_info_handle *err = nullptr) {
	StrArray name_array(names);
	StrArray value_array(values);
	return duckdb_v2_instance_open_shared(env, path.data(), path.size(), name_array.ptrs(), name_array.lengths(),
	                                      value_array.ptrs(), value_array.lengths(), name_array.size(), out_instance,
	                                      err);
}

//! A connection to `instance`, which must succeed.
duckdb_v2_connection_handle Connect(duckdb_v2_instance_handle instance) {
	duckdb_v2_connection_handle conn = nullptr;
	REQUIRE(duckdb_v2_connection_create(instance, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);
	return conn;
}

//! The number of rows `sql` returns on `conn`.
int64_t RowCount(duckdb_v2_connection_handle conn, std::string_view sql) {
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(conn, sql, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto rows = static_cast<int64_t>(DrainRowCount(r));
	duckdb_v2_result_destroy(&r);
	return rows;
}

} // namespace

TEST_CASE("V2: instance_open_shared shares one instance per file", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);
	auto path = duckdb::TestCreatePath("v2_open_shared.db");
	duckdb::DeleteDatabase(path);

	duckdb_v2_instance_handle a = nullptr;
	REQUIRE(OpenShared(env, path, {}, {}, &a) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(a != nullptr);
	auto conn_a = Connect(a);
	REQUIRE(Runs(conn_a, "CREATE TABLE t AS SELECT range AS i FROM range(5)"));

	// the same file, also when its path is written differently: a new handle to the same instance
	auto separator = path.find_last_of("/\\");
	REQUIRE(separator != std::string::npos);
	auto other_path = path.substr(0, separator + 1) + "." + path.substr(separator);
	duckdb_v2_instance_handle b = nullptr;
	REQUIRE(OpenShared(env, other_path, {}, {}, &b) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(b != a);
	idx_t count = 0;
	duckdb_v2_environment_get_instance_count(env, &count, nullptr);
	REQUIRE(count == 2);
	auto conn_b = Connect(b);
	REQUIRE(RowCount(conn_b, "SELECT * FROM t") == 5);
	REQUIRE(Runs(conn_b, "INSERT INTO t VALUES (42)"));
	REQUIRE(RowCount(conn_a, "SELECT * FROM t") == 6);

	// the file is open: another instance cannot attach it
	duckdb_v2_instance_handle other = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(OpenInstance(env, path, &other, &err) != DUCKDB_V2_ERROR_NONE);
	duckdb_v2_error_info_destroy(&err);

	// destroying one handle leaves the other working
	duckdb_v2_connection_destroy(&conn_a);
	duckdb_v2_instance_destroy(&a);
	REQUIRE(RowCount(conn_b, "SELECT * FROM t") == 6);

	duckdb_v2_connection_destroy(&conn_b);
	duckdb_v2_instance_destroy(&b);
	duckdb_v2_environment_destroy(&env);
	duckdb::DeleteDatabase(path);
}

TEST_CASE("V2: instance_open_shared shares an instance only with the same startup options", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);
	auto path = duckdb::TestCreatePath("v2_open_shared_options.db");
	duckdb::DeleteDatabase(path);

	duckdb_v2_instance_handle a = nullptr;
	REQUIRE(OpenShared(env, path, {"threads", "access_mode"}, {"2", "READ_WRITE"}, &a) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_instance_handle b = nullptr;
	REQUIRE(OpenShared(env, path, {"threads", "access_mode"}, {"2", "READ_WRITE"}, &b) == DUCKDB_V2_ERROR_NONE);

	// a different one
	duckdb_v2_instance_handle c = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(OpenShared(env, path, {"threads", "access_mode"}, {"3", "READ_WRITE"}, &c, &err) ==
	        DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(c == nullptr);
	const char *msg = nullptr;
	idx_t msg_length = 0;
	duckdb_v2_error_info_get_text(err, &msg, &msg_length);
	REQUIRE(std::string(msg, msg_length).find("different configuration") != std::string::npos);
	duckdb_v2_error_info_destroy(&err);

	// an option that does not exist fails like it does for instance_create_with_options
	REQUIRE(OpenShared(env, path, {"no_such_option"}, {"1"}, &c, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(c == nullptr);
	duckdb_v2_error_info_destroy(&err);

	duckdb_v2_instance_destroy(&a);
	duckdb_v2_instance_destroy(&b);

	// once every handle is destroyed, the file can be opened with another configuration
	REQUIRE(OpenShared(env, path, {"threads"}, {"3"}, &c) == DUCKDB_V2_ERROR_NONE);
	auto conn = Connect(c);
	duckdb_v2_instance_destroy(&c);

	// ... but not while a connection keeps the instance alive
	REQUIRE(OpenShared(env, path, {"threads"}, {"1"}, &c, &err) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	duckdb_v2_error_info_destroy(&err);
	REQUIRE(Runs(conn, "SELECT 1"));
	duckdb_v2_connection_destroy(&conn);
	REQUIRE(OpenShared(env, path, {"threads"}, {"1"}, &c) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_instance_destroy(&c);

	duckdb_v2_environment_destroy(&env);
	duckdb::DeleteDatabase(path);
}

TEST_CASE("V2: instance_open_shared with in-memory databases", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);

	// unnamed: a new database every time
	for (auto path : {std::string_view(":memory:"), std::string_view()}) {
		duckdb_v2_instance_handle a = nullptr;
		duckdb_v2_instance_handle b = nullptr;
		REQUIRE(OpenShared(env, path, {}, {}, &a) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(OpenShared(env, path, {}, {}, &b) == DUCKDB_V2_ERROR_NONE);
		auto conn_a = Connect(a);
		auto conn_b = Connect(b);
		REQUIRE(Runs(conn_a, "CREATE TABLE t AS SELECT 1 AS i"));
		REQUIRE(!Runs(conn_b, "SELECT * FROM t"));
		duckdb_v2_connection_destroy(&conn_a);
		duckdb_v2_connection_destroy(&conn_b);
		duckdb_v2_instance_destroy(&a);
		duckdb_v2_instance_destroy(&b);
	}
	duckdb_v2_instance_handle a = nullptr;
	REQUIRE(duckdb_v2_instance_open_shared(env, nullptr, 0, nullptr, nullptr, nullptr, nullptr, 0, &a, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_instance_destroy(&a);

	// named: shared like a file
	duckdb_v2_instance_handle b = nullptr;
	REQUIRE(OpenShared(env, ":memory:shared_by_name", {}, {}, &a) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(OpenShared(env, ":memory:shared_by_name", {}, {}, &b) == DUCKDB_V2_ERROR_NONE);
	auto conn_a = Connect(a);
	auto conn_b = Connect(b);
	REQUIRE(Runs(conn_a, "CREATE TABLE t AS SELECT 1 AS i"));
	REQUIRE(RowCount(conn_b, "SELECT * FROM t") == 1);
	duckdb_v2_connection_destroy(&conn_a);
	duckdb_v2_connection_destroy(&conn_b);
	duckdb_v2_instance_destroy(&a);
	duckdb_v2_instance_destroy(&b);
	duckdb_v2_environment_destroy(&env);
}

TEST_CASE("V2: instance_open_shared does not share a file attached to another instance", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);
	auto path = duckdb::TestCreatePath("v2_open_shared_attached.db");
	duckdb::DeleteDatabase(path);

	duckdb_v2_instance_handle attached = nullptr;
	REQUIRE(OpenInstance(env, path, &attached, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_instance_handle shared = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(OpenShared(env, path, {}, {}, &shared, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(shared == nullptr);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_instance_destroy(&attached);

	// once it is detached, it can be
	REQUIRE(OpenShared(env, path, {}, {}, &shared) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_instance_destroy(&shared);

	duckdb_v2_environment_destroy(&env);
	duckdb::DeleteDatabase(path);
}

TEST_CASE("V2: instance_open_shared while the last handle closes the database", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);
	auto path = duckdb::TestCreatePath("v2_open_shared_reopen.db");
	duckdb::DeleteDatabase(path);
	{
		duckdb_v2_instance_handle instance = nullptr;
		REQUIRE(OpenShared(env, path, {}, {}, &instance) == DUCKDB_V2_ERROR_NONE);
		auto conn = Connect(instance);
		REQUIRE(Runs(conn, "CREATE TABLE t (i INTEGER)"));
		duckdb_v2_connection_destroy(&conn);
		duckdb_v2_instance_destroy(&instance);
	}

	// every thread opens and closes the file over and over: often, one opens it while another one closes it.
	// Catch's assertions are not thread-safe: the threads only count their failures.
	const idx_t thread_count = 4;
	const idx_t iterations = 25;
	std::vector<idx_t> failures(thread_count, 0);
	std::vector<std::thread> threads;
	for (idx_t t = 0; t < thread_count; t++) {
		threads.emplace_back([&, t]() {
			for (idx_t i = 0; i < iterations; i++) {
				duckdb_v2_instance_handle instance = nullptr;
				if (OpenShared(env, path, {}, {}, &instance) != DUCKDB_V2_ERROR_NONE) {
					failures[t]++;
					continue;
				}
				duckdb_v2_connection_handle conn = nullptr;
				duckdb_v2_result_handle r = nullptr;
				if (duckdb_v2_connection_create(instance, &conn, nullptr) != DUCKDB_V2_ERROR_NONE ||
				    Query(conn, "INSERT INTO t VALUES (1)", &r, nullptr) != DUCKDB_V2_ERROR_NONE) {
					failures[t]++;
				} else {
					while (true) {
						duckdb_v2_data_chunk_handle chunk = nullptr;
						if (duckdb_v2_result_fetch_chunk(r, &chunk, nullptr) != DUCKDB_V2_ERROR_NONE) {
							failures[t]++;
							break;
						}
						if (!chunk) {
							break;
						}
						duckdb_v2_data_chunk_destroy(&chunk);
					}
				}
				duckdb_v2_result_destroy(&r);
				duckdb_v2_connection_destroy(&conn);
				duckdb_v2_instance_destroy(&instance);
			}
		});
	}
	for (auto &thread : threads) {
		thread.join();
	}
	for (auto failure_count : failures) {
		REQUIRE(failure_count == 0);
	}

	duckdb_v2_instance_handle instance = nullptr;
	REQUIRE(OpenShared(env, path, {}, {}, &instance) == DUCKDB_V2_ERROR_NONE);
	auto conn = Connect(instance);
	REQUIRE(RowCount(conn, "SELECT * FROM t") == static_cast<int64_t>(thread_count * iterations));
	duckdb_v2_connection_destroy(&conn);
	duckdb_v2_instance_destroy(&instance);
	duckdb_v2_environment_destroy(&env);
	duckdb::DeleteDatabase(path);
}

TEST_CASE("V2: instance_open_shared null-arg validation", "[capi_v2][db]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);
	std::string_view path = ":memory:";
	duckdb_v2_instance_handle instance = reinterpret_cast<duckdb_v2_instance_handle>(uintptr_t(1));
	REQUIRE(duckdb_v2_instance_open_shared(nullptr, path.data(), path.size(), nullptr, nullptr, nullptr, nullptr, 0,
	                                       &instance, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_instance_open_shared(env, path.data(), path.size(), nullptr, nullptr, nullptr, nullptr, 0,
	                                       nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_instance_open_shared(env, nullptr, 3, nullptr, nullptr, nullptr, nullptr, 0, &instance,
	                                       nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(instance == nullptr);
	StrArray values({"1"});
	REQUIRE(duckdb_v2_instance_open_shared(env, path.data(), path.size(), nullptr, nullptr, values.ptrs(),
	                                       values.lengths(), 1, &instance, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(instance == nullptr);
	duckdb_v2_environment_destroy(&env);
}

} // namespace test_capi_v2
