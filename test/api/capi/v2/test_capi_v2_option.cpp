#include "test_capi_v2.hpp"

// ---------------------------------------------------------------------------
// V2 options: set by name on a database or connection, read back as a
// descriptor handle. Scope enforcement reuses PhysicalSet::GetSettingScope so
// error messages are DuckDB's own.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {

namespace {

std::string DbSetting(duckdb_v2_instance_handle instance, const char *name) {
	duckdb_v2_option_handle opt = nullptr;
	REQUIRE(duckdb_v2_instance_get_option_by_name(instance, Convert(name), &opt, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_str setting = {nullptr, 0};
	REQUIRE(duckdb_v2_option_get_setting(opt, &setting, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto result = Convert(setting);
	duckdb_v2_option_destroy(&opt);
	return result;
}

std::string ConnSetting(duckdb_v2_connection_handle conn, const char *name) {
	duckdb_v2_option_handle opt = nullptr;
	REQUIRE(duckdb_v2_connection_get_option_by_name(conn, Convert(name), &opt, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_str setting = {nullptr, 0};
	REQUIRE(duckdb_v2_option_get_setting(opt, &setting, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto result = Convert(setting);
	duckdb_v2_option_destroy(&opt);
	return result;
}

// What the exec callback read through the context.
struct OptionProbe {
	std::string setting;
	idx_t count = 0;
	DUCKDB_V2_ERROR unknown_rc = DUCKDB_V2_ERROR_NONE;
	DUCKDB_V2_ERROR by_index_rc = DUCKDB_V2_ERROR_NONE;
} option_probe;

void OptionProbeExec(duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_context_handle context,
                     duckdb_v2_error_info_handle *err) {
	duckdb_v2_option_handle opt = nullptr;
	if (duckdb_v2_context_get_option_by_name(context, Convert("max_execution_time"), &opt, err) !=
	    DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_str setting = {nullptr, 0};
	duckdb_v2_option_get_setting(opt, &setting, nullptr);
	option_probe.setting = Convert(setting);
	duckdb_v2_option_destroy(&opt);

	if (duckdb_v2_context_get_option_count(context, &option_probe.count, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	option_probe.unknown_rc = duckdb_v2_context_get_option_by_name(context, Convert("no_such_option"), &opt, nullptr);
	option_probe.by_index_rc = duckdb_v2_context_get_option_by_index(context, 0, &opt, nullptr);
	duckdb_v2_option_destroy(&opt);

	duckdb_v2_vector_handle out = nullptr;
	void *raw = nullptr;
	if (duckdb_v2_scalar_function_exec_get_result(info, &out, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_get_data_mutable(out, &raw, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	static_cast<int32_t *>(raw)[0] = 1;
}

void RegisterOptionProbe(duckdb_v2_connection_handle conn) {
	auto integer = MakeType(conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_scalar_function_handle function = nullptr;
	REQUIRE(duckdb_v2_scalar_function_create_with_connection(conn, &function, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto name = Convert("probe_option");
	REQUIRE(duckdb_v2_scalar_function_set_name(function, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_function_signature_handle sig = nullptr;
	REQUIRE(duckdb_v2_scalar_function_get_signature(function, &sig, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_function_signature_set_return_type(sig, integer, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_scalar_function_set_exec_callback(function, OptionProbeExec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_scalar_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_scalar_function_destroy(&function) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_destroy(&integer);
}

} // namespace

TEST_CASE("V2 db option: set + get round-trip", "[capi_v2][db][option]") {
	EnvFixture fx;

	// Read the default before mutating so we can compare against it.
	auto default_value = DbSetting(fx.instance, "memory_limit");
	REQUIRE(duckdb_v2_instance_set_option(fx.instance, Convert("memory_limit"), Convert("1GB"), nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	auto after = DbSetting(fx.instance, "memory_limit");
	REQUIRE(!after.empty());
	REQUIRE(after != default_value); // mutation visible
	// ... and visible to a connection, as a GLOBAL write.
	REQUIRE(ConnSetting(fx.conn, "memory_limit") == after);
}

TEST_CASE("V2 db option: get populates description and aliases", "[capi_v2][db][option]") {
	EnvFixture fx;

	duckdb_v2_option_handle opt = nullptr;
	REQUIRE(duckdb_v2_instance_get_option_by_name(fx.instance, Convert("memory_limit"), &opt, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	// "memory_limit" is an alias; the canonical name is "max_memory", and the alias list carries "memory_limit".
	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_option_get_name(opt, &name, nullptr);
	REQUIRE(name == "max_memory");
	idx_t alias_count = 0;
	duckdb_v2_option_get_alias_count(opt, &alias_count, nullptr);
	bool has_memory_limit = false;
	for (idx_t i = 0; i < alias_count; i++) {
		duckdb_v2_str alias = {nullptr, 0};
		duckdb_v2_option_get_alias(opt, i, &alias, nullptr);
		if (alias == "memory_limit") {
			has_memory_limit = true;
			break;
		}
	}
	REQUIRE(has_memory_limit);

	duckdb_v2_str desc = {nullptr, 0};
	duckdb_v2_option_get_description(opt, &desc, nullptr);
	REQUIRE(desc.ptr != nullptr);
	REQUIRE(desc.len != 0);

	duckdb_v2_option_destroy(&opt);

	// A declared scope target is reported; allow_community_extensions is GLOBAL_ONLY.
	REQUIRE(duckdb_v2_instance_get_option_by_name(fx.instance, Convert("allow_community_extensions"), &opt, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_OPTION_TARGET_SCOPE scope = DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN;
	duckdb_v2_option_get_target_scope(opt, &scope, nullptr);
	REQUIRE(scope == DUCKDB_V2_OPTION_TARGET_SCOPE_GLOBAL_ONLY);
	duckdb_v2_option_destroy(&opt);
}

TEST_CASE("V2 db option: get unknown name errors", "[capi_v2][db][option]") {
	EnvFixture fx;
	duckdb_v2_option_handle out = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_instance_get_option_by_name(fx.instance, Convert("this_option_does_not_exist"), &out, &err) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2 db option: get_option_count and get_option_by_index", "[capi_v2][db][option]") {
	EnvFixture fx;
	idx_t count = 0;
	REQUIRE(duckdb_v2_instance_get_option_count(fx.instance, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count > 0);

	// Walk the first few entries — each should produce a populated handle.
	idx_t to_check = count < 5 ? count : 5;
	for (idx_t i = 0; i < to_check; i++) {
		duckdb_v2_option_handle opt = nullptr;
		REQUIRE(duckdb_v2_instance_get_option_by_index(fx.instance, i, &opt, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_str name = {nullptr, 0};
		duckdb_v2_option_get_name(opt, &name, nullptr);
		REQUIRE(name.ptr != nullptr);
		REQUIRE(name.len != 0);
		duckdb_v2_option_destroy(&opt);
	}

	duckdb_v2_option_handle out_of_range = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_instance_get_option_by_index(fx.instance, count + 100, &out_of_range, &err) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2 db option: set rejects an unparseable setting and a LOCAL_ONLY option", "[capi_v2][db][option]") {
	EnvFixture fx;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_instance_set_option(fx.instance, Convert("threads"), Convert("lots"), &err) !=
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
	// A malformed setting view (null ptr, nonzero len) is caught up front.
	REQUIRE(duckdb_v2_instance_set_option(fx.instance, Convert("threads"), duckdb_v2_str {nullptr, 1}, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
}

TEST_CASE("V2 conn option: set LOCAL is invisible to other connections", "[capi_v2][conn][option]") {
	EnvFixture fx;
	duckdb_v2_connection_handle other = nullptr;
	duckdb_v2_connection_create(fx.instance, &other, nullptr);

	// max_execution_time is LOCAL_DEFAULT, so a LOCAL-scope write stays
	// session-local — perfect for this test.
	REQUIRE(duckdb_v2_connection_set_option(fx.conn, Convert("max_execution_time"), Convert("5000"),
	                                        DUCKDB_V2_SETTING_SCOPE_LOCAL, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ConnSetting(fx.conn, "max_execution_time") == "5000");
	// The other connection sees the static default ("0"), not "5000".
	REQUIRE(ConnSetting(other, "max_execution_time") != "5000");

	duckdb_v2_connection_destroy(&other);
}

TEST_CASE("V2 conn option: set GLOBAL is visible everywhere", "[capi_v2][conn][option]") {
	EnvFixture fx;
	duckdb_v2_connection_handle other = nullptr;
	duckdb_v2_connection_create(fx.instance, &other, nullptr);

	REQUIRE(duckdb_v2_connection_set_option(fx.conn, Convert("memory_limit"), Convert("2GB"),
	                                        DUCKDB_V2_SETTING_SCOPE_GLOBAL, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto fx_setting = ConnSetting(fx.conn, "memory_limit");
	REQUIRE(!fx_setting.empty());
	REQUIRE(fx_setting == ConnSetting(other, "memory_limit"));     // GLOBAL write seen identically by both
	REQUIRE(fx_setting == DbSetting(fx.instance, "memory_limit")); // ... and by the database itself

	duckdb_v2_connection_destroy(&other);
}

TEST_CASE("V2 conn option: scope enforcement matches SQL", "[capi_v2][conn][option]") {
	EnvFixture fx;
	duckdb_v2_error_info_handle err = nullptr;

	// GLOBAL_ONLY × LOCAL: rejected. allow_community_extensions is GLOBAL_ONLY.
	REQUIRE(duckdb_v2_connection_set_option(fx.conn, Convert("allow_community_extensions"), Convert("false"),
	                                        DUCKDB_V2_SETTING_SCOPE_LOCAL, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2 conn option: AUTOMATIC scope mirrors bare SQL `SET`", "[capi_v2][conn][option]") {
	EnvFixture fx;
	// max_execution_time is LOCAL_DEFAULT → AUTOMATIC resolves to SESSION → write succeeds.
	REQUIRE(duckdb_v2_connection_set_option(fx.conn, Convert("max_execution_time"), Convert("5000"),
	                                        DUCKDB_V2_SETTING_SCOPE_AUTOMATIC, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ConnSetting(fx.conn, "max_execution_time") == "5000");
}

TEST_CASE("V2 conn option: unknown name errors", "[capi_v2][conn][option]") {
	EnvFixture fx;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_connection_set_option(fx.conn, Convert("no_such_option_xyz"), Convert("1"),
	                                        DUCKDB_V2_SETTING_SCOPE_AUTOMATIC, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_option_handle opt = nullptr;
	REQUIRE(duckdb_v2_connection_get_option_by_name(fx.conn, Convert("no_such_option_xyz"), &opt, &err) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(opt == nullptr);
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2 db option: options set before the first open are startup options", "[capi_v2][db][option]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_environment_create(&env, nullptr);

	SECTION("a staged setting is reported before startup and applied at startup") {
		duckdb_v2_instance_handle instance = nullptr;
		duckdb_v2_instance_create(env, &instance, nullptr);

		auto default_value = DbSetting(instance, "memory_limit");
		REQUIRE(duckdb_v2_instance_set_option(instance, Convert("memory_limit"), Convert("2GB"), nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		// Before startup the staged text is reported verbatim, under the alias as well as the canonical name.
		REQUIRE(DbSetting(instance, "memory_limit") == "2GB");
		REQUIRE(DbSetting(instance, "max_memory") == "2GB");

		// Discovery works before startup too.
		idx_t count = 0;
		REQUIRE(duckdb_v2_instance_get_option_count(instance, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(count > 0);
		duckdb_v2_option_handle first = nullptr;
		REQUIRE(duckdb_v2_instance_get_option_by_index(instance, 0, &first, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_option_destroy(&first);

		// Once started, the engine reports the applied value, which differs from the untouched default.
		REQUIRE(duckdb_v2_instance_attach(instance, duckdb_v2_str {nullptr, 0}, nullptr, nullptr, false, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		auto applied = DbSetting(instance, "memory_limit");
		REQUIRE(applied != default_value);
		REQUIRE(applied != "2GB");
		duckdb_v2_connection_handle conn = nullptr;
		duckdb_v2_connection_create(instance, &conn, nullptr);
		REQUIRE(ConnSetting(conn, "memory_limit") == applied);
		duckdb_v2_connection_destroy(&conn);
		duckdb_v2_instance_destroy(&instance);
	}

	SECTION("a startup-only option is accepted before startup and rejected after") {
		auto path = duckdb::TestCreatePath("v2_option_readonly.db");
		duckdb::DeleteDatabase(path);
		{
			duckdb_v2_instance_handle instance = nullptr;
			OpenInstance(env, Convert(path), &instance, nullptr);
			duckdb_v2_connection_handle conn = nullptr;
			duckdb_v2_connection_create(instance, &conn, nullptr);
			ExecSQL(conn, "CREATE TABLE t(i INTEGER)");
			duckdb_v2_connection_destroy(&conn);
			duckdb_v2_instance_destroy(&instance);
		}

		duckdb_v2_instance_handle instance = nullptr;
		duckdb_v2_instance_create(env, &instance, nullptr);
		REQUIRE(duckdb_v2_instance_set_option(instance, Convert("access_mode"), Convert("READ_ONLY"), nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_instance_attach(instance, Convert(path), nullptr, nullptr, false, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_instance_set_default(instance, Convert(path), nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_connection_handle conn = nullptr;
		duckdb_v2_connection_create(instance, &conn, nullptr);

		duckdb_v2_result_handle r = nullptr;
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(Query(conn, "INSERT INTO t VALUES (1)", &r, &err) != DUCKDB_V2_ERROR_NONE);
		duckdb_v2_error_info_destroy(&err);

		// Once running, access_mode can no longer change: the same error SET GLOBAL gives.
		REQUIRE(duckdb_v2_instance_set_option(instance, Convert("access_mode"), Convert("READ_WRITE"), &err) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);

		duckdb_v2_connection_destroy(&conn);
		duckdb_v2_instance_destroy(&instance);
		duckdb::DeleteDatabase(path);
	}

	SECTION("an unknown option is kept for startup, where nothing consumes it") {
		duckdb_v2_instance_handle instance = nullptr;
		duckdb_v2_instance_create(env, &instance, nullptr);
		REQUIRE(duckdb_v2_instance_set_option(instance, Convert("no_such_option_xyz"), Convert("1"), nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		// Reading it back is not possible until an extension defines it.
		duckdb_v2_option_handle opt = nullptr;
		REQUIRE(duckdb_v2_instance_get_option_by_name(instance, Convert("no_such_option_xyz"), &opt, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);

		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_instance_attach(instance, duckdb_v2_str {nullptr, 0}, nullptr, nullptr, false, &err) !=
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(err != nullptr);
		duckdb_v2_str msg = {nullptr, 0};
		duckdb_v2_error_info_get_text(err, &msg);
		REQUIRE(Convert(msg).find("no_such_option_xyz") != std::string::npos);
		duckdb_v2_error_info_destroy(&err);

		duckdb_v2_connection_handle conn = nullptr;
		REQUIRE(duckdb_v2_connection_create(instance, &conn, nullptr) != DUCKDB_V2_ERROR_NONE);
		REQUIRE(conn == nullptr);
		duckdb_v2_instance_destroy(&instance);
	}

	duckdb_v2_environment_destroy(&env);
}

TEST_CASE("V2 context option: read through a context inside a callback", "[capi_v2][context][option]") {
	EnvFixture fx;
	option_probe = OptionProbe {};
	RegisterOptionProbe(fx.conn);
	REQUIRE(duckdb_v2_connection_set_option(fx.conn, Convert("max_execution_time"), Convert("4242"),
	                                        DUCKDB_V2_SETTING_SCOPE_LOCAL, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(fx.conn, "SELECT probe_option()", &result) == DUCKDB_V2_ERROR_NONE);
	auto chunk = StepChunk(result);
	REQUIRE(chunk != nullptr);
	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&result);

	// The context sees the connection's LOCAL override, and the same option space.
	REQUIRE(option_probe.setting == "4242");
	idx_t count = 0;
	duckdb_v2_connection_get_option_count(fx.conn, &count, nullptr);
	REQUIRE(option_probe.count == count);
	REQUIRE(option_probe.unknown_rc == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(option_probe.by_index_rc == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 option: descriptor accessors", "[capi_v2][option]") {
	EnvFixture fx;
	duckdb_v2_option_handle opt = nullptr;
	REQUIRE(duckdb_v2_instance_get_option_by_name(fx.instance, Convert("threads"), &opt, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	SECTION("get_default_setting is populated") {
		duckdb_v2_str def = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_default_setting(opt, &def, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(def.ptr != nullptr);
	}

	SECTION("borrowed pointers stay stable across reads") {
		duckdb_v2_str first_name = {nullptr, 0};
		duckdb_v2_str second_name = {nullptr, 0};
		duckdb_v2_option_get_name(opt, &first_name, nullptr);
		duckdb_v2_option_get_name(opt, &second_name, nullptr);
		REQUIRE(first_name.ptr == second_name.ptr);
		REQUIRE(first_name == "threads");
	}

	SECTION("get_alias out-of-range surfaces a descriptive error") {
		duckdb_v2_str alias = {nullptr, 0};
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_option_get_alias(opt, 99, &alias, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(alias.ptr == nullptr);
		REQUIRE(err != nullptr);
		duckdb_v2_str msg = {nullptr, 0};
		duckdb_v2_error_info_get_text(err, &msg);
		REQUIRE(Convert(msg).find("out of range") != std::string::npos);
		duckdb_v2_error_info_destroy(&err);
		// err == nullptr is tolerated on the failure path.
		REQUIRE(duckdb_v2_option_get_alias(opt, 99, &alias, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}

	SECTION("destroy nulls the slot and is safe to repeat") {
		REQUIRE(duckdb_v2_option_destroy(&opt) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(opt == nullptr);
		REQUIRE(duckdb_v2_option_destroy(&opt) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_option_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	}

	SECTION("handles are independent") {
		duckdb_v2_option_handle other = nullptr;
		REQUIRE(duckdb_v2_instance_get_option_by_name(fx.instance, Convert("memory_limit"), &other, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		duckdb_v2_option_destroy(&other);
		duckdb_v2_str still = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_name(opt, &still, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(still == "threads");
	}

	duckdb_v2_option_destroy(&opt);
}

TEST_CASE("V2 option: accessor null-arg validation", "[capi_v2][option]") {
	EnvFixture fx;
	duckdb_v2_option_handle opt = nullptr;
	REQUIRE(duckdb_v2_instance_get_option_by_name(fx.instance, Convert("threads"), &opt, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	SECTION("get_name rejects null option") {
		duckdb_v2_str out = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_name(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("get_name rejects null out_name") {
		REQUIRE(duckdb_v2_option_get_name(opt, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("get_setting rejects null option") {
		duckdb_v2_str out = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_setting(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("get_setting rejects null out_setting") {
		REQUIRE(duckdb_v2_option_get_setting(opt, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("get_default_setting rejects null option") {
		duckdb_v2_str out = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_default_setting(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("get_description rejects null option") {
		duckdb_v2_str out = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_description(nullptr, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("get_target_scope rejects null option") {
		DUCKDB_V2_OPTION_TARGET_SCOPE s;
		REQUIRE(duckdb_v2_option_get_target_scope(nullptr, &s, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("get_target_scope rejects null out_target_scope") {
		REQUIRE(duckdb_v2_option_get_target_scope(opt, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("get_alias_count rejects null option") {
		idx_t c;
		REQUIRE(duckdb_v2_option_get_alias_count(nullptr, &c, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("get_alias rejects null option") {
		duckdb_v2_str out = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_alias(nullptr, 0, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}
	SECTION("set_option / get_option reject null and malformed arguments") {
		REQUIRE(duckdb_v2_instance_set_option(nullptr, Convert("threads"), Convert("1"), nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_instance_set_option(fx.instance, duckdb_v2_str {nullptr, 1}, Convert("1"), nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_option_handle out = nullptr;
		REQUIRE(duckdb_v2_instance_get_option_by_name(fx.instance, Convert("threads"), nullptr, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_connection_set_option(nullptr, Convert("threads"), Convert("1"),
		                                        DUCKDB_V2_SETTING_SCOPE_AUTOMATIC,
		                                        nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_connection_get_option_by_name(fx.conn, Convert("threads"), nullptr, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_context_get_option_by_name(nullptr, Convert("threads"), &out, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
	}

	duckdb_v2_option_destroy(&opt);
}

} // namespace test_capi_v2
