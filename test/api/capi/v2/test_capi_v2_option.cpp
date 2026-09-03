#include "test_capi_v2.hpp"

// ---------------------------------------------------------------------------
// V2 option handle + database/connection option get/set. Scope enforcement
// reuses PhysicalSet::GetSettingScope so error messages are DuckDB's own.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {
TEST_CASE("V2 db option: set + get round-trip", "[capi_v2][db][option]") {
	EnvFixture fx;

	// Read the default before mutating so we can compare against it.
	duckdb_v2_option_handle before = nullptr;
	REQUIRE(duckdb_v2_database_option_get(fx.db, Convert("memory_limit"), &before, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_str before_setting = {nullptr, 0};
	duckdb_v2_option_get_setting(before, &before_setting, nullptr);
	std::string default_value = Convert(before_setting);
	duckdb_v2_option_destroy(&before);

	duckdb_v2_option_handle in_opt = nullptr;
	duckdb_v2_option_create(Convert("memory_limit"), Convert("1GB"), &in_opt, nullptr);
	REQUIRE(duckdb_v2_database_option_set(fx.db, in_opt, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_option_destroy(&in_opt);

	duckdb_v2_option_handle after = nullptr;
	REQUIRE(duckdb_v2_database_option_get(fx.db, Convert("memory_limit"), &after, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_str after_setting = {nullptr, 0};
	duckdb_v2_option_get_setting(after, &after_setting, nullptr);
	REQUIRE(after_setting.ptr != nullptr);
	REQUIRE(after_setting != default_value); // mutation visible
	duckdb_v2_option_destroy(&after);
}
TEST_CASE("V2 db option: get populates description and aliases", "[capi_v2][db][option]") {
	EnvFixture fx;

	duckdb_v2_option_handle opt = nullptr;
	REQUIRE(duckdb_v2_database_option_get(fx.db, Convert("memory_limit"), &opt, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_option_get_name(opt, &name, nullptr);
	// "memory_limit" is an alias; the canonical name is something else
	// (e.g. "max_memory"). Either way, the alias list should contain
	// "memory_limit".
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
}
TEST_CASE("V2 db option: get unknown name errors", "[capi_v2][db][option]") {
	EnvFixture fx;
	duckdb_v2_option_handle out = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_database_option_get(fx.db, Convert("this_option_does_not_exist"), &out, &err) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(out == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
}
TEST_CASE("V2 db option: get_count and get_by_index", "[capi_v2][db][option]") {
	EnvFixture fx;
	idx_t count = 0;
	REQUIRE(duckdb_v2_database_option_get_count(fx.db, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count > 0);

	// Walk the first few entries — each should produce a populated handle.
	idx_t to_check = count < 5 ? count : 5;
	for (idx_t i = 0; i < to_check; i++) {
		duckdb_v2_option_handle opt = nullptr;
		REQUIRE(duckdb_v2_database_option_get_by_index(fx.db, i, &opt, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_str name = {nullptr, 0};
		duckdb_v2_option_get_name(opt, &name, nullptr);
		REQUIRE(name.ptr != nullptr);
		REQUIRE(name.len != 0);
		duckdb_v2_option_destroy(&opt);
	}

	duckdb_v2_option_handle out_of_range = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_database_option_get_by_index(fx.db, count + 100, &out_of_range, &err) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_error_info_destroy(&err);
}
TEST_CASE("V2 conn option: set LOCAL is invisible to other connections", "[capi_v2][conn][option]") {
	EnvFixture fx;
	duckdb_v2_connection_handle other = nullptr;
	duckdb_v2_connect(fx.db, &other, nullptr);

	// max_execution_time is LOCAL_DEFAULT, so a LOCAL-scope write stays
	// session-local — perfect for this test.
	duckdb_v2_option_handle opt = nullptr;
	duckdb_v2_option_create(Convert("max_execution_time"), Convert("5000"), &opt, nullptr);
	REQUIRE(duckdb_v2_connection_option_set(fx.conn, opt, DUCKDB_V2_SETTING_SCOPE_LOCAL, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_option_destroy(&opt);

	duckdb_v2_option_handle on_fx = nullptr;
	duckdb_v2_connection_option_get(fx.conn, Convert("max_execution_time"), &on_fx, nullptr);
	duckdb_v2_str fx_setting = {nullptr, 0};
	duckdb_v2_option_get_setting(on_fx, &fx_setting, nullptr);
	REQUIRE(fx_setting == "5000");
	duckdb_v2_option_destroy(&on_fx);

	duckdb_v2_option_handle on_other = nullptr;
	duckdb_v2_connection_option_get(other, Convert("max_execution_time"), &on_other, nullptr);
	duckdb_v2_str other_setting = {nullptr, 0};
	duckdb_v2_option_get_setting(on_other, &other_setting, nullptr);
	// The other connection sees the static default ("0"), not "5000".
	REQUIRE(other_setting != "5000");
	duckdb_v2_option_destroy(&on_other);

	duckdb_v2_disconnect(&other);
}
TEST_CASE("V2 conn option: set GLOBAL is visible everywhere", "[capi_v2][conn][option]") {
	EnvFixture fx;
	duckdb_v2_connection_handle other = nullptr;
	duckdb_v2_connect(fx.db, &other, nullptr);

	duckdb_v2_option_handle opt = nullptr;
	duckdb_v2_option_create(Convert("memory_limit"), Convert("2GB"), &opt, nullptr);
	REQUIRE(duckdb_v2_connection_option_set(fx.conn, opt, DUCKDB_V2_SETTING_SCOPE_GLOBAL, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_option_destroy(&opt);

	std::string fx_setting, other_setting;
	for (auto target : {fx.conn, other}) {
		duckdb_v2_option_handle seen = nullptr;
		duckdb_v2_connection_option_get(target, Convert("memory_limit"), &seen, nullptr);
		duckdb_v2_str setting = {nullptr, 0};
		duckdb_v2_option_get_setting(seen, &setting, nullptr);
		(target == fx.conn ? fx_setting : other_setting) = Convert(setting);
		duckdb_v2_option_destroy(&seen);
	}
	REQUIRE(!fx_setting.empty());
	REQUIRE(fx_setting == other_setting); // GLOBAL write seen identically by both

	duckdb_v2_disconnect(&other);
}
TEST_CASE("V2 conn option: scope enforcement matches SQL", "[capi_v2][conn][option]") {
	EnvFixture fx;
	duckdb_v2_error_info_handle err = nullptr;

	// GLOBAL_ONLY × LOCAL: rejected. allow_community_extensions is GLOBAL_ONLY.
	duckdb_v2_option_handle global_only = nullptr;
	duckdb_v2_option_create(Convert("allow_community_extensions"), Convert("false"), &global_only, nullptr);
	REQUIRE(duckdb_v2_connection_option_set(fx.conn, global_only, DUCKDB_V2_SETTING_SCOPE_LOCAL, &err) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_option_destroy(&global_only);
}
TEST_CASE("V2 conn option: AUTOMATIC scope mirrors bare SQL `SET`", "[capi_v2][conn][option]") {
	EnvFixture fx;
	// max_execution_time is LOCAL_DEFAULT → AUTOMATIC resolves to SESSION
	// → write succeeds.
	duckdb_v2_option_handle local = nullptr;
	duckdb_v2_option_create(Convert("max_execution_time"), Convert("5000"), &local, nullptr);
	REQUIRE(duckdb_v2_connection_option_set(fx.conn, local, DUCKDB_V2_SETTING_SCOPE_AUTOMATIC, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_option_destroy(&local);
}
TEST_CASE("V2 db/conn option: open with options applies them at GLOBAL scope", "[capi_v2][db][option]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_create_environment(&env, nullptr);

	duckdb_v2_option_handle o1 = nullptr;
	duckdb_v2_option_create(Convert("memory_limit"), Convert("2GB"), &o1, nullptr);
	duckdb_v2_option_handle opts[] = {o1};

	duckdb_v2_database_handle db = nullptr;
	REQUIRE(duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, opts, 1, &db, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_option_handle seen = nullptr;
	duckdb_v2_database_option_get(db, Convert("memory_limit"), &seen, nullptr);
	duckdb_v2_str setting = {nullptr, 0};
	duckdb_v2_option_get_setting(seen, &setting, nullptr);
	REQUIRE(setting.ptr != nullptr);
	REQUIRE(setting.len > 0);
	// Compare against the un-set baseline by opening a second db.
	duckdb_v2_database_handle db_default = nullptr;
	duckdb_v2_open(env, duckdb_v2_str {nullptr, 0}, nullptr, 0, &db_default, nullptr);
	duckdb_v2_option_handle def_opt = nullptr;
	duckdb_v2_database_option_get(db_default, Convert("memory_limit"), &def_opt, nullptr);
	duckdb_v2_str def_setting = {nullptr, 0};
	duckdb_v2_option_get_setting(def_opt, &def_setting, nullptr);
	REQUIRE(Convert(setting) != def_setting);
	duckdb_v2_option_destroy(&def_opt);
	duckdb_v2_close(&db_default);
	duckdb_v2_option_destroy(&seen);

	duckdb_v2_option_destroy(&o1);
	duckdb_v2_close(&db);
	duckdb_v2_destroy_environment(&env);
}
TEST_CASE("V2 option: create / destroy", "[capi_v2][option]") {
	SECTION("create succeeds and destroy nulls the slot") {
		duckdb_v2_option_handle opt = nullptr;
		REQUIRE(duckdb_v2_option_create(Convert("memory_limit"), Convert("1GB"), &opt, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(opt != nullptr);
		REQUIRE(duckdb_v2_option_destroy(&opt) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(opt == nullptr);
	}

	SECTION("create with empty name and setting succeeds (strings are just copied)") {
		duckdb_v2_option_handle opt = nullptr;
		REQUIRE(duckdb_v2_option_create(Convert(""), Convert(""), &opt, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_str name = {nullptr, 0};
		duckdb_v2_str setting = {nullptr, 0};
		duckdb_v2_option_get_name(opt, &name, nullptr);
		duckdb_v2_option_get_setting(opt, &setting, nullptr);
		REQUIRE(name.len == 0);
		REQUIRE(setting.len == 0);
		duckdb_v2_option_destroy(&opt);
	}

	SECTION("create rejects a malformed name view (null ptr, nonzero len)") {
		duckdb_v2_option_handle opt = nullptr;
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_option_create(duckdb_v2_str {nullptr, 1}, Convert("x"), &opt, &err) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(opt == nullptr);
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);
	}

	SECTION("create rejects a malformed setting view (null ptr, nonzero len)") {
		duckdb_v2_option_handle opt = nullptr;
		REQUIRE(duckdb_v2_option_create(Convert("x"), duckdb_v2_str {nullptr, 1}, &opt, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(opt == nullptr);
	}

	SECTION("create with an empty (null, 0) name and setting succeeds") {
		duckdb_v2_option_handle opt = nullptr;
		REQUIRE(duckdb_v2_option_create(duckdb_v2_str {nullptr, 0}, duckdb_v2_str {nullptr, 0}, &opt, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(opt != nullptr);
		duckdb_v2_option_destroy(&opt);
	}

	SECTION("create rejects null out_option") {
		REQUIRE(duckdb_v2_option_create(Convert("x"), Convert("y"), nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	}

	SECTION("destroy with null pointer-to-handle is a no-op") {
		REQUIRE(duckdb_v2_option_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	}

	SECTION("destroy on already-null slot is a no-op") {
		duckdb_v2_option_handle opt = nullptr;
		REQUIRE(duckdb_v2_option_destroy(&opt) == DUCKDB_V2_ERROR_NONE);
	}

	SECTION("double destroy is safe (slot was nulled by first destroy)") {
		duckdb_v2_option_handle opt = nullptr;
		duckdb_v2_option_create(Convert("memory_limit"), Convert("1GB"), &opt, nullptr);
		REQUIRE(duckdb_v2_option_destroy(&opt) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_option_destroy(&opt) == DUCKDB_V2_ERROR_NONE);
	}
}
TEST_CASE("V2 option: accessors round-trip user-supplied values", "[capi_v2][option]") {
	duckdb_v2_option_handle opt = nullptr;
	REQUIRE(duckdb_v2_option_create(Convert("memory_limit"), Convert("2GB"), &opt, nullptr) == DUCKDB_V2_ERROR_NONE);

	SECTION("get_name returns the user-supplied name") {
		duckdb_v2_str name = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_name(opt, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(name == "memory_limit");
	}

	SECTION("get_setting returns the user-supplied setting") {
		duckdb_v2_str setting = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_setting(opt, &setting, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(setting == "2GB");
	}

	SECTION("get_default_setting returns empty string until populated by a get") {
		duckdb_v2_str def = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_default_setting(opt, &def, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(def.len == 0);
	}

	SECTION("get_description returns empty string until populated by a get") {
		duckdb_v2_str desc = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_description(opt, &desc, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(desc.len == 0);
	}

	SECTION("get_target_scope returns UNKNOWN until populated by a get") {
		DUCKDB_V2_OPTION_TARGET_SCOPE scope = DUCKDB_V2_OPTION_TARGET_SCOPE_GLOBAL_ONLY;
		REQUIRE(duckdb_v2_option_get_target_scope(opt, &scope, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(scope == DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN);
	}

	SECTION("get_alias_count returns 0 until populated by a get") {
		idx_t count = 99;
		REQUIRE(duckdb_v2_option_get_alias_count(opt, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(count == 0);
	}

	SECTION("get_alias on empty alias list returns INVALID_INPUT") {
		duckdb_v2_str alias = {nullptr, 0};
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_option_get_alias(opt, 0, &alias, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(alias.ptr == nullptr);
		REQUIRE(err != nullptr);
		duckdb_v2_error_info_destroy(&err);
	}

	duckdb_v2_option_destroy(&opt);
}
TEST_CASE("V2 option: accessor null-arg validation", "[capi_v2][option]") {
	duckdb_v2_option_handle opt = nullptr;
	duckdb_v2_option_create(Convert("k"), Convert("v"), &opt, nullptr);

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

	duckdb_v2_option_destroy(&opt);
}
TEST_CASE("V2 option: handles are independent", "[capi_v2][option]") {
	// Two options created back-to-back must not alias each other's storage,
	// and destroying one must not affect the other.
	duckdb_v2_option_handle a = nullptr;
	duckdb_v2_option_handle b = nullptr;
	duckdb_v2_option_create(Convert("memory_limit"), Convert("1GB"), &a, nullptr);
	duckdb_v2_option_create(Convert("threads"), Convert("4"), &b, nullptr);

	duckdb_v2_str name_a = {nullptr, 0};
	duckdb_v2_str name_b = {nullptr, 0};
	duckdb_v2_option_get_name(a, &name_a, nullptr);
	duckdb_v2_option_get_name(b, &name_b, nullptr);
	REQUIRE(name_a == "memory_limit");
	REQUIRE(name_b == "threads");

	duckdb_v2_option_destroy(&a);
	REQUIRE(a == nullptr);

	// b's accessors still work after a's destruction.
	duckdb_v2_str still_b = {nullptr, 0};
	REQUIRE(duckdb_v2_option_get_name(b, &still_b, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(still_b == "threads");

	duckdb_v2_option_destroy(&b);
}
TEST_CASE("V2 option: borrowed pointers stay valid until destroy", "[capi_v2][option]") {
	// Per the contract, accessors return borrowed pointers valid until the
	// option is destroyed. Repeated reads must return stable pointers
	// (the strings are owned by the wrapper and don't move on access).
	duckdb_v2_option_handle opt = nullptr;
	duckdb_v2_option_create(Convert("foo"), Convert("bar"), &opt, nullptr);

	duckdb_v2_str first_name = {nullptr, 0};
	duckdb_v2_str second_name = {nullptr, 0};
	duckdb_v2_option_get_name(opt, &first_name, nullptr);
	duckdb_v2_option_get_name(opt, &second_name, nullptr);
	REQUIRE(first_name.ptr == second_name.ptr); // borrowed pointer is stable across reads
	REQUIRE(first_name == "foo");

	duckdb_v2_option_destroy(&opt);
}
TEST_CASE("V2 option: error info is populated on failure paths", "[capi_v2][option]") {
	SECTION("create with a malformed name view surfaces a descriptive error") {
		duckdb_v2_option_handle opt = nullptr;
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_option_create(duckdb_v2_str {nullptr, 1}, Convert("v"), &opt, &err) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(err != nullptr);
		duckdb_v2_str msg = {nullptr, 0};
		duckdb_v2_error_info_get_text(err, &msg);
		REQUIRE(Convert(msg).find("duckdb_v2_option_create") != std::string::npos);
		duckdb_v2_error_info_destroy(&err);
	}

	SECTION("get_alias out-of-range surfaces a descriptive error") {
		duckdb_v2_option_handle opt = nullptr;
		duckdb_v2_option_create(Convert("k"), Convert("v"), &opt, nullptr);
		duckdb_v2_str alias = {nullptr, 0};
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_option_get_alias(opt, 5, &alias, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(err != nullptr);
		duckdb_v2_str msg = {nullptr, 0};
		duckdb_v2_error_info_get_text(err, &msg);
		REQUIRE(Convert(msg).find("out of range") != std::string::npos);
		duckdb_v2_error_info_destroy(&err);
		duckdb_v2_option_destroy(&opt);
	}

	SECTION("err == nullptr is tolerated on every failure path") {
		duckdb_v2_option_handle opt = nullptr;
		REQUIRE(duckdb_v2_option_create(duckdb_v2_str {nullptr, 1}, Convert("v"), &opt, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_option_create(Convert("k"), Convert("v"), &opt, nullptr);
		duckdb_v2_str alias = {nullptr, 0};
		REQUIRE(duckdb_v2_option_get_alias(opt, 99, &alias, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_option_destroy(&opt);
	}
}

} // namespace test_capi_v2
