#include "catch.hpp"
#include "test_helpers.hpp"

#include <set>
#include <map>

using namespace duckdb;
using namespace std;

TEST_CASE("Test DB config configuration", "[api]") {
	DBConfig config;

	auto options = config.GetOptions();

	map<string, duckdb::vector<string>> test_options;
	test_options["access_mode"] = {"automatic", "read_only", "read_write"};
	test_options["default_order"] = {"asc", "desc"};
	test_options["default_null_order"] = {"nulls_first", "nulls_last"};
	test_options["enable_external_access"] = {"true", "false"};
	test_options["max_memory"] = {"-1", "16GB"};
	test_options["threads"] = {"1", "4"};

	REQUIRE(config.GetOptionByName("unknownoption") == nullptr);

	for (auto &option : options) {
		auto op = config.GetOptionByName(option.name);
		REQUIRE(op);

		auto entry = test_options.find(option.name);
		if (entry != test_options.end()) {
			for (auto &str_val : entry->second) {
				Value val(str_val);
				REQUIRE_NOTHROW(config.SetOption(option, val));
			}
			Value invalid_val("___this_is_probably_invalid");
			REQUIRE_THROWS(config.SetOption(option, invalid_val));
		}
	}
}

TEST_CASE("Test allowed options", "[api]") {
	case_insensitive_map_t<Value> config_dict;
	string option;

	SECTION("allowed_directories") {
		config_dict.emplace("allowed_directories", Value::LIST({Value("test")}));
		option = "allowed_directories";
	}
	SECTION("allowed_paths") {
		config_dict.emplace("allowed_paths", Value::LIST({Value("test")}));
		option = "allowed_paths";
	}

	SECTION("enable_logging") {
		config_dict.emplace("enable_logging", Value::BOOLEAN(false));
		option = "enable_logging";
	}

	SECTION("disabled_filesystems") {
		config_dict.emplace("disabled_filesystems", Value::BOOLEAN(false));
		option = "disabled_filesystems";
	}

	SECTION("logging_mode") {
		config_dict.emplace("logging_mode", Value::BOOLEAN(false));
		option = "logging_mode";
	}

	SECTION("logging_storage") {
		config_dict.emplace("logging_storage", Value::BOOLEAN(false));
		option = "logging_storage";
	}

	SECTION("logging_level") {
		config_dict.emplace("logging_level", Value::BOOLEAN(false));
		option = "logging_level";
	}

	SECTION("enabled_log_types") {
		config_dict.emplace("enabled_log_types", Value::BOOLEAN(false));
		option = "enabled_log_types";
	}

	SECTION("disabled_log_types") {
		config_dict.emplace("disabled_log_types", Value::BOOLEAN(false));
		option = "disabled_log_types";
	}

	try {
		DBConfig config(config_dict, false);
	} catch (std::exception &ex) {
		ErrorData error_data(ex);
		REQUIRE(error_data.Type() == ExceptionType::INVALID_INPUT);
		REQUIRE(error_data.RawMessage() ==
		        StringUtil::Format("Cannot change/set %s before the database is started", option));
	}
}

TEST_CASE("Test user_agent", "[api]") {
	{
		// Default duckdb_api is cpp
		DuckDB db(nullptr);
		Connection con(db);
		auto res = con.Query("PRAGMA user_agent");
		REQUIRE_THAT(res->GetValue(0, 0).ToString(), Catch::Matchers::Matches("duckdb/.*(.*) cpp"));
	}
	{
		// The latest provided duckdb_api is used
		DBConfig config;
		config.SetOptionByName("duckdb_api", "capi");
		config.SetOptionByName("duckdb_api", "go");
		DuckDB db("", &config);
		Connection con(db);
		auto res = con.Query("PRAGMA user_agent");
		REQUIRE_THAT(res->GetValue(0, 0).ToString(), Catch::Matchers::Matches("duckdb/.*(.*) go"));
	}
}
