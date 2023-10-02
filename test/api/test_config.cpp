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
	test_options["enable_object_cache"] = {"true", "false"};
	test_options["max_memory"] = {"-1", "16GB"};
	test_options["threads"] = {"-1", "4"};

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
