#include "catch.hpp"
#include "test_helpers.hpp"

#include <iostream>
#include <map>
#include <set>

using namespace duckdb;
using namespace std;

TEST_CASE("Test autoload of extension settings", "[api]") {
	DBConfig config;
	config.SetOptionByName("timezone", "America/Los_Angeles");

	config.options.allow_unsigned_extensions = true;
	config.options.autoload_known_extensions = true;
	auto env_var = std::getenv("LOCAL_EXTENSION_REPO");
	if (!env_var) {
		return;
	}
	config.options.autoinstall_extension_repo = std::string(env_var);
	REQUIRE(config.options.unrecognized_options.count("timezone"));

	// Create a connection
	duckdb::unique_ptr<DuckDB> db;
	REQUIRE_NOTHROW(db = make_uniq<DuckDB>(nullptr, &config));
	Connection con(*db);

	auto res = con.Query("select current_setting('timezone')");
	REQUIRE(CHECK_COLUMN(res, 0, {Value("America/Los_Angeles")}));
}
