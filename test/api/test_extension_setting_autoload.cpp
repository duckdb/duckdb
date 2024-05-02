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

	// ENABLE_EXTENSION_AUTOLOADING
	// ENABLE_EXTENSION_AUTOINSTALL
	// LOCAL_EXTENSION_REPO

	config.options.autoload_known_extensions = true;
	auto env_var = std::getenv("LOCAL_EXTENSION_REPO");
	if (!env_var) {
		return;
	}
	config.options.autoinstall_extension_repo = std::string(env_var);
	REQUIRE(config.options.unrecognized_options.count("timezone"));

	// Create a connection
	DuckDB db(nullptr, &config);
	Connection con(db);
}
