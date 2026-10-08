#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/os_util.hpp"
#include "test_helpers.hpp"

#include <cstdlib>

using namespace duckdb;
using namespace std;

#ifndef _WIN32
TEST_CASE("Environment variables are read through OSUtil", "[api]") {
	setenv("DUCKDB_TEST_OS_UTIL", "quack", 1);
	setenv("http_proxy_username", "env_user", 1);
	{
		DuckDB db(nullptr);
		auto &os_util = OSUtil::Get(*db.instance);
		string value;
		REQUIRE(os_util.GetEnv("DUCKDB_TEST_OS_UTIL", value));
		REQUIRE(value == "quack");
		REQUIRE(os_util.GetEnv("DUCKDB_TEST_OS_UTIL") == "quack");
		REQUIRE(!os_util.GetEnv("DUCKDB_TEST_OS_UTIL_UNSET", value));
		// unrestricted reads are for known configuration variables only, unless the embedder registers them
		REQUIRE(OSUtil::IsConfigurationEnv("HOME"));
		REQUIRE(!OSUtil::IsConfigurationEnv("DUCKDB_TEST_OS_UTIL"));
		REQUIRE_THROWS(os_util.GetEnvUnrestricted("DUCKDB_TEST_OS_UTIL", value));
		os_util.RegisterConfigurationEnv("DUCKDB_TEST_OS_UTIL");
		REQUIRE(os_util.GetEnvUnrestricted("DUCKDB_TEST_OS_UTIL", value));
		REQUIRE(value == "quack");
		REQUIRE(os_util.GetEnv("DUCKDB_TEST_OS_UTIL_UNSET").empty());
		REQUIRE(!FileSystem::GetHomeDirectory(*db.instance).empty());

		// the env secret provider picks up proxy settings from the environment
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE SECRET http_env (TYPE http, PROVIDER env)"));
		auto result = con.Query("SELECT secret_string LIKE '%env_user%' FROM duckdb_secrets() WHERE name = 'http_env'");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
	}
	{
		// without external access only the variables the engine needs to configure itself are readable
		DBConfig config;
		config.SetOptionByName("enable_external_access", Value::BOOLEAN(false));
		// the persistent secret directory cannot be scanned without external access
		config.SetOptionByName("allow_persistent_secrets", Value::BOOLEAN(false));
		DuckDB db(nullptr, &config);
		auto &os_util = OSUtil::Get(*db.instance);
		string value;
		REQUIRE_THROWS(os_util.GetEnv("DUCKDB_TEST_OS_UTIL", value));
		REQUIRE_THROWS(os_util.GetEnv("DUCKDB_TEST_OS_UTIL"));
		// configuration reads are not restricted by the setting
		REQUIRE_THROWS(os_util.GetEnvUnrestricted("DUCKDB_TEST_OS_UTIL", value));
		os_util.RegisterConfigurationEnv("DUCKDB_TEST_OS_UTIL");
		REQUIRE(os_util.GetEnvUnrestricted("DUCKDB_TEST_OS_UTIL", value));
		REQUIRE(value == "quack");
		REQUIRE(!FileSystem::GetHomeDirectory(*db.instance).empty());

		// the env secret provider is refused rather than silently producing an empty secret
		Connection con(db);
		auto result = con.Query("CREATE SECRET http_env (TYPE http, PROVIDER env)");
		REQUIRE_FAIL(result);
		REQUIRE(StringUtil::Contains(result->GetError(), "environment access is disabled"));
	}
	unsetenv("DUCKDB_TEST_OS_UTIL");
	unsetenv("http_proxy_username");
}
#endif
