#include "catch.hpp"
#include "duckdb.hpp"
#include "test_helpers.hpp"

#include <fstream>

#ifndef _WIN32
#include <sys/stat.h>
#endif

using namespace duckdb;

#ifndef _WIN32
//! Copy a persistent secret into the secret directory, with the permissions DuckDB requires for persistent secrets
static void CopySecretFile(const string &source, const string &target_dir, const string &file_name) {
	std::ifstream input(source, std::ios::binary);
	REQUIRE(input.good());
	auto target = target_dir + "/" + file_name;
	{
		std::ofstream output(target, std::ios::binary);
		output << input.rdbuf();
		REQUIRE(output.good());
	}
	REQUIRE(chmod(target.c_str(), S_IRUSR | S_IWUSR) == 0);
}

TEST_CASE("Test reading persistent secrets written by older versions", "[secret]") {
	auto secret_dir = TestCreatePath("persistent_secret_compatibility");
	TestCreateDirectory(secret_dir);
	for (auto &version : {"1_1_0", "1_1_2", "1_1_3"}) {
		auto file_name = string("http_v_") + version + ".duckdb_secret";
		CopySecretFile("data/secrets/http/" + file_name, secret_dir, file_name);
	}

	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET secret_directory='" + secret_dir + "'"));
	auto result = con.Query("SELECT count(*) FROM duckdb_secrets() WHERE contains(secret_string, "
	                        "'extra_http_headers={Authorization=Bearer sk_test_VePHdqKTYQjKNInc7u56JBrQ}')");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(3)}));
}
#endif
