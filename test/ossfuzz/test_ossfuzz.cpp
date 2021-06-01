#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

#include <fstream>
#include <streambuf>
#include <string>

using namespace duckdb;
using namespace std;

constexpr const char *QUERY_DIRECTORY = "test/ossfuzz/cases";
static FileSystem fs;

static void test_runner() {
	auto file_name = Catch::getResultCapture().getCurrentTestName();
	auto fname = fs.JoinPath(QUERY_DIRECTORY, file_name);

	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	ifstream t(fname);
	string query((istreambuf_iterator<char>(t)), istreambuf_iterator<char>());
	con.Query(query.c_str());
	// we don't know whether the query fails or not and we don't know the
	// correct result we just don't want it to crash
	REQUIRE(1 == 1);
}

struct RegisterOssfuzzTests {
	RegisterOssfuzzTests() {
		// register a separate test for each file in the QUERY_DIRECTORY
		fs.ListFiles(QUERY_DIRECTORY, [&](const string &path, bool) {
			REGISTER_TEST_CASE(test_runner, string(QUERY_DIRECTORY) + "/" + path, "[ossfuzz][.]");
		});
	}
};
RegisterOssfuzzTests register_ossfuzz_test;
