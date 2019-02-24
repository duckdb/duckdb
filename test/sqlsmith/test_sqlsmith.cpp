#include "catch.hpp"
#include "common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

#include <fstream>
#include <streambuf>
#include <string>

using namespace duckdb;
using namespace std;

constexpr const char *QUERY_DIRECTORY = "test/sqlsmith/queries";

static void test_runner() {
	auto file_name = Catch::getResultCapture().getCurrentTestName();
	auto fname = JoinPath(QUERY_DIRECTORY, file_name);

	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	con.EnableProfiling();

	tpch::dbgen(0.1, db);

	ifstream t(fname);
	string query((istreambuf_iterator<char>(t)), istreambuf_iterator<char>());
	con.Query(query.c_str());
	// we don't know whether the query fails or not and we don't know the
	// correct result we just don't want it to crash
	REQUIRE(1 == 1);
}

struct RegisterSQLSmithTests {
	RegisterSQLSmithTests() {
		return;
		// register a separate SQL Smith test for each file in the QUERY_DIRECTORY
		ListFiles(QUERY_DIRECTORY, [&](const string &path) { REGISTER_TEST_CASE(test_runner, path, "[sqlsmith][.]"); });
	}
};
RegisterSQLSmithTests register_sqlsmith_test;
