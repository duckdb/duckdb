#include "catch.hpp"
#include "common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

#include <fstream>
#include <streambuf>
#include <string>

using namespace duckdb;
using namespace std;

#define FILE_COUNT

TEST_CASE("Test crashing SQLSmith queries", "[sqlsmith][.]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	con.EnableProfiling();

	tpch::dbgen(0.1, db);
	auto query_directory = JoinPath(GetWorkingDirectory(), "test/sqlsmith/queries");
	ListFiles(query_directory, [&](string file_name) {
		fprintf(stderr, "%s\n", file_name.c_str());
		auto fname = JoinPath(query_directory, file_name);

		ifstream t(fname);
		string query((istreambuf_iterator<char>(t)), istreambuf_iterator<char>());
		con.Query(query.c_str());
		// we don't know whether the query fails or not and we don't know the
		// correct result we just don't want it to crash
		REQUIRE(1 == 1);
	});
}
