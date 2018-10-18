
#include <string>
#include <fstream>
#include <streambuf>

#include "catch.hpp"
#include "test_helpers.hpp"
#include "common/file_system.hpp"
#include "dbgen.hpp"

using namespace duckdb;
using namespace std;

#define FILE_COUNT

TEST_CASE("Test crashing SQLSmith queries", "[sqlsmith][.]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	tpch::dbgen(0.1, db);
	auto query_directory = JoinPath(GetWorkingDirectory(), "test/sqlsmith/queries");
	ListFiles(query_directory, [&](string file_name) {
		fprintf(stderr, "%s\n", file_name.c_str());
		auto fname = JoinPath(query_directory, file_name);

		ifstream t(fname);
		string query((istreambuf_iterator<char>(t)),
		                 istreambuf_iterator<char>());
		con.Query(query.c_str());
	});

}
