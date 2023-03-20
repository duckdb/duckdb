#include "catch.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

//! CSV Files
const string csv = "*.csv";
const string tsv = "*.tsv";

void RunSingleThread(){

}

void RunParallel(){

}

bool RunFull(std::string &path, std::set<std::string> &skip, duckdb::Connection &conn){
	// Here we run the csv file first with the single thread reader.
	// Then the parallel csv reader with a combination of multiple threads and buffer sizes.
    if (skip.find(path) == skip.end()){
		// Gotta skip this
		return true;
	}

}

void RunTestCaseOnFolder(const string& path){
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto &fs = duckdb::FileSystem::GetFileSystem(*con.context);
	std::set<std::string> skip;
	auto csv_files = fs.Glob(path+ "*.csv");
	for (auto &csv_file : csv_files) {
		REQUIRE(RunFull(csv_file, skip, con));
	}
}
}



TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/auto", "[parallel-csv]") {
	RunTestCaseOnFolder("test/sql/copy/csv/data/auto/");
}
