#include "catch.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/execution/operator/persistent/csv_reader_options.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"

#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <string>

using namespace duckdb;
using namespace std;

//! CSV Files
const string csv = "*.csv";
const string tsv = "*.tsv";
const string csv_gz = "csv.gz";
const string csv_zst = "csv.zst";
const string tbl_zst = "tbl.zst";

const string csv_extensions[5] = {csv, tsv, csv_gz, csv_zst, tbl_zst};
//! FIXME: Could have this done in the actual CSV first run, but i'm lazy and it seems a lot of fiddling
idx_t MaxLineSize(string &path) {
	idx_t max_line_size = 0;

	ifstream infile(path);

	string line;
	while (getline(infile, line)) {
		int line_size = line.length();
		if (line_size > max_line_size) {
			max_line_size = line_size;
		}
	}
	infile.close();
	return max_line_size;
}

bool RunFull(std::string &path, std::set<std::string> &skip, duckdb::Connection &conn) {
	// Here we run the csv file first with the single thread reader.
	// Then the parallel csv reader with a combination of multiple threads and buffer sizes.
	if (skip.find(path) != skip.end()) {
		// Gotta skip this
		return true;
	}
	unique_ptr<MaterializedQueryResult> single_threaded_res;
	ColumnDataCollection *ground_truth;
	try {
		single_threaded_res = conn.Query("SELECT * FROM read_csv_auto('" + path + "', parallel = 0)");
		ground_truth = &single_threaded_res->Collection();
	} catch (...) {
		// This means we can't read the file on ST, it's probably a borked file that exists for error checking
		return true;
	}
	// For parallel CSV Reading the buffer must be at least the size of the biggest line in the File.
	idx_t min_buffer_size = MaxLineSize(path) + 2;
	// So our tests don't take infinite time, we will go till a max buffer size of 25 positions higher than the minimum.
	idx_t max_buffer_size = min_buffer_size + 25;
	// Let's go from 1 to 8 threads.
	// TODO: Iterate over different buffer sizes
	DuckDB db(nullptr);
	Connection multi_conn(db);
	std::cout << path << std::endl;
	for (auto thread_count = 1; thread_count <= 8; thread_count++) {
		for (auto buffer_size = min_buffer_size; buffer_size < max_buffer_size; buffer_size++) {
			try {
				multi_conn.Query("PRAGMA threads=" + to_string(thread_count));
				unique_ptr<MaterializedQueryResult> multi_threaded_result = multi_conn.Query(
				    "SELECT * FROM read_csv_auto('" + path + "', buffer_size = " + to_string(buffer_size) + ")");
				auto &result = multi_threaded_result->Collection();
				// Results do not match
				string error_message;
				if (!ColumnDataCollection::ResultEquals(*ground_truth, result, error_message)) {
					std::cout << path << " Thread count: " << to_string(thread_count)
					          << " Buffer Size: " << to_string(buffer_size) << std::endl;
					std::cout << error_message << std::endl;
					return true;
				}
			} catch (...) {
				std::cout << "The house is burning" << std::endl;
				std::cout << path << " Thread count: " << to_string(thread_count)
				          << " Buffer Size: " << to_string(buffer_size) << std::endl;
				return true;
			}
		}
	}

	return true;
}

// Collects All CSV-Like files from folder and execute Parallel Scans on it
void RunTestOnFolder(const string &path, std::set<std::string> &skip) {
	DuckDB db(nullptr);
	Connection con(db);

	auto &fs = duckdb::FileSystem::GetFileSystem(*con.context);
	for (auto &ext : csv_extensions) {
		auto csv_files = fs.Glob(path + "*" + ext);
		for (auto &csv_file : csv_files) {
			REQUIRE(RunFull(csv_file, skip, con));
		}
	}
}

// TEST_CASE("Test One File", "[parallel-csv]") {
//	DuckDB db(nullptr);
//	Connection con(db);
//	std::set<std::string> skip;
//
//	string file = "test/sql/copy/csv/data/auto/issue_1254.csv";
//	REQUIRE(RunFull(file, skip, con));
// }

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data", "[parallel-csv]") {
	std::set<std::string> skip;
	RunTestOnFolder("test/sql/copy/csv/data/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/auto", "[parallel-csv]") {
	std::set<std::string> skip;
	// Thread count: 1 Buffer Size: 5
	skip.insert("test/sql/copy/csv/data/auto/issue_1254.csv");
	//  Thread count: 1 Buffer Size: 6
	skip.insert("test/sql/copy/csv/data/auto/issue_1254_rn.csv");
	// This file is from a 'mode skip' test
	skip.insert("test/sql/copy/csv/data/auto/titlebasicsdebug.tsv");
	RunTestOnFolder("test/sql/copy/csv/data/auto/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/auto/glob", "[parallel-csv]") {
	std::set<std::string> skip;
	RunTestOnFolder("test/sql/copy/csv/data/auto/glob/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/error/date_multiple_file", "[parallel-csv]") {
	std::set<std::string> skip;
	RunTestOnFolder("test/sql/copy/csv/data/error/date_multiple_file/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/glob/a1", "[parallel-csv]") {
	std::set<std::string> skip;
	RunTestOnFolder("test/sql/copy/csv/data/glob/a1/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/glob/a2", "[parallel-csv]") {
	std::set<std::string> skip;
	RunTestOnFolder("test/sql/copy/csv/data/glob/a2/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/glob/a3", "[parallel-csv]") {
	std::set<std::string> skip;
	RunTestOnFolder("test/sql/copy/csv/data/glob/a3/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/glob/empty", "[parallel-csv]") {
	std::set<std::string> skip;
	RunTestOnFolder("test/sql/copy/csv/data/glob/empty/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/glob/i1", "[parallel-csv]") {
	std::set<std::string> skip;
	RunTestOnFolder("test/sql/copy/csv/data/glob/i1/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/real", "[parallel-csv]") {
	std::set<std::string> skip;
	//! Segfault on buffer test
	skip.insert("test/sql/copy/csv/data/real/voter.tsv");
	skip.insert("test/sql/copy/csv/data/real/tmp2013-06-15.csv.gz");
	RunTestOnFolder("test/sql/copy/csv/data/real/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/test", "[parallel-csv]") {
	std::set<std::string> skip;
	//  Thread count: 1 Buffer Size: 52
	skip.insert("test/sql/copy/csv/data/test/multi_char_large.csv");
	// Thread count: 1 Buffer Size: 6
	skip.insert("test/sql/copy/csv/data/test/error_too_little_end_of_filled_chunk.csv");
	// Thread count: 1 Buffer Size: 5
	skip.insert("test/sql/copy/csv/data/test/error_too_little.csv");
	// Thread count: 1 Buffer Size: 35
	skip.insert("test/sql/copy/csv/data/test/big_header.csv");
	// Thread count: 1 Buffer Size: 19
	skip.insert("test/sql/copy/csv/data/test/new_line_string.csv");
	// Thread count: 1 Buffer Size: 19
	skip.insert("test/sql/copy/csv/data/test/quoted_newline.csv");
	// Stuck?
	skip.insert("test/sql/copy/csv/data/test/windows_newline.csv");
	// Thread count: 1 Buffer Size: 19
	skip.insert("test/sql/copy/csv/data/test/new_line_string_rn.csv");
	// Thread count: 1 Buffer Size: 19
	skip.insert("test/sql/copy/csv/data/test/new_line_string_rn_exc.csv");
	// Row count mismatch
	skip.insert("test/sql/copy/csv/data/test/test_comp.csv.gz");
	// Thread count: 1 Buffer Size: 2198
	skip.insert("test/sql/copy/csv/data/test/issue3562_assertion.csv.gz");
	RunTestOnFolder("test/sql/copy/csv/data/test/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/zstd", "[parallel-csv]") {
	std::set<std::string> skip;
	// Thread count: 1 Buffer Size: 541
	skip.insert("test/sql/copy/csv/data/zstd/ncvoter.csv.zst");
	// Thread count: 1 Buffer Size: 1432
	skip.insert("test/sql/copy/csv/data/zstd/lineitem1k.tbl.zst");
	RunTestOnFolder("test/sql/copy/csv/data/zstd/", skip);
}
