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
bool CompareResults(vector<unique_ptr<DataChunk>> &ground_truth, vector<unique_ptr<DataChunk>> &result) {
	if (ground_truth.size() != result.size()) {
		// Sizes dont' match
		return false;
	}
	for (idx_t i = 0; i < ground_truth.size(); i++) {
		auto lchunk = ground_truth[i].get();
		auto rchunk = result[i].get();
		if (!lchunk && !rchunk) {
			return true;
		}
		if (!lchunk || !rchunk) {
			return false;
		}
		if (lchunk->size() == 0 && rchunk->size() == 0) {
			return true;
		}
		if (lchunk->size() != rchunk->size()) {
			return false;
		}
		D_ASSERT(lchunk->ColumnCount() == rchunk->ColumnCount());
		for (idx_t col = 0; col < rchunk->ColumnCount(); col++) {
			for (idx_t row = 0; row < rchunk->size(); row++) {
				auto lvalue = lchunk->GetValue(col, row);
				auto rvalue = rchunk->GetValue(col, row);
				if (lvalue.IsNull() && rvalue.IsNull()) {
					continue;
				}
				if (lvalue.IsNull() != rvalue.IsNull()) {
					return false;
				}
				if (lvalue != rvalue) {
					return false;
				}
			}
		}
	}
	return true;
}

vector<unique_ptr<DataChunk>> RunSingleThread(duckdb::Connection &conn, std::string &path) {
	vector<unique_ptr<DataChunk>> ground_truth;
	auto q_res = conn.Query("SELECT * FROM read_csv_auto('" + path + "', parallel = 0)");
	auto data_chunk = q_res->Fetch();
	while (data_chunk) {
		ground_truth.emplace_back(std::move(data_chunk));
	}
	return ground_truth;
}
vector<unique_ptr<DataChunk>> RunParallel(duckdb::Connection &conn, std::string &path, idx_t thread_count,
                                          idx_t buffer_size) {
	vector<unique_ptr<DataChunk>> result;
	auto q_res = conn.Query("SELECT * FROM read_csv_auto('" + path + "')");
	auto data_chunk = q_res->Fetch();
	while (data_chunk) {
		result.emplace_back(std::move(data_chunk));
	}
	return result;
}

bool RunFull(std::string &path, std::set<std::string> &skip, duckdb::Connection &conn) {
	// Here we run the csv file first with the single thread reader.
	// Then the parallel csv reader with a combination of multiple threads and buffer sizes.
	if (skip.find(path) != skip.end()) {
		// Gotta skip this
		return true;
	}
	vector<unique_ptr<DataChunk>> ground_truth;
	try {
		ground_truth = RunSingleThread(conn, path);
	} catch (...) {
		// This means we can't read the file on ST, it's probably a borked file that exists for error checking
		return true;
	}
	// For parallel CSV Reading the buffer must be at least the size of the biggest line in the File.
	idx_t min_buffer_size = MaxLineSize(path);
	// So our tests don't take infinite time, we will go till a max buffer size of 25 positions higher than the minimum.
	idx_t max_buffer_size = MaxLineSize(path);
	// Let's go from 1 to 8 threads.
	// TODO: Iterate over different buffer sizes
	for (auto thread_count = 1; thread_count <= 8; thread_count++) {
		for (auto buffer_size = min_buffer_size; buffer_size < max_buffer_size; buffer_size++) {
			try {
				auto parallel_result = RunParallel(conn, path, thread_count, buffer_size);
				// Results do not match
				if (!CompareResults(ground_truth, parallel_result)) {
					std::cout << path << " Thread count: " << to_string(thread_count)
					          << " Buffer Size: " << to_string(buffer_size) << std::endl;
					return true;
				}
			} catch (...) {
				// Results do not match
				std::cout << "The house is burning" << std::endl;
				std::cout << path << " Thread count: " << to_string(thread_count)
				          << " Buffer Size: " << to_string(buffer_size) << std::endl;
				return true;
			}
		}
	}
	//	std::cout << path << std::endl;
	return true;
}

// Collects All CSV-Like files from folder and execute Parallel Scans on it
void RunTestOnFolder(const string &path, std::set<std::string> &skip) {
	unique_ptr<QueryResult> result;
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

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data", "[parallel-csv]") {
	std::set<std::string> skip;
	skip.insert("test/sql/copy/csv/data/people.csv");
	RunTestOnFolder("test/sql/copy/csv/data/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/auto", "[parallel-csv]") {
	std::set<std::string> skip;
	skip.insert("test/sql/copy/csv/data/auto/issue_1254.csv");
	skip.insert("test/sql/copy/csv/data/auto/issue_1254_rn.csv");
	skip.insert("test/sql/copy/csv/data/auto/normalize_names_1.csv");
	skip.insert("test/sql/copy/csv/data/auto/normalize_names_3.csv");
	skip.insert("test/sql/copy/csv/data/auto/normalize_names_2.csv");
	skip.insert("test/sql/copy/csv/data/auto/normalize_names_4.csv");
	skip.insert("test/sql/copy/csv/data/auto/normalize_names_5.csv");
	skip.insert("test/sql/copy/csv/data/auto/rfc_conform_quote.csv");
	skip.insert("test/sql/copy/csv/data/auto/rfc_conform.csv");
	skip.insert("test/sql/copy/csv/data/auto/titlebasicsdebug.tsv");
	RunTestOnFolder("test/sql/copy/csv/data/auto/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/auto/glob", "[parallel-csv]") {
	std::set<std::string> skip;
	RunTestOnFolder("test/sql/copy/csv/data/auto/glob/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/error/date_multiple_file", "[parallel-csv]") {
	std::set<std::string> skip;
	skip.insert("test/sql/copy/csv/data/error/date_multiple_file/1.csv");
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
	skip.insert("test/sql/copy/csv/data/glob/empty/empty.csv");
	RunTestOnFolder("test/sql/copy/csv/data/glob/empty/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/glob/i1", "[parallel-csv]") {
	std::set<std::string> skip;
	RunTestOnFolder("test/sql/copy/csv/data/glob/i1/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/real", "[parallel-csv]") {
	std::set<std::string> skip;
	skip.insert("test/sql/copy/csv/data/real/ncvoter.csv");
	skip.insert("test/sql/copy/csv/data/real/lineitem_sample.csv");
	skip.insert("test/sql/copy/csv/data/real/web_page.csv");
	skip.insert("test/sql/copy/csv/data/real/imdb_movie_info_escaped.csv");
	skip.insert("test/sql/copy/csv/data/real/nfc_normalization.csv");
	skip.insert("test/sql/copy/csv/data/real/ontime_sample.csv");
	skip.insert("test/sql/copy/csv/data/real/voter.tsv");
	skip.insert("test/sql/copy/csv/data/real/tmp2013-06-15.csv.gz");
	RunTestOnFolder("test/sql/copy/csv/data/real/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/test", "[parallel-csv]") {
	std::set<std::string> skip;
	skip.insert("test/sql/copy/csv/data/test/no_newline_unicode.csv");
	skip.insert("test/sql/copy/csv/data/test/long_escaped_value_unicode.csv");
	skip.insert("test/sql/copy/csv/data/test/big_header.csv");
	skip.insert("test/sql/copy/csv/data/test/new_line_string.csv");
	skip.insert("test/sql/copy/csv/data/test/blob.csv");
	skip.insert("test/sql/copy/csv/data/test/test_long_line.csv");
	skip.insert("test/sql/copy/csv/data/test/long_escaped_value.csv");
	skip.insert("test/sql/copy/csv/data/test/shared_substring_large.csv");
	skip.insert("test/sql/copy/csv/data/test/windows_newline_empty.csv");
	skip.insert("test/sql/copy/csv/data/test/multi_char.csv");
	skip.insert("test/sql/copy/csv/data/test/new_line_string_rn.csv");
	skip.insert("test/sql/copy/csv/data/test/new_line_string_rn_exc.csv");
	skip.insert("test/sql/copy/csv/data/test/issue3562_assertion.csv.gz");
	RunTestOnFolder("test/sql/copy/csv/data/test/", skip);
}

TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/zstd", "[parallel-csv]") {
	std::set<std::string> skip;
	skip.insert("test/sql/copy/csv/data/zstd/ncvoter.csv.zst");
	RunTestOnFolder("test/sql/copy/csv/data/zstd/", skip);
}
