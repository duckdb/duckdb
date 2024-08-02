#include "catch.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/client_data.hpp"

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

const char *run = std::getenv("DUCKDB_RUN_PARALLEL_CSV_TESTS");

bool RunVariableBuffer(const string &path, idx_t buffer_size, bool set_temp_dir,
                       ColumnDataCollection *ground_truth = nullptr, const string &add_parameters = "") {
	DuckDB db(nullptr);
	Connection multi_conn(db);
	if (set_temp_dir) {
		multi_conn.Query("PRAGMA temp_directory='offload.tmp'");
	}
	multi_conn.Query("SET preserve_insertion_order=false;");
	duckdb::unique_ptr<MaterializedQueryResult> variable_buffer_size_result =
	    multi_conn.Query("SELECT * FROM read_csv_auto('" + path + "'" + add_parameters +
	                     ", buffer_size = " + to_string(buffer_size) + ") ORDER BY ALL");
	bool variable_buffer_size_passed;
	ColumnDataCollection *result = nullptr;
	if (variable_buffer_size_result->HasError()) {
		variable_buffer_size_passed = false;
	} else {
		variable_buffer_size_passed = true;
		result = &variable_buffer_size_result->Collection();
	}
	if (!ground_truth && !variable_buffer_size_passed) {
		// Two wrongs can make a right
		return true;
	}
	if (!ground_truth) {
		//! oh oh, this should not pass
		std::cout << path << " Failed on max buffer but succeeded on variable buffer reading" << '\n';
		return false;
	}
	if (!variable_buffer_size_passed) {
		std::cout << path << " Variable Buffer failed" << '\n';
		std::cout << path << " Buffer Size: " << to_string(buffer_size) << '\n';
		std::cout << variable_buffer_size_result->GetError() << '\n';
		return false;
	}
	// Results do not match
	string error_message;
	if (!ColumnDataCollection::ResultEquals(*ground_truth, *result, error_message, false)) {
		std::cout << "truth: " << ground_truth->Count() << std::endl;
		std::cout << "resul: " << result->Count() << std::endl;

		std::cout << path << " Buffer Size: " << to_string(buffer_size) << '\n';
		std::cout << error_message << '\n';
		return false;
	}
	return true;
}

bool RunFull(std::string &path, std::set<std::string> *skip = nullptr, const string &add_parameters = "",
             bool set_temp_dir = false) {
	DuckDB db(nullptr);
	Connection conn(db);
	if (!run) {
		return true;
	}
	// Here we run the csv file first with the full buffer.
	// Then a combination of multiple buffers.
	if (skip) {
		if (skip->find(path) != skip->end()) {
			// Gotta skip this
			return true;
		}
	}
	// Set max line length to 0 when starting a ST CSV Read
	conn.context->client_data->debug_set_max_line_length = true;
	conn.context->client_data->debug_max_line_length = 0;
	duckdb::unique_ptr<MaterializedQueryResult> full_buffer_res;
	ColumnDataCollection *ground_truth = nullptr;
	full_buffer_res = conn.Query("SELECT * FROM read_csv_auto('" + path + "'" + add_parameters + ") ORDER BY ALL");
	if (!full_buffer_res->HasError()) {
		ground_truth = &full_buffer_res->Collection();
	}
	if (!ground_truth) {
		return true;
	}
	// For parallel CSV Reading the buffer must be at least the size of the biggest line in the File.
	idx_t min_buffer_size = conn.context->client_data->debug_max_line_length + 3;
	// So our tests don't take infinite time, we will go till a max buffer size of 5 positions higher than the minimum.
	idx_t max_buffer_size = min_buffer_size + 5;
	// Let's go from 1 to 8 threads.
	bool all_tests_passed = true;

	for (auto buffer_size = min_buffer_size; buffer_size < max_buffer_size; buffer_size++) {
		all_tests_passed =
		    all_tests_passed && RunVariableBuffer(path, buffer_size, set_temp_dir, ground_truth, add_parameters);
	}
	return all_tests_passed;
}

// Collects All CSV-Like files from folder and execute Parallel Scans on it
void RunTestOnFolder(const string &path, std::set<std::string> *skip = nullptr, const string &add_parameters = "") {
	DuckDB db(nullptr);
	Connection con(db);
	bool all_tests_passed = true;
	auto &fs = duckdb::FileSystem::GetFileSystem(*con.context);
	for (auto &ext : csv_extensions) {
		auto csv_files = fs.Glob(path + "*" + ext);
		for (auto &csv_file : csv_files) {
			all_tests_passed = all_tests_passed && RunFull(csv_file, skip, add_parameters);
		}
	}
	REQUIRE(all_tests_passed);
}

TEST_CASE("Test File Full", "[parallel-csv][.]") {
	string path = "data/csv/auto/test_single_column_rn.csv";
	RunFull(path);
}

//! Test case with specific parameters that allow us to run the no_quote.tsv we were skipping
TEST_CASE("Test Parallel CSV All Files - data/csv/no_quote.csv", "[parallel-csv][.]") {
	string add_parameters = ", quote=''";
	string file = "data/csv/no_quote.csv";
	REQUIRE(RunFull(file, nullptr, add_parameters));
}

TEST_CASE("Test Parallel CSV All Files - data/csv/auto", "[parallel-csv][.]") {
	std::set<std::string> skip;
	// This file requires additional parameters, we test it on the following test.
	skip.insert("data/csv/auto/titlebasicsdebug.tsv");
	// This file mixes newline separators
	skip.insert("data/csv/auto/multi_column_string_mix.csv");
	RunTestOnFolder("data/csv/auto/", &skip);
}

//! Test case with specific parameters that allow us to run the titlebasicsdebug.tsv we were skipping
TEST_CASE("Test Parallel CSV All Files - data/csv/auto/titlebasicsdebug.tsv", "[parallel-csv][.]") {
	string add_parameters = ", nullstr=\'\\N\', sample_size = -1";
	string file = "data/csv/auto/titlebasicsdebug.tsv";
	REQUIRE(RunFull(file, nullptr, add_parameters));
}

TEST_CASE("Test Parallel CSV All Files - data/csv/auto/glob", "[parallel-csv][.]") {
	RunTestOnFolder("data/csv/auto/glob/");
}

TEST_CASE("Test Parallel CSV All Files - data/csv/error/date_multiple_file", "[parallel-csv][.]") {
	RunTestOnFolder("data/csv/error/date_multiple_file/");
}

TEST_CASE("Test Parallel CSV All Files - data/csv/glob/a1", "[parallel-csv][.]") {
	RunTestOnFolder("data/csv/glob/a1/");
}

TEST_CASE("Test Parallel CSV All Files - data/csv/glob/a2", "[parallel-csv][.]") {
	RunTestOnFolder("data/csv/glob/a2/");
}

TEST_CASE("Test Parallel CSV All Files - data/csv/glob/a3", "[parallel-csv][.]") {
	RunTestOnFolder("data/csv/glob/a3/");
}

TEST_CASE("Test Parallel CSV All Files - data/csv/glob/empty", "[parallel-csv][.]") {
	RunTestOnFolder("data/csv/glob/empty/");
}

TEST_CASE("Test Parallel CSV All Files - data/csv/glob/i1", "[parallel-csv][.]") {
	RunTestOnFolder("data/csv/glob/i1/");
}

TEST_CASE("Test Parallel CSV All Files - data/csv/real", "[parallel-csv][.]") {
	std::set<std::string> skip;
	// This file requires a temp_dir for offloading
	skip.insert("data/csv/real/tmp2013-06-15.csv.gz");
	RunTestOnFolder("data/csv/real/", &skip);
}

TEST_CASE("Test Parallel CSV All Files - data/csv/test", "[parallel-csv][.]") {
	std::set<std::string> skip;
	// This file requires additional parameters, we test it on the following test.
	skip.insert("data/csv/test/5438.csv");
	// This file requires additional parameters, we test it on the following test.
	skip.insert("data/csv/test/windows_newline_empty.csv");
	// This file mixes newline separators
	skip.insert("data/csv/test/mixed_line_endings.csv");
	RunTestOnFolder("data/csv/test/", &skip);
}

//! Test case with specific parameters that allow us to run the titlebasicsdebug.tsv we were skipping
TEST_CASE("Test Parallel CSV All Files - data/csv/test/5438.csv", "[parallel-csv][.]") {
	string add_parameters = ", delim=\'\', columns={\'j\': \'JSON\'}";
	string file = "data/csv/test/5438.csv";
	REQUIRE(RunFull(file, nullptr, add_parameters));
}

//! Test case with specific parameters that allow us to run the titlebasicsdebug.tsv we were skipping
TEST_CASE("Test Parallel CSV All Files - data/csv/test/windows_newline_empty.csv", "[parallel-csv][.]") {
	string add_parameters = "HEADER 0";
	string file = "data/csv/test/windows_newline_empty.csv";
	REQUIRE(RunFull(file, nullptr, add_parameters));
}

TEST_CASE("Test Parallel CSV All Files - data/csv/zstd", "[parallel-csv][.]") {
	RunTestOnFolder("data/csv/zstd/");
}

TEST_CASE("Test Parallel CSV All Files - data/csv", "[parallel-csv][.]") {
	std::set<std::string> skip;
	// This file is too big, executing on it is slow and unreliable
	skip.insert("data/csv/sequences.csv.gz");
	// This file requires specific parameters
	skip.insert("data/csv/bug_7578.csv");
	// This file requires a temp_dir for offloading
	skip.insert("data/csv/hebere.csv.gz");
	skip.insert("data/csv/no_quote.csv");
	RunTestOnFolder("data/csv/", &skip);
}

//! Test case with specific parameters that allow us to run the bug_7578.csv we were skipping
TEST_CASE("Test Parallel CSV All Files - data/csv/bug_7578.csv", "[parallel-csv][.]") {
	string add_parameters = ", delim=\'\\t\', quote = \'`\', columns={ \'transaction_id\': \'VARCHAR\', "
	                        "\'team_id\': \'INT\', \'direction\': \'INT\', \'amount\':\'DOUBLE\', "
	                        "\'account_id\':\'INT\', \'transaction_date\':\'DATE\', \'recorded_date\':\'DATE\', "
	                        "\'tags.transaction_id\':\'VARCHAR\', \'tags.team_id\':\'INT\', \'tags\':\'varchar\'}";
	string file = "data/csv/bug_7578.csv";
	REQUIRE(RunFull(file, nullptr, add_parameters));
}

TEST_CASE("Test Parallel CSV All Files - data/csv/decimal_separators", "[parallel-csv][.]") {
	RunTestOnFolder("data/csv/decimal_separators/");
}

TEST_CASE("Test Parallel CSV All Files - data/csv/中文", "[parallel-csv][.]") {
	RunTestOnFolder("data/csv/中文/");
}

TEST_CASE("Test Parallel CSV All Files - data/csv/abac", "[parallel-csv][.]") {
	RunTestOnFolder("data/csv/abac/");
}

TEST_CASE("Test Parallel CSV All Files - test/sqlserver/data", "[parallel-csv][.]") {
	std::set<std::string> skip;
	// This file is too big, executing on it is slow and unreliable
	skip.insert("test/sqlserver/data/Person.csv.gz");
	RunTestOnFolder("test/sqlserver/data/", &skip);
}

//! Test case with specific parameters that allow us to run the Person.tsv we were skipping
TEST_CASE("Test Parallel CSV All Files - test/sqlserver/data/Person.csv.gz", "[parallel-csv][.]") {
	string add_parameters = ", delim=\'|\', quote=\'*\'";
	string file = "test/sqlserver/data/Person.csv.gz";
	REQUIRE(RunFull(file, nullptr, add_parameters));
}

//! Test case with specific files that require a temp_dir for offloading
TEST_CASE("Test Parallel CSV All Files - Temp Dir for Offloading", "[parallel-csv][.]") {
	string file = "data/csv/real/tmp2013-06-15.csv.gz";
	REQUIRE(RunFull(file, nullptr, "", true));

	file = "data/csv/hebere.csv.gz";
	REQUIRE(RunFull(file, nullptr, "", true));
}
