#include "catch.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/execution/operator/persistent/csv_reader_options.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"

#include <iostream>
#include <vector>

using namespace duckdb;
using namespace std;

//! CSV Files
const string csv = "*.csv";
const string tsv = "*.tsv";

unique_ptr<QueryResult> RunSingleThread( duckdb::Connection &conn, std::string &path){
	BufferedCSVReaderOptions csv_options;
	csv_options.auto_detect = true;
	csv_options.run_parallel = false;
	auto q_res = conn.ReadCSV(path,csv_options)->Execute();
	return q_res;

}

unique_ptr<QueryResult> RunParallel(duckdb::Connection &conn, std::string &path, idx_t thread_count, idx_t buffer_size){
	BufferedCSVReaderOptions csv_options;
	csv_options.auto_detect = true;
	csv_options.run_parallel = true;
	csv_options.buffer_size = buffer_size;
	// Set number of threads
	conn.Query("PRAGMA threads="+ to_string(thread_count));
	// Read CSV
	auto q_res = conn.ReadCSV(path,csv_options)->Execute();
	return q_res;
}

bool RunFull(std::string &path, std::set<std::string> &skip, duckdb::Connection &conn){
	// Here we run the csv file first with the single thread reader.
	// Then the parallel csv reader with a combination of multiple threads and buffer sizes.
    if (skip.find(path) == skip.end()){
		// Gotta skip this
		return true;
	}
	auto ground_truth = RunSingleThread(conn,path);

	// Let's go from 1 to 8 threads.
	for (auto thread_count = 1; thread_count <= 8; thread_count++){
		idx_t buffer_size = 300; // TODO; Figure out reasonable numbers to this
		auto parallel_result = RunParallel(conn,path,thread_count,buffer_size);
		if (!parallel_result->Equals(*ground_truth)){
			// Results do not match
			std::cout << path << " Thread count: " << to_string(thread_count) << " Buffer Size: " << to_string(buffer_size) << std::endl;
			return false;
		}
	}
	return true;
}

// Collects All CSV-Like files from folder and execute Parallel Scans on it
void RunTestOnFolder(const string& path, std::set<std::string>& skip){
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto &fs = duckdb::FileSystem::GetFileSystem(*con.context);
	auto csv_files = fs.Glob(path+ "*.csv");
	for (auto &csv_file : csv_files) {
		REQUIRE(RunFull(csv_file, skip, con));
	}
}




TEST_CASE("Test Parallel CSV All Files - test/sql/copy/csv/data/auto", "[parallel-csv]") {
	std::set<std::string> skip;
	RunTestOnFolder("test/sql/copy/csv/data/auto/",skip);
}
