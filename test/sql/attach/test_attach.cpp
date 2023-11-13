#include "catch.hpp"
#include "test_helpers.hpp"

#include <thread>
#include <iostream>
#include <fstream>

using namespace duckdb;

struct Config {
	Config(idx_t file_count, idx_t thread_count, idx_t elapsed_ms)
	    : file_count(file_count), thread_count(thread_count), elapsed_ms(elapsed_ms) {};

	idx_t file_count;
	idx_t thread_count;
	idx_t elapsed_ms;

	void Print() {
		std::cout << "file count: " << file_count << "\n";
		std::cout << "thread count: " << thread_count << "\n";
		std::cout << "elapsed ms: " << elapsed_ms << "\n\n";
		std::cout << "elapsed ms per file: " << double(elapsed_ms) / double(file_count) << "\n\n";
	}
};

void FileWorker(const string &dir, const string &template_path, const idx_t start, const idx_t end) {

	for (idx_t i = start; i < end; i++) {

		auto duplicate_path = dir + "/board_" + to_string(i) + ".db";
		std::ifstream template_file(template_path, std::ios::binary);
		std::ofstream duplicate(duplicate_path, std::ios::binary);
		duplicate << template_file.rdbuf();
	}
}

void CreateFiles(const idx_t file_count, string db_file_dir) {

	db_file_dir += "/" + to_string(file_count) + "_files";
	DuckDB db(nullptr);
	Connection con(db);

	// create the template file
	auto template_path = db_file_dir + "/template.db";
	auto attach_result = con.Query("ATTACH " + template_path + "';");
	REQUIRE_NO_FAIL(*attach_result);
	auto create_table_result = con.Query("CREATE TABLE tbl AS "
	                                     "SELECT range::INTEGER AS id, range::BIGINT AS status, range::DOUBLE AS "
	                                     "amount, repeat(range::VARCHAR, 20) AS text"
	                                     "FROM range(100);");
	REQUIRE_NO_FAIL(*create_table_result);
	auto detach_result = con.Query("DETACH template;");
	REQUIRE_NO_FAIL(*detach_result);

	// loop setup
	const idx_t thread_count = 32;
	vector<std::thread> threads;
	idx_t files_per_thread = double(file_count) / double(thread_count);
	idx_t remaining_files = file_count % thread_count;
	idx_t end = 0;

	// spawn and run attach workers
	for (idx_t i = 0; i < thread_count; i++) {

		idx_t thread_file_count = files_per_thread;
		if (i < remaining_files) {
			thread_file_count++;
		}
		idx_t start = end;
		end += thread_file_count;

		threads.push_back(std::thread(FileWorker, db_file_dir, template_path, start, end));
	}

	for (idx_t i = 0; i < thread_count; i++) {
		threads[i].join();
	}
}

void AttachWorker(const string &dir, const idx_t start, const idx_t end, DuckDB &db) {

	Connection con(db);

	for (idx_t i = start; i < end; i++) {
		auto filepath = dir + "/board_" + to_string(i) + ".db";
		REQUIRE_NO_FAIL(con.Query("ATTACH '" + filepath + "' (READ_ONLY);"));
	}
}

void Attach(const idx_t file_count, const idx_t thread_count, string db_file_dir) {

	db_file_dir += "/" + to_string(file_count) + "_files";

	auto timer_start = high_resolution_clock::now();
	DuckDB db(nullptr);

	// loop setup
	vector<std::thread> threads;
	idx_t files_per_thread = double(file_count) / double(thread_count);
	idx_t remaining_files = file_count % thread_count;
	idx_t end = 0;

	// spawn and run attach workers
	for (idx_t i = 0; i < thread_count; i++) {

		idx_t thread_file_count = files_per_thread;
		if (i < remaining_files) {
			thread_file_count++;
		}
		idx_t start = end;
		end += thread_file_count;

		threads.push_back(std::thread(AttachWorker, db_file_dir, start, end, std::ref(db)));
	}

	for (idx_t i = 0; i < thread_count; i++) {
		threads[i].join();
	}

	// verify the result
	Connection con(db);
	auto result = con.Query("SELECT count(*) > $1 AS count FROM duckdb_databases()", file_count);
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	// print the elapsed time
	auto timer_stop = high_resolution_clock::now();
	auto duration = duration_cast<std::chrono::milliseconds>(timer_stop - timer_start);

	Config config(file_count, thread_count, duration.count());
	config.Print();
}

TEST_CASE("Attach perf", "[attach][.]") {

	// NOTE: this test requires to increase the max_file_limit of the OS
	return;

	// set up the directories
	const string DB_DIR = "/Users/tania/DuckDB/mondaycom";
	const string DB_FILE_DIR = DB_DIR + "/db_files";

	//	// set up the directories
	//	const string DB_DIR = TestDirectoryPath() + "/attach";
	//	const string DB_FILE_DIR = DB_DIR + "/db_files";
	//
	//	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	//	if (!fs->DirectoryExists(DB_DIR)) {
	//		fs->CreateDirectory(DB_DIR);
	//	}
	//	if (!fs->DirectoryExists(DB_FILE_DIR)) {
	//		fs->CreateDirectory(DB_FILE_DIR);
	//	}

	// test different scenarios and collect results
	//		vector<idx_t> file_counts = {1000, 10000, 30000, 50000, 60000};
	//		vector<idx_t> thread_counts = {32, 64, 128, 256, 512};
	vector<idx_t> thread_counts = {32};
	vector<idx_t> file_counts = {30000};
	for (const auto file_count : file_counts) {

		//		// create the files
		//		CreateFiles(file_count, DB_FILE_DIR);

		// run ATTACH
		for (const auto thread_count : thread_counts) {
			Attach(file_count, thread_count, DB_FILE_DIR);
		}
	}
}
