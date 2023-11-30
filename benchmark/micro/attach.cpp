#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"

#include <thread>
#include <iostream>
#include <fstream>

using namespace duckdb;

DUCKDB_BENCHMARK(ParallelAttach, "[attach]")

// NOTE: the FILE_COUNT number is intentionally low. However, this test is intended to run with
// higher numbers after increasing the OS open file limit

static void FileWorker(const string &dir, const string &template_path, const idx_t start, const idx_t end) {

	for (idx_t i = start; i < end; i++) {

		auto duplicate_path = dir + "/board_" + to_string(i) + ".db";
		std::ifstream template_file(template_path, std::ios::binary);
		std::ofstream duplicate(duplicate_path, std::ios::binary);
		duplicate << template_file.rdbuf();
	}
}

void CreateFiles(const idx_t file_count, string db_file_dir) {

	db_file_dir += "/" + to_string(file_count) + "_files";
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	if (!fs->DirectoryExists(db_file_dir)) {
		fs->CreateDirectory(db_file_dir);
	}

	DuckDB db(nullptr);
	Connection con(db);

	// create the template file
	auto template_path = db_file_dir + "/template_file.db";
	auto attach_result = con.Query("ATTACH '" + template_path + "';");
	D_ASSERT(!attach_result->HasError());

	auto create_table_result = con.Query("CREATE TABLE tbl AS "
	                                     "SELECT range::INTEGER AS id, range::BIGINT AS status, range::DOUBLE AS "
	                                     "amount, repeat(range::VARCHAR, 20) AS text "
	                                     "FROM range(100);");
	D_ASSERT(!create_table_result->HasError());

	auto detach_result = con.Query("DETACH template_file;");
	D_ASSERT(!detach_result->HasError());

	// loop setup
	const idx_t thread_count = 32;
	vector<std::thread> threads;
	idx_t files_per_thread = double(file_count) / double(thread_count);
	idx_t remaining_files = file_count % thread_count;
	idx_t end = 0;

	// spawn and run file creation workers
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

static void AttachWorker(const string &dir, const idx_t start, const idx_t end, DuckDB &db) {

	Connection con(db);

	for (idx_t i = start; i < end; i++) {
		auto filepath = dir + "/board_" + to_string(i) + ".db";
		auto result = con.Query("ATTACH '" + filepath + "' (READ_ONLY, TYPE DUCKDB);");
		D_ASSERT(!result->HasError());
	}
}

void Attach(const idx_t file_count, const idx_t thread_count, string db_file_dir) {

	db_file_dir += "/" + to_string(file_count) + "_files";
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
	D_ASSERT(!result->HasError());

	auto result_str = result->ToString();
	D_ASSERT(result_str.find("true") != string::npos);
}

void Load(DuckDBBenchmarkState *state) override {

	const string DB_DIR = TestDirectoryPath() + "/attach";
	const string DB_FILE_DIR = DB_DIR + "/db_files";

	// set up the directories
	unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	if (!fs->DirectoryExists(DB_DIR)) {
		fs->CreateDirectory(DB_DIR);
	}
	if (!fs->DirectoryExists(DB_FILE_DIR)) {
		fs->CreateDirectory(DB_FILE_DIR);
	}

	// create the files
	const idx_t FILE_COUNT = 100;
	CreateFiles(FILE_COUNT, DB_FILE_DIR);
}

void RunBenchmark(DuckDBBenchmarkState *state) override {

	const string DB_DIR = TestDirectoryPath() + "/attach";
	const string DB_FILE_DIR = DB_DIR + "/db_files";

	const idx_t FILE_COUNT = 100;
	const idx_t THREAD_COUNT = 64;
	Attach(FILE_COUNT, THREAD_COUNT, DB_FILE_DIR);
}

string VerifyResult(QueryResult *result) override {

	const string DB_DIR = TestDirectoryPath() + "/attach";
	const string DB_FILE_DIR = DB_DIR + "/db_files";

	// we use this function to clean up the directories
	unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	fs->RemoveDirectory(DB_FILE_DIR);

	return string();
}

string BenchmarkInfo() override {
	return "Run parallel attach statements";
}
FINISH_BENCHMARK(ParallelAttach)
