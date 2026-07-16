//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqllogic_test_runner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/mutex.hpp"
#include "sqllogic_command.hpp"
#include "test_config.hpp"
#include <istream>

namespace duckdb {

class Command;
class LoopCommand;
class SQLLogicParser;

enum class RequireResult { PRESENT, MISSING };

struct CachedLabelData {
public:
	CachedLabelData(const string &hash, string result_str_p) : hash(hash), result_str(std::move(result_str_p)) {
	}

public:
	string hash;
	string result_str;
};

struct HashLabelMap {
public:
	void WithLock(std::function<void(unordered_map<string, CachedLabelData> &map)> cb) {
		std::lock_guard<std::mutex> guard(lock);
		cb(map);
	}

public:
	std::mutex lock;
	unordered_map<string, CachedLabelData> map;
};

struct NewDatabaseConnection {
	unique_ptr<DuckDB> db;
	unique_ptr<Connection> con;
};

class SQLLogicTestRunner {
public:
	explicit SQLLogicTestRunner(string dbpath);
	~SQLLogicTestRunner();

	string file_name;
	string dbpath;
	vector<string> loaded_databases;
	duckdb::unique_ptr<DuckDB> db;
	duckdb::unique_ptr<Connection> con;
	duckdb::unique_ptr<DBConfig> config;
	unordered_set<string> extensions;
	unordered_map<string, duckdb::unique_ptr<DuckDB>> named_db;
	unordered_map<string, duckdb::unique_ptr<Connection>> named_connection_map;
	bool output_hash_mode = false;
	bool output_result_mode = false;
	bool debug_mode = false;
	atomic<bool> finished_processing_file;
	int32_t hash_threshold = 0;
	vector<LoopCommand *> active_loops;
	duckdb::unique_ptr<Command> top_level_loop;
	bool original_sqlite_test = false;
	bool output_sql = false;
	bool skip_reload = false;
	unordered_map<string, string> environment_variables;
	string local_extension_repo;
	TestConfiguration::ExtensionAutoLoadingMode autoloading_mode;
	bool autoinstall_is_checked;

	// If these error msgs occur in a test, the test will abort but still count as passed
	unordered_set<string> ignore_error_messages;
	// If these error msgs occur a statement that is expected to fail, the test will fail
	unordered_set<string> always_fail_error_messages = {"INTERNAL"};

	//! The map converting the labels to the hash values
	HashLabelMap hash_label_map;
	mutex log_lock;

	//! Per-test statement tallies for --emit-test-events. Atomic: concurrent loops run the countable
	//! commands on multiple threads; the begin/end events are emitted single-threaded at boundaries.
	atomic<idx_t> test_stat_passes {0};
	atomic<idx_t> test_stat_fails {0};
	atomic<idx_t> test_stat_skip_mode {0};
	//! Whole-test verdict, set at the terminal (test_sqllogictest.cpp) before this runner is destroyed.
	//! The destructor's DB cleanup consults it for --database-destroy on-success. Defaults to false so
	//! an abort/unwind path that never reaches the terminal retains DB files (treated as a failure).
	bool test_succeeded = false;
	//! Whole-test skip disposition (require/require-env/config/tag), recorded by SkipTest and read at
	//! the terminal to emit end{status:"skip-requirement"}.
	bool test_skipped_requirement = false;
	string test_skip_reason;
	//! Locator for the failing command (file:line), stashed at the throw site for --emit-test-events.
	//! A Catch FAIL carries no message; consumers get this anchor to correlate with captured output.
	//! Written single-threaded: serial fails throw directly; concurrent fails are re-raised post-join.
	string test_failure_locator;

public:
	void ExecuteFile(string script);
	void ExecuteStream(std::istream &input, const string &source_name);
	virtual void LoadDatabase(string dbpath, bool load_extensions);

	string ReplaceKeywords(string input);

	bool InLoop() {
		return !active_loops.empty();
	}
	void ExecuteCommand(unique_ptr<Command> command);
	void Reconnect();
	void StartLoop(LoopDefinition loop);
	void EndLoop();
	string ReplaceLoopIterator(string text, string loop_iterator_name, string replacement);
	string LoopReplacement(string text, const vector<LoopDefinition> &loops);
	static ExtensionLoadResult LoadExtension(DuckDB &db, const std::string &extension);
	void SkipTest(const string &reason);
	static string GetSkipReasonSummary();
	//! --emit-test-events: statement tallies (counted, not emitted) + the begin/end JSON events.
	void CountStatement(bool passed);
	void CountSkipMode();
	void EmitBegin(const string &test_name);
	void EmitEnd(const string &test_name, const string &status, const string &data);
	NewDatabaseConnection CreateDatabase(const string &db_path, bool load_database);
	unique_ptr<Connection> ConnectToDatabase(DuckDB &db_ref);
	bool IsVariableReplacement(const string &token_name);
	Value GetVariableReplacement(const string &token_name, string &variable_name);

private:
	void ExecuteInternal(SQLLogicParser &parser, const string &script);
	RequireResult CheckRequire(SQLLogicParser &parser, const vector<string> &params);
	void ConfigureDefaultInMemoryTemporaryDirectory(const string &script);
	static void AddSkipReason(const string &reason);

private:
	static mutex skip_reason_lock;
	static map<string, idx_t> skip_reason_counts;
};

} // namespace duckdb
