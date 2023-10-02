//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqllogic_test_runner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/mutex.hpp"
#include "sqllogic_command.hpp"

namespace duckdb {

class Command;
class LoopCommand;

class SQLLogicTestRunner {
public:
	SQLLogicTestRunner(string dbpath);
	~SQLLogicTestRunner();

	string dbpath;
	vector<string> loaded_databases;
	duckdb::unique_ptr<DuckDB> db;
	duckdb::unique_ptr<Connection> con;
	duckdb::unique_ptr<DBConfig> config;
	unordered_set<string> extensions;
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
	bool enable_verification = false;
	bool skip_reload = false;
	unordered_map<string, string> environment_variables;

	// If these error msgs occur in a test, the test will abort but still count as passed
	unordered_set<string> ignore_error_messages = {"HTTP", "Unable to connect"};
	// If these error msgs occur a statement that is expected to fail, the test will fail
	unordered_set<string> always_fail_error_messages = {"differs from original result!", "INTERNAL"};

	//! The map converting the labels to the hash values
	unordered_map<string, string> hash_label_map;
	unordered_map<string, duckdb::unique_ptr<QueryResult>> result_label_map;
	mutex log_lock;

public:
	void ExecuteFile(string script);
	virtual void LoadDatabase(string dbpath);

	string ReplaceKeywords(string input);

	bool InLoop() {
		return !active_loops.empty();
	}
	void ExecuteCommand(unique_ptr<Command> command);
	void Reconnect();
	void StartLoop(LoopDefinition loop);
	void EndLoop();
	static string ReplaceLoopIterator(string text, string loop_iterator_name, string replacement);
	static string LoopReplacement(string text, const vector<LoopDefinition> &loops);
	static bool ForEachTokenReplace(const string &parameter, vector<string> &result);
};

} // namespace duckdb
