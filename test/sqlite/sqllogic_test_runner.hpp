//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqllogic_test_runner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
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
	unique_ptr<DuckDB> db;
	unique_ptr<Connection> con;
	unique_ptr<DBConfig> config;
	unordered_set<string> extensions;
	unordered_map<string, unique_ptr<Connection>> named_connection_map;
	bool output_hash_mode = false;
	bool output_result_mode = false;
	bool debug_mode = false;
	bool finished_processing_file = false;
	int hash_threshold = 0;
	vector<LoopCommand *> active_loops;
	unique_ptr<Command> top_level_loop;
	vector<LoopDefinition *> running_loops;
	bool original_sqlite_test = false;

	//! The map converting the labels to the hash values
	unordered_map<string, string> hash_label_map;
	unordered_map<string, unique_ptr<QueryResult>> result_label_map;

public:
	void ExecuteFile(string script);
	void LoadDatabase(string dbpath);

	string ReplaceKeywords(string input);

	bool InLoop() {
		return !active_loops.empty();
	}
	void ExecuteCommand(unique_ptr<Command> command);
	void StartLoop(LoopDefinition loop);
	void EndLoop();
	static string ReplaceLoopIterator(string text, string loop_iterator_name, string replacement);
	static string LoopReplacement(string text, const vector<LoopDefinition *> &loops);
	static bool ForEachTokenReplace(const string &parameter, vector<string> &result);
};

} // namespace duckdb
