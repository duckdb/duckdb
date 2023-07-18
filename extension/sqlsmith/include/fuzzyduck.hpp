//===----------------------------------------------------------------------===//
//                         DuckDB
//
// fuzzyduck.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {
struct FileHandle;

class FuzzyDuck {
public:
	FuzzyDuck(ClientContext &context);
	~FuzzyDuck();

	ClientContext &context;
	uint32_t seed = 0;
	idx_t max_queries = 0;
	string complete_log;
	string log;
	bool verbose_output = false;
	idx_t timeout = 30;

public:
	void Fuzz();
	void FuzzAllFunctions();

private:
	void BeginFuzzing();
	void EndFuzzing();

	string GenerateQuery();
	void RunQuery(string query);

	void LogMessage(const string &message);
	void LogTask(const string &message);
	void LogQuery(const string &message);

	void LogToCurrent(const string &message);
	void LogToComplete(const string &message);

	void TryRemoveFile(const string &path);

private:
	unique_ptr<FileHandle> complete_log_handle;
};

} // namespace duckdb
