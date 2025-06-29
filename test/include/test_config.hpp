//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// test_config.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/enums/debug_vector_verification.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/enums/debug_initialize.hpp"

namespace duckdb {

class TestConfiguration {
public:
	static TestConfiguration &Get();

	void Initialize();
	bool ParseArgument(const string &arg, idx_t argc, char **argv, idx_t &i);
	bool TryParseOption(const string &name, const Value &value);
	void ParseOption(const string &name, const Value &value);
	void LoadConfig(const string &config_path);

	void ProcessPath(string &path, const string &test_name);

	string GetDescription();
	string GetInitialDBPath();
	optional_idx GetMaxThreads();
	idx_t GetCheckpointWALSize();
	bool GetForceRestart();
	bool GetCheckpointOnShutdown();
	bool GetTestMemoryLeaks();
	bool GetSummarizeFailures();
	bool GetSkipCompiledTests();
	DebugVectorVerification GetVectorVerification();
	DebugInitialize GetDebugInitialize();
	bool ShouldSkipTest(const string &test_name);
	string OnInitCommand();
	string OnLoadCommand();
	string OnConnectionCommand();
	vector<string> ExtensionToBeLoadedOnLoad();
	vector<string> ErrorMessagesToBeSkipped();

	static bool TestForceStorage();
	static bool TestForceReload();
	static bool TestMemoryLeaks();

	static void ParseConnectScript(const Value &input);

private:
	case_insensitive_map_t<Value> options;
	unordered_set<string> tests_to_be_skipped;

private:
	template <class T, class VAL_T = T>
	T GetOptionOrDefault(const string &name, T default_val);

	static string ReadFileToString(const string &path);
};

class FailureSummary {
public:
	FailureSummary();

	static void Log(string message);
	static string GetFailureSummary();
	static idx_t GetSummaryCounter();

private:
	static FailureSummary &Instance();

private:
	mutex failures_lock;
	atomic<idx_t> failures_summary_counter;
	vector<string> failures_summary;
};

} // namespace duckdb
