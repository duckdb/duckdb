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

enum class SortStyle : uint8_t { NO_SORT, ROW_SORT, VALUE_SORT };

class TestConfiguration {
public:
	enum class ExtensionAutoLoadingMode { NONE = 0, AVAILABLE = 1, ALL = 2 };

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
	optional_idx GetBlockAllocSize();
	idx_t GetCheckpointWALSize();
	bool GetForceRestart();
	bool GetCheckpointOnShutdown();
	bool GetTestMemoryLeaks();
	bool RunStorageFuzzer();
	bool GetSummarizeFailures();
	bool GetSkipCompiledTests();
	DebugVectorVerification GetVectorVerification();
	DebugInitialize GetDebugInitialize();
	ExtensionAutoLoadingMode GetExtensionAutoLoadingMode();
	bool ShouldSkipTest(const string &test_name);
	string DataLocation();
	string OnInitCommand();
	string OnLoadCommand();
	string OnConnectionCommand();
	string OnCleanupCommand();
	SortStyle GetDefaultSortStyle();
	vector<string> ExtensionToBeLoadedOnLoad();
	vector<string> ErrorMessagesToBeSkipped();
	string GetStorageVersion();
	string GetTestEnv(const string &key, const string &default_value);

	static bool TestForceStorage();
	static bool TestForceReload();
	static bool TestMemoryLeaks();
	static bool TestRunStorageFuzzer();

	static void ParseConnectScript(const Value &input);
	static void CheckSortStyle(const Value &input);
	static bool TryParseSortStyle(const string &sort_style, SortStyle &result);

private:
	case_insensitive_map_t<Value> options;
	unordered_set<string> tests_to_be_skipped;
	unordered_map<string, string> test_env;

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
	static bool SkipLoggingSameError(const string &file_name);

private:
	static FailureSummary &Instance();
	bool SkipLoggingSameErrorInternal(const string &file_name);

private:
	mutex failures_lock;
	atomic<idx_t> failures_summary_counter;
	vector<string> failures_summary;
	set<string> reported_files;
};

} // namespace duckdb
