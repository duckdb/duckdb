//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// test_config.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/debug_initialize.hpp"
#include "duckdb/common/enums/debug_vector_verification.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/re2_regex.hpp"
#include "duckdb/common/types/value.hpp"

#include <sys/types.h>
#include <unordered_map>

namespace duckdb {

using Regex = duckdb_re2::Regex;

enum class SortStyle : uint8_t { NO_SORT, ROW_SORT, VALUE_SORT };

struct ConfigSetting {
	string name;
	Value value;
};

class TestConfiguration {
public:
	static const string DATA_DIR_DEFAULT;
	static const string TEMP_DIR_BASE_DEFAULT;

	enum class ExtensionAutoLoadingMode { NONE = 0, AVAILABLE = 1, ALL = 2 };

	enum class SelectPolicy : uint8_t {
		NONE,   // does not match any explicit policy (default: policy=SELECT)
		SELECT, // matches explicit select
		SKIP    // matches explicit skip
	};

	static TestConfiguration &Get();

	void Initialize(); // call before configs, arg parsing, etc.
	void Finalize();   // call after configs, arg parsing, to finish variable setups, etc.
	bool ParseArgument(const string &arg, idx_t argc, char **argv, idx_t &i);
	bool TryParseOption(const string &name, const Value &value);
	void ParseOption(const string &name, const Value &value);
	void LoadConfig(const string &config_path);

	string GetWorkingDirectory();
	bool ChangeWorkingDirectory(const string &dir); // true -> actually changed

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
	string OnInitCommand();
	string OnLoadCommand();
	string OnConnectionCommand();
	string OnCleanupCommand();
	SortStyle GetDefaultSortStyle();
	vector<string> ExtensionToBeLoadedOnLoad();
	vector<string> ErrorMessagesToBeSkipped();
	string GetStorageVersion();
	string GetVariable(const string &key, const string &default_value);
	const unordered_map<string, string> &GetVariables();
	vector<unordered_set<string>> GetSelectTagSets();
	vector<unordered_set<string>> GetSkipTagSets();
	SelectPolicy GetPolicyForTagSet(const vector<string> &tag_set);
	SelectPolicy GetPolicyForSourceREs(const vector<string> &source_lines);
	vector<ConfigSetting> GetConfigSettings();
	string GetDataDirectoryFromWorking(); // -> "$PWD/DATA_DIR_DEFAULT"
	string GetDataDirectory();
	string AssureTempDirectoryFromBase(const string &base); // -> "$base/$PID"
	string AssureTempDirectory();

	static bool TestForceStorage();
	static bool TestForceReload();
	static bool TestMemoryLeaks();
	static bool TestRunStorageFuzzer();

	static void ParseConnectScript(const Value &input);
	static void CheckSortStyle(const Value &input);
	static bool TryParseSortStyle(const string &sort_style, SortStyle &result);
	static void AppendSelectTagSet(const Value &tag_set);
	static void AppendSkipTagSet(const Value &tag_set);
	static void AppendSelectRE(const Value &re);
	static void AppendSkipRE(const Value &re);

private:
	case_insensitive_map_t<Value> options;
	unordered_set<string> tests_to_be_skipped;

	// explicitly take ownership of working_dir here, giving runners an API to chdir,
	// and get env updates to match
	string working_dir;
	string test_uuid;
	unordered_map<string, string> variables;

	vector<unordered_set<string>> select_tag_sets;
	vector<unordered_set<string>> skip_tag_sets;

	vector<Regex> select_res;
	vector<Regex> skip_res;

	bool is_finalized;

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
