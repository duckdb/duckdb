#include "test_config.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/enum_util.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/uuid.hpp"
#include <fstream>
#include <sstream>

namespace duckdb {

typedef void (*on_set_option_t)(const Value &input);

struct TestConfigOption {
	const char *name;
	const char *description;
	LogicalType type;
	on_set_option_t on_set_option;
};

static const TestConfigOption test_config_options[] = {
    {"description", "Config description", LogicalType::VARCHAR, nullptr},
    {"comment", "Extra free form comment line", LogicalType::VARCHAR, nullptr},
    {"initial_db", "Initial database path", LogicalType::VARCHAR, nullptr},
    {"max_threads", "Max threads to use during tests", LogicalType::BIGINT, nullptr},
    {"checkpoint_wal_size", "Size in bytes after which to trigger automatic checkpointing", LogicalType::BIGINT,
     nullptr},
    {"checkpoint_on_shutdown", "Whether or not to checkpoint on database shutdown", LogicalType::BOOLEAN, nullptr},
    {"force_restart", "Force restart the database between runs", LogicalType::BOOLEAN, nullptr},
    {"summarize_failures", "Print a summary of all test failures after running", LogicalType::BOOLEAN, nullptr},
    {"test_memory_leaks", "Run memory leak tests", LogicalType::BOOLEAN, nullptr},
    {"verify_vector", "Run vector verification for a specific vector type", LogicalType::VARCHAR, nullptr},
    {"debug_initialize", "Initialize buffers with all 0 or all 1", LogicalType::VARCHAR, nullptr},
    {"init_script", "Script to execute on init", LogicalType::VARCHAR, TestConfiguration::ParseConnectScript},
    {"on_init", "SQL statements to execute on init", LogicalType::VARCHAR, nullptr},
    {"on_load", "SQL statements to execute on explicit load", LogicalType::VARCHAR, nullptr},
    {"on_new_connection", "SQL statements to execute on connection", LogicalType::VARCHAR, nullptr},
    {"skip_tests", "Tests to be skipped", LogicalType::LIST(LogicalType::VARCHAR), nullptr},
    {"skip_compiled", "Skip compiled tests", LogicalType::BOOLEAN, nullptr},
    {"skip_error_messages", "Skip compiled tests", LogicalType::LIST(LogicalType::VARCHAR), nullptr},
    {"statically_loaded_extensions", "Extensions to be loaded (from the statically available one)",
     LogicalType::LIST(LogicalType::VARCHAR), nullptr},
    {"storage_version", "Database storage version to use by default", LogicalType::VARCHAR, nullptr},
    {nullptr, nullptr, LogicalType::INVALID, nullptr},
};

TestConfiguration &TestConfiguration::Get() {
	static TestConfiguration instance;
	return instance;
}

void TestConfiguration::Initialize() {
	// load config file
	auto config_file = std::getenv("DUCKDB_TEST_CONFIG");
	if (config_file) {
		LoadConfig(config_file);
	}

	// load configuration options from the environment
	for (idx_t index = 0; test_config_options[index].name != nullptr; index++) {
		auto &config = test_config_options[index];
		string env_name = "DUCKDB_TEST_" + StringUtil::Upper(config.name);
		auto env_arg = std::getenv(env_name.c_str());
		if (!env_arg) {
			continue;
		}
		ParseOption(config.name, Value(env_arg));
	}

	// load summarize failures
	const char *summarize = std::getenv("SUMMARIZE_FAILURES");
	if (summarize) {
		if (std::string(summarize) == "1") {
			ParseOption("summarize_failures", Value(true));
		}
	} else {
		// SUMMARIZE_FAILURES not passed in explicitly - enable by default on CI
		const char *ci = std::getenv("CI");
		if (ci) {
			ParseOption("summarize_failures", Value(true));
		}
	}
}

bool TestConfiguration::ParseArgument(const string &arg, idx_t argc, char **argv, idx_t &i) {
	if (arg == "--test-config") {
		if (i >= argc) {
			throw std::runtime_error("--test-config expected a path to a configuration file");
		}
		auto config_path = string(argv[++i]);
		LoadConfig(config_path);
		return true;
	}
	if (arg == "--force-storage") {
		ParseOption("initial_db", Value("{TEST_DIR}/{BASE_TEST_NAME}/memory.db"));
		return true;
	}
	if (arg == "--force-reload") {
		ParseOption("force_restart", Value(true));
		return true;
	}
	if (arg == "--single-threaded") {
		ParseOption("max_threads", Value::BIGINT(1));
		return true;
	}
	if (arg == "--zero-initialize") {
		ParseOption("debug_initialize", Value("DEBUG_ZERO_INITIALIZE"));
		return true;
	}
	if (arg == "--one-initialize") {
		ParseOption("debug_initialize", Value("DEBUG_ONE_INITIALIZE"));
		return true;
	}
	if (StringUtil::StartsWith(arg, "--memory-leak") || StringUtil::StartsWith(arg, "--test-memory-leak")) {
		ParseOption("test_memory_leaks", Value(true));
		return true;
	}

	for (idx_t index = 0; test_config_options[index].name != nullptr; index++) {
		auto &config_option = test_config_options[index];
		string option_name = "--" + StringUtil::Replace(config_option.name, "_", "-");
		if (config_option.type == LogicalTypeId::BOOLEAN) {
			// for booleans we allow "--[option]" and "--no-[option]"
			string no_option_name = "--no-" + StringUtil::Replace(config_option.name, "_", "-");
			if (arg == option_name) {
				ParseOption(config_option.name, Value(true));
				return true;
			} else if (arg == no_option_name) {
				ParseOption(config_option.name, Value(false));
				return true;
			}
		} else if (arg == option_name) {
			if (i >= argc) {
				throw std::runtime_error(option_name + " expected an argument");
			}
			auto option_value = string(argv[++i]);
			ParseOption(config_option.name, option_value);
			return true;
		}
	}
	return false;
}

bool TestConfiguration::TryParseOption(const string &name, const Value &value) {
	// find the option
	optional_idx config_index;
	for (idx_t index = 0; test_config_options[index].name != nullptr; index++) {
		if (StringUtil::CIEquals(test_config_options[index].name, name)) {
			config_index = index;
			break;
		}
	}
	if (!config_index.IsValid()) {
		return false;
	}
	auto &test_config = test_config_options[config_index.GetIndex()];
	auto parameter = value.DefaultCastAs(test_config.type);
	if (test_config.on_set_option) {
		test_config.on_set_option(parameter);
	}
	options.insert(make_pair(test_config.name, parameter));
	return true;
}

void TestConfiguration::ParseOption(const string &name, const Value &value) {
	if (!TryParseOption(name, value)) {
		throw std::runtime_error("Failed to find option " + name + " - it does not exist");
	}
}

bool TestConfiguration::ShouldSkipTest(const string &test) {
	return tests_to_be_skipped.count(test);
}

string TestConfiguration::OnInitCommand() {
	return GetOptionOrDefault("on_init", string());
}

string TestConfiguration::OnLoadCommand() {
	auto res = GetOptionOrDefault("on_load", string(""));
	if (res != "" && res != "skip") {
		throw std::runtime_error("Unsupported parameter to on_load");
	}
	return res;
}

string TestConfiguration::OnConnectionCommand() {
	return GetOptionOrDefault("on_new_connection", string());
}

vector<string> TestConfiguration::ExtensionToBeLoadedOnLoad() {
	vector<string> res;
	auto entry = options.find("statically_loaded_extensions");
	if (entry != options.end()) {
		vector<Value> ext_list = ListValue::GetChildren(entry->second);

		for (auto ext : ext_list) {
			res.push_back(ext.GetValue<string>());
		}
	} else {
		res.push_back("core_functions");
	}
	return res;
}

vector<string> TestConfiguration::ErrorMessagesToBeSkipped() {
	vector<string> res;
	auto entry = options.find("skip_error_messages");
	if (entry != options.end()) {
		vector<Value> ext_list = ListValue::GetChildren(entry->second);

		for (auto ext : ext_list) {
			res.push_back(ext.GetValue<string>());
		}
	} else {
		res.push_back("HTTP");
		res.push_back("Unable to connect");
	}
	return res;
}

void TestConfiguration::ParseConnectScript(const Value &input) {
	auto init_cmd = ReadFileToString(input.ToString());

	auto &test_config = TestConfiguration::Get();
	test_config.ParseOption("on_init", Value(init_cmd));
}

string TestConfiguration::ReadFileToString(const string &path) {
	std::ifstream infile(path);
	if (infile.bad() || infile.fail()) {
		throw std::runtime_error("Failed to open configuration file " + path);
	}
	std::stringstream buffer;
	buffer << infile.rdbuf();
	return buffer.str();
}

void TestConfiguration::LoadConfig(const string &config_path) {
	// read the config file
	auto buffer = ReadFileToString(config_path);
	// parse json
	auto json = StringUtil::ParseJSONMap(buffer);
	auto json_values = json->Flatten();
	for (auto &entry : json_values) {
		ParseOption(entry.first, Value(entry.second));
	}

	// Convert to unordered_set<string> the list of tests to be skipped
	auto entry = options.find("skip_tests");
	if (entry != options.end()) {
		vector<Value> skip_list = ListValue::GetChildren(entry->second);

		for (auto x : skip_list) {
			tests_to_be_skipped.insert(x.GetValue<string>());
		}
	}
}

void TestConfiguration::ProcessPath(string &path, const string &test_name) {
	path = StringUtil::Replace(path, "{TEST_DIR}", TestDirectoryPath());
	path = StringUtil::Replace(path, "{UUID}", UUID::ToString(UUID::GenerateRandomUUID()));
	path = StringUtil::Replace(path, "{TEST_NAME}", test_name);

	auto base_test_name = StringUtil::Replace(test_name, "/", "_");
	path = StringUtil::Replace(path, "{BASE_TEST_NAME}", base_test_name);
}

template <class T, class VAL_T>
T TestConfiguration::GetOptionOrDefault(const string &name, T default_val) {
	auto entry = options.find(name);
	if (entry == options.end()) {
		return default_val;
	}
	return entry->second.GetValue<VAL_T>();
}

string TestConfiguration::GetDescription() {
	return GetOptionOrDefault("description", string());
}

string TestConfiguration::GetInitialDBPath() {
	return GetOptionOrDefault("initial_db", string());
}

optional_idx TestConfiguration::GetMaxThreads() {
	return GetOptionOrDefault<optional_idx, idx_t>("max_threads", optional_idx());
}

idx_t TestConfiguration::GetCheckpointWALSize() {
	return GetOptionOrDefault<idx_t>("checkpoint_wal_size", 0);
}

bool TestConfiguration::GetForceRestart() {
	return GetOptionOrDefault("force_restart", false);
}

bool TestConfiguration::GetCheckpointOnShutdown() {
	return GetOptionOrDefault("checkpoint_on_shutdown", true);
}

bool TestConfiguration::GetTestMemoryLeaks() {
	return GetOptionOrDefault("test_memory_leaks", false);
}

bool TestConfiguration::GetSummarizeFailures() {
	return GetOptionOrDefault("summarize_failures", false);
}

bool TestConfiguration::GetSkipCompiledTests() {
	return GetOptionOrDefault("skip_compiled", false);
}

string TestConfiguration::GetStorageVersion() {
	return GetOptionOrDefault("storage_version", string());
}

DebugVectorVerification TestConfiguration::GetVectorVerification() {
	return EnumUtil::FromString<DebugVectorVerification>(GetOptionOrDefault<string>("verify_vector", "NONE"));
}

DebugInitialize TestConfiguration::GetDebugInitialize() {
	return EnumUtil::FromString<DebugInitialize>(GetOptionOrDefault<string>("debug_initialize", "NO_INITIALIZE"));
}

bool TestConfiguration::TestForceStorage() {
	auto &test_config = TestConfiguration::Get();
	return !test_config.GetInitialDBPath().empty();
}

bool TestConfiguration::TestForceReload() {
	auto &test_config = TestConfiguration::Get();
	return test_config.GetForceRestart();
}

bool TestConfiguration::TestMemoryLeaks() {
	auto &test_config = TestConfiguration::Get();
	return test_config.GetTestMemoryLeaks();
}

FailureSummary::FailureSummary() : failures_summary_counter(0) {
}

FailureSummary &FailureSummary::Instance() {
	static FailureSummary instance;
	return instance;
}

string FailureSummary::GetFailureSummary() {
	auto &test_config = TestConfiguration::Get();
	if (!test_config.GetSummarizeFailures()) {
		return string();
	}
	auto &summary = FailureSummary::Instance();
	lock_guard<mutex> guard(summary.failures_lock);
	std::ostringstream oss;
	for (auto &line : summary.failures_summary) {
		oss << line;
	}
	return oss.str();
}

void FailureSummary::Log(string log_message) {
	auto &summary = FailureSummary::Instance();
	lock_guard<mutex> lock(summary.failures_lock);
	summary.failures_summary.push_back(std::move(log_message));
}

idx_t FailureSummary::GetSummaryCounter() {
	auto &summary = FailureSummary::Instance();
	return ++summary.failures_summary_counter;
}

} // namespace duckdb
