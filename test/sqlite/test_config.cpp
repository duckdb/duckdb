#include "test_config.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/enum_util.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/uuid.hpp"
#include <fstream>
#include <sstream>

namespace duckdb {

struct TestConfigOption {
	const char *name;
	const char *description;
	LogicalTypeId type;
};

static const TestConfigOption test_config_options[] = {
    {"description", "Config description", LogicalTypeId::VARCHAR},
    {"initial_db", "Initial database path", LogicalTypeId::VARCHAR},
    {"max_threads", "Max threads to use during tests", LogicalTypeId::BIGINT},
    {"checkpoint_wal_size", "Size in bytes after which to trigger automatic checkpointing", LogicalTypeId::BIGINT},
    {"checkpoint_on_shutdown", "Whether or not to checkpoint on database shutdown", LogicalTypeId::BOOLEAN},
    {"force_restart", "Force restart the database between runs", LogicalTypeId::BOOLEAN},
    {"test_memory_leaks", "Run memory leak tests", LogicalTypeId::BOOLEAN},
    {"verify_vector", "Run vector verification for a specific vector type", LogicalTypeId::VARCHAR},
    {nullptr, nullptr, LogicalTypeId::INVALID},
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
		ParseOption(env_name, Value(env_arg));
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
	if (arg == "--force-reload" || arg == "--force-restart") {
		ParseOption("force_restart", Value(true));
		return true;
	}
	if (StringUtil::StartsWith(arg, "--memory-leak") ||
		StringUtil::StartsWith(arg, "--test-memory-leak")) {
		ParseOption("test_memory_leaks", Value(true));
		return true;
	}

	if (!StringUtil::Contains(arg, "=")) {
		return false;
	}
	auto splits = StringUtil::Split(arg, "=");
	if (splits.size() != 2) {
		return false;
	}
	return TryParseOption(splits[0], Value(splits[1]));
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
	options.insert(make_pair(test_config.name, parameter));
	return true;
}

void TestConfiguration::ParseOption(const string &name, const Value &value) {
	if (!TryParseOption(name, value)) {
		throw std::runtime_error("Failed to find option " + name + " - it does not exist");
	}
}

void TestConfiguration::LoadConfig(const string &config_path) {
	// read the config file
	std::ifstream infile(config_path);
	if (infile.bad() || infile.fail()) {
		throw std::runtime_error("Failed to open configuration file " + config_path);
	}
	std::stringstream buffer;
	buffer << infile.rdbuf();
	// parse json
	auto json = StringUtil::ParseJSONMap(buffer.str());
	auto json_values = json->Flatten();
	for (auto &entry : json_values) {
		ParseOption(entry.first, Value(entry.second));
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

optional_idx TestConfiguration::GetCheckpointWALSize() {
	return GetOptionOrDefault<optional_idx, idx_t>("checkpoint_wal_size", optional_idx());
}

bool TestConfiguration::GetForceRestart() {
	return GetOptionOrDefault("force_restart", false);
}

bool TestConfiguration::GetCheckpointOnShutdown() {
	return GetOptionOrDefault("checkpoint_on_shutdown", false);
}

bool TestConfiguration::GetTestMemoryLeaks() {
	return GetOptionOrDefault("test_memory_leaks", false);
}

DebugVectorVerification TestConfiguration::GetVectorVerification() {
	return EnumUtil::FromString<DebugVectorVerification>(GetOptionOrDefault<string>("verify_vector", "NONE"));
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

} // namespace duckdb
