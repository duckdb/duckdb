#include "duckdb/main/config.hpp"

#include "duckdb/common/cgroups.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/common/serializer/serializer.hpp"

#ifndef DUCKDB_NO_THREADS
#include "duckdb/common/thread.hpp"
#endif

#include <cinttypes>
#include <cstdio>

namespace duckdb {

#ifdef DEBUG
bool DBConfigOptions::debug_print_bindings = false;
#endif

#define DUCKDB_GLOBAL(_PARAM)                                                                                          \
	{                                                                                                                  \
		_PARAM::Name, _PARAM::Description, _PARAM::InputType, _PARAM::SetGlobal, nullptr, _PARAM::ResetGlobal,         \
		    nullptr, _PARAM::GetSetting                                                                                \
	}
#define DUCKDB_GLOBAL_ALIAS(_ALIAS, _PARAM)                                                                            \
	{                                                                                                                  \
		_ALIAS, _PARAM::Description, _PARAM::InputType, _PARAM::SetGlobal, nullptr, _PARAM::ResetGlobal, nullptr,      \
		    _PARAM::GetSetting                                                                                         \
	}

#define DUCKDB_LOCAL(_PARAM)                                                                                           \
	{                                                                                                                  \
		_PARAM::Name, _PARAM::Description, _PARAM::InputType, nullptr, _PARAM::SetLocal, nullptr, _PARAM::ResetLocal,  \
		    _PARAM::GetSetting                                                                                         \
	}
#define DUCKDB_LOCAL_ALIAS(_ALIAS, _PARAM)                                                                             \
	{                                                                                                                  \
		_ALIAS, _PARAM::Description, _PARAM::InputType, nullptr, _PARAM::SetLocal, nullptr, _PARAM::ResetLocal,        \
		    _PARAM::GetSetting                                                                                         \
	}

#define DUCKDB_GLOBAL_LOCAL(_PARAM)                                                                                    \
	{                                                                                                                  \
		_PARAM::Name, _PARAM::Description, _PARAM::InputType, _PARAM::SetGlobal, _PARAM::SetLocal,                     \
		    _PARAM::ResetGlobal, _PARAM::ResetLocal, _PARAM::GetSetting                                                \
	}
#define DUCKDB_GLOBAL_LOCAL_ALIAS(_ALIAS, _PARAM)                                                                      \
	{                                                                                                                  \
		_ALIAS, _PARAM::Description, _PARAM::InputType, _PARAM::SetGlobal, _PARAM::SetLocal, _PARAM::ResetGlobal,      \
		    _PARAM::ResetLocal, _PARAM::GetSetting                                                                     \
	}
#define FINAL_SETTING                                                                                                  \
	{ nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr }

static const ConfigurationOption internal_options[] = {
    DUCKDB_GLOBAL(AccessModeSetting),
    DUCKDB_GLOBAL(AllocatorBackgroundThreadsSetting),
    DUCKDB_GLOBAL(AllocatorBulkDeallocationFlushThresholdSetting),
    DUCKDB_GLOBAL(AllocatorFlushThresholdSetting),
    DUCKDB_GLOBAL(AllowCommunityExtensionsSetting),
    DUCKDB_GLOBAL(AllowExtensionsMetadataMismatchSetting),
    DUCKDB_GLOBAL(AllowPersistentSecretsSetting),
    DUCKDB_GLOBAL(AllowUnredactedSecretsSetting),
    DUCKDB_GLOBAL(AllowUnsignedExtensionsSetting),
    DUCKDB_GLOBAL(AllowedDirectoriesSetting),
    DUCKDB_GLOBAL(AllowedPathsSetting),
    DUCKDB_GLOBAL(ArrowLargeBufferSizeSetting),
    DUCKDB_GLOBAL(ArrowLosslessConversionSetting),
    DUCKDB_GLOBAL(ArrowOutputListViewSetting),
    DUCKDB_GLOBAL(AutoinstallExtensionRepositorySetting),
    DUCKDB_GLOBAL(AutoinstallKnownExtensionsSetting),
    DUCKDB_GLOBAL(AutoloadKnownExtensionsSetting),
    DUCKDB_GLOBAL(CatalogErrorMaxSchemasSetting),
    DUCKDB_GLOBAL(CheckpointThresholdSetting),
    DUCKDB_GLOBAL_ALIAS("wal_autocheckpoint", CheckpointThresholdSetting),
    DUCKDB_GLOBAL(CustomExtensionRepositorySetting),
    DUCKDB_LOCAL(CustomProfilingSettingsSetting),
    DUCKDB_GLOBAL(CustomUserAgentSetting),
    DUCKDB_LOCAL(DebugAsofIejoinSetting),
    DUCKDB_GLOBAL(DebugCheckpointAbortSetting),
    DUCKDB_LOCAL(DebugForceExternalSetting),
    DUCKDB_LOCAL(DebugForceNoCrossProductSetting),
    DUCKDB_GLOBAL(DebugSkipCheckpointOnCommitSetting),
    DUCKDB_GLOBAL(DebugWindowModeSetting),
    DUCKDB_GLOBAL(DefaultBlockSizeSetting),
    DUCKDB_GLOBAL_LOCAL(DefaultCollationSetting),
    DUCKDB_GLOBAL(DefaultNullOrderSetting),
    DUCKDB_GLOBAL_ALIAS("null_order", DefaultNullOrderSetting),
    DUCKDB_GLOBAL(DefaultOrderSetting),
    DUCKDB_GLOBAL(DefaultSecretStorageSetting),
    DUCKDB_GLOBAL(DisabledCompressionMethodsSetting),
    DUCKDB_GLOBAL(DisabledFilesystemsSetting),
    DUCKDB_GLOBAL(DisabledLogTypes),
    DUCKDB_GLOBAL(DisabledOptimizersSetting),
    DUCKDB_GLOBAL(DuckDBAPISetting),
    DUCKDB_LOCAL(DynamicOrFilterThresholdSetting),
    DUCKDB_GLOBAL(EnableExternalAccessSetting),
    DUCKDB_GLOBAL(EnableFSSTVectorsSetting),
    DUCKDB_LOCAL(EnableHTTPLoggingSetting),
    DUCKDB_GLOBAL(EnableHTTPMetadataCacheSetting),
    DUCKDB_GLOBAL(EnableLogging),
    DUCKDB_GLOBAL(EnableMacroDependenciesSetting),
    DUCKDB_GLOBAL(EnableObjectCacheSetting),
    DUCKDB_LOCAL(EnableProfilingSetting),
    DUCKDB_LOCAL(EnableProgressBarSetting),
    DUCKDB_LOCAL(EnableProgressBarPrintSetting),
    DUCKDB_GLOBAL(EnableViewDependenciesSetting),
    DUCKDB_GLOBAL(EnabledLogTypes),
    DUCKDB_LOCAL(ErrorsAsJSONSetting),
    DUCKDB_LOCAL(ExplainOutputSetting),
    DUCKDB_GLOBAL(ExtensionDirectorySetting),
    DUCKDB_GLOBAL(ExternalThreadsSetting),
    DUCKDB_LOCAL(FileSearchPathSetting),
    DUCKDB_GLOBAL(ForceBitpackingModeSetting),
    DUCKDB_GLOBAL(ForceCompressionSetting),
    DUCKDB_LOCAL(HomeDirectorySetting),
    DUCKDB_LOCAL(HTTPLoggingOutputSetting),
    DUCKDB_GLOBAL(HTTPProxySetting),
    DUCKDB_GLOBAL(HTTPProxyPasswordSetting),
    DUCKDB_GLOBAL(HTTPProxyUsernameSetting),
    DUCKDB_LOCAL(IEEEFloatingPointOpsSetting),
    DUCKDB_GLOBAL(ImmediateTransactionModeSetting),
    DUCKDB_GLOBAL(IndexScanMaxCountSetting),
    DUCKDB_GLOBAL(IndexScanPercentageSetting),
    DUCKDB_LOCAL(IntegerDivisionSetting),
    DUCKDB_LOCAL(LateMaterializationMaxRowsSetting),
    DUCKDB_GLOBAL(LockConfigurationSetting),
    DUCKDB_LOCAL(LogQueryPathSetting),
    DUCKDB_GLOBAL(LoggingLevel),
    DUCKDB_GLOBAL(LoggingMode),
    DUCKDB_GLOBAL(LoggingStorage),
    DUCKDB_LOCAL(MaxExpressionDepthSetting),
    DUCKDB_GLOBAL(MaxMemorySetting),
    DUCKDB_GLOBAL_ALIAS("memory_limit", MaxMemorySetting),
    DUCKDB_GLOBAL(MaxTempDirectorySizeSetting),
    DUCKDB_GLOBAL(MaxVacuumTasksSetting),
    DUCKDB_LOCAL(MergeJoinThresholdSetting),
    DUCKDB_LOCAL(NestedLoopJoinThresholdSetting),
    DUCKDB_GLOBAL(OldImplicitCastingSetting),
    DUCKDB_LOCAL(OrderByNonIntegerLiteralSetting),
    DUCKDB_LOCAL(OrderedAggregateThresholdSetting),
    DUCKDB_LOCAL(PartitionedWriteFlushThresholdSetting),
    DUCKDB_LOCAL(PartitionedWriteMaxOpenFilesSetting),
    DUCKDB_GLOBAL(PasswordSetting),
    DUCKDB_LOCAL(PerfectHtThresholdSetting),
    DUCKDB_LOCAL(PivotFilterThresholdSetting),
    DUCKDB_LOCAL(PivotLimitSetting),
    DUCKDB_LOCAL(PreferRangeJoinsSetting),
    DUCKDB_LOCAL(PreserveIdentifierCaseSetting),
    DUCKDB_GLOBAL(PreserveInsertionOrderSetting),
    DUCKDB_GLOBAL(ProduceArrowStringViewSetting),
    DUCKDB_LOCAL(ProfileOutputSetting),
    DUCKDB_LOCAL_ALIAS("profiling_output", ProfileOutputSetting),
    DUCKDB_LOCAL(ProfilingModeSetting),
    DUCKDB_LOCAL(ProgressBarTimeSetting),
    DUCKDB_LOCAL(ScalarSubqueryErrorOnMultipleRowsSetting),
    DUCKDB_LOCAL(SchemaSetting),
    DUCKDB_LOCAL(SearchPathSetting),
    DUCKDB_GLOBAL(SecretDirectorySetting),
    DUCKDB_GLOBAL(StorageCompatibilityVersionSetting),
    DUCKDB_LOCAL(StreamingBufferSizeSetting),
    DUCKDB_GLOBAL(TempDirectorySetting),
    DUCKDB_GLOBAL(ThreadsSetting),
    DUCKDB_GLOBAL_ALIAS("worker_threads", ThreadsSetting),
    DUCKDB_GLOBAL(UsernameSetting),
    DUCKDB_GLOBAL_ALIAS("user", UsernameSetting),
    DUCKDB_GLOBAL(ZstdMinStringLengthSetting),
    FINAL_SETTING};

vector<ConfigurationOption> DBConfig::GetOptions() {
	vector<ConfigurationOption> options;
	for (idx_t index = 0; internal_options[index].name; index++) {
		options.push_back(internal_options[index]);
	}
	return options;
}

idx_t DBConfig::GetOptionCount() {
	idx_t count = 0;
	for (idx_t index = 0; internal_options[index].name; index++) {
		count++;
	}
	return count;
}

vector<std::string> DBConfig::GetOptionNames() {
	vector<string> names;
	for (idx_t i = 0, option_count = DBConfig::GetOptionCount(); i < option_count; i++) {
		names.emplace_back(DBConfig::GetOptionByIndex(i)->name);
	}
	return names;
}

optional_ptr<const ConfigurationOption> DBConfig::GetOptionByIndex(idx_t target_index) {
	for (idx_t index = 0; internal_options[index].name; index++) {
		if (index == target_index) {
			return internal_options + index;
		}
	}
	return nullptr;
}

optional_ptr<const ConfigurationOption> DBConfig::GetOptionByName(const string &name) {
	auto lname = StringUtil::Lower(name);
	for (idx_t index = 0; internal_options[index].name; index++) {
		D_ASSERT(StringUtil::Lower(internal_options[index].name) == string(internal_options[index].name));
		if (internal_options[index].name == lname) {
			return internal_options + index;
		}
	}
	return nullptr;
}

void DBConfig::SetOption(const ConfigurationOption &option, const Value &value) {
	SetOption(nullptr, option, value);
}

void DBConfig::SetOptionByName(const string &name, const Value &value) {
	if (is_user_config) {
		// for user config we just set the option in the `user_options`
		options.user_options[name] = value;
	}
	auto option = DBConfig::GetOptionByName(name);
	if (option) {
		SetOption(*option, value);
		return;
	}

	auto param = extension_parameters.find(name);
	if (param != extension_parameters.end()) {
		Value target_value = value.DefaultCastAs(param->second.type);
		SetOption(name, std::move(target_value));
	} else {
		options.unrecognized_options[name] = value;
	}
}

void DBConfig::SetOptionsByName(const case_insensitive_map_t<Value> &values) {
	for (auto &kv : values) {
		auto &name = kv.first;
		auto &value = kv.second;
		SetOptionByName(name, value);
	}
}

void DBConfig::SetOption(DatabaseInstance *db, const ConfigurationOption &option, const Value &value) {
	lock_guard<mutex> l(config_lock);
	if (!option.set_global) {
		throw InvalidInputException("Could not set option \"%s\" as a global option", option.name);
	}
	D_ASSERT(option.reset_global);
	Value input = value.DefaultCastAs(ParseLogicalType(option.parameter_type));
	option.set_global(db, *this, input);
}

void DBConfig::ResetOption(DatabaseInstance *db, const ConfigurationOption &option) {
	lock_guard<mutex> l(config_lock);
	if (!option.reset_global) {
		throw InternalException("Could not reset option \"%s\" as a global option", option.name);
	}
	D_ASSERT(option.set_global);
	option.reset_global(db, *this);
}

void DBConfig::SetOption(const string &name, Value value) {
	lock_guard<mutex> l(config_lock);
	options.set_variables[name] = std::move(value);
}

void DBConfig::ResetOption(const string &name) {
	lock_guard<mutex> l(config_lock);
	auto extension_option = extension_parameters.find(name);
	D_ASSERT(extension_option != extension_parameters.end());
	auto &default_value = extension_option->second.default_value;
	if (!default_value.IsNull()) {
		// Default is not NULL, override the setting
		options.set_variables[name] = default_value;
	} else {
		// Otherwise just remove it from the 'set_variables' map
		options.set_variables.erase(name);
	}
}

LogicalType DBConfig::ParseLogicalType(const string &type) {
	if (StringUtil::EndsWith(type, "[]")) {
		// list - recurse
		auto child_type = ParseLogicalType(type.substr(0, type.size() - 2));
		return LogicalType::LIST(child_type);
	}

	if (StringUtil::EndsWith(type, "]")) {
		// array - recurse
		auto bracket_open_idx = type.rfind('[');
		if (bracket_open_idx == DConstants::INVALID_INDEX || bracket_open_idx == 0) {
			throw InternalException("Ill formatted type: '%s'", type);
		}
		idx_t array_size = 0;
		for (auto length_idx = bracket_open_idx + 1; length_idx < type.size() - 1; length_idx++) {
			if (!isdigit(type[length_idx])) {
				throw InternalException("Ill formatted array type: '%s'", type);
			}
			array_size = array_size * 10 + static_cast<idx_t>(type[length_idx] - '0');
		}
		if (array_size == 0 || array_size > ArrayType::MAX_ARRAY_SIZE) {
			throw InternalException("Invalid array size: '%s'", type);
		}
		auto child_type = ParseLogicalType(type.substr(0, bracket_open_idx));
		return LogicalType::ARRAY(child_type, array_size);
	}

	if (StringUtil::StartsWith(type, "MAP(") && StringUtil::EndsWith(type, ")")) {
		// map - recurse
		string map_args = type.substr(4, type.size() - 5);
		vector<string> map_args_vect = StringUtil::SplitWithParentheses(map_args);
		if (map_args_vect.size() != 2) {
			throw InternalException("Ill formatted map type: '%s'", type);
		}
		StringUtil::Trim(map_args_vect[0]);
		StringUtil::Trim(map_args_vect[1]);
		auto key_type = ParseLogicalType(map_args_vect[0]);
		auto value_type = ParseLogicalType(map_args_vect[1]);
		return LogicalType::MAP(key_type, value_type);
	}

	if (StringUtil::StartsWith(type, "UNION(") && StringUtil::EndsWith(type, ")")) {
		// union - recurse
		string union_members_str = type.substr(6, type.size() - 7);
		vector<string> union_members_vect = StringUtil::SplitWithParentheses(union_members_str);
		child_list_t<LogicalType> union_members;
		for (idx_t member_idx = 0; member_idx < union_members_vect.size(); member_idx++) {
			StringUtil::Trim(union_members_vect[member_idx]);
			vector<string> union_member_parts = StringUtil::SplitWithParentheses(union_members_vect[member_idx], ' ');
			if (union_member_parts.size() != 2) {
				throw InternalException("Ill formatted union type: %s", type);
			}
			StringUtil::Trim(union_member_parts[0]);
			StringUtil::Trim(union_member_parts[1]);
			auto value_type = ParseLogicalType(union_member_parts[1]);
			union_members.emplace_back(make_pair(union_member_parts[0], value_type));
		}
		if (union_members.empty() || union_members.size() > UnionType::MAX_UNION_MEMBERS) {
			throw InternalException("Invalid number of union members: '%s'", type);
		}
		return LogicalType::UNION(union_members);
	}

	if (StringUtil::StartsWith(type, "STRUCT(") && StringUtil::EndsWith(type, ")")) {
		// struct - recurse
		string struct_members_str = type.substr(7, type.size() - 8);
		vector<string> struct_members_vect = StringUtil::SplitWithParentheses(struct_members_str);
		child_list_t<LogicalType> struct_members;
		for (idx_t member_idx = 0; member_idx < struct_members_vect.size(); member_idx++) {
			StringUtil::Trim(struct_members_vect[member_idx]);
			vector<string> struct_member_parts = StringUtil::SplitWithParentheses(struct_members_vect[member_idx], ' ');
			if (struct_member_parts.size() != 2) {
				throw InternalException("Ill formatted struct type: %s", type);
			}
			StringUtil::Trim(struct_member_parts[0]);
			StringUtil::Trim(struct_member_parts[1]);
			auto value_type = ParseLogicalType(struct_member_parts[1]);
			struct_members.emplace_back(make_pair(struct_member_parts[0], value_type));
		}
		return LogicalType::STRUCT(struct_members);
	}

	LogicalType type_id = StringUtil::CIEquals(type, "ANY") ? LogicalType::ANY : TransformStringToLogicalTypeId(type);
	if (type_id == LogicalTypeId::USER) {
		throw InternalException("Error while generating extension function overloads - unrecognized logical type %s",
		                        type);
	}
	return type_id;
}

void DBConfig::AddExtensionOption(const string &name, string description, LogicalType parameter,
                                  const Value &default_value, set_option_callback_t function) {
	extension_parameters.insert(
	    make_pair(name, ExtensionOption(std::move(description), std::move(parameter), function, default_value)));
	// copy over unrecognized options, if they match the new extension option
	auto iter = options.unrecognized_options.find(name);
	if (iter != options.unrecognized_options.end()) {
		options.set_variables[name] = iter->second;
		options.unrecognized_options.erase(iter);
	}
	if (!default_value.IsNull() && options.set_variables.find(name) == options.set_variables.end()) {
		// Default value is set, insert it into the 'set_variables' list
		options.set_variables[name] = default_value;
	}
}

bool DBConfig::IsInMemoryDatabase(const char *database_path) {
	if (!database_path) {
		// Entirely empty
		return true;
	}
	if (strlen(database_path) == 0) {
		// '' empty string
		return true;
	}
	if (strcmp(database_path, ":memory:") == 0) {
		return true;
	}
	return false;
}

CastFunctionSet &DBConfig::GetCastFunctions() {
	return *cast_functions;
}

CollationBinding &DBConfig::GetCollationBinding() {
	return *collation_bindings;
}

IndexTypeSet &DBConfig::GetIndexTypes() {
	return *index_types;
}

void DBConfig::SetDefaultMaxMemory() {
	auto memory = GetSystemAvailableMemory(*file_system);
	if (memory == DBConfigOptions().maximum_memory) {
		// If GetSystemAvailableMemory returned the default, use it as is
		options.maximum_memory = memory;
	} else {
		// Otherwise, use 80% of the available memory
		options.maximum_memory = memory * 8 / 10;
	}
}

void DBConfig::SetDefaultTempDirectory() {
	if (!options.use_temporary_directory) {
		options.temporary_directory = string();
	} else if (DBConfig::IsInMemoryDatabase(options.database_path.c_str())) {
		options.temporary_directory = ".tmp";
	} else {
		options.temporary_directory = options.database_path + ".tmp";
	}
}

void DBConfig::CheckLock(const string &name) {
	if (!options.lock_configuration) {
		// not locked
		return;
	}
	case_insensitive_set_t allowed_settings {"schema", "search_path"};
	if (allowed_settings.find(name) != allowed_settings.end()) {
		// we are always allowed to change these settings
		return;
	}
	// not allowed!
	throw InvalidInputException("Cannot change configuration option \"%s\" - the configuration has been locked", name);
}

idx_t DBConfig::GetSystemMaxThreads(FileSystem &fs) {
#ifdef DUCKDB_NO_THREADS
	return 1;
#else
	idx_t physical_cores = std::thread::hardware_concurrency();
#ifdef __linux__
	if (const char *slurm_cpus = getenv("SLURM_CPUS_ON_NODE")) {
		idx_t slurm_threads;
		if (TryCast::Operation<string_t, idx_t>(string_t(slurm_cpus), slurm_threads)) {
			return MaxValue<idx_t>(slurm_threads, 1);
		}
	}
	return MaxValue<idx_t>(CGroups::GetCPULimit(fs, physical_cores), 1);
#else
	return MaxValue<idx_t>(physical_cores, 1);
#endif
#endif
}

idx_t DBConfig::GetSystemAvailableMemory(FileSystem &fs) {
#ifdef __linux__
	// Check SLURM environment variables first
	const char *slurm_mem_per_node = getenv("SLURM_MEM_PER_NODE");
	const char *slurm_mem_per_cpu = getenv("SLURM_MEM_PER_CPU");

	if (slurm_mem_per_node) {
		auto limit = ParseMemoryLimitSlurm(slurm_mem_per_node);
		if (limit.IsValid()) {
			return limit.GetIndex();
		}
	} else if (slurm_mem_per_cpu) {
		auto mem_per_cpu = ParseMemoryLimitSlurm(slurm_mem_per_cpu);
		if (mem_per_cpu.IsValid()) {
			idx_t num_threads = GetSystemMaxThreads(fs);
			return mem_per_cpu.GetIndex() * num_threads;
		}
	}

	// Check cgroup memory limit
	auto cgroup_memory_limit = CGroups::GetMemoryLimit(fs);
	if (cgroup_memory_limit.IsValid()) {
		return cgroup_memory_limit.GetIndex();
	}
#endif

	// System memory detection
	auto memory = FileSystem::GetAvailableMemory();
	if (!memory.IsValid()) {
		return DBConfigOptions().maximum_memory;
	}
	return memory.GetIndex();
}

idx_t DBConfig::ParseMemoryLimit(const string &arg) {
	if (arg[0] == '-' || arg == "null" || arg == "none") {
		// infinite
		return NumericLimits<idx_t>::Maximum();
	}
	// split based on the number/non-number
	idx_t idx = 0;
	while (StringUtil::CharacterIsSpace(arg[idx])) {
		idx++;
	}
	idx_t num_start = idx;
	while ((arg[idx] >= '0' && arg[idx] <= '9') || arg[idx] == '.' || arg[idx] == 'e' || arg[idx] == 'E' ||
	       arg[idx] == '-') {
		idx++;
	}
	if (idx == num_start) {
		throw ParserException("Memory limit must have a number (e.g. SET memory_limit=1GB");
	}
	string number = arg.substr(num_start, idx - num_start);

	// try to parse the number
	double limit = Cast::Operation<string_t, double>(string_t(number));

	// now parse the memory limit unit (e.g. bytes, gb, etc)
	while (StringUtil::CharacterIsSpace(arg[idx])) {
		idx++;
	}
	idx_t start = idx;
	while (idx < arg.size() && !StringUtil::CharacterIsSpace(arg[idx])) {
		idx++;
	}
	if (limit < 0) {
		// limit < 0, set limit to infinite
		return (idx_t)-1;
	}
	string unit = StringUtil::Lower(arg.substr(start, idx - start));
	idx_t multiplier;
	if (unit == "byte" || unit == "bytes" || unit == "b") {
		multiplier = 1;
	} else if (unit == "kilobyte" || unit == "kilobytes" || unit == "kb" || unit == "k") {
		multiplier = 1000LL;
	} else if (unit == "megabyte" || unit == "megabytes" || unit == "mb" || unit == "m") {
		multiplier = 1000LL * 1000LL;
	} else if (unit == "gigabyte" || unit == "gigabytes" || unit == "gb" || unit == "g") {
		multiplier = 1000LL * 1000LL * 1000LL;
	} else if (unit == "terabyte" || unit == "terabytes" || unit == "tb" || unit == "t") {
		multiplier = 1000LL * 1000LL * 1000LL * 1000LL;
	} else if (unit == "kib") {
		multiplier = 1024LL;
	} else if (unit == "mib") {
		multiplier = 1024LL * 1024LL;
	} else if (unit == "gib") {
		multiplier = 1024LL * 1024LL * 1024LL;
	} else if (unit == "tib") {
		multiplier = 1024LL * 1024LL * 1024LL * 1024LL;
	} else {
		throw ParserException("Unknown unit for memory_limit: '%s' (expected: KB, MB, GB, TB for 1000^i units or KiB, "
		                      "MiB, GiB, TiB for 1024^i units)",
		                      unit);
	}
	return LossyNumericCast<idx_t>(static_cast<double>(multiplier) * limit);
}

optional_idx DBConfig::ParseMemoryLimitSlurm(const string &arg) {
	if (arg.empty()) {
		return optional_idx();
	}

	string number_str = arg;
	idx_t multiplier = 1000LL * 1000LL; // Default to MB if no unit specified

	// Check for SLURM-style suffixes
	if (arg.back() == 'K' || arg.back() == 'k') {
		number_str = arg.substr(0, arg.size() - 1);
		multiplier = 1000LL;
	} else if (arg.back() == 'M' || arg.back() == 'm') {
		number_str = arg.substr(0, arg.size() - 1);
		multiplier = 1000LL * 1000LL;
	} else if (arg.back() == 'G' || arg.back() == 'g') {
		number_str = arg.substr(0, arg.size() - 1);
		multiplier = 1000LL * 1000LL * 1000LL;
	} else if (arg.back() == 'T' || arg.back() == 't') {
		number_str = arg.substr(0, arg.size() - 1);
		multiplier = 1000LL * 1000LL * 1000LL * 1000LL;
	}

	// Parse the number
	double limit;
	if (!TryCast::Operation<string_t, double>(string_t(number_str), limit)) {
		return optional_idx();
	}

	if (limit < 0) {
		return static_cast<idx_t>(NumericLimits<int64_t>::Maximum());
	}
	idx_t actual_limit = LossyNumericCast<idx_t>(static_cast<double>(multiplier) * limit);
	if (actual_limit == NumericLimits<idx_t>::Maximum()) {
		return static_cast<idx_t>(NumericLimits<int64_t>::Maximum());
	}
	return actual_limit;
}

// Right now we only really care about access mode when comparing DBConfigs
bool DBConfigOptions::operator==(const DBConfigOptions &other) const {
	return other.access_mode == access_mode && other.user_options == user_options;
}

bool DBConfig::operator==(const DBConfig &other) {
	return other.options == options;
}

bool DBConfig::operator!=(const DBConfig &other) {
	return !(other.options == options);
}

OrderType DBConfig::ResolveOrder(OrderType order_type) const {
	if (order_type != OrderType::ORDER_DEFAULT) {
		return order_type;
	}
	return options.default_order_type;
}

OrderByNullType DBConfig::ResolveNullOrder(OrderType order_type, OrderByNullType null_type) const {
	if (null_type != OrderByNullType::ORDER_DEFAULT) {
		return null_type;
	}
	switch (options.default_null_order) {
	case DefaultOrderByNullType::NULLS_FIRST:
		return OrderByNullType::NULLS_FIRST;
	case DefaultOrderByNullType::NULLS_LAST:
		return OrderByNullType::NULLS_LAST;
	case DefaultOrderByNullType::NULLS_FIRST_ON_ASC_LAST_ON_DESC:
		return order_type == OrderType::ASCENDING ? OrderByNullType::NULLS_FIRST : OrderByNullType::NULLS_LAST;
	case DefaultOrderByNullType::NULLS_LAST_ON_ASC_FIRST_ON_DESC:
		return order_type == OrderType::ASCENDING ? OrderByNullType::NULLS_LAST : OrderByNullType::NULLS_FIRST;
	default:
		throw InternalException("Unknown null order setting");
	}
}

const string DBConfig::UserAgent() const {
	auto user_agent = GetDefaultUserAgent();

	if (!options.duckdb_api.empty()) {
		user_agent += " " + options.duckdb_api;
	}

	if (!options.custom_user_agent.empty()) {
		user_agent += " " + options.custom_user_agent;
	}
	return user_agent;
}

string DBConfig::SanitizeAllowedPath(const string &path) const {
	auto path_sep = file_system->PathSeparator(path);
	if (path_sep != "/") {
		// allowed_directories/allowed_path always uses forward slashes regardless of the OS
		return StringUtil::Replace(path, path_sep, "/");
	}
	return path;
}

void DBConfig::AddAllowedDirectory(const string &path) {
	auto allowed_directory = SanitizeAllowedPath(path);
	if (allowed_directory.empty()) {
		throw InvalidInputException("Cannot provide an empty string for allowed_directory");
	}
	// ensure the directory ends with a path separator
	if (!StringUtil::EndsWith(allowed_directory, "/")) {
		allowed_directory += "/";
	}
	options.allowed_directories.insert(allowed_directory);
}

void DBConfig::AddAllowedPath(const string &path) {
	auto allowed_path = SanitizeAllowedPath(path);
	options.allowed_paths.insert(allowed_path);
}

bool DBConfig::CanAccessFile(const string &input_path, FileType type) {
	if (options.enable_external_access) {
		// all external access is allowed
		return true;
	}
	string path = SanitizeAllowedPath(input_path);
	if (options.allowed_paths.count(path) > 0) {
		// path is explicitly allowed
		return true;
	}
	if (options.allowed_directories.empty()) {
		// no prefix directories specified
		return false;
	}
	if (type == FileType::FILE_TYPE_DIR) {
		// make sure directories end with a /
		if (!StringUtil::EndsWith(path, "/")) {
			path += "/";
		}
	}
	auto start_bound = options.allowed_directories.lower_bound(path);
	if (start_bound != options.allowed_directories.begin()) {
		--start_bound;
	}
	auto end_bound = options.allowed_directories.upper_bound(path);

	string prefix;
	for (auto it = start_bound; it != end_bound; ++it) {
		if (StringUtil::StartsWith(path, *it)) {
			prefix = *it;
			break;
		}
	}
	if (prefix.empty()) {
		// no common prefix found - path is not inside an allowed directory
		return false;
	}
	D_ASSERT(StringUtil::EndsWith(prefix, "/"));
	// path is inside an allowed directory - HOWEVER, we could still exit the allowed directory using ".."
	// we check if we ever exit the allowed directory using ".." by looking at the path fragments
	idx_t directory_level = 0;
	idx_t current_pos = prefix.size();
	for (; current_pos < path.size(); current_pos++) {
		idx_t dir_begin = current_pos;
		// find either the end of the path or the directory separator
		for (; path[current_pos] != '/' && current_pos < path.size(); current_pos++) {
		}
		idx_t path_length = current_pos - dir_begin;
		if (path_length == 2 && path[dir_begin] == '.' && path[dir_begin + 1] == '.') {
			// go up a directory
			if (directory_level == 0) {
				// we cannot go up past the prefix
				return false;
			}
			--directory_level;
		} else if (path_length > 0) {
			directory_level++;
		}
	}
	return true;
}

SerializationOptions::SerializationOptions(AttachedDatabase &db) {
	serialization_compatibility = SerializationCompatibility::FromDatabase(db);
}

SerializationCompatibility SerializationCompatibility::FromDatabase(AttachedDatabase &db) {
	return FromIndex(db.GetStorageManager().GetStorageVersion());
}

SerializationCompatibility SerializationCompatibility::FromIndex(const idx_t version) {
	SerializationCompatibility result;
	result.duckdb_version = "";
	result.serialization_version = version;
	result.manually_set = false;
	return result;
}

SerializationCompatibility SerializationCompatibility::FromString(const string &input) {
	if (input.empty()) {
		throw InvalidInputException("Version string can not be empty");
	}

	auto serialization_version = GetSerializationVersion(input.c_str());
	if (!serialization_version.IsValid()) {
		auto candidates = GetSerializationCandidates();
		throw InvalidInputException("The version string '%s' is not a known DuckDB version, valid options are: %s",
		                            input, StringUtil::Join(candidates, ", "));
	}
	SerializationCompatibility result;
	result.duckdb_version = input;
	result.serialization_version = serialization_version.GetIndex();
	result.manually_set = true;
	return result;
}

SerializationCompatibility SerializationCompatibility::Default() {
#ifdef DUCKDB_ALTERNATIVE_VERIFY
	auto res = FromString("latest");
	res.manually_set = false;
	return res;
#else
#ifdef DUCKDB_LATEST_STORAGE
	auto res = FromString("latest");
	res.manually_set = false;
	return res;
#else
	auto res = FromString("v0.10.2");
	res.manually_set = false;
	return res;
#endif
#endif
}

SerializationCompatibility SerializationCompatibility::Latest() {
	auto res = FromString("latest");
	res.manually_set = false;
	return res;
}

bool SerializationCompatibility::Compare(idx_t property_version) const {
	return property_version <= serialization_version;
}

} // namespace duckdb
