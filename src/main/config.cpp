#include "duckdb/main/config.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/storage_extension.hpp"

#ifndef DUCKDB_NO_THREADS
#include "duckdb/common/thread.hpp"
#endif

#include <cstdio>
#include <cinttypes>

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
	{ nullptr, nullptr, LogicalTypeId::INVALID, nullptr, nullptr, nullptr, nullptr, nullptr }

static ConfigurationOption internal_options[] = {DUCKDB_GLOBAL(AccessModeSetting),
                                                 DUCKDB_GLOBAL(CheckpointThresholdSetting),
                                                 DUCKDB_GLOBAL(DebugCheckpointAbort),
                                                 DUCKDB_LOCAL(DebugForceExternal),
                                                 DUCKDB_LOCAL(DebugForceNoCrossProduct),
                                                 DUCKDB_LOCAL(DebugAsOfIEJoin),
                                                 DUCKDB_LOCAL(PreferRangeJoins),
                                                 DUCKDB_GLOBAL(DebugWindowMode),
                                                 DUCKDB_GLOBAL_LOCAL(DefaultCollationSetting),
                                                 DUCKDB_GLOBAL(DefaultOrderSetting),
                                                 DUCKDB_GLOBAL(DefaultNullOrderSetting),
                                                 DUCKDB_GLOBAL(DisabledFileSystemsSetting),
                                                 DUCKDB_GLOBAL(DisabledOptimizersSetting),
                                                 DUCKDB_GLOBAL(EnableExternalAccessSetting),
                                                 DUCKDB_GLOBAL(EnableFSSTVectors),
                                                 DUCKDB_GLOBAL(AllowUnsignedExtensionsSetting),
                                                 DUCKDB_LOCAL(CustomExtensionRepository),
                                                 DUCKDB_LOCAL(AutoloadExtensionRepository),
                                                 DUCKDB_GLOBAL(AutoinstallKnownExtensions),
                                                 DUCKDB_GLOBAL(AutoloadKnownExtensions),
                                                 DUCKDB_GLOBAL(EnableObjectCacheSetting),
                                                 DUCKDB_GLOBAL(EnableHTTPMetadataCacheSetting),
                                                 DUCKDB_LOCAL(EnableProfilingSetting),
                                                 DUCKDB_LOCAL(EnableProgressBarSetting),
                                                 DUCKDB_LOCAL(EnableProgressBarPrintSetting),
                                                 DUCKDB_LOCAL(ExplainOutputSetting),
                                                 DUCKDB_GLOBAL(ExtensionDirectorySetting),
                                                 DUCKDB_GLOBAL(ExternalThreadsSetting),
                                                 DUCKDB_LOCAL(FileSearchPathSetting),
                                                 DUCKDB_GLOBAL(ForceCompressionSetting),
                                                 DUCKDB_GLOBAL(ForceBitpackingModeSetting),
                                                 DUCKDB_LOCAL(HomeDirectorySetting),
                                                 DUCKDB_LOCAL(LogQueryPathSetting),
                                                 DUCKDB_GLOBAL(LockConfigurationSetting),
                                                 DUCKDB_GLOBAL(ImmediateTransactionModeSetting),
                                                 DUCKDB_LOCAL(IntegerDivisionSetting),
                                                 DUCKDB_LOCAL(MaximumExpressionDepthSetting),
                                                 DUCKDB_GLOBAL(MaximumMemorySetting),
                                                 DUCKDB_GLOBAL_ALIAS("memory_limit", MaximumMemorySetting),
                                                 DUCKDB_GLOBAL_ALIAS("null_order", DefaultNullOrderSetting),
                                                 DUCKDB_LOCAL(OrderedAggregateThreshold),
                                                 DUCKDB_GLOBAL(PasswordSetting),
                                                 DUCKDB_LOCAL(PerfectHashThresholdSetting),
                                                 DUCKDB_LOCAL(PivotFilterThreshold),
                                                 DUCKDB_LOCAL(PivotLimitSetting),
                                                 DUCKDB_LOCAL(PreserveIdentifierCase),
                                                 DUCKDB_GLOBAL(PreserveInsertionOrder),
                                                 DUCKDB_LOCAL(ProfilerHistorySize),
                                                 DUCKDB_LOCAL(ProfileOutputSetting),
                                                 DUCKDB_LOCAL(ProfilingModeSetting),
                                                 DUCKDB_LOCAL_ALIAS("profiling_output", ProfileOutputSetting),
                                                 DUCKDB_LOCAL(ProgressBarTimeSetting),
                                                 DUCKDB_LOCAL(SchemaSetting),
                                                 DUCKDB_LOCAL(SearchPathSetting),
                                                 DUCKDB_GLOBAL(TempDirectorySetting),
                                                 DUCKDB_GLOBAL(ThreadsSetting),
                                                 DUCKDB_GLOBAL(UsernameSetting),
                                                 DUCKDB_GLOBAL(ExportLargeBufferArrow),
                                                 DUCKDB_GLOBAL_ALIAS("user", UsernameSetting),
                                                 DUCKDB_GLOBAL_ALIAS("wal_autocheckpoint", CheckpointThresholdSetting),
                                                 DUCKDB_GLOBAL_ALIAS("worker_threads", ThreadsSetting),
                                                 DUCKDB_GLOBAL(FlushAllocatorSetting),
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

ConfigurationOption *DBConfig::GetOptionByIndex(idx_t target_index) {
	for (idx_t index = 0; internal_options[index].name; index++) {
		if (index == target_index) {
			return internal_options + index;
		}
	}
	return nullptr;
}

ConfigurationOption *DBConfig::GetOptionByName(const string &name) {
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
	auto option = DBConfig::GetOptionByName(name);
	if (option) {
		SetOption(*option, value);
	} else {
		options.unrecognized_options[name] = value;
	}
}

void DBConfig::SetOption(DatabaseInstance *db, const ConfigurationOption &option, const Value &value) {
	lock_guard<mutex> l(config_lock);
	if (!option.set_global) {
		throw InternalException("Could not set option \"%s\" as a global option", option.name);
	}
	D_ASSERT(option.reset_global);
	Value input = value.DefaultCastAs(option.parameter_type);
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

void DBConfig::AddExtensionOption(const string &name, string description, LogicalType parameter,
                                  const Value &default_value, set_option_callback_t function) {
	extension_parameters.insert(
	    make_pair(name, ExtensionOption(std::move(description), std::move(parameter), function, default_value)));
	if (!default_value.IsNull()) {
		// Default value is set, insert it into the 'set_variables' list
		options.set_variables[name] = default_value;
	}
}

CastFunctionSet &DBConfig::GetCastFunctions() {
	return *cast_functions;
}

void DBConfig::SetDefaultMaxMemory() {
	auto memory = FileSystem::GetAvailableMemory();
	if (memory != DConstants::INVALID_INDEX) {
		options.maximum_memory = memory * 8 / 10;
	}
}

idx_t CGroupBandwidthQuota(idx_t physical_cores, FileSystem &fs) {
	static constexpr const char *CPU_MAX = "/sys/fs/cgroup/cpu.max";
	static constexpr const char *CFS_QUOTA = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";
	static constexpr const char *CFS_PERIOD = "/sys/fs/cgroup/cpu/cpu.cfs_period_us";

	int64_t quota, period;
	char byte_buffer[1000];
	unique_ptr<FileHandle> handle;
	int64_t read_bytes;

	if (fs.FileExists(CPU_MAX)) {
		// cgroup v2
		// https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html
		handle =
		    fs.OpenFile(CPU_MAX, FileFlags::FILE_FLAGS_READ, FileSystem::DEFAULT_LOCK, FileSystem::DEFAULT_COMPRESSION);
		read_bytes = fs.Read(*handle, (void *)byte_buffer, 999);
		byte_buffer[read_bytes] = '\0';
		if (std::sscanf(byte_buffer, "%" SCNd64 " %" SCNd64 "", &quota, &period) != 2) {
			return physical_cores;
		}
	} else if (fs.FileExists(CFS_QUOTA) && fs.FileExists(CFS_PERIOD)) {
		// cgroup v1
		// https://www.kernel.org/doc/html/latest/scheduler/sched-bwc.html#management

		// Read the quota, this indicates how many microseconds the CPU can be utilized by this cgroup per period
		handle = fs.OpenFile(CFS_QUOTA, FileFlags::FILE_FLAGS_READ, FileSystem::DEFAULT_LOCK,
		                     FileSystem::DEFAULT_COMPRESSION);
		read_bytes = fs.Read(*handle, (void *)byte_buffer, 999);
		byte_buffer[read_bytes] = '\0';
		if (std::sscanf(byte_buffer, "%" SCNd64 "", &quota) != 1) {
			return physical_cores;
		}

		// Read the time period, a cgroup can utilize the CPU up to quota microseconds every period
		handle = fs.OpenFile(CFS_PERIOD, FileFlags::FILE_FLAGS_READ, FileSystem::DEFAULT_LOCK,
		                     FileSystem::DEFAULT_COMPRESSION);
		read_bytes = fs.Read(*handle, (void *)byte_buffer, 999);
		byte_buffer[read_bytes] = '\0';
		if (std::sscanf(byte_buffer, "%" SCNd64 "", &period) != 1) {
			return physical_cores;
		}
	} else {
		// No cgroup quota
		return physical_cores;
	}
	if (quota > 0 && period > 0) {
		return idx_t(std::ceil((double)quota / (double)period));
	} else {
		return physical_cores;
	}
}

idx_t DBConfig::GetSystemMaxThreads(FileSystem &fs) {
#ifndef DUCKDB_NO_THREADS
	idx_t physical_cores = std::thread::hardware_concurrency();
#ifdef __linux__
	auto cores_available_per_period = CGroupBandwidthQuota(physical_cores, fs);
	return MaxValue<idx_t>(cores_available_per_period, 1);
#else
	return physical_cores;
#endif
#else
	return 1;
#endif
}

void DBConfig::SetDefaultMaxThreads() {
#ifndef DUCKDB_NO_THREADS
	options.maximum_threads = GetSystemMaxThreads(*file_system);
#else
	options.maximum_threads = 1;
#endif
}

idx_t DBConfig::ParseMemoryLimit(const string &arg) {
	if (arg[0] == '-' || arg == "null" || arg == "none") {
		return DConstants::INVALID_INDEX;
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
	} else {
		throw ParserException("Unknown unit for memory_limit: %s (expected: b, mb, gb or tb)", unit);
	}
	return (idx_t)multiplier * limit;
}

// Right now we only really care about access mode when comparing DBConfigs
bool DBConfigOptions::operator==(const DBConfigOptions &other) const {
	return other.access_mode == access_mode;
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

} // namespace duckdb
