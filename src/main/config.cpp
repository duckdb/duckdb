#include "duckdb/main/config.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

#define DUCKDB_GLOBAL(_PARAM)                                                                                          \
	{ _PARAM::Name, _PARAM::Description, _PARAM::InputType, _PARAM::SetGlobal, nullptr, _PARAM::GetSetting }
#define DUCKDB_GLOBAL_ALIAS(_ALIAS, _PARAM)                                                                            \
	{ _ALIAS, _PARAM::Description, _PARAM::InputType, _PARAM::SetGlobal, nullptr, _PARAM::GetSetting }

#define DUCKDB_LOCAL(_PARAM)                                                                                           \
	{ _PARAM::Name, _PARAM::Description, _PARAM::InputType, nullptr, _PARAM::SetLocal, _PARAM::GetSetting }
#define DUCKDB_LOCAL_ALIAS(_ALIAS, _PARAM)                                                                             \
	{ _ALIAS, _PARAM::Description, _PARAM::InputType, nullptr, _PARAM::SetLocal, _PARAM::GetSetting }

#define DUCKDB_GLOBAL_LOCAL(_PARAM)                                                                                    \
	{ _PARAM::Name, _PARAM::Description, _PARAM::InputType, _PARAM::SetGlobal, _PARAM::SetLocal, _PARAM::GetSetting }
#define DUCKDB_GLOBAL_LOCAL_ALIAS(_ALIAS, _PARAM)                                                                      \
	{ _ALIAS, _PARAM::Description, _PARAM::InputType, _PARAM::SetGlobal, _PARAM::SetLocal, _PARAM::GetSetting }
#define FINAL_SETTING                                                                                                  \
	{ nullptr, nullptr, LogicalTypeId::INVALID, nullptr, nullptr, nullptr }

static ConfigurationOption internal_options[] = {DUCKDB_GLOBAL(AccessModeSetting),
                                                 DUCKDB_GLOBAL(CheckpointThresholdSetting),
                                                 DUCKDB_GLOBAL(DebugCheckpointAbort),
                                                 DUCKDB_LOCAL(DebugForceExternal),
                                                 DUCKDB_LOCAL(DebugForceNoCrossProduct),
                                                 DUCKDB_GLOBAL(DebugManyFreeListBlocks),
                                                 DUCKDB_GLOBAL(DebugWindowMode),
                                                 DUCKDB_GLOBAL_LOCAL(DefaultCollationSetting),
                                                 DUCKDB_GLOBAL(DefaultOrderSetting),
                                                 DUCKDB_GLOBAL(DefaultNullOrderSetting),
                                                 DUCKDB_GLOBAL(DisabledOptimizersSetting),
                                                 DUCKDB_GLOBAL(EnableExternalAccessSetting),
                                                 DUCKDB_GLOBAL(EnableObjectCacheSetting),
                                                 DUCKDB_LOCAL(EnableProfilingSetting),
                                                 DUCKDB_LOCAL(EnableProgressBarSetting),
                                                 DUCKDB_LOCAL(ExplainOutputSetting),
                                                 DUCKDB_GLOBAL(ExternalThreadsSetting),
                                                 DUCKDB_LOCAL(FileSearchPathSetting),
                                                 DUCKDB_GLOBAL(ForceCompressionSetting),
                                                 DUCKDB_LOCAL(LogQueryPathSetting),
                                                 DUCKDB_LOCAL(MaximumExpressionDepthSetting),
                                                 DUCKDB_GLOBAL(MaximumMemorySetting),
                                                 DUCKDB_GLOBAL_ALIAS("memory_limit", MaximumMemorySetting),
                                                 DUCKDB_GLOBAL_ALIAS("null_order", DefaultNullOrderSetting),
                                                 DUCKDB_LOCAL(PerfectHashThresholdSetting),
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
                                                 DUCKDB_GLOBAL_ALIAS("wal_autocheckpoint", CheckpointThresholdSetting),
                                                 DUCKDB_GLOBAL_ALIAS("worker_threads", ThreadsSetting),
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
	if (!option.set_global) {
		throw InternalException("Could not set option \"%s\" as a global option", option.name);
	}
	Value input = value.CastAs(option.parameter_type);
	option.set_global(nullptr, *this, input);
}

void DBConfig::AddExtensionOption(string name, string description, LogicalType parameter,
                                  set_option_callback_t function) {
	extension_parameters.insert(make_pair(move(name), ExtensionOption(move(description), move(parameter), function)));
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

} // namespace duckdb
