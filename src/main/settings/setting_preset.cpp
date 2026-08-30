#include "duckdb/main/setting_preset.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/operator/helper/physical_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

namespace {

//! The available system memory. The config's filesystem is only set once the database is running,
//! so fall back to a temporary local filesystem.
idx_t AvailableMemory(DBConfig &config) {
	if (config.file_system) {
		return DBConfig::GetSystemAvailableMemory(*config.file_system);
	}
	auto local_fs = FileSystem::CreateLocal();
	return DBConfig::GetSystemAvailableMemory(*local_fs);
}

idx_t SystemThreads(DBConfig &config) {
	if (config.file_system) {
		return DBConfig::GetSystemMaxThreads(*config.file_system);
	}
	auto local_fs = FileSystem::CreateLocal();
	return DBConfig::GetSystemMaxThreads(*local_fs);
}

Value MemoryFraction(DBConfig &config, idx_t percent) {
	const auto bytes = AvailableMemory(config) / 100 * percent;
	return Value(StringUtil::Format("%lluB", static_cast<unsigned long long>(bytes)));
}

Value ThreadFraction(DBConfig &config, idx_t divisor) {
	return Value::BIGINT(NumericCast<int64_t>(MaxValue<idx_t>(SystemThreads(config) / divisor, 1)));
}

//! The default async thread count: four per core, capped
Value DefaultAsyncThreads(DBConfig &config) {
	return Value::BIGINT(NumericCast<int64_t>(MinValue<idx_t>(4 * SystemThreads(config), 256)));
}

//===--------------------------------------------------------------------===//
// host: how much of this machine may DuckDB use?
//===--------------------------------------------------------------------===//
// The three rungs differ only in degree, but the fact they encode - whether DuckDB is the main
// process on this machine - is one the engine cannot observe. Each rung sets the same settings so
// that switching between them is complete; a setting that would be identical across all three is
// left out rather than pinned to its default.

const PresetMember HOST_DEDICATED[] = {
	{"threads", [](DBConfig &c) { return ThreadFraction(c, 1); }, false},
	{"async_threads", [](DBConfig &c) { return DefaultAsyncThreads(c); }, false},
	{"max_memory", [](DBConfig &c) { return MemoryFraction(c, 80); }, false},
	{"pin_threads", [](DBConfig &) { return Value("auto"); }, false},
};

const PresetMember HOST_SHARED[] = {
	// threads before async_threads: the async pool is sized relative to it
	{"threads", [](DBConfig &c) { return ThreadFraction(c, 2); }, false},
	{"async_threads", [](DBConfig &c) { return ThreadFraction(c, 2); }, false},
	{"max_memory", [](DBConfig &c) { return MemoryFraction(c, 50); }, false},
	{"pin_threads", [](DBConfig &) { return Value("off"); }, false},
};

const PresetMember HOST_BACKGROUND[] = {
	{"threads", [](DBConfig &c) { return ThreadFraction(c, 4); }, false},
	{"async_threads", [](DBConfig &c) { return ThreadFraction(c, 4); }, false},
	{"max_memory", [](DBConfig &c) { return MemoryFraction(c, 25); }, false},
	{"pin_threads", [](DBConfig &) { return Value("off"); }, false},
};

//===--------------------------------------------------------------------===//
// network: how reliable is the link to remote storage?
//===--------------------------------------------------------------------===//
// autoload_known_extensions comes first so that the httpfs settings below can be resolved; they
// are optional because httpfs may not be installed.

const PresetMember NETWORK_FLAKY[] = {
	{"autoload_known_extensions", [](DBConfig &) { return Value::BOOLEAN(true); }, false},
	{"async_threads", [](DBConfig &c) { return ThreadFraction(c, 1); }, false},
	{"http_retries", [](DBConfig &) { return Value::BIGINT(10); }, true},
	{"http_retry_backoff", [](DBConfig &) { return Value::FLOAT(2); }, true},
	{"http_timeout", [](DBConfig &) { return Value::BIGINT(120000); }, true},
};

//===--------------------------------------------------------------------===//
// memory / execution
//===--------------------------------------------------------------------===//

const PresetMember MEMORY_NO_SPILL[] = {
	{"max_temp_directory_size", [](DBConfig &) { return Value("0B"); }, false},
};

const PresetMember EXECUTION_BULK_INGEST[] = {
	{"preserve_insertion_order", [](DBConfig &) { return Value::BOOLEAN(false); }, false},
	{"checkpoint_threshold", [](DBConfig &) { return Value("1GB"); }, false},
};

#define DUCKDB_PRESET(NAME, DESC, MEMBERS) {NAME, DESC, MEMBERS, sizeof(MEMBERS) / sizeof(PresetMember)}

const PresetDefinition BUILTIN_PRESETS[] = {
	DUCKDB_PRESET("execution:bulk_ingest", "Bulk loading rather than querying", EXECUTION_BULK_INGEST),
	DUCKDB_PRESET("host:background", "DuckDB is not the main process on this machine", HOST_BACKGROUND),
	DUCKDB_PRESET("host:dedicated", "DuckDB may use the whole machine", HOST_DEDICATED),
	DUCKDB_PRESET("host:shared", "DuckDB shares this machine with other work", HOST_SHARED),
	DUCKDB_PRESET("memory:no_spill", "Fail rather than spill to disk", MEMORY_NO_SPILL),
	DUCKDB_PRESET("network:flaky", "The link to remote storage drops or rate-limits", NETWORK_FLAKY),
};

} // namespace

idx_t PresetRegistry::GetPresetCount() {
	return sizeof(BUILTIN_PRESETS) / sizeof(PresetDefinition);
}

const PresetDefinition &PresetRegistry::GetPresetByIndex(idx_t index) {
	D_ASSERT(index < GetPresetCount());
	return BUILTIN_PRESETS[index];
}

optional_ptr<const PresetDefinition> PresetRegistry::Find(const string &name) {
	for (idx_t i = 0; i < GetPresetCount(); i++) {
		if (StringUtil::CIEquals(BUILTIN_PRESETS[i].name, name)) {
			return &BUILTIN_PRESETS[i];
		}
	}
	return nullptr;
}

vector<PresetMemberResult> PresetRegistry::Apply(ClientContext &context, const PresetDefinition &preset) {
	auto &config = DBConfig::GetConfig(context);
	vector<PresetMemberResult> results;
	for (idx_t i = 0; i < preset.member_count; i++) {
		auto &member = preset.members[i];
		Identifier name(member.setting);

		PresetMemberResult result;
		result.setting = member.setting;
		Value previous;
		if (!context.TryGetCurrentSetting(member.setting, previous)) {
			if (member.optional) {
				result.status = "skipped";
				result.note = "setting is not available";
				results.push_back(std::move(result));
				continue;
			}
		}
		result.old_value = previous;
		auto target = member.value(config);
		PhysicalSet::SetVariable(context, name, SetScope::AUTOMATIC, target);
		Value applied;
		context.TryGetCurrentSetting(member.setting, applied);
		result.new_value = applied;
		result.status = "applied";
		results.push_back(std::move(result));
	}
	return results;
}

} // namespace duckdb
