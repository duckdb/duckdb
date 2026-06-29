#include "debug_fs_extension.hpp"

#include "debug_file_system.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

namespace {

//! Non-owning handle to the DebugFileSystem, ownership lies in DBConfig.
class DebugFileSystemCacheEntry : public ObjectCacheEntry {
public:
	explicit DebugFileSystemCacheEntry(DebugFileSystem &fs) : debug_fs(fs) {
	}

	static string ObjectType() {
		return "debug_fs_instance";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx {};
	}

	DebugFileSystem &Get() {
		return debug_fs;
	}

private:
	DebugFileSystem &debug_fs;
};

void EnsureDebugFileSystemInstalled(DatabaseInstance &db) {
	auto &cache = db.GetObjectCache();
	if (cache.GetWithTypePrefix<DebugFileSystemCacheEntry>("instance")) {
		return;
	}
	auto &config = DBConfig::GetConfig(db);
	config.file_system = make_uniq<DebugFileSystem>(std::move(config.file_system), db);
	auto &debug_fs = static_cast<DebugFileSystem &>(*config.file_system);
	cache.PutWithTypePrefix<DebugFileSystemCacheEntry>("instance",
	                                                   make_shared_ptr<DebugFileSystemCacheEntry>(debug_fs));
}

DebugFileSystem &GetDebugFileSystemOrThrow(DatabaseInstance &db) {
	auto entry = db.GetObjectCache().GetWithTypePrefix<DebugFileSystemCacheEntry>("instance");
	if (!entry) {
		throw InternalException("DebugFileSystem is not installed");
	}
	return entry->Get();
}

void OnSetDelayMeanMs(ClientContext &context, SetScope, Value &parameter) {
	auto delay_ms = parameter.GetValue<double>();
	if (delay_ms < 0) {
		throw InvalidInputException(
		    "Invalid option for debug_fs_delay_mean_ms: value must be greater than or equal to 0");
	}
	GetDebugFileSystemOrThrow(DatabaseInstance::GetDatabase(context)).SetDelayMeanMs(delay_ms);
}

void OnSetDelayStddevMs(ClientContext &context, SetScope, Value &parameter) {
	auto delay_ms = parameter.GetValue<double>();
	if (delay_ms < 0) {
		throw InvalidInputException(
		    "Invalid option for debug_fs_delay_stddev_ms: value must be greater than or equal to 0");
	}
	GetDebugFileSystemOrThrow(DatabaseInstance::GetDatabase(context)).SetDelayStddevMs(delay_ms);
}

void OnSetRandomSeed(ClientContext &context, SetScope, Value &parameter) {
	auto &debug_fs = GetDebugFileSystemOrThrow(DatabaseInstance::GetDatabase(context));
	if (parameter.IsNull()) {
		debug_fs.SetRandomSeed(optional_idx());
		return;
	}
	debug_fs.SetRandomSeed(parameter.GetValue<uint64_t>());
}

void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	EnsureDebugFileSystemInstalled(db);

	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("debug_fs_delay_mean_ms", "DEBUG SETTING: mean latency (ms) for filesystem operations",
	                          LogicalType::DOUBLE, Value(0.0), OnSetDelayMeanMs);
	config.AddExtensionOption("debug_fs_delay_stddev_ms",
	                          "DEBUG SETTING: standard deviation (ms) for filesystem operation latency",
	                          LogicalType::DOUBLE, Value(0.0), OnSetDelayStddevMs);
	// Random seed is tunable for reproducibility.
	config.AddExtensionOption("debug_fs_random_seed",
	                          "DEBUG SETTING: random seed for filesystem latency sampling; defaults to the current "
	                          "timestamp on the first delayed I/O operation",
	                          LogicalType::UBIGINT, Value(), OnSetRandomSeed);
}

} // namespace

void DebugFsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string DebugFsExtension::Name() {
	return "debug_fs";
}

std::string DebugFsExtension::Version() const {
#ifdef EXT_VERSION_DEBUG_FS
	return EXT_VERSION_DEBUG_FS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(debug_fs, loader) { // NOLINT
	duckdb::LoadInternal(loader);
}
}
