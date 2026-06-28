#include "debug_fs_extension.hpp"

#include "debug_file_system.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

namespace {

void EnsureDebugFileSystemInstalled(DatabaseInstance &db) {
	auto &config = DBConfig::GetConfig(db);
	if (!dynamic_cast<DebugFileSystem *>(config.file_system.get())) {
		config.file_system = make_uniq<DebugFileSystem>(std::move(config.file_system));
	}
}

void OnSetDelayMeanMs(ClientContext &context, SetScope, Value &parameter) {
	auto delay_ms = parameter.GetValue<double>();
	if (delay_ms < 0) {
		throw InvalidInputException("Invalid option for debug_fs_delay_mean_ms: value must be greater than or equal to 0");
	}
	auto &db = DatabaseInstance::GetDatabase(context);
	if (delay_ms > 0) {
		EnsureDebugFileSystemInstalled(db);
	}
	auto *debug_fs = dynamic_cast<DebugFileSystem *>(DBConfig::GetConfig(db).file_system.get());
	if (debug_fs) {
		debug_fs->SetDelayMeanMs(delay_ms);
	}
}

void OnSetDelayStddevMs(ClientContext &context, SetScope, Value &parameter) {
	auto delay_ms = parameter.GetValue<double>();
	if (delay_ms < 0) {
		throw InvalidInputException("Invalid option for debug_fs_delay_stddev_ms: value must be greater than or equal to 0");
	}
	auto &db = DatabaseInstance::GetDatabase(context);
	if (delay_ms > 0) {
		EnsureDebugFileSystemInstalled(db);
	}
	auto *debug_fs = dynamic_cast<DebugFileSystem *>(DBConfig::GetConfig(db).file_system.get());
	if (debug_fs) {
		debug_fs->SetDelayStddevMs(delay_ms);
	}
}

void LoadInternal(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());

	config.AddExtensionOption("debug_fs_delay_mean_ms", "DEBUG SETTING: mean latency (ms) for filesystem operations",
	                          LogicalType::DOUBLE, Value(0.0), OnSetDelayMeanMs);
	config.AddExtensionOption("debug_fs_delay_stddev_ms",
	                          "DEBUG SETTING: standard deviation (ms) for filesystem operation latency",
	                          LogicalType::DOUBLE, Value(0.0), OnSetDelayStddevMs);
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
