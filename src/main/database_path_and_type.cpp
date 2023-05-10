#include "duckdb/main/database_path_and_type.hpp"

#include "duckdb/main/extension_helper.hpp"
#include "duckdb/storage/magic_bytes.hpp"
#include "duckdb/common/local_file_system.hpp"

namespace duckdb {

DBPathAndType DBPathAndType::Parse(const string &combined_path, const DBConfig &config) {
	if (config.file_system) {
		return Parse(combined_path, config, *config.file_system);
	} else {
		LocalFileSystem lfs;
		return Parse(combined_path, config, lfs);
	}
}

DBPathAndType DBPathAndType::Parse(const string &combined_path, const DBConfig &config, FileSystem &fs) {
	if (combined_path.empty()) {
		return {"", ""};
	}
	auto extension = ExtensionHelper::ExtractExtensionPrefixFromPath(combined_path);
	if (!extension.empty()) {
		// path is prefixed with an extension - remove it
		auto path = StringUtil::Replace(combined_path, extension + ":", "");
		auto type = ExtensionHelper::ApplyExtensionAlias(extension);
		return {path, type};
	}
	// if there isn't - check the magic bytes of the file (if any)
	auto file_type = MagicBytes::CheckMagicBytes(fs, combined_path);
	if (file_type == DataFileType::SQLITE_FILE) {
		return {combined_path, "sqlite"};
	}
	return {combined_path, string()};
}

} // namespace duckdb
