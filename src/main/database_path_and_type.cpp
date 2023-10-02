#include "duckdb/main/database_path_and_type.hpp"

#include "duckdb/main/extension_helper.hpp"
#include "duckdb/storage/magic_bytes.hpp"

namespace duckdb {

DBPathAndType DBPathAndType::Parse(const string &combined_path, const DBConfig &config) {
	auto extension = ExtensionHelper::ExtractExtensionPrefixFromPath(combined_path);
	if (!extension.empty()) {
		// path is prefixed with an extension - remove it
		auto path = StringUtil::Replace(combined_path, extension + ":", "");
		auto type = ExtensionHelper::ApplyExtensionAlias(extension);
		return {path, type};
	}
	// if there isn't - check the magic bytes of the file (if any)
	auto file_type = MagicBytes::CheckMagicBytes(config.file_system.get(), combined_path);
	if (file_type == DataFileType::SQLITE_FILE) {
		return {combined_path, "sqlite"};
	}
	return {combined_path, string()};
}
} // namespace duckdb
