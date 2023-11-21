#include "duckdb/main/database_path_and_type.hpp"

#include "duckdb/main/extension_helper.hpp"
#include "duckdb/storage/magic_bytes.hpp"

namespace duckdb {

void DBPathAndType::ExtractExtensionPrefix(string &path, string &db_type) {
	auto extension = ExtensionHelper::ExtractExtensionPrefixFromPath(path);
	if (!extension.empty()) {
		// path is prefixed with an extension - remove it
		path = StringUtil::Replace(path, extension + ":", "");
		db_type = ExtensionHelper::ApplyExtensionAlias(extension);
	}
}

void DBPathAndType::CheckMagicBytes(string &path, string &db_type, const DBConfig &config) {
	// if there isn't - check the magic bytes of the file (if any)
	auto file_type = MagicBytes::CheckMagicBytes(config.file_system.get(), path);
	if (file_type == DataFileType::SQLITE_FILE) {
		db_type = "sqlite";
	} else {
		db_type = "";
	}
}

} // namespace duckdb
