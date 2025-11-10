#include "duckdb/main/database_path_and_type.hpp"

#include "duckdb/main/extension_helper.hpp"
#include "duckdb/storage/magic_bytes.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void DBPathAndType::ExtractExtensionPrefix(string &path, string &db_type) {
	auto extension = ExtensionHelper::ExtractExtensionPrefixFromPath(path);
	if (!extension.empty()) {
		// path is prefixed with an extension - remove the first occurence of it
		path = path.substr(extension.length() + 1);
		db_type = ExtensionHelper::ApplyExtensionAlias(extension);
	}
}

void DBPathAndType::CheckMagicBytes(QueryContext context, FileSystem &fs, string &path, string &db_type) {
	// if there isn't - check the magic bytes of the file (if any)
	auto file_type = MagicBytes::CheckMagicBytes(context, fs, path);
	db_type = string();
	switch (file_type) {
	case DataFileType::SQLITE_FILE:
		db_type = "sqlite";
		break;
	case DataFileType::PARQUET_FILE:
	case DataFileType::UNKNOWN_FILE: {
		// FIXME: we should get this from the registered replacement scans instead of hardcoding it here
		vector<string> supported_suffixes {"parquet", "csv", "tsv", "json", "jsonl", "ndjson"};
		if (ReplacementScan::CanReplace(path, supported_suffixes)) {
			db_type = "__open_file__";
			break;
		}
		break;
	}
	default:
		break;
	}
}

void DBPathAndType::ResolveDatabaseType(FileSystem &fs, string &path, string &db_type) {
	if (!db_type.empty()) {
		// database type specified explicitly - no need to check
		return;
	}
	// check for an extension prefix
	ExtractExtensionPrefix(path, db_type);
	if (!db_type.empty()) {
		// extension prefix was provided (e.g. sqlite:/path/to/file.db) - we are done
		return;
	}
	// check database type by reading the magic bytes of a file
	DBPathAndType::CheckMagicBytes(QueryContext(), fs, path, db_type);
}

} // namespace duckdb
