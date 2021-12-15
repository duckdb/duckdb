//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include "duckdb.hpp"

namespace duckdb {
class DuckDB;

enum class ExtensionLoadResult : uint8_t { LOADED_EXTENSION = 0, EXTENSION_UNKNOWN = 1, NOT_LOADED = 2 };

class ExtensionHelper {
public:
	static void LoadAllExtensions(DuckDB &db);

	static ExtensionLoadResult LoadExtension(DuckDB &db, const std::string &extension);

	static void InstallExtension(DatabaseInstance &db, const string &extension, bool force_install);
	static void LoadExternalExtension(DatabaseInstance &db, const string &extension);

private:
	static const vector<string> PathComponents();

private:
	static ExtensionLoadResult LoadExtensionInternal(DuckDB &db, const std::string &extension, bool initial_load);
};

} // namespace duckdb
