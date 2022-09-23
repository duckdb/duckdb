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

struct DefaultExtension {
	const char *name;
	const char *description;
	bool statically_loaded;
};

class ExtensionHelper {
public:
	static void LoadAllExtensions(DuckDB &db);

	static ExtensionLoadResult LoadExtension(DuckDB &db, const std::string &extension);

	static void InstallExtension(ClientContext &context, const string &extension, bool force_install);
	static void LoadExternalExtension(ClientContext &context, const string &extension);

	static string ExtensionDirectory(ClientContext &context);

	static idx_t DefaultExtensionCount();
	static DefaultExtension GetDefaultExtension(idx_t index);

	static const vector<string> GetPublicKeys();

private:
	static const vector<string> PathComponents();
	//! For tagged releases we use the tag, else we use the git commit hash
	static const string GetVersionDirectoryName();
	//! Version tags occur with and without 'v', tag in extension path is always with 'v'
	static const string NormalizeVersionTag(const string &version_tag);
	static bool IsRelease(const string &version_tag);

private:
	static ExtensionLoadResult LoadExtensionInternal(DuckDB &db, const std::string &extension, bool initial_load);
};

} // namespace duckdb
