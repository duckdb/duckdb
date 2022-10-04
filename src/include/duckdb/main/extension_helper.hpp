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

struct ExtensionInitResult {
	string filename;
	string basename;

	void *lib_hdl;
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

	static unique_ptr<ReplacementOpenData> ReplacementOpenPre(const string &extension, DBConfig &config);
	static void ReplacementOpenPost(ClientContext &context, const string &extension, DatabaseInstance &instance,
	                                ReplacementOpenData *open_data);

private:
	static const vector<string> PathComponents();
	static ExtensionInitResult InitialLoad(DBConfig &context, FileOpener *opener, const string &extension);
	//! For tagged releases we use the tag, else we use the git commit hash
	static const string GetVersionDirectoryName();
	//! Version tags occur with and without 'v', tag in extension path is always with 'v'
	static const string NormalizeVersionTag(const string &version_tag);
	static bool IsRelease(const string &version_tag);

private:
	static ExtensionLoadResult LoadExtensionInternal(DuckDB &db, const std::string &extension, bool initial_load);
};

} // namespace duckdb
