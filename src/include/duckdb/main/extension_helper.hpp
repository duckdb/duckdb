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
#include "duckdb/main/extension_entries.hpp"

namespace duckdb {
class DuckDB;

enum class ExtensionLoadResult : uint8_t { LOADED_EXTENSION = 0, EXTENSION_UNKNOWN = 1, NOT_LOADED = 2 };

struct DefaultExtension {
	const char *name;
	const char *description;
	bool statically_loaded;
};

struct ExtensionAlias {
	const char *alias;
	const char *extension;
};

struct ExtensionInitResult {
	string filename;
	string filebase;

	void *lib_hdl;
};

class ExtensionHelper {
public:
	static void LoadAllExtensions(DuckDB &db);

	static ExtensionLoadResult LoadExtension(DuckDB &db, const std::string &extension);

	//! Install an extension
	static void InstallExtension(ClientContext &context, const string &extension, bool force_install,
	                             const string &respository = "");
	static void InstallExtension(DBConfig &config, FileSystem &fs, const string &extension, bool force_install,
	                             const string &respository = "");
	//! Load an extension
	static void LoadExternalExtension(ClientContext &context, const string &extension);
	static void LoadExternalExtension(DatabaseInstance &db, FileSystem &fs, const string &extension);

	//! Autoload an extension (depending on config, potentially a nop. Throws when installation fails)
	static void AutoLoadExtension(ClientContext &context, const string &extension_name);
	static void AutoLoadExtension(DatabaseInstance &db, const string &extension_name);

	//! Autoload an extension (depending on config, potentially a nop. Returns false on failure)
	DUCKDB_API static bool TryAutoLoadExtension(ClientContext &context, const string &extension_name) noexcept;

	//! Get the extension directory base on the current config
	static string ExtensionDirectory(ClientContext &context);
	static string ExtensionDirectory(DBConfig &config, FileSystem &fs);

	//! Get the extension url template, containing placeholders for version, platform and extension name
	static string ExtensionUrlTemplate(optional_ptr<const DBConfig> config, const string &repository);
	//! Return the extension url template with the variables replaced
	static string ExtensionFinalizeUrlTemplate(const string &url, const string &name);

	//! Default extensions are all extensions that DuckDB knows and expect to be available (both in-tree and
	//! out-of-tree)
	static idx_t DefaultExtensionCount();
	static DefaultExtension GetDefaultExtension(idx_t index);

	//! Extension can have aliases
	static idx_t ExtensionAliasCount();
	static ExtensionAlias GetExtensionAlias(idx_t index);

	//! Get public signing keys for extension signing
	static const vector<string> GetPublicKeys();

	// Returns extension name, or empty string if not a replacement open path
	static string ExtractExtensionPrefixFromPath(const string &path);

	//! Apply any known extension aliases, return the lowercase name
	static string ApplyExtensionAlias(const string &extension_name);

	static string GetExtensionName(const string &extension);
	static bool IsFullPath(const string &extension);

	//! Lookup a name + type in an ExtensionFunctionEntry list
	template <size_t N>
	static vector<pair<string, CatalogType>>
	FindExtensionInFunctionEntries(const string &name, const ExtensionFunctionEntry (&entries)[N]) {
		auto lcase = StringUtil::Lower(name);

		vector<pair<string, CatalogType>> result;
		for (idx_t i = 0; i < N; i++) {
			auto &element = entries[i];
			if (element.name == lcase) {
				result.push_back(make_pair(element.extension, element.type));
			}
		}
		return result;
	}

	//! Lookup a name in an ExtensionEntry list
	template <idx_t N>
	static string FindExtensionInEntries(const string &name, const ExtensionEntry (&entries)[N]) {
		auto lcase = StringUtil::Lower(name);

		auto it =
		    std::find_if(entries, entries + N, [&](const ExtensionEntry &element) { return element.name == lcase; });

		if (it != entries + N && it->name == lcase) {
			return it->extension;
		}
		return "";
	}

	//! Lookup a name in an extension entry and try to autoload it
	template <idx_t N>
	static void TryAutoloadFromEntry(DatabaseInstance &db, const string &entry, const ExtensionEntry (&entries)[N]) {
		auto &dbconfig = DBConfig::GetConfig(db);
#ifndef DUCKDB_DISABLE_EXTENSION_LOAD
		if (dbconfig.options.autoload_known_extensions) {
			auto extension_name = ExtensionHelper::FindExtensionInEntries(entry, entries);
			if (ExtensionHelper::CanAutoloadExtension(extension_name)) {
				ExtensionHelper::AutoLoadExtension(db, extension_name);
			}
		}
#endif
	}

	//! Whether an extension can be autoloaded (i.e. it's registered as an autoloadable extension in
	//! extension_entries.hpp)
	static bool CanAutoloadExtension(const string &ext_name);

	//! Utility functions for creating meaningful error messages regarding missing extensions
	static string WrapAutoLoadExtensionErrorMsg(ClientContext &context, const string &base_error,
	                                            const string &extension_name);
	static string AddExtensionInstallHintToErrorMsg(ClientContext &context, const string &base_error,
	                                                const string &extension_name);

	//! For tagged releases we use the tag, else we use the git commit hash
	static const string GetVersionDirectoryName();

private:
	static void InstallExtensionInternal(DBConfig &config, FileSystem &fs, const string &local_path,
	                                     const string &extension, bool force_install, const string &repository);
	static const vector<string> PathComponents();
	static string DefaultExtensionFolder(FileSystem &fs);
	static bool AllowAutoInstall(const string &extension);
	static ExtensionInitResult InitialLoad(DBConfig &config, FileSystem &fs, const string &extension);
	static bool TryInitialLoad(DBConfig &config, FileSystem &fs, const string &extension, ExtensionInitResult &result,
	                           string &error);
	//! Version tags occur with and without 'v', tag in extension path is always with 'v'
	static const string NormalizeVersionTag(const string &version_tag);
	static bool IsRelease(const string &version_tag);
	static bool CreateSuggestions(const string &extension_name, string &message);

private:
	static ExtensionLoadResult LoadExtensionInternal(DuckDB &db, const std::string &extension, bool initial_load);
};

} // namespace duckdb
