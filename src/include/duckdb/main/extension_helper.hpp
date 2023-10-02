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
	string basename;

	void *lib_hdl;
};

class ExtensionHelper {
public:
	static void LoadAllExtensions(DuckDB &db);

	static ExtensionLoadResult LoadExtension(DuckDB &db, const std::string &extension);

	static void InstallExtension(ClientContext &context, const string &extension, bool force_install,
	                             const string &respository = "");
	static void InstallExtension(DBConfig &config, FileSystem &fs, const string &extension, bool force_install,
	                             const string &respository = "");
	static void LoadExternalExtension(ClientContext &context, const string &extension);
	static void LoadExternalExtension(DatabaseInstance &db, FileSystem &fs, const string &extension,
	                                  optional_ptr<const ClientConfig> client_config);

	//! Autoload an extension by name. Depending on the current settings, this will either load or install+load
	static void AutoLoadExtension(ClientContext &context, const string &extension_name);
	DUCKDB_API static bool TryAutoLoadExtension(ClientContext &context, const string &extension_name) noexcept;

	static string ExtensionDirectory(ClientContext &context);
	static string ExtensionDirectory(DBConfig &config, FileSystem &fs);
	static string ExtensionUrlTemplate(optional_ptr<const ClientConfig> config, const string &repository);
	static string ExtensionFinalizeUrlTemplate(const string &url, const string &name);

	static idx_t DefaultExtensionCount();
	static DefaultExtension GetDefaultExtension(idx_t index);

	static idx_t ExtensionAliasCount();
	static ExtensionAlias GetExtensionAlias(idx_t index);

	static const vector<string> GetPublicKeys();

	// Returns extension name, or empty string if not a replacement open path
	static string ExtractExtensionPrefixFromPath(const string &path);

	//! Apply any known extension aliases
	static string ApplyExtensionAlias(string extension_name);

	static string GetExtensionName(const string &extension);
	static bool IsFullPath(const string &extension);

	//! Lookup a name in an ExtensionEntry list
	template <size_t N>
	static string FindExtensionInEntries(const string &name, const ExtensionEntry (&entries)[N]) {
		auto lcase = StringUtil::Lower(name);

		auto it =
		    std::find_if(entries, entries + N, [&](const ExtensionEntry &element) { return element.name == lcase; });

		if (it != entries + N && it->name == lcase) {
			return it->extension;
		}
		return "";
	}

	//! Whether an extension can be autoloaded (i.e. it's registered as an autoloadable extension in
	//! extension_entries.hpp)
	static bool CanAutoloadExtension(const string &ext_name);

	//! Utility functions for creating meaningful error messages regarding missing extensions
	static string WrapAutoLoadExtensionErrorMsg(ClientContext &context, const string &base_error,
	                                            const string &extension_name);
	static string AddExtensionInstallHintToErrorMsg(ClientContext &context, const string &base_error,
	                                                const string &extension_name);

private:
	static void InstallExtensionInternal(DBConfig &config, ClientConfig *client_config, FileSystem &fs,
	                                     const string &local_path, const string &extension, bool force_install,
	                                     const string &repository);
	static const vector<string> PathComponents();
	static bool AllowAutoInstall(const string &extension);
	static ExtensionInitResult InitialLoad(DBConfig &config, FileSystem &fs, const string &extension,
	                                       optional_ptr<const ClientConfig> client_config);
	static bool TryInitialLoad(DBConfig &config, FileSystem &fs, const string &extension, ExtensionInitResult &result,
	                           string &error, optional_ptr<const ClientConfig> client_config);
	//! For tagged releases we use the tag, else we use the git commit hash
	static const string GetVersionDirectoryName();
	//! Version tags occur with and without 'v', tag in extension path is always with 'v'
	static const string NormalizeVersionTag(const string &version_tag);
	static bool IsRelease(const string &version_tag);
	static bool CreateSuggestions(const string &extension_name, string &message);

private:
	static ExtensionLoadResult LoadExtensionInternal(DuckDB &db, const std::string &extension, bool initial_load);
};

} // namespace duckdb
