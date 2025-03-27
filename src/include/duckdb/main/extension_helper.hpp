//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension_entries.hpp"
#include "duckdb/main/extension_install_info.hpp"

#include <string>

namespace duckdb {

class DuckDB;
class HTTPLogger;

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
	ExtensionABIType abi_type = ExtensionABIType::UNKNOWN;

	// The deserialized install from the `<ext>.duckdb_extension.info` file
	unique_ptr<ExtensionInstallInfo> install_info;

	void *lib_hdl;
};

// Tags describe what happened during the updating process
enum class ExtensionUpdateResultTag : uint8_t {
	// Fallback for when installation information is missing
	UNKNOWN = 0,

	// Either a fresh file was downloaded and versions are identical
	NO_UPDATE_AVAILABLE = 1,
	// Only extensions from repositories can be updated
	NOT_A_REPOSITORY = 2,
	// Only known, currently installed extensions can be updated
	NOT_INSTALLED = 3,
	// Statically loaded extensions can not be updated; they are baked into the DuckDB executable
	STATICALLY_LOADED = 4,
	// This means the .info file written during installation was missing or malformed
	MISSING_INSTALL_INFO = 5,

	// The extension was re-downloaded from the repository, but due to a lack of version information
	// its impossible to tell if the extension is actually updated
	REDOWNLOADED = 254,
	// The version was updated to a new version
	UPDATED = 255,
};

struct ExtensionUpdateResult {
	ExtensionUpdateResultTag tag = ExtensionUpdateResultTag::UNKNOWN;

	string extension_name;
	string repository;

	string extension_version;
	string prev_version;
	string installed_version;
};

struct ExtensionInstallOptions {
	//! Install from a different repository that the default one
	optional_ptr<ExtensionRepository> repository;
	//! Install a specific version of the extension
	string version;

	//! Overwrite existing installation
	bool force_install = false;
	//! Use etags to avoid downloading unchanged extension files
	bool use_etags = false;
	//! Throw an error when installing an extension with a different origin than the one that is installed
	bool throw_on_origin_mismatch = false;
};

class ExtensionHelper {
public:
	static void LoadAllExtensions(DuckDB &db);

	static ExtensionLoadResult LoadExtension(DuckDB &db, const std::string &extension);

	//! Install an extension
	static unique_ptr<ExtensionInstallInfo> InstallExtension(ClientContext &context, const string &extension,
	                                                         ExtensionInstallOptions &options);
	static unique_ptr<ExtensionInstallInfo> InstallExtension(DatabaseInstance &db, FileSystem &fs,
	                                                         const string &extension, ExtensionInstallOptions &options);
	//! Load an extension
	static void LoadExternalExtension(ClientContext &context, const string &extension);
	static void LoadExternalExtension(DatabaseInstance &db, FileSystem &fs, const string &extension);

	//! Autoload an extension (depending on config, potentially a nop. Throws when installation fails)
	static void AutoLoadExtension(ClientContext &context, const string &extension_name);
	static void AutoLoadExtension(DatabaseInstance &db, const string &extension_name);

	//! Autoload an extension (depending on config, potentially a nop. Returns false on failure)
	DUCKDB_API static bool TryAutoLoadExtension(DatabaseInstance &db, const string &extension_name) noexcept;
	DUCKDB_API static bool TryAutoLoadExtension(ClientContext &context, const string &extension_name) noexcept;

	//! Update all extensions, return a vector of extension names that were updated;
	static vector<ExtensionUpdateResult> UpdateExtensions(ClientContext &context);
	//! Update a specific extension
	static ExtensionUpdateResult UpdateExtension(ClientContext &context, const string &extension_name);

	//! Get the extension directory base on the current config
	static string ExtensionDirectory(ClientContext &context);
	static string ExtensionDirectory(DatabaseInstance &db, FileSystem &fs);

	// Get the extension directory path
	static string GetExtensionDirectoryPath(ClientContext &context);
	static string GetExtensionDirectoryPath(DatabaseInstance &db, FileSystem &fs);

	static bool CheckExtensionSignature(FileHandle &handle, ParsedExtensionMetaData &parsed_metadata,
	                                    const bool allow_community_extensions);
	static ParsedExtensionMetaData ParseExtensionMetaData(const char *metadata) noexcept;
	static ParsedExtensionMetaData ParseExtensionMetaData(FileHandle &handle);

	//! Get the extension url template, containing placeholders for version, platform and extension name
	static string ExtensionUrlTemplate(optional_ptr<const DatabaseInstance> db, const ExtensionRepository &repository,
	                                   const string &version);
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
	static const vector<string> GetPublicKeys(bool allow_community_extension = false);

	// Returns extension name, or empty string if not a replacement open path
	static string ExtractExtensionPrefixFromPath(const string &path);

	// Returns the user-readable name of a repository URL
	static string GetRepositoryName(const string &repository_base_url);

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

	template <idx_t N>
	static idx_t ArraySize(const ExtensionEntry (&entries)[N]) {
		return N;
	}

	template <idx_t N>
	static const ExtensionEntry *GetArrayEntry(const ExtensionEntry (&entries)[N], idx_t entry) {
		if (entry >= N) {
			return nullptr;
		}
		return entries + entry;
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
	static string AddExtensionInstallHintToErrorMsg(DatabaseInstance &db, const string &base_error,
	                                                const string &extension_name);

	//! For tagged releases we use the tag, else we use the git commit hash
	static const string GetVersionDirectoryName();

	static bool IsRelease(const string &version_tag);
	static bool CreateSuggestions(const string &extension_name, string &message);
	static string ExtensionInstallDocumentationLink(const string &extension_name);

private:
	static unique_ptr<ExtensionInstallInfo> InstallExtensionInternal(DatabaseInstance &db, FileSystem &fs,
	                                                                 const string &local_path, const string &extension,
	                                                                 ExtensionInstallOptions &options,
	                                                                 optional_ptr<HTTPLogger> http_logger = nullptr,
	                                                                 optional_ptr<ClientContext> context = nullptr);
	static const vector<string> PathComponents();
	static string DefaultExtensionFolder(FileSystem &fs);
	static bool AllowAutoInstall(const string &extension);
	static ExtensionInitResult InitialLoad(DatabaseInstance &db, FileSystem &fs, const string &extension);
	static bool TryInitialLoad(DatabaseInstance &db, FileSystem &fs, const string &extension,
	                           ExtensionInitResult &result, string &error);
	//! Version tags occur with and without 'v', tag in extension path is always with 'v'
	static const string NormalizeVersionTag(const string &version_tag);

private:
	static ExtensionLoadResult LoadExtensionInternal(DuckDB &db, const std::string &extension, bool initial_load);
};

} // namespace duckdb
