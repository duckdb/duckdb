//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension_install_info.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/common/types.hpp"
#include "duckdb/main/config.hpp"

#pragma once

namespace duckdb {
class FileSystem;

enum class ExtensionInstallMode : uint8_t {
	// Fallback for when install info is missing
	UNKNOWN = 0,
	//! Extension was installed using a url deduced from a repository base url
	REPOSITORY = 1,
	//! Extension was install from a custom path, this could be either local or remote
	CUSTOM_PATH = 2,
	//! Extension was statically linked
	STATICALLY_LINKED = 3,
	//! Extension is not installed, for example the extension might be directly loaded without installing
	NOT_INSTALLED = 4
};

struct ExtensionLoadedInfo {
	string description;
};

class ExtensionInstallInfo {
public:
	//! How the extension was installed
	ExtensionInstallMode mode = ExtensionInstallMode::UNKNOWN;
	//! (optional) Full path where the extension came from
	string full_path;
	//! (optional) Repository url where the extension came from
	string repository_url;
	//! (optional) Version of the extension
	string version;
	//! (optional) ETag of last fetched resource
	string etag;

	void Serialize(Serializer &serializer) const;

	//! Try to read install info. returns ExtensionInstallMode::UNKNOWN on missing file, and throws on corrupt file
	static unique_ptr<ExtensionInstallInfo> TryReadInfoFile(FileSystem &fs, const string &info_file_path,
	                                                        const string &extension_name);

	static unique_ptr<ExtensionInstallInfo> Deserialize(Deserializer &deserializer);
};

struct ExtensionRepository {
	//! All currently available repositories
	static constexpr const char *CORE_REPOSITORY_URL = "http://extensions.duckdb.org";
	static constexpr const char *CORE_NIGHTLY_REPOSITORY_URL = "http://nightly-extensions.duckdb.org";
	static constexpr const char *COMMUNITY_REPOSITORY_URL = "http://community-extensions.duckdb.org";

	//! Debugging repositories (target local, relative paths that are produced by DuckDB's build system)
	static constexpr const char *BUILD_DEBUG_REPOSITORY_PATH = "./build/debug/repository";
	static constexpr const char *BUILD_RELEASE_REPOSITORY_PATH = "./build/release/repository";

	//! The default is CORE
	static constexpr const char *DEFAULT_REPOSITORY_URL = CORE_REPOSITORY_URL;

	//! Returns the repository name is this is a known repository, or the full url if it is not
	static string GetRepository(const string &repository_url);
	//! Try to convert a repository to a url, will return empty string if the repository is unknown
	static string TryGetRepositoryUrl(const string &repository);
	//! Try to convert a url to a known repository name, will return empty string if the repository is unknown
	static string TryConvertUrlToKnownRepository(const string &url);

	//! Get the default repository, optionally passing a config to allow
	static ExtensionRepository GetDefaultRepository(optional_ptr<DBConfig> config);
	static ExtensionRepository GetDefaultRepository(ClientContext &context);

	static ExtensionRepository GetCoreRepository();
	static ExtensionRepository GetRepositoryByUrl(const string &url);

	ExtensionRepository();
	ExtensionRepository(const string &name, const string &url);

	//! Print the name if it has one, or the full path if not
	string ToReadableString();

	//! Repository name
	string name;
	//! Repository path/url
	string path;
};

} // namespace duckdb
