//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension_install_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

enum class ExtensionInstallMode : uint8_t {
	// Fallback for exceptional cases
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

	void Serialize(Serializer &serializer) const;
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
};

} // namespace duckdb
