#include "duckdb/main/extension_install_info.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

namespace duckdb {

string ExtensionRepository::GetRepository(const string &repository_url) {
	auto resolved_repository = TryConvertUrlToKnownRepository(repository_url);
	if (resolved_repository.empty()) {
		return repository_url;
	}
	return resolved_repository;
}

string ExtensionRepository::TryGetRepositoryUrl(const string &repository) {
	if (repository == "core") {
		return CORE_REPOSITORY_URL;
	} else if (repository == "core_nightly") {
		return CORE_NIGHTLY_REPOSITORY_URL;
	} else if (repository == "community") {
		return COMMUNITY_REPOSITORY_URL;
	} else if (repository == "local_build_debug") {
		return BUILD_DEBUG_REPOSITORY_PATH;
	} else if (repository == "local_build_release") {
		return BUILD_RELEASE_REPOSITORY_PATH;
	}
	return "";
}

string ExtensionRepository::TryConvertUrlToKnownRepository(const string &url) {
	if (url == CORE_REPOSITORY_URL) {
		return "core";
	} else if (url == CORE_NIGHTLY_REPOSITORY_URL) {
		return "core_nightly";
	} else if (url == COMMUNITY_REPOSITORY_URL) {
		return "community";
	} else if (url == BUILD_DEBUG_REPOSITORY_PATH) {
		return "local_build_debug";
	} else if (url == BUILD_RELEASE_REPOSITORY_PATH) {
		return "local_build_release";
	}
	return "";
}

ExtensionRepository ExtensionRepository::GetDefaultRepository(optional_ptr<DBConfig> config) {
	if (config && !config->options.custom_extension_repo.empty()) {
		return ExtensionRepository("", config->options.custom_extension_repo);
	}

	return GetCoreRepository();
}
ExtensionRepository ExtensionRepository::GetDefaultRepository(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return GetDefaultRepository(config);
}

ExtensionRepository ExtensionRepository::GetCoreRepository() {
	return {"core", CORE_REPOSITORY_URL};
}

ExtensionRepository ExtensionRepository::GetRepositoryByUrl(const string &url) {
	if (url.empty()) {
		return GetCoreRepository();
	}

	auto repo_name = TryConvertUrlToKnownRepository(url);
	return {repo_name, url};
}

ExtensionRepository::ExtensionRepository() : name("core"), path(CORE_REPOSITORY_URL) {
}
ExtensionRepository::ExtensionRepository(const string &name_p, const string &path_p) : name(name_p), path(path_p) {
}

string ExtensionRepository::ToReadableString() {
	if (!name.empty()) {
		return name;
	}
	return path;
}

unique_ptr<ExtensionInstallInfo> ExtensionInstallInfo::TryReadInfoFile(FileSystem &fs,
                                                                       const std::string &info_file_path,
                                                                       const std::string &extension_name) {
	unique_ptr<ExtensionInstallInfo> result;

	string hint = StringUtil::Format("Try reinstalling the extension using 'FORCE INSTALL %s;'", extension_name);

	// Return empty info if the file is missing (TODO: throw error here in the future?)
	if (!fs.FileExists(info_file_path)) {
		return make_uniq<ExtensionInstallInfo>();
	}

	BufferedFileReader file_reader(fs, info_file_path.c_str());
	if (!file_reader.Finished()) {
		try {
			result = BinaryDeserializer::Deserialize<ExtensionInstallInfo>(file_reader);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			throw IOException(
			    "Failed to read info file for '%s' extension: '%s'.\nA serialization error occurred: '%s'\n%s",
			    extension_name, info_file_path, error.RawMessage(), hint);
		}
	}

	if (!result) {
		throw IOException("Failed to read info file for '%s' extension: '%s'.\nThe file appears to be empty!\n%s",
		                  extension_name, info_file_path, hint);
	}

	return result;
}

} // namespace duckdb
