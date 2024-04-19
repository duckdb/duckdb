#include "duckdb/main/extension_install_info.hpp"
#include "duckdb/common/string.hpp"

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

} // namespace duckdb
