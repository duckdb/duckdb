#include "duckdb/main/extension_install_info.hpp"

namespace duckdb {

constexpr const ExtensionRepository::RepositoryEntry ExtensionRepository::REPOSITORIES[];

string ExtensionRepository::TryGetRepositoryUrl(const string &repository) {
	for (const auto& val : REPOSITORIES) {
		if (val.name == repository) {
			return val.url;
		}
	}
	return "";
}

string ExtensionRepository::TryConvertUrlToKnownRepository(const string &url) {
	for (const auto& val : REPOSITORIES) {
		if (val.url == url) {
			return val.name;
		}
	}
	return "";
}

} // namespace duckdb
