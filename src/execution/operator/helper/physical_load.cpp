#include "duckdb/execution/operator/helper/physical_load.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

static void InstallFromRepository(ClientContext &context, const LoadInfo &info) {
	ExtensionRepository repository;
	if (!info.repository.empty() && info.repo_is_alias) {
		auto repository_url = ExtensionRepository::TryGetRepositoryUrl(info.repository);
		// This has been checked during bind, so it should not fail here
		if (repository_url.empty()) {
			throw InternalException("The repository alias failed to resolve");
		}
		repository = ExtensionRepository(info.repository, repository_url);
	} else if (!info.repository.empty()) {
		repository = ExtensionRepository::GetRepositoryByUrl(info.repository);
	}

	ExtensionHelper::InstallExtension(context, info.filename, info.load_type == LoadType::FORCE_INSTALL, repository,
	                                  true, info.version);
}

SourceResultType PhysicalLoad::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	if (info->load_type == LoadType::INSTALL || info->load_type == LoadType::FORCE_INSTALL) {
		if (info->repository.empty()) {
			ExtensionHelper::InstallExtension(context.client, info->filename,
			                                  info->load_type == LoadType::FORCE_INSTALL, nullptr, true, info->version);
		} else {
			InstallFromRepository(context.client, *info);
		}

	} else {
		ExtensionHelper::LoadExternalExtension(context.client, info->filename);
	}

	return SourceResultType::FINISHED;
}

} // namespace duckdb
