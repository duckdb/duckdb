#include "duckdb/execution/operator/helper/physical_load.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

static void InstallFromRepository(ClientContext &context, const LoadInfo &info) {
	ExtensionRepository repository;
	bool custom_repository = false;
	if (!info.repository.empty() && info.repo_is_alias) {
		auto repository_url = ExtensionRepository::TryGetRepositoryUrl(info.repository);
		if (repository_url.empty()) {
			// Not a built-in alias: resolve a registered extension_repository secret named like the alias.
			// Only this named form authorizes trusting the repository's pinned signing key.
			repository_url = ExtensionHelper::TryGetRepositoryUrlFromSecret(context, info.repository);
			if (repository_url.empty()) {
				// This has been checked during bind, so it should not fail here
				throw InternalException("The repository alias failed to resolve");
			}
			custom_repository = true;
		}
		repository = ExtensionRepository(info.repository, repository_url);
	} else if (!info.repository.empty()) {
		repository = ExtensionRepository::GetRepositoryByUrl(info.repository);
	}

	ExtensionInstallOptions options;
	options.force_install = info.load_type == LoadType::FORCE_INSTALL;
	options.throw_on_origin_mismatch = true;
	options.version = info.version;
	options.repository = repository;
	options.custom_repository = custom_repository;

	ExtensionHelper::InstallExtension(context, info.filename, options);
}

SourceResultType PhysicalLoad::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	if (info->load_type == LoadType::INSTALL || info->load_type == LoadType::FORCE_INSTALL) {
		if (info->repository.empty()) {
			ExtensionInstallOptions options;
			options.force_install = info->load_type == LoadType::FORCE_INSTALL;
			options.throw_on_origin_mismatch = true;
			options.version = info->version;
			ExtensionHelper::InstallExtension(context.client, info->filename, options);
		} else {
			InstallFromRepository(context.client, *info);
		}

	} else {
		ExtensionLoadOptions options;
		options.extension_name = info->filename;
		options.alias = info->alias;
		ExtensionHelper::LoadExternalExtension(context.client, options);
		// adds an explicitly set extension schema to the search path
		ExtensionLoader::RefreshSearchPath(context.client);
	}

	return SourceResultType::FINISHED;
}

} // namespace duckdb
