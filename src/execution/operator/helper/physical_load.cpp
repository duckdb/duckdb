#include "duckdb/execution/operator/helper/physical_load.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension_repository_manager.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

static void InstallFromRepository(ClientContext &context, const LoadInfo &info) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &fs = FileSystem::GetLocal(db);

	ExtensionRepository repository;
	// the repository can be a trusted repository that was added by the user, a built-in repository name or a url
	if (!ExtensionRepositoryManager::TryGetRepository(db, fs, info.repository, repository)) {
		if (info.repo_is_alias) {
			// This has been checked during bind, so it should not fail here
			if (!ExtensionRepository::TryGetKnownRepository(info.repository, repository)) {
				throw InternalException("The repository alias failed to resolve");
			}
		} else {
			repository = ExtensionRepository::GetRepositoryByUrl(info.repository);
		}
	}

	ExtensionInstallOptions options;
	options.force_install = info.load_type == LoadType::FORCE_INSTALL;
	options.throw_on_origin_mismatch = true;
	options.version = info.version;
	options.repository = repository;

	ExtensionHelper::InstallExtension(context, info.filename, options);
}

static void ExecuteRepositoryStatement(ClientContext &context, const LoadInfo &info, DataChunk &chunk) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &fs = FileSystem::GetLocal(db);

	if (info.load_type == LoadType::CREATE_REPOSITORY) {
		ExtensionRepository repository(info.repository, info.repository_url, info.public_key);
		auto result = ExtensionRepositoryManager::CreateRepository(db, fs, context, repository, info.on_conflict);

		// report the repository as it was stored, so that the key can be compared with the published fingerprint
		chunk.data[0].Append(Value(result.name));
		chunk.data[1].Append(Value(result.path));
		chunk.data[2].Append(Value(ExtensionRepositoryManager::GetPublicKeyFingerprint(result.public_key)));
	} else {
		auto on_entry_not_found = info.missing_ok ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
		ExtensionRepositoryManager::DropRepository(db, fs, info.repository, on_entry_not_found);
	}
}

SourceResultType PhysicalLoad::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	if (info->load_type == LoadType::CREATE_REPOSITORY || info->load_type == LoadType::DROP_REPOSITORY) {
		ExecuteRepositoryStatement(context.client, *info, chunk);
	} else if (info->load_type == LoadType::INSTALL || info->load_type == LoadType::FORCE_INSTALL) {
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
