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
		ExtensionRepository repository(info.repository, info.repository_url, info.public_keys);
		auto result = ExtensionRepositoryManager::CreateRepository(db, fs, context, repository, info.on_conflict);

		// report the repository as it was stored, so that the keys can be compared with the published fingerprints
		vector<Value> fingerprints;
		for (auto &public_key : result.public_keys) {
			fingerprints.push_back(Value(ExtensionRepositoryManager::GetPublicKeyFingerprint(public_key)));
		}
		chunk.data[0].Append(Value(result.name));
		chunk.data[1].Append(Value(result.path));
		chunk.data[2].Append(Value::LIST(LogicalType::VARCHAR, std::move(fingerprints)));
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

		// INSTALL AND LOAD: load the extension immediately after installing it. Only a named repository (core,
		// community or a user repository alias) is passed on to the load - a URL or bare install lands in the flat
		// top-level layout, which a bare load resolves
		if (info->load_after_install) {
			ExtensionLoadOptions load_options;
			load_options.extension_name = info->filename;
			load_options.repository = info->repo_is_alias ? info->repository : string();
			ExtensionHelper::LoadExternalExtension(context.client, load_options);
			ExtensionLoader::RefreshSearchPath(context.client);
		}

	} else {
		ExtensionLoadOptions options;
		options.extension_name = info->filename;
		options.alias = info->alias;
		options.repository = info->repository;
		ExtensionHelper::LoadExternalExtension(context.client, options);
		// adds an explicitly set extension schema to the search path
		ExtensionLoader::RefreshSearchPath(context.client);
	}

	return SourceResultType::FINISHED;
}

} // namespace duckdb
