#include "duckdb/execution/operator/schema/physical_attach.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/operator/helper/launch_external_resource.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/external_resources_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/main/database_path_and_type.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalAttach::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &config = DBConfig::GetConfig(context.client);

	// Attach mutates the info (name resolution, path prefix stripping, injected provision options), so
	// work on a copy: the plan-owned info must stay pristine for a re-executed prepared statement.
	auto attach_info = info->Copy();

	// `ATTACH TO [NEW] EXTERNAL RESOURCE`: get the resource's endpoint, then attach under this alias.
	// NEW PROVISIONS a fresh resource this attachment OWNS (deleter bound, DETACH tears it down). A bare
	// identifier REFERENCES a registered resource this attachment only BORROWS (no deleter; DETACH leaves
	// it registered for DESTROY).
	LaunchedResource launched;
	string resource_type, resource_name;
	bool owns_resource = false;
	if (attach_info->external_resource) {
		auto &external_resource = *attach_info->external_resource;
		if (!external_resource.reference_name.empty()) {
			auto instance = ExternalResourcesManager::Get(context.client).Lookup(external_resource.reference_name);
			if (!instance) {
				throw InvalidInputException("no external resource named \"%s\" is registered",
				                            external_resource.reference_name);
			}
			// Resolve the current endpoint from the registered handle (status); bind no deleter.
			launched = ProvisionExternalResource(context.client, instance->type, {}, instance->name, instance->handle);
		} else {
			resource_type = external_resource.provider;
			resource_name = external_resource.alias.GetIdentifierName();
			launched = ProvisionExternalResource(context.client, external_resource.provider, external_resource.params,
			                                     resource_name);
			owns_resource = true;
		}
		ApplyLaunchedResource(launched, *attach_info);
	}

	// construct the options
	AttachOptions options(attach_info->options, config.options.access_mode);
	if (owns_resource) {
		options.deleter_function = launched.deleter_function;
		options.deleter_payload = launched.deleter_payload;
		options.deleter_resource_type = resource_type;
		options.deleter_resource_name = resource_name;
	}

	// get the name and path of the database
	auto &name = attach_info->name;
	auto &path = attach_info->path;
	// preserve the verbatim path before extension-prefix stripping
	options.original_path = path;
	if (options.db_type.empty()) {
		DBPathAndType::ExtractExtensionPrefix(path, options.db_type);
	}
	if (name.empty()) {
		auto &fs = FileSystem::GetFileSystem(context.client);
		name = Identifier(AttachedDatabase::ExtractDatabaseName(path, fs));
	}

	// check ATTACH IF NOT EXISTS
	auto &db_manager = DatabaseManager::Get(context.client);
	try {
		db_manager.AttachDatabase(context.client, *attach_info, options);
	} catch (...) {
		// Compensating teardown (best-effort): a failed attach of a resource we just PROVISIONED leaves
		// nothing owning it. A borrowed (referenced) resource is never torn down here.
		if (owns_resource) {
			ResourceDeleter(DatabaseInstance::GetDatabase(context.client), launched.deleter_function,
			                launched.deleter_payload, resource_type, resource_name)
			    .TryDelete();
		}
		throw;
	}
	return SourceResultType::FINISHED;
}

} // namespace duckdb
