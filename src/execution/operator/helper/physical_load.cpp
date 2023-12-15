#include "duckdb/execution/operator/helper/physical_load.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

SourceResultType PhysicalLoad::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	if (info->load_type == LoadType::INSTALL || info->load_type == LoadType::FORCE_INSTALL) {
		ExtensionHelper::InstallExtension(context.client, info->filename, info->load_type == LoadType::FORCE_INSTALL,
		                                  info->repository);
	} else {
		ExtensionHelper::LoadExternalExtension(context.client, info->filename);
	}

	return SourceResultType::FINISHED;
}

} // namespace duckdb
