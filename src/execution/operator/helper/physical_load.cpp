#include "duckdb/execution/operator/helper/physical_load.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

void PhysicalLoad::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                           LocalSourceState &lstate) const {
	auto &db = DatabaseInstance::GetDatabase(context.client);
	if (info->load_type == LoadType::INSTALL || info->load_type == LoadType::FORCE_INSTALL) {
		ExtensionHelper::InstallExtension(db, info->filename, info->load_type == LoadType::FORCE_INSTALL);
	} else {
		ExtensionHelper::LoadExternalExtension(db, info->filename);
	}
}

} // namespace duckdb
