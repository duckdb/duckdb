#include "duckdb/execution/operator/schema/physical_detach.hpp"
#include "duckdb/parser/parsed_data/detach_info.hpp"
#include "duckdb/main/database_manager.hpp"

namespace duckdb {

// DETACH always drops the catalog name. If this transaction already used the database,
// MetaTransaction keeps the AttachedDatabase alive (used_databases / referenced_databases)
// until commit/rollback, and UseDatabase rejects re-ATTACH of the same alias.
SourceResultType PhysicalDetach::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &db_manager = DatabaseManager::Get(context.client);
	db_manager.DetachDatabase(context.client, info->name, info->if_not_found);
	return SourceResultType::FINISHED;
}

} // namespace duckdb
