#include "duckdb/execution/operator/schema/physical_detach.hpp"
#include "duckdb/parser/parsed_data/detach_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalDetach::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	if (context.client.transaction.HasActiveTransaction() && MetaTransaction::Get(context.client).IsShared()) {
		throw TransactionException(
		    "DETACH cannot be issued inside a shared transaction; detach from the snapshot first");
	}
	auto &db_manager = DatabaseManager::Get(context.client);
	db_manager.DetachDatabase(context.client, info->name, info->if_not_found);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
