#include "duckdb/execution/operator/schema/physical_detach.hpp"
#include "duckdb/parser/parsed_data/detach_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalDetach::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &db_manager = DatabaseManager::Get(context.client);
	// DETACH is rejected only when the target database has a shared transaction in this
	// MetaTransaction — detaching unrelated databases while sharing main is allowed.
	//
	// Use the no-context GetDatabase overload so we do NOT call MetaTransaction::UseDatabase
	// as a side effect: that would pin the AttachedDatabase (and its StoredDatabasePath) alive
	// across the DETACH, preventing the file-path entry from being freed and breaking
	// subsequent ATTACHes of the same path within the same transaction.
	if (context.client.transaction.HasActiveTransaction()) {
		auto target_db = db_manager.GetDatabase(info->name);
		if (target_db) {
			auto &meta = MetaTransaction::Get(context.client);
			auto txn = meta.TryGetTransaction(*target_db);
			if (txn && txn->IsDuckTransaction()) {
				auto &duck = txn->Cast<DuckTransaction>();
				if (duck.IsShared() || duck.RollbackRequested()) {
					throw TransactionException(
					    "DETACH cannot be issued on database '%s' while it is part of a shared transaction",
					    info->name);
				}
			}
		}
	}
	db_manager.DetachDatabase(context.client, info->name, info->if_not_found);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
