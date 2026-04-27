#include "duckdb/execution/operator/schema/physical_attach.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/main/database_path_and_type.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalAttach::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	// ATTACH inside a shared transaction would mutate the database set the meta sees,
	// asymmetrically (other participants and the owner would not see the new database).
	// Rejecting at the participant side keeps the shared transaction's database set
	// fixed at import time.
	if (context.client.transaction.HasActiveTransaction() && MetaTransaction::Get(context.client).IsShared()) {
		throw TransactionException(
		    "ATTACH cannot be issued inside a shared transaction; detach from the snapshot first");
	}
	// parse the options
	auto &config = DBConfig::GetConfig(context.client);
	// construct the options
	AttachOptions options(info->options, config.options.access_mode);

	// get the name and path of the database
	auto &name = info->name;
	auto &path = info->path;
	if (options.db_type.empty()) {
		DBPathAndType::ExtractExtensionPrefix(path, options.db_type);
	}
	if (name.empty()) {
		auto &fs = FileSystem::GetFileSystem(context.client);
		name = AttachedDatabase::ExtractDatabaseName(path, fs);
	}

	// check ATTACH IF NOT EXISTS
	auto &db_manager = DatabaseManager::Get(context.client);
	db_manager.AttachDatabase(context.client, *info, options);
	return SourceResultType::FINISHED;
}

} // namespace duckdb
