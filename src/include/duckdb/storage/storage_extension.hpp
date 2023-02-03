//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/storage_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {
class AttachedDatabase;
struct AttachInfo;
class Catalog;
class TransactionManager;

//! The StorageExtensionInfo holds static information relevant to the storage extension
struct StorageExtensionInfo {
	DUCKDB_API virtual ~StorageExtensionInfo() {
	}
};

typedef unique_ptr<Catalog> (*attach_function_t)(AttachedDatabase &db, const string &name, AttachInfo &info,
                                                 AccessMode access_mode);
typedef unique_ptr<TransactionManager> (*create_transaction_manager_t)(AttachedDatabase &db, Catalog &catalog);
typedef unique_ptr<TableFunctionRef> (*create_database_t)(ClientContext &context, const string &database_name,
                                                          const string &source_path, StorageExtensionInfo *info);
typedef unique_ptr<TableFunctionRef> (*drop_database_t)(ClientContext &context, const string &database_name,
                                                        StorageExtensionInfo *info);

class StorageExtension {
public:
	attach_function_t attach;
	create_transaction_manager_t create_transaction_manager;
	create_database_t create_database;
	drop_database_t drop_database;

	//! Additional info passed to the various storage functions
	shared_ptr<StorageExtensionInfo> storage_info;

	DUCKDB_API virtual ~StorageExtension() {
	}
};

} // namespace duckdb
