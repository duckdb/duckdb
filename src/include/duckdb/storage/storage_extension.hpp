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

typedef unique_ptr<Catalog> (*attach_function_t)(StorageExtensionInfo *storage_info, AttachedDatabase &db,
                                                 const string &name, AttachInfo &info, AccessMode access_mode);
typedef unique_ptr<TransactionManager> (*create_transaction_manager_t)(StorageExtensionInfo *storage_info,
                                                                       AttachedDatabase &db, Catalog &catalog);

class StorageExtension {
public:
	attach_function_t attach;
	create_transaction_manager_t create_transaction_manager;

	//! Additional info passed to the various storage functions
	shared_ptr<StorageExtensionInfo> storage_info;

	DUCKDB_API virtual ~StorageExtension() {
	}
};

} // namespace duckdb
