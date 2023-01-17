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

//! The StorageExtensionInfo holds static information relevant to the storage extension
struct StorageExtensionInfo {
	DUCKDB_API virtual ~StorageExtensionInfo() {
	}
};

typedef unique_ptr<Catalog> (*attach_function_t)(AttachedDatabase &db, const string &name, AttachInfo &info,
                                                 AccessMode access_mode);

class StorageExtension {
public:
	attach_function_t attach;

	//! Additional info passed to the various storage functions
	shared_ptr<StorageExtensionInfo> storage_info;

	DUCKDB_API virtual ~StorageExtension() {
	}
};

} // namespace duckdb
