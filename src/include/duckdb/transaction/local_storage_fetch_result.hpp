//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/local_storage_fetch_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

class LocalTableManager;
class LocalTableStorage;

struct LocalTableStorageFetchResult {
	//! Whether the storage was created in the fetch
	bool created;
	// The storage we fetched
	LocalTableStorage *storage;
	// The manager that owns the storage
	LocalTableManager *manager;
};

} // namespace duckdb
