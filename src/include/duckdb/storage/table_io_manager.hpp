//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table_io_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
class BlockManager;
class DataTable;
class MetadataManager;

class TableIOManager {
public:
	virtual ~TableIOManager() {
	}

	//! Obtains a reference to the TableIOManager of a specific table
	static TableIOManager &Get(DataTable &table);

	//! The block manager used for managing index data
	virtual BlockManager &GetIndexBlockManager() = 0;

	//! The block manager used for storing row group data
	virtual BlockManager &GetBlockManagerForRowData() = 0;

	virtual MetadataManager &GetMetadataManager() = 0;

	//! Returns the target row group size for the table
	virtual idx_t GetRowGroupSize() const = 0;
};

} // namespace duckdb
