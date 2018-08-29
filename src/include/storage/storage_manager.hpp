//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// storage/storage_manager.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/helper.hpp"

#include "storage/data_table.hpp"

namespace duckdb {

//! StorageManager is responsible for managing the physical storage of the
//! database on disk
class StorageManager {
  public:
	// //! Create a new table from a catalog entry
	// void CreateTable(TableCatalogEntry &table);

	// //! Create a new table from a catalog entry
	// void DropTable(TableCatalogEntry &table);

	//! The set of tables managed by the storage engine
	// std::vector<std::unique_ptr<DataTable>> tables;
};

} // namespace duckdb
