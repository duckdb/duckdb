//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// storage/data_column.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "catalog/table_catalog.hpp"
#include "common/internal_types.hpp"
#include "common/types/statistics.hpp"
#include "common/types/vector.hpp"

namespace duckdb {
class DataTable;

//! DataColumn represents a physical column on disk
class DataColumn {
  public:
	DataColumn(DataTable &table, ColumnDefinition &column)
	    : table(table), column(column), stats(column.type) {}

	void AddData(Vector &data);

	//! The physical table that this column belongs to
	DataTable &table;
	//! A reference to the column in the catalog
	ColumnDefinition &column;
	//! The actual data of the column
	std::vector<std::unique_ptr<Vector>> data;
	//! Statistics about the column
	Statistics stats;
};
} // namespace duckdb
