
#pragma once

#include <vector>

#include "catalog/table_catalog.hpp"
#include "common/internal_types.hpp"
#include "common/types/vector.hpp"

namespace duckdb {
class DataTable;

class DataColumn {
  public:
	DataColumn(DataTable &table, ColumnCatalogEntry &column)
	    : table(table), column(column) {}

	void AddData(Vector &data);

	DataTable &table;
	ColumnCatalogEntry &column;
	std::vector<std::unique_ptr<Vector>> data;
};
} // namespace duckdb
