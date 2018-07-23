
#pragma once

#include <vector>

#include "common/internal_types.hpp"
#include "execution/vector/vector.hpp"
#include "catalog/table_catalog.hpp"


namespace duckdb {
class DataTable;

class DataColumn {
  public:
  	DataColumn(DataTable& table, ColumnCatalogEntry& column) : table(table), column(column) {}

  	void AddData(Vector& data);
  	DataTable& table;
  	ColumnCatalogEntry& column;
  	std::vector<std::unique_ptr<Vector>> data;
};
}
