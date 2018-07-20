
#pragma once

#include <vector>

#include "storage/data_column.hpp"

namespace duckdb {
class DataTable {
  public:
  	DataTable() {}

  	void AddColumn(TypeId type);

  	size_t size;

  	std::vector<std::unique_ptr<DataColumn>> columns;
};
}
