
#pragma once

#include <vector>

#include "common/helper.hpp"

#include "storage/data_table.hpp"

namespace duckdb {

class StorageManager {
  public:
	void CreateTable(TableCatalogEntry &table);

	std::vector<std::unique_ptr<DataTable>> tables;
};
}
