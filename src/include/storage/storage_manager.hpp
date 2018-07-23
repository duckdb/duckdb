
#pragma once

#include <vector>

#include "common/helper.hpp"

#include "storage/data_table.hpp"

namespace duckdb {

class StorageManager {
  public:
  	void CreateTable(TableCatalogEntry& table);

  	DataTable* GetTable(size_t oid);
  	std::vector<std::unique_ptr<DataTable>> tables;
};
}
