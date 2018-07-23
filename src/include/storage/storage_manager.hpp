
#pragma once

#include <vector>

#include "common/helper.hpp"

#include "storage/data_table.hpp"

namespace duckdb {

class StorageManager {
  public:
	size_t CreateTable();

	DataTable *GetTable(size_t oid);

	std::vector<std::unique_ptr<DataTable>> tables;
};
}
