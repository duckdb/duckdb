
#include "storage/storage_manager.hpp"

using namespace duckdb;
using namespace std;

size_t StorageManager::CreateTable() {
	size_t oid = tables.size();
	tables.push_back(make_unique<DataTable>());
	return oid;
}

DataTable *StorageManager::GetTable(size_t oid) { return tables[oid].get(); }
