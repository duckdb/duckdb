
#include "storage/storage_manager.hpp"

using namespace duckdb;
using namespace std;

void StorageManager::CreateTable(TableCatalogEntry& table) {
	size_t oid = tables.size();
	tables.push_back(make_unique<DataTable>(*this, table));
	table.oid = oid;
}

DataTable* StorageManager::GetTable(size_t oid) {
	return tables[oid].get();
}
