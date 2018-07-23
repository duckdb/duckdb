
#include "storage/storage_manager.hpp"

using namespace duckdb;
using namespace std;

void StorageManager::CreateTable(TableCatalogEntry &table) {
	auto storage = make_unique<DataTable>(*this, table);
	tables.push_back(move(storage));
	table.storage = tables.back().get();
}
