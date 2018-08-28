
#include "storage/storage_manager.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

void StorageManager::CreateTable(TableCatalogEntry &table) {
	auto storage = make_unique<DataTable>(*this, table);
	tables.push_back(move(storage));
	table.storage = tables.back().get();
}

void StorageManager::DropTable(TableCatalogEntry &table) {
	for (size_t i = 0; i < tables.size(); i++) {
		if (tables[i]->table.name == table.name) {
			tables.erase(tables.begin() + i);
			return;
		}
	}
	throw Exception("Could not find catalog entry to delete");
}
