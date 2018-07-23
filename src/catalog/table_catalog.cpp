
#include "catalog/table_catalog.hpp"
#include "catalog/catalog.hpp"
#include "common/exception.hpp"

#include "storage/storage_manager.hpp"

using namespace duckdb;
using namespace std;

TableCatalogEntry::TableCatalogEntry(Catalog *catalog, string name)
    : AbstractCatalogEntry(catalog, name), storage(nullptr) {}

void TableCatalogEntry::AddColumn(ColumnCatalogEntry entry) {
	if (ColumnExists(entry.name)) {
		throw CatalogException("Column with name %s already exists!",
		                       entry.name.c_str());
	}
	if (!storage) {
		throw Exception("Storage of table has not been initialized!");
	}

	size_t oid = columns.size();
	name_map[entry.name] = oid;
	entry.oid = oid;
	entry.catalog = this->catalog;
	auto column = make_shared<ColumnCatalogEntry>(entry);
	columns.push_back(column);
	storage->AddColumn(*column.get());
}

bool TableCatalogEntry::ColumnExists(const string &name) {
	return name_map.find(name) != name_map.end();
}

shared_ptr<ColumnCatalogEntry>
TableCatalogEntry::GetColumn(const std::string &name) {
	if (!ColumnExists(name)) {
		throw CatalogException("Column with name %s does not exist!",
		                       name.c_str());
	}
	return columns[name_map[name]];
}
