
#include "catalog/catalog.hpp"
#include "catalog/table_catalog.hpp"
#include "common/exception.hpp"

#include "storage/storage_manager.hpp"

using namespace duckdb;
using namespace std;

TableCatalogEntry::TableCatalogEntry(Catalog* catalog, string name, size_t oid)
    : AbstractCatalogEntry(catalog, name), oid(oid) {}

void TableCatalogEntry::AddColumn(ColumnCatalogEntry entry) {
	if (ColumnExists(entry.name)) {
		throw CatalogException("Column with name %s already exists!",
		                       entry.name.c_str());
	}

	auto table = catalog->storage_manager->GetTable(this->oid);
	table->AddColumn(entry.type);

	size_t oid = columns.size();
	name_map[entry.name] = oid;
	entry.oid = oid;
	entry.catalog = this->catalog;
	columns.push_back(make_shared<ColumnCatalogEntry>(entry));
}

bool TableCatalogEntry::ColumnExists(const string &name) {
	return name_map.find(name) != name_map.end();
}
