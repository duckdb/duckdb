
#include "catalog/table_catalog.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

TableCatalogEntry::TableCatalogEntry(string name)
    : AbstractCatalogEntry(name), size(0) {}

void TableCatalogEntry::AddColumn(ColumnCatalogEntry entry) {
	if (ColumnExists(entry.name)) {
		throw CatalogException("Column with name %s already exists!",
		                       entry.name.c_str());
	}
	name_map[entry.name] = columns.size();
	columns.push_back(make_shared<ColumnCatalogEntry>(entry));
}

bool TableCatalogEntry::ColumnExists(const string &name) {
	return name_map.find(name) != name_map.end();
}
