
#include "catalog/table_catalog.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

TableCatalogEntry::TableCatalogEntry(string name,
                                     shared_ptr<AbstractCatalogEntry> parent)
    : AbstractCatalogEntry(name, parent), size(0) {}

void TableCatalogEntry::AddColumn(ColumnCatalogEntry entry) {
	if (ColumnExists(entry.name)) {
		throw CatalogException("Column with name %s already exists!",
		                       entry.name.c_str());
	}
	columns[entry.name] =
	    make_shared<ColumnCatalogEntry>(entry, shared_from_this());
}

bool TableCatalogEntry::ColumnExists(const string &name) {
	return columns.find(name) != columns.end();
}
