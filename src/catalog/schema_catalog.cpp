
#include "catalog/schema_catalog.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

SchemaCatalogEntry::SchemaCatalogEntry(string name,
                                       shared_ptr<AbstractCatalogEntry> parent)
    : AbstractCatalogEntry(name, parent) {}

void SchemaCatalogEntry::CreateTable(
    const string &table_name,
    const std::vector<ColumnCatalogEntry> &columns) {
	if (TableExists(table_name)) {
		throw CatalogException("Table with name %s already exists!",
		                       table_name.c_str());
	}
	auto table = make_shared<TableCatalogEntry>(table_name, shared_from_this());
	for (auto& column : columns) {
		table->AddColumn(column);
	}
	tables[table_name] = table;
}

bool SchemaCatalogEntry::TableExists(const string &table_name) {
	return tables.find(table_name) != tables.end();
}

shared_ptr<TableCatalogEntry> SchemaCatalogEntry::GetTable(const string &name) {
	if (!TableExists(name)) {
		throw CatalogException("Table with name %s does not exist!",
		                       name.c_str());
	}
	return tables[name];
}
