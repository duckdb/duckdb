
#include "catalog/schema_catalog.hpp"
#include "catalog/catalog.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

SchemaCatalogEntry::SchemaCatalogEntry(Catalog *catalog, string name)
    : AbstractCatalogEntry(catalog, name) {}

void SchemaCatalogEntry::CreateTable(
    Transaction &transaction, const string &table_name,
    const std::vector<ColumnDefinition> &columns) {

	auto table = new TableCatalogEntry(catalog, table_name);
	auto table_entry = unique_ptr<AbstractCatalogEntry>(table);
	if (!tables.CreateEntry(transaction, table_name, move(table_entry))) {
		throw CatalogException("Table with name %s already exists!",
		                       table_name.c_str());
	}

	catalog->storage.CreateTable(*table);
	for (auto &column : columns) {
		table->AddColumn(column);
	}
}

void SchemaCatalogEntry::DropTable(Transaction &transaction,
                                   const string &table_name) {
	GetTable(transaction, table_name);
	if (!tables.DropEntry(transaction, table_name)) {
		// TODO: do we care if its already marked as deleted?
	}
}

bool SchemaCatalogEntry::TableExists(Transaction &transaction,
                                     const string &table_name) {
	return tables.EntryExists(transaction, table_name);
}

TableCatalogEntry *SchemaCatalogEntry::GetTable(Transaction &transaction,
                                                const string &table_name) {
	auto entry = tables.GetEntry(transaction, table_name);
	if (!entry) {
		throw CatalogException("Table with name %s does not exist!",
		                       table_name.c_str());
	}
	return (TableCatalogEntry *)entry;
}
