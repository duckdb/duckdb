
#include "catalog/catalog.hpp"
#include "common/exception.hpp"

#include "storage/storage_manager.hpp"

using namespace duckdb;
using namespace std;

Catalog::Catalog() : AbstractCatalogEntry(nullptr, "catalog"), storage_manager(make_unique<StorageManager>()) {}

Catalog::~Catalog() {}

void Catalog::CreateSchema(const string &schema_name) {
	if (SchemaExists(schema_name)) {
		throw CatalogException("Schema with name %s already exists!",
		                       schema_name.c_str());
	}
	schemas[schema_name] =
	    make_shared<SchemaCatalogEntry>(this, schema_name);
}

void Catalog::CreateTable(const string &schema_name, const string &table_name,
                          const std::vector<ColumnCatalogEntry> &columns) {
	if (!SchemaExists(schema_name)) {
		throw CatalogException("Schema with name %s does not exist!",
		                       schema_name.c_str());
	}
	auto schema = GetSchema(schema_name);
	size_t oid = storage_manager->CreateTable();
	schema->CreateTable(table_name, columns, oid);

}

bool Catalog::SchemaExists(const string &name) {
	return schemas.find(name) != schemas.end();
}

bool Catalog::TableExists(const std::string &schema_name,
                          const std::string &table_name) {
	if (!SchemaExists(schema_name)) {
		return false;
	}
	auto schema = GetSchema(schema_name);
	return schema->TableExists(table_name);
}

shared_ptr<SchemaCatalogEntry> Catalog::GetSchema(const string &name) {
	if (!SchemaExists(name)) {
		throw CatalogException("Schema with name %s does not exist!",
		                       name.c_str());
	}
	return schemas[name];
}

shared_ptr<TableCatalogEntry> Catalog::GetTable(const string &schema_name,
                                                const string &table_name) {
	auto schema = GetSchema(schema_name);
	return schema->GetTable(table_name);
}
