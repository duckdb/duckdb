#include "catalog/catalog_entry/table_function_catalog_entry.hpp"

#include "common/exception.hpp"
#include "parser/constraints/list.hpp"
#include "parser/parsed_data/create_table_function_info.hpp"
#include "storage/storage_manager.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

TableFunctionCatalogEntry::TableFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
                                                     CreateTableFunctionInfo *info)
    : CatalogEntry(CatalogType::TABLE_FUNCTION, catalog, info->name), schema(schema) {
	for (auto &entry : info->return_values) {
		if (name_map.find(entry.name) != name_map.end()) {
			throw CatalogException("Column with name %s already exists!", entry.name.c_str());
		}

		column_t oid = return_values.size();
		name_map[entry.name] = oid;
		entry.oid = oid;
		return_values.push_back(move(entry));
	}
	arguments = info->arguments;

	init = info->init;
	function = info->function;
	final = info->final;
}

bool TableFunctionCatalogEntry::ColumnExists(const string &name) {
	return name_map.find(name) != name_map.end();
}

ColumnDefinition &TableFunctionCatalogEntry::GetColumn(const string &name) {
	if (!ColumnExists(name)) {
		throw CatalogException("Column with name %s does not exist!", name.c_str());
	}
	return return_values[name_map[name]];
}

vector<TypeId> TableFunctionCatalogEntry::GetTypes() {
	vector<TypeId> types;
	for (auto &it : return_values) {
		types.push_back(GetInternalType(it.type));
	}
	return types;
}
