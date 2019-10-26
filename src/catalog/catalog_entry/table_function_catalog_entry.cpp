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
    : CatalogEntry(CatalogType::TABLE_FUNCTION, catalog, info->name), schema(schema), function(info->function) {
	column_t oid = 0;
	for (auto &name : function.names) {
		if (name_map.find(name) != name_map.end()) {
			throw CatalogException("Column with name %s already exists!", name.c_str());
		}

		name_map[name] = oid;
		oid++;
	}
}

bool TableFunctionCatalogEntry::ColumnExists(const string &name) {
	return name_map.find(name) != name_map.end();
}

vector<TypeId> TableFunctionCatalogEntry::GetTypes() {
	vector<TypeId> types;
	transform(function.types.begin(), function.types.end(), std::back_inserter(types), [](const SQLType& type) { return GetInternalType(type); });
	return types;
}
