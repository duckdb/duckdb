#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

TableFunctionCatalogEntry::TableFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
                                                     CreateTableFunctionInfo *info)
    : StandardEntry(CatalogType::TABLE_FUNCTION, schema, catalog, info->name), function(info->function) {
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
	for (auto &type : function.types) {
		types.push_back(GetInternalType(type));
	}
	return types;
}
