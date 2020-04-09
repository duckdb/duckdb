#include "duckdb/planner/table_binding.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

using namespace duckdb;
using namespace std;

TableBinding::TableBinding(const string &alias, TableCatalogEntry &table, LogicalGet &get, idx_t index)
    : Binding(BindingType::TABLE, alias, index), table(table), get(get) {
}

bool TableBinding::HasMatchingBinding(const string &column_name) {
	return table.ColumnExists(column_name);
}

BindResult TableBinding::Bind(ColumnRefExpression &colref, idx_t depth) {
	auto entry = table.name_map.find(colref.column_name);
	if (entry == table.name_map.end()) {
		return BindResult(StringUtil::Format("Table \"%s\" does not have a column named \"%s\"",
		                                     colref.table_name.c_str(), colref.column_name.c_str()));
	}
	auto col_index = entry->second;
	// fetch the type of the column
	SQLType col_type;
	if (entry->second == COLUMN_IDENTIFIER_ROW_ID) {
		// row id: BIGINT type
		col_type = SQLType::BIGINT;
	} else {
		// normal column: fetch type from base column
		auto &col = table.columns[col_index];
		col_type = col.type;
	}

	auto &column_ids = get.column_ids;
	// check if the entry already exists in the column list for the table
	ColumnBinding binding;

	binding.column_index = column_ids.size();
	for (idx_t i = 0; i < column_ids.size(); i++) {
		if (column_ids[i] == col_index) {
			binding.column_index = i;
			break;
		}
	}
	if (binding.column_index == column_ids.size()) {
		// column binding not found: add it to the list of bindings
		column_ids.push_back(col_index);
	}
	binding.table_index = index;
	return BindResult(
	    make_unique<BoundColumnRefExpression>(colref.GetName(), GetInternalType(col_type), binding, depth), col_type);
}

void TableBinding::GenerateAllColumnExpressions(BindContext &context,
                                                vector<unique_ptr<ParsedExpression>> &select_list) {
	for (auto &column : table.columns) {
		if (context.BindingIsHidden(alias, column.name)) {
			continue;
		}
		assert(!column.name.empty());
		select_list.push_back(make_unique<ColumnRefExpression>(column.name, alias));
	}
}

GenericBinding::GenericBinding(const string &alias, vector<SQLType> coltypes, vector<string> colnames, idx_t index)
    : Binding(BindingType::GENERIC, alias, index), types(move(coltypes)), names(move(colnames)) {
	assert(types.size() == names.size());
	for (idx_t i = 0; i < names.size(); i++) {
		auto &name = names[i];
		assert(!name.empty());
		if (name_map.find(name) != name_map.end()) {
			throw BinderException("table \"%s\" has duplicate column name \"%s\"", alias.c_str(), name.c_str());
		}
		name_map[name] = i;
	}
}

bool GenericBinding::HasMatchingBinding(const string &column_name) {
	auto entry = name_map.find(column_name);
	return entry != name_map.end();
}

BindResult GenericBinding::Bind(ColumnRefExpression &colref, idx_t depth) {
	auto column_entry = name_map.find(colref.column_name);
	if (column_entry == name_map.end()) {
		return BindResult(StringUtil::Format("Values list \"%s\" does not have a column named \"%s\"", alias.c_str(),
		                                     colref.column_name.c_str()));
	}
	ColumnBinding binding;
	binding.table_index = index;
	binding.column_index = column_entry->second;
	SQLType sql_type = types[column_entry->second];
	return BindResult(
	    make_unique<BoundColumnRefExpression>(colref.GetName(), GetInternalType(sql_type), binding, depth), sql_type);
}

void GenericBinding::GenerateAllColumnExpressions(BindContext &context,
                                                  vector<unique_ptr<ParsedExpression>> &select_list) {
	for (auto &column_name : names) {
		assert(!column_name.empty());
		if (context.BindingIsHidden(alias, column_name)) {
			continue;
		}
		select_list.push_back(make_unique<ColumnRefExpression>(column_name, alias));
	}
}
