#include "planner/table_binding.hpp"

#include "common/string_util.hpp"
#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/tableref/subqueryref.hpp"
#include "planner/bind_context.hpp"
#include "planner/bound_query_node.hpp"
#include "planner/expression/bound_columnref_expression.hpp"
#include "planner/tableref/bound_basetableref.hpp"

using namespace duckdb;
using namespace std;

TableBinding::TableBinding(const string &alias, BoundBaseTableRef *bound)
    : Binding(BindingType::TABLE, alias, bound->bind_index), bound(bound) {
}

bool TableBinding::HasMatchingBinding(const string &column_name) {
	return bound->table->ColumnExists(column_name);
}

BindResult TableBinding::Bind(ColumnRefExpression &colref, index_t depth) {
	auto entry = bound->table->name_map.find(colref.column_name);
	if (entry == bound->table->name_map.end()) {
		return BindResult(StringUtil::Format("Table \"%s\" does not have a column named \"%s\"",
		                                     colref.table_name.c_str(), colref.column_name.c_str()));
	}
	// fetch the type of the column
	SQLType col_type;
	if (entry->second == COLUMN_IDENTIFIER_ROW_ID) {
		// row id: BIGINT type
		col_type = SQLType(SQLTypeId::BIGINT);
	} else {
		// normal column: fetch type from base column
		auto &col = bound->table->columns[entry->second];
		col_type = col.type;
	}
	auto &column_list = bound->bound_columns;
	// check if the entry already exists in the column list for the table
	ColumnBinding binding;

	binding.column_index = column_list.size();
	for (index_t i = 0; i < column_list.size(); i++) {
		auto &column = column_list[i];
		if (column == colref.column_name) {
			binding.column_index = i;
			break;
		}
	}
	if (binding.column_index == column_list.size()) {
		// column binding not found: add it to the list of bindings
		column_list.push_back(colref.column_name);
	}
	binding.table_index = index;
	return BindResult(
	    make_unique<BoundColumnRefExpression>(colref.GetName(), GetInternalType(col_type), binding, depth), col_type);
}

void TableBinding::GenerateAllColumnExpressions(BindContext &context,
                                                vector<unique_ptr<ParsedExpression>> &select_list) {
	for (auto &column : bound->table->columns) {
		string column_string = alias + "." + column.name;
		if (context.hidden_columns.find(column_string) != context.hidden_columns.end()) {
			continue;
		}
		select_list.push_back(make_unique<ColumnRefExpression>(column.name, alias));
	}
}

SubqueryBinding::SubqueryBinding(const string &alias, SubqueryRef &ref, BoundQueryNode &subquery, index_t index)
    : Binding(BindingType::SUBQUERY, alias, index), subquery(subquery) {
	if (ref.column_name_alias.size() > subquery.names.size()) {
		throw BinderException("table \"%s\" has %lld columns available but %lld columns specified", alias.c_str(),
		                      (int64_t)subquery.names.size(), (int64_t)ref.column_name_alias.size());
	}
	index_t i;
	// use any provided aliases from the subquery
	for (i = 0; i < ref.column_name_alias.size(); i++) {
		auto &name = ref.column_name_alias[i];
		AddName(name);
	}
	// if not enough aliases were provided, use the default names
	for (; i < subquery.names.size(); i++) {
		auto &name = subquery.names[i];
		AddName(name);
	}
}

void SubqueryBinding::AddName(string &name) {
	if (name_map.find(name) != name_map.end()) {
		throw BinderException("table \"%s\" has duplicate column name \"%s\"", alias.c_str(), name.c_str());
	}
	name_map[name] = names.size();
	names.push_back(name);
}

bool SubqueryBinding::HasMatchingBinding(const string &column_name) {
	auto entry = name_map.find(column_name);
	return entry != name_map.end();
}

BindResult SubqueryBinding::Bind(ColumnRefExpression &colref, index_t depth) {
	auto column_entry = name_map.find(colref.column_name);
	if (column_entry == name_map.end()) {
		return BindResult(StringUtil::Format("Subquery \"%s\" does not have a column named \"%s\"", alias.c_str(),
		                                     colref.column_name.c_str()));
	}
	ColumnBinding binding;

	binding.table_index = index;
	binding.column_index = column_entry->second;
	assert(column_entry->second < subquery.types.size());
	SQLType sql_type = subquery.types[column_entry->second];
	return BindResult(
	    make_unique<BoundColumnRefExpression>(colref.GetName(), GetInternalType(sql_type), binding, depth), sql_type);
}

void SubqueryBinding::GenerateAllColumnExpressions(BindContext &context,
                                                   vector<unique_ptr<ParsedExpression>> &select_list) {
	for (auto &column_name : names) {
		select_list.push_back(make_unique<ColumnRefExpression>(column_name, alias));
	}
}

TableFunctionBinding::TableFunctionBinding(const string &alias, TableFunctionCatalogEntry *function, index_t index)
    : Binding(BindingType::TABLE_FUNCTION, alias, index), function(function) {
}

bool TableFunctionBinding::HasMatchingBinding(const string &column_name) {
	return function->ColumnExists(column_name);
}

BindResult TableFunctionBinding::Bind(ColumnRefExpression &colref, index_t depth) {
	auto column_entry = function->name_map.find(colref.column_name);
	if (column_entry == function->name_map.end()) {
		return BindResult(StringUtil::Format("Table Function \"%s\" does not have a column named \"%s\"", alias.c_str(),
		                                     colref.column_name.c_str()));
	}
	ColumnBinding binding;
	binding.table_index = index;
	binding.column_index = column_entry->second;
	SQLType sql_type = function->return_values[column_entry->second].type;
	return BindResult(
	    make_unique<BoundColumnRefExpression>(colref.GetName(), GetInternalType(sql_type), binding, depth), sql_type);
}

void TableFunctionBinding::GenerateAllColumnExpressions(BindContext &context,
                                                        vector<unique_ptr<ParsedExpression>> &select_list) {
	for (auto &column : function->return_values) {
		select_list.push_back(make_unique<ColumnRefExpression>(column.name, alias));
	}
}
