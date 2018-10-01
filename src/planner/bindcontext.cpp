
#include "planner/bindcontext.hpp"

#include "parser/expression/columnref_expression.hpp"

using namespace duckdb;
using namespace std;

string BindContext::GetMatchingTable(const string &column_name) {
	string result;
	if (dummy_table) {
		// bind to dummy table
		return "";
	}

	for (auto &kv : regular_table_alias_map) {
		auto table = kv.second.table;
		if (table->ColumnExists(column_name)) {
			if (!result.empty()) {
				throw BinderException(
				    "Ambiguous reference to column name\"%s\" (use: \"%s.%s\" "
				    "or \"%s.%s\")",
				    column_name.c_str(), result.c_str(), column_name.c_str(),
				    table->name.c_str(), column_name.c_str());
			}
			result = kv.first;
		}
	}

	for (auto &kv : subquery_alias_map) {
		auto subquery = kv.second;
		(void)subquery;
		throw BinderException("Subquery binding not implemented yet!");
	}

	if (result.empty() &&
	    expression_alias_map.find(column_name) != expression_alias_map.end())
		return result; // empty result, will be bound to alias

	if (result.empty()) {
		if (parent) {
			return parent->GetMatchingTable(column_name);
		}
		throw BinderException(
		    "Referenced column \"%s\" not found in FROM clause!",
		    column_name.c_str());
	}
	return result;
}

ColumnDefinition *BindContext::BindColumn(ColumnRefExpression &expr,
                                          size_t depth) {
	if (dummy_table) {
		// bind to the dummy table if present
		auto entry = dummy_table->bound_columns.find(expr.column_name);
		if (entry == dummy_table->bound_columns.end()) {
			throw BinderException("Referenced column \"%s\" not found table!",
			                      expr.column_name.c_str());
		}
		expr.index = entry->second->oid;
		expr.return_type = entry->second->type;
		return entry->second;
	}

	if (expr.table_name.empty()) {
		auto entry = expression_alias_map.find(expr.column_name);
		if (entry == expression_alias_map.end()) {
			throw BinderException("Could not bind alias \"%s\"!",
			                      expr.column_name.c_str());
		}
		expr.index = entry->second.first;
		expr.reference = entry->second.second;
		expr.return_type = entry->second.second->return_type;
		return nullptr;
	}

	if (!HasAlias(expr.table_name)) {
		if (parent) {
			auto result = parent->BindColumn(expr, ++depth);
			max_depth = max(expr.depth, max_depth);
			return result;
		}
		throw BinderException("Referenced table \"%s\" not found!",
		                      expr.table_name.c_str());
	}
	ColumnDefinition *entry;

	size_t table_index = 0;
	auto table_entry = regular_table_alias_map.find(expr.table_name);
	if (table_entry != regular_table_alias_map.end()) {
		// base table
		auto &table = table_entry->second;
		if (!table.table->ColumnExists(expr.column_name)) {
			throw BinderException(
			    "Table \"%s\" does not have a column named \"%s\"",
			    expr.table_name.c_str(), expr.column_name.c_str());
		}
		table_index = table.index;
		entry = &table.table->GetColumn(expr.column_name);
		expr.stats = table.table->GetStatistics(entry->oid);
	} else {
		// subquery
		throw BinderException("Subquery binding not implemented yet!");
	}

	auto &column_list = bound_columns[expr.table_name];
	// check if the entry already exists in the column list for the table
	expr.binding.column_index = column_list.size();
	for (size_t i = 0; i < column_list.size(); i++) {
		auto &column = column_list[i];
		if (column == expr.column_name) {
			expr.binding.column_index = i;
			break;
		}
	}
	if (expr.binding.column_index == column_list.size()) {
		// column binding not found: add it to the list of bindings
		column_list.push_back(expr.column_name);
	}
	expr.binding.table_index = table_index;
	expr.depth = depth;
	expr.return_type = entry->type;
	return entry;
}

void BindContext::GenerateAllColumnExpressions(
    vector<unique_ptr<Expression>> &new_select_list) {
	if (regular_table_alias_map.size() == 0 && subquery_alias_map.size() == 0) {
		throw BinderException("SELECT * expression without FROM clause!");
	}
	for (auto table_name : regular_table_alias_list) {
		auto &table = regular_table_alias_map.find(table_name)->second;
		for (auto &column : table.table->columns) {
			new_select_list.push_back(
			    make_unique<ColumnRefExpression>(column.name, table_name));
		}
	}
	for (auto &kv : subquery_alias_map) {
		auto subquery = kv.second;
		(void)subquery;
		throw BinderException("Subquery binding not implemented yet!");
	}
}

void BindContext::AddBaseTable(const string &alias,
                               TableCatalogEntry *table_entry) {
	if (HasAlias(alias)) {
		throw BinderException("Duplicate alias \"%s\" in query!",
		                      alias.c_str());
	}
	regular_table_alias_map.insert(
	    make_pair(alias, TableBinding(table_entry, GenerateTableIndex())));
	regular_table_alias_list.push_back(alias);
}

void BindContext::AddDummyTable(std::vector<ColumnDefinition> &columns) {
	// initialize the OIDs of the column definitions
	for (size_t i = 0; i < columns.size(); i++) {
		columns[i].oid = i;
	}
	dummy_table = make_unique<DummyTableBinding>(columns);
}

void BindContext::AddSubquery(const string &alias, SelectStatement *subquery) {
	if (HasAlias(alias)) {
		throw BinderException("Duplicate alias \"%s\" in query!",
		                      alias.c_str());
	}
	subquery_alias_map.insert(make_pair(alias, subquery));
}

void BindContext::AddExpression(const string &alias, Expression *expression,
                                size_t i) {
	expression_alias_map[alias] = make_pair(i, expression);
}

bool BindContext::HasAlias(const string &alias) {
	return regular_table_alias_map.find(alias) !=
	           regular_table_alias_map.end() ||
	       subquery_alias_map.find(alias) != subquery_alias_map.end();
}

size_t BindContext::GetTableIndex(const std::string &alias) {
	auto table_entry = regular_table_alias_map.find(alias);
	if (table_entry == regular_table_alias_map.end()) {
		throw Exception("Could not find table alias binding!");
	}
	return table_entry->second.index;
}

size_t BindContext::GenerateTableIndex() {
	if (parent) {
		return parent->GenerateTableIndex();
	}
	return bound_tables++;
}
