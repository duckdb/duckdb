
#include "planner/bindcontext.hpp"

#include "parser/expression/columnref_expression.hpp"

using namespace duckdb;
using namespace std;

string BindContext::GetMatchingTable(const string &column_name) {
	string result;
	for (auto &kv : regular_table_alias_map) {
		auto table = kv.second;
		if (table->ColumnExists(column_name)) {
			if (!result.empty()) {
				throw BinderException(
				    "Ambiguous reference to column name\"%s\" (use: \"%s.%s\" "
				    "or \"%s.%s\")",
				    column_name.c_str(), result.c_str(), column_name.c_str(),
				    table->name.c_str(), column_name.c_str());
			}
			result = table->name;
		}
	}
	for (auto &kv : subquery_alias_map) {
		auto subquery = kv.second;
		throw BinderException("Subquery binding not implemented yet!");
	}

	if (result.empty()) {
		throw BinderException(
		    "Referenced column \"%s\" not found in FROM clause!",
		    column_name.c_str());
	}
	return result;
}

shared_ptr<ColumnCatalogEntry>
BindContext::BindColumn(const std::string &table_name,
                        const std::string column_name) {
	if (!HasAlias(table_name)) {
		throw BinderException("Referenced table \"%s\" not found!",
		                      table_name.c_str());
	}
	std::shared_ptr<ColumnCatalogEntry> entry;

	if (regular_table_alias_map.find(table_name) !=
	    regular_table_alias_map.end()) {
		// base table
		auto table = regular_table_alias_map[table_name];
		if (!table->ColumnExists(column_name)) {
			throw BinderException(
			    "Table \"%s\" does not have a column named \"%s\"",
			    table_name.c_str(), column_name.c_str());
		}
		entry = table->GetColumn(column_name);
	} else {
		// subquery
		throw BinderException("Subquery binding not implemented yet!");
	}
	bound_columns[table_name].push_back(column_name);
	return entry;
}

void BindContext::GenerateAllColumnExpressions(
    vector<unique_ptr<AbstractExpression>> &new_select_list) {
	if (regular_table_alias_map.size() == 0 && subquery_alias_map.size() == 0) {
		throw BinderException("SELECT * expression without FROM clause!");
	}
	for (auto &kv : regular_table_alias_map) {
		auto table = kv.second;
		string table_name = table->name;
		for (auto &column : table->columns) {
			string column_name = column->name;
			new_select_list.push_back(
			    make_unique<ColumnRefExpression>(column_name, table_name));
		}
	}
	for (auto &kv : subquery_alias_map) {
		auto subquery = kv.second;
		throw BinderException("Subquery binding not implemented yet!");
	}
}

void BindContext::AddBaseTable(const std::string &alias,
                               std::shared_ptr<TableCatalogEntry> table_entry) {
	if (HasAlias(alias)) {
		throw BinderException("Duplicate alias \"%s\" in query!",
		                      alias.c_str());
	}
	regular_table_alias_map[alias] = table_entry;
}

void BindContext::AddSubquery(const std::string &alias,
                              SelectStatement *subquery) {
	if (HasAlias(alias)) {
		throw BinderException("Duplicate alias \"%s\" in query!",
		                      alias.c_str());
	}
	subquery_alias_map[alias] = subquery;
}

bool BindContext::HasAlias(const std::string &alias) {
	return regular_table_alias_map.find(alias) !=
	           regular_table_alias_map.end() ||
	       subquery_alias_map.find(alias) != subquery_alias_map.end();
}
