//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/bindcontext.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/catalog.hpp"
#include "catalog/table_catalog.hpp"
#include "parser/column_definition.hpp"
#include "parser/expression.hpp"
#include "parser/sql_statement.hpp"
#include "parser/statement/select_statement.hpp"

namespace duckdb {

struct DummyTableBinding {
	std::unordered_map<std::string, ColumnDefinition *> bound_columns;

	DummyTableBinding(std::vector<ColumnDefinition> &columns) {
		for (auto &it : columns) {
			bound_columns[it.name] = &it;
		}
	}
};

struct TableBinding {
	TableCatalogEntry *table;
	size_t index;

	TableBinding(TableCatalogEntry *table, size_t index)
	    : table(table), index(index) {}
};

struct SubqueryBinding {
	SelectStatement *subquery;
	size_t index;
	//! Column names of the subquery
	std::vector<std::string> names;
	//! Name -> index for the names
	std::unordered_map<std::string, size_t> name_map;

	SubqueryBinding(SelectStatement *subquery_, size_t index)
	    : subquery(subquery_), index(index) {
		for (auto &entry : subquery->select_list) {
			auto name = entry->GetName();
			name_map[name] = names.size();
			names.push_back(name);
		}
	}
};

//! The BindContext object keeps track of all the tables and columns that are
//! encountered during the binding process.
class BindContext {
  public:
	BindContext() : bound_tables(0), max_depth(0) {}

	//! Given a column name, find the matching table it belongs to. Throws an
	//! exception if no table has a column of the given name.
	std::string GetMatchingTable(const std::string &column_name);
	//! Binds a column expression to the base table. Returns the column catalog
	//! entry or throws an exception if the column could not be bound.
	void BindColumn(ColumnRefExpression &expr, size_t depth = 0);

	//! Generate column expressions for all columns that are present in the
	//! referenced tables. This is used to resolve the * expression in a
	//! selection list.
	void GenerateAllColumnExpressions(
	    std::vector<std::unique_ptr<Expression>> &new_select_list);

	//! Adds a base table with the given alias to the BindContext.
	void AddBaseTable(const std::string &alias, TableCatalogEntry *table_entry);
	//! Adds a dummy table with the given set of columns to the BindContext.
	void AddDummyTable(std::vector<ColumnDefinition> &columns);
	//! Adds a subquery with a given alias to the BindContext.
	void AddSubquery(const std::string &alias, SelectStatement *subquery);
	//! Adds an expression that has an alias to the BindContext
	void AddExpression(const std::string &alias, Expression *expression,
	                   size_t i);

	//! Returns true if the table/subquery alias exists, false otherwise.
	bool HasAlias(const std::string &alias);
	//! Gets the table index of a given table alias. Throws an exception if the
	//! alias was not found.
	size_t GetTableIndex(const std::string &alias);
	//! Gets the table index of a given subquery
	size_t GetSubqueryIndex(const std::string &alias);
	//! Returns the maximum depth of column references in the current context
	size_t GetMaxDepth() { return max_depth; }

	//! The set of columns that are bound for each table/subquery alias
	std::unordered_map<std::string, std::vector<std::string>> bound_columns;

	BindContext *parent = nullptr;

  private:
	size_t GenerateTableIndex();

	size_t bound_tables;
	size_t max_depth;

	//! The set of expression aliases
	std::unordered_map<std::string, std::pair<size_t, Expression *>>
	    expression_alias_map;
	//! The set of bound tables
	std::unordered_map<std::string, TableBinding> regular_table_alias_map;
	//! The list of aliases in insertion order with true if subquery, false if
	//! table
	std::vector<std::pair<std::string, bool>> alias_list;
	//! Dummy table binding
	std::unique_ptr<DummyTableBinding> dummy_table;
	//! The set of bound subqueries
	std::unordered_map<std::string, SubqueryBinding> subquery_alias_map;
};
} // namespace duckdb
