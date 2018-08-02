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
#include "catalog/column_catalog.hpp"
#include "catalog/table_catalog.hpp"
#include "parser/expression/abstract_expression.hpp"
#include "parser/statement/sql_statement.hpp"

namespace duckdb {

//! The BindContext object keeps track of all the tables and columns that are
//! encountered during the binding process.
class BindContext {
  public:
	BindContext() {}

	//! Given a column name, find the matching table it belongs to. Throws an
	//! exception if no table has a column of the given name.
	std::string GetMatchingTable(const std::string &column_name);
	//! Binds a column expression to the base table. Returns the column catalog
	//! entry or throws an exception if the column could not be bound.
	std::shared_ptr<ColumnCatalogEntry> BindColumn(ColumnRefExpression &expr);

	//! Generate column expressions for all columns that are present in the
	//! referenced tables. This is used to resolve the * expression in a
	//! selection list.
	void GenerateAllColumnExpressions(
	    std::vector<std::unique_ptr<AbstractExpression>> &new_select_list);

	//! Adds a base table with the given alias to the BindContext.
	void AddBaseTable(const std::string &alias,
	                  std::shared_ptr<TableCatalogEntry> table_entry);
	//! Adds a subquery with a given alias to the BindContext.
	void AddSubquery(const std::string &alias, SelectStatement *subquery);
	//! Adds an expression that has an alias to the BindContext
	void AddExpression(const std::string &alias, AbstractExpression *expression,
	                   size_t i);

	//! Returns true if the table/subquery alias exists, false otherwise.
	bool HasAlias(const std::string &alias);

	//! The set of columns that are bound for each table/subquery alias
	std::unordered_map<std::string, std::vector<std::string>> bound_columns;

  private:
	//! The set of expression aliases
	std::unordered_map<std::string, std::pair<size_t, AbstractExpression *>>
	    expression_alias_map;
	//! The set of bound tables
	std::unordered_map<std::string, std::shared_ptr<TableCatalogEntry>>
	    regular_table_alias_map;
	//! The set of bound subqueries
	std::unordered_map<std::string, SelectStatement *> subquery_alias_map;

	std::unique_ptr<BindContext> child;
};
} // namespace duckdb
