
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

class BindContext {
  public:
	BindContext() {}

	std::string GetMatchingTable(const std::string &column_name);
	std::shared_ptr<ColumnCatalogEntry> BindColumn(ColumnRefExpression &expr);

	void GenerateAllColumnExpressions(
	    std::vector<std::unique_ptr<AbstractExpression>> &new_select_list);

	void AddBaseTable(const std::string &alias,
	                  std::shared_ptr<TableCatalogEntry> table_entry);
	void AddSubquery(const std::string &alias, SelectStatement *subquery);

	bool HasAlias(const std::string &alias);

	std::unordered_map<std::string, std::vector<std::string>> bound_columns;

  private:
	std::unordered_map<std::string, std::shared_ptr<TableCatalogEntry>>
	    regular_table_alias_map;
	std::unordered_map<std::string, SelectStatement *> subquery_alias_map;

	std::unique_ptr<BindContext> child;
};
}
