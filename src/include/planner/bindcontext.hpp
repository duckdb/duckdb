
#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/catalog.hpp"
#include "catalog/table_catalog.hpp"
#include "parser/statement/sql_statement.hpp"
#include "parser/expression/abstract_expression.hpp"

namespace duckdb {

class BindContext {
  public:
	BindContext() {}

	std::string GetMatchingTable(const std::string &column_name);
	void BindColumn(const std::string &table_name,
	                const std::string column_name);

	void GenerateAllColumnExpressions(std::vector<std::unique_ptr<AbstractExpression>>& new_select_list);

	void AddBaseTable(const std::string &alias,
	                  std::shared_ptr<TableCatalogEntry> table_entry);
	void AddSubquery(const std::string &alias, SelectStatement *subquery);

	bool HasAlias(const std::string &alias);

  private:
	std::unordered_map<std::string, std::shared_ptr<TableCatalogEntry>>
	    regular_table_alias_map;
	std::unordered_map<std::string, SelectStatement *> subquery_alias_map;

	std::shared_ptr<BindContext> child;
};
}
