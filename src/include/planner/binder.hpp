
// The binder is responsible for binding tables and columns
// to actual physical tables and columns in the catalog
// In the process, it also resolves types of all expressions

#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.hpp"

#include "parser/sql_node_visitor.hpp"
#include "parser/statement/sql_statement.hpp"

#include "planner/bindcontext.hpp"

namespace duckdb {

class Binder : public SQLNodeVisitor {
  public:
	Binder(Catalog &catalog) : catalog(catalog) {}

	void Visit(SelectStatement &statement);

	void Visit(BaseTableRefExpression &expr);
	void Visit(ColumnRefExpression &expr);
	void Visit(JoinExpression &expr);
	void Visit(SubqueryExpression &expr);

	std::unique_ptr<BindContext> context;

  private:
	Catalog &catalog;
};
}
