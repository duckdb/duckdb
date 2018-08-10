//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/binder.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.hpp"

#include "parser/sql_node_visitor.hpp"
#include "parser/statement/sql_statement.hpp"

#include "planner/bindcontext.hpp"

namespace duckdb {

//! Bind the parsed query tree to the actual columns present in the catalog.
/*!
  The binder is responsible for binding tables and columns to actual physical
  tables and columns in the catalog. In the process, it also resolves types of
  all expressions.
*/
class Binder : public SQLNodeVisitor {
  public:
	Binder(Catalog &catalog)
	    : catalog(catalog), context(make_unique<BindContext>()) {}

	void Visit(SelectStatement &statement);

	void Visit(ColumnRefExpression &expr);
	void Visit(SubqueryExpression &expr);

	void Visit(BaseTableRef &expr);
	void Visit(CrossProductRef &expr);
	void Visit(JoinRef &expr);
	void Visit(SubqueryRef &expr);
	//! The BindContext created and used by the Binder.
	std::unique_ptr<BindContext> context;

  private:
	Catalog &catalog;
};
} // namespace duckdb
