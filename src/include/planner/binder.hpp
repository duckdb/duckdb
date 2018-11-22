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

#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"

#include "planner/bindcontext.hpp"

namespace duckdb {
class ClientContext;

//! Bind the parsed query tree to the actual columns present in the catalog.
/*!
  The binder is responsible for binding tables and columns to actual physical
  tables and columns in the catalog. In the process, it also resolves types of
  all expressions.
*/
class Binder : public SQLNodeVisitor {
  public:
	Binder(ClientContext &context)
	    : bind_context(make_unique<BindContext>()), context(context) {
	}

	std::unique_ptr<SQLStatement> Visit(SelectStatement &statement);
	std::unique_ptr<SQLStatement> Visit(InsertStatement &stmt);
	std::unique_ptr<SQLStatement> Visit(CopyStatement &stmt);
	std::unique_ptr<SQLStatement> Visit(DeleteStatement &stmt);
	std::unique_ptr<SQLStatement> Visit(UpdateStatement &stmt);
	std::unique_ptr<SQLStatement> Visit(AlterTableStatement &stmt);
	std::unique_ptr<SQLStatement> Visit(CreateTableStatement &stmt);

	std::unique_ptr<Constraint> Visit(CheckConstraint &constraint);

	std::unique_ptr<Expression> Visit(ColumnRefExpression &expr);
	std::unique_ptr<Expression> Visit(FunctionExpression &expr);
	std::unique_ptr<Expression> Visit(SubqueryExpression &expr);

	std::unique_ptr<TableRef> Visit(BaseTableRef &expr);
	std::unique_ptr<TableRef> Visit(CrossProductRef &expr);
	std::unique_ptr<TableRef> Visit(JoinRef &expr);
	std::unique_ptr<TableRef> Visit(SubqueryRef &expr);
	std::unique_ptr<TableRef> Visit(TableFunction &expr);

	//! The BindContext created and used by the Binder.
	std::unique_ptr<BindContext> bind_context;

  private:
	ClientContext &context;
};
} // namespace duckdb
