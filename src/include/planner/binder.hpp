//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// planner/binder.hpp
// 
// 
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
	Binder(ClientContext &context, Binder *parent = nullptr)
	    : bind_context(make_unique<BindContext>()), context(context),
	      parent(parent) {
	}

	std::unique_ptr<SQLStatement> Visit(SelectStatement &statement);
	std::unique_ptr<SQLStatement> Visit(InsertStatement &stmt);
	std::unique_ptr<SQLStatement> Visit(CopyStatement &stmt);
	std::unique_ptr<SQLStatement> Visit(DeleteStatement &stmt);
	std::unique_ptr<SQLStatement> Visit(UpdateStatement &stmt);
	std::unique_ptr<SQLStatement> Visit(AlterTableStatement &stmt);
	std::unique_ptr<SQLStatement> Visit(CreateTableStatement &stmt);
	std::unique_ptr<SQLStatement> Visit(CreateIndexStatement &stmt);

	std::unique_ptr<Constraint> Visit(CheckConstraint &constraint);

	std::unique_ptr<Expression> Visit(ColumnRefExpression &expr);
	std::unique_ptr<Expression> Visit(FunctionExpression &expr);
	std::unique_ptr<Expression> Visit(SubqueryExpression &expr);

	std::unique_ptr<TableRef> Visit(BaseTableRef &expr);
	std::unique_ptr<TableRef> Visit(CrossProductRef &expr);
	std::unique_ptr<TableRef> Visit(JoinRef &expr);
	std::unique_ptr<TableRef> Visit(SubqueryRef &expr);
	std::unique_ptr<TableRef> Visit(TableFunction &expr);

	void AddCTE(const std::string &name, SelectStatement *cte);
	std::unique_ptr<SelectStatement> FindCTE(const std::string &name);

	//! The BindContext created and used by the Binder.
	std::unique_ptr<BindContext> bind_context;

	std::unordered_map<std::string, SelectStatement *> CTE_bindings;

  private:
	ClientContext &context;
	Binder *parent;
};
} // namespace duckdb
