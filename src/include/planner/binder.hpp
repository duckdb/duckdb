//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"
#include "planner/bindcontext.hpp"

#include <string>
#include <vector>

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
	    : bind_context(make_unique<BindContext>()), context(context), parent(parent) {
	}

	unique_ptr<SQLStatement> Visit(SelectStatement &statement);
	unique_ptr<SQLStatement> Visit(InsertStatement &stmt);
	unique_ptr<SQLStatement> Visit(CopyStatement &stmt);
	unique_ptr<SQLStatement> Visit(DeleteStatement &stmt);
	unique_ptr<SQLStatement> Visit(UpdateStatement &stmt);
	unique_ptr<SQLStatement> Visit(AlterTableStatement &stmt);
	unique_ptr<SQLStatement> Visit(CreateTableStatement &stmt);
	unique_ptr<SQLStatement> Visit(CreateIndexStatement &stmt);

	void Visit(SelectNode &node);
	void Visit(SetOperationNode &node);

	unique_ptr<Constraint> Visit(CheckConstraint &constraint);

	unique_ptr<Expression> Visit(ColumnRefExpression &expr);
	unique_ptr<Expression> Visit(FunctionExpression &expr);
	unique_ptr<Expression> Visit(SubqueryExpression &expr);

	unique_ptr<TableRef> Visit(BaseTableRef &expr);
	unique_ptr<TableRef> Visit(CrossProductRef &expr);
	unique_ptr<TableRef> Visit(JoinRef &expr);
	unique_ptr<TableRef> Visit(SubqueryRef &expr);
	unique_ptr<TableRef> Visit(TableFunction &expr);

	void AddCTE(const string &name, QueryNode *cte);
	unique_ptr<QueryNode> FindCTE(const string &name);

	//! The BindContext created and used by the Binder.
	unique_ptr<BindContext> bind_context;

	std::unordered_map<string, QueryNode *> CTE_bindings;

private:
	ClientContext &context;
	Binder *parent;
};
} // namespace duckdb
