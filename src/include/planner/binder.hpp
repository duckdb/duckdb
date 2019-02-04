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
struct OrderByDescription;

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

	void Bind(SQLStatement &statement);

protected:
	void Bind(SelectStatement &stmt);
	void Bind(InsertStatement &stmt);
	void Bind(CopyStatement &stmt);
	void Bind(DeleteStatement &stmt);
	void Bind(UpdateStatement &stmt);
	void Bind(AlterTableStatement &stmt);
	void Bind(CreateTableStatement &stmt);
	void Bind(CreateIndexStatement &stmt);
	void Bind(CreateViewStatement &stmt);

	void Bind(QueryNode &node);
	void Bind(SelectNode &node);
	void Bind(SetOperationNode &node);

	void BindOrderBy(OrderByDescription &orders, vector<unique_ptr<Expression>> &select_list, size_t max_index);

	void VisitAndResolveType(unique_ptr<Expression>* expr);

	unique_ptr<Expression> VisitReplace(ColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	unique_ptr<Expression> VisitReplace(FunctionExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	unique_ptr<Expression> VisitReplace(SubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override;

	unique_ptr<Expression> ExtractAggregatesAndGroups(unique_ptr<Expression> expr, SelectNode &node, expression_map_t<uint32_t>& groups);
	unique_ptr<Expression> ExtractAggregates(unique_ptr<Expression> expr, vector<unique_ptr<Expression>> &result, size_t aggregate_index);
	unique_ptr<Expression> ExtractWindowFunctions(unique_ptr<Expression> expr, vector<unique_ptr<Expression>> &result, size_t window_index);
	unique_ptr<Expression> ExtractGroupReferences(unique_ptr<Expression> expr, size_t group_index, expression_map_t<uint32_t> &groups);
	unique_ptr<Expression> WrapInFirstAggregate(unique_ptr<Expression> expr, vector<unique_ptr<Expression>> &result, size_t aggregate_index);

	unique_ptr<Expression> ExtractExpressionClass(unique_ptr<Expression> expr, vector<unique_ptr<Expression>> &result, size_t bind_index, ExpressionClass expr_class);
public:
	void Visit(CheckConstraint &constraint) override;

	unique_ptr<TableRef> Visit(BaseTableRef &expr) override;
	unique_ptr<TableRef> Visit(CrossProductRef &expr) override;
	unique_ptr<TableRef> Visit(JoinRef &expr) override;
	unique_ptr<TableRef> Visit(SubqueryRef &expr) override;
	unique_ptr<TableRef> Visit(TableFunction &expr) override;

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
