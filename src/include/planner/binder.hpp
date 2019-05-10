//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/unordered_map.hpp"
#include "parser/column_definition.hpp"
#include "parser/tokens.hpp"
#include "planner/bind_context.hpp"
#include "planner/bound_tokens.hpp"
#include "planner/expression/bound_columnref_expression.hpp"

namespace duckdb {
class ClientContext;
class ExpressionBinder;

struct CorrelatedColumnInfo {
	ColumnBinding binding;
	TypeId type;
	string name;
	uint64_t depth;

	CorrelatedColumnInfo(BoundColumnRefExpression &expr)
	    : binding(expr.binding), type(expr.return_type), name(expr.GetName()), depth(expr.depth) {
	}

	bool operator==(const CorrelatedColumnInfo &rhs) const {
		return binding == rhs.binding;
	}
};

//! Bind the parsed query tree to the actual columns present in the catalog.
/*!
  The binder is responsible for binding tables and columns to actual physical
  tables and columns in the catalog. In the process, it also resolves types of
  all expressions.
*/
class Binder {
public:
	Binder(ClientContext &context, Binder *parent = nullptr);

	unique_ptr<BoundSQLStatement> Bind(SQLStatement &statement);

	unique_ptr<BoundQueryNode> Bind(QueryNode &node);

	void BindConstraints(string table, vector<ColumnDefinition> &columns, vector<unique_ptr<Constraint>> &constraints);

private:
	unique_ptr<BoundSQLStatement> Bind(SelectStatement &stmt);
	unique_ptr<BoundSQLStatement> Bind(InsertStatement &stmt);
	unique_ptr<BoundSQLStatement> Bind(CopyStatement &stmt);
	unique_ptr<BoundSQLStatement> Bind(DeleteStatement &stmt);
	unique_ptr<BoundSQLStatement> Bind(UpdateStatement &stmt);
	unique_ptr<BoundSQLStatement> Bind(CreateTableStatement &stmt);
	unique_ptr<BoundSQLStatement> Bind(CreateIndexStatement &stmt);
	unique_ptr<BoundSQLStatement> Bind(ExecuteStatement &stmt);

	unique_ptr<BoundQueryNode> Bind(SelectNode &node);
	unique_ptr<BoundQueryNode> Bind(SetOperationNode &node);

	unique_ptr<BoundTableRef> Bind(TableRef &ref);
	unique_ptr<BoundTableRef> Bind(BaseTableRef &ref);
	unique_ptr<BoundTableRef> Bind(CrossProductRef &ref);
	unique_ptr<BoundTableRef> Bind(JoinRef &ref);
	unique_ptr<BoundTableRef> Bind(SubqueryRef &ref);
	unique_ptr<BoundTableRef> Bind(TableFunction &ref);

public:
	void AddCTE(const string &name, QueryNode *cte);
	unique_ptr<QueryNode> FindCTE(const string &name);

	unordered_map<string, QueryNode *> CTE_bindings;

	//! Generates an unused index for a table
	uint64_t GenerateTableIndex();

	BindContext bind_context;

	void PushExpressionBinder(ExpressionBinder *binder);
	void PopExpressionBinder();
	void SetActiveBinder(ExpressionBinder *binder);
	ExpressionBinder *GetActiveBinder();
	bool HasActiveBinder();

	vector<ExpressionBinder *> &GetActiveBinders();

	void MergeCorrelatedColumns(vector<CorrelatedColumnInfo> &other);
	void AddCorrelatedColumn(CorrelatedColumnInfo info);

	vector<CorrelatedColumnInfo> correlated_columns;
	vector<BoundParameterExpression *> *parameters;

private:
	//! Move correlated expressions from the child binder to this binder
	void MoveCorrelatedExpressions(Binder &other);

	ClientContext &context;
	Binder *parent;

	vector<ExpressionBinder *> active_binders;

	uint64_t bound_tables;
};

} // namespace duckdb
