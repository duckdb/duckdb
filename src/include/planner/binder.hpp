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
struct BoundCreateTableInfo;
struct CreateTableInfo;

struct CorrelatedColumnInfo {
	ColumnBinding binding;
	TypeId type;
	string name;
	index_t depth;

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

	//! The client context
	ClientContext &context;
	//! A mapping of names to common table expressions
	unordered_map<string, QueryNode *> CTE_bindings;
	//! The bind context
	BindContext bind_context;
	//! The set of correlated columns bound by this binder (FIXME: this should probably be an unordered_set and not a
	//! vector)
	vector<CorrelatedColumnInfo> correlated_columns;
	//! The set of parameter expressions bound by this binder
	vector<BoundParameterExpression *> *parameters;

public:
	unique_ptr<BoundSQLStatement> Bind(SQLStatement &statement);
	unique_ptr<BoundQueryNode> Bind(QueryNode &node);

	unique_ptr<BoundCreateTableInfo> BindCreateTableInfo(unique_ptr<CreateTableInfo> info);

	//! Generates an unused index for a table
	index_t GenerateTableIndex();

	//! Add a common table expression to the binder
	void AddCTE(const string &name, QueryNode *cte);
	//! Find a common table expression by name; returns nullptr if none exists
	unique_ptr<QueryNode> FindCTE(const string &name);

	void PushExpressionBinder(ExpressionBinder *binder);
	void PopExpressionBinder();
	void SetActiveBinder(ExpressionBinder *binder);
	ExpressionBinder *GetActiveBinder();
	bool HasActiveBinder();

	vector<ExpressionBinder *> &GetActiveBinders();

	void MergeCorrelatedColumns(vector<CorrelatedColumnInfo> &other);
	//! Add a correlated column to this binder (if it does not exist)
	void AddCorrelatedColumn(CorrelatedColumnInfo info);

private:
	//! The parent binder (if any)
	Binder *parent;
	//! The vector of active binders
	vector<ExpressionBinder *> active_binders;
	//! The count of bound_tables
	index_t bound_tables;

private:
	//! Bind the default values of the columns of a table
	void BindDefaultValues(vector<ColumnDefinition> &columns, vector<unique_ptr<Expression>> &bound_defaults);

	//! Move correlated expressions from the child binder to this binder
	void MoveCorrelatedExpressions(Binder &other);

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
};

} // namespace duckdb
