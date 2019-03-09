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
#include "planner/expression_binder.hpp"

#include <string>
#include <vector>

namespace duckdb {
class ClientContext;
struct OrderByDescription;

struct CorrelatedColumnInfo {
	ColumnBinding binding;
	TypeId type;
	string name;
	size_t depth;

	CorrelatedColumnInfo(BoundColumnRefExpression &expr) {
		binding = expr.binding;
		type = expr.return_type;
		name = expr.GetName();
		depth = expr.depth;
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
class Binder : public SQLNodeVisitor {
public:
	Binder(ClientContext &context, Binder *parent = nullptr);

	void Bind(SQLStatement &statement);

	void Bind(SelectStatement &stmt);
	void Bind(InsertStatement &stmt);
	void Bind(CopyStatement &stmt);
	void Bind(DeleteStatement &stmt);
	void Bind(UpdateStatement &stmt);
	void Bind(AlterTableStatement &stmt);
	void Bind(CreateTableStatement &stmt);
	void Bind(CreateIndexStatement &stmt);
	void Bind(CreateViewStatement &stmt);
	void Bind(CreateTableAsStatement &stmt);

	void Bind(QueryNode &node);
	void Bind(SelectNode &node);
	void Bind(SetOperationNode &node);

public:
	void Visit(CheckConstraint &constraint) override;

	unique_ptr<TableRef> Visit(BaseTableRef &expr) override;
	unique_ptr<TableRef> Visit(CrossProductRef &expr) override;
	unique_ptr<TableRef> Visit(JoinRef &expr) override;
	unique_ptr<TableRef> Visit(SubqueryRef &expr) override;
	unique_ptr<TableRef> Visit(TableFunction &expr) override;

	void AddCTE(const string &name, QueryNode *cte);
	unique_ptr<QueryNode> FindCTE(const string &name);

	std::unordered_map<string, QueryNode *> CTE_bindings;

	//! Generates an unused index for a table
	size_t GenerateTableIndex();

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

private:
	//! Move correlated expressions from the child binder to this binder
	void MoveCorrelatedExpressions(Binder &other);

	ClientContext &context;
	Binder *parent;

	vector<ExpressionBinder *> active_binders;

	size_t bound_tables;
	bool encountered_select_node = false;
};

} // namespace duckdb
