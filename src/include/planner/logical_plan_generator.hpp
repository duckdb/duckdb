//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/logical_plan_generator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/printable.hpp"
#include "parser/sql_node_visitor.hpp"
#include "planner/bindcontext.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;

//! The logical plan generator generates a logical query plan from a parsed SQL
//! statement
class LogicalPlanGenerator : public SQLNodeVisitor {
public:
	LogicalPlanGenerator(Binder &binder, ClientContext &context, bool allow_parameter = false);

	void CreatePlan(SQLStatement &statement);

	void CreatePlan(SelectStatement &statement);
	void CreatePlan(InsertStatement &statement);
	void CreatePlan(CopyStatement &statement);
	void CreatePlan(DeleteStatement &statement);
	void CreatePlan(UpdateStatement &statement);
	void CreatePlan(CreateTableAsStatement &statement);
	void CreatePlan(CreateTableStatement &statement);
	void CreatePlan(CreateIndexStatement &statement);
	void CreatePlan(ExecuteStatement &statement);

	void CreatePlan(QueryNode &statement);
	void CreatePlan(SelectNode &statement);
	void CreatePlan(SetOperationNode &statement);

protected:
	void VisitQueryNode(QueryNode &statement);

	void Visit(ParameterExpression &expr) override {
		if (!allow_parameter) {
			throw BinderException("Parameter Expression without PREPARE!");
		}
	}
	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	unique_ptr<Expression> VisitReplace(OperatorExpression &expr, unique_ptr<Expression> *expr_ptr) override;

public:
	unique_ptr<TableRef> Visit(BaseTableRef &expr) override;
	unique_ptr<TableRef> Visit(CrossProductRef &expr) override;
	unique_ptr<TableRef> Visit(JoinRef &expr) override;
	unique_ptr<TableRef> Visit(SubqueryRef &expr) override;
	unique_ptr<TableRef> Visit(TableFunction &expr) override;

	void Print() {
		root->Print();
	}

	//! The resulting plan
	unique_ptr<LogicalOperator> root;

	//! Whether or not subqueries should be planned already
	bool plan_subquery = true;
	bool has_unplanned_subqueries = false;
	bool allow_parameter = false;

private:
	//! A reference to the current binder
	Binder &binder;
	//! Whether or not we require row ids to be projected
	bool require_row_id = false;
	//! A reference to the catalog
	ClientContext &context;

private:
	unique_ptr<LogicalOperator> CastSetOpToTypes(vector<TypeId> &source_types, vector<TypeId> &target_types,
	                                             unique_ptr<LogicalOperator> op);
};
} // namespace duckdb
