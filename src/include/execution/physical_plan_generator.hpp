//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/physical_plan_generator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/printable.hpp"
#include "execution/physical_operator.hpp"
#include "planner/bindcontext.hpp"
#include "planner/logical_operator.hpp"
#include "planner/logical_operator_visitor.hpp"

#include <vector>

namespace duckdb {
class ClientContext;

//! The physical plan generator generates a physical execution plan from a
//! logical query plan
class PhysicalPlanGenerator : public LogicalOperatorVisitor {
public:
	PhysicalPlanGenerator(ClientContext &context, PhysicalPlanGenerator *parent = nullptr)
	    : parent(parent), context(context) {
	}
	using LogicalOperatorVisitor::Visit;

	void CreatePlan(unique_ptr<LogicalOperator> logical);

	virtual void Visit(LogicalAggregate &op);
	virtual void Visit(LogicalCreate &op);
	virtual void Visit(LogicalCreateIndex &op);
	virtual void Visit(LogicalCrossProduct &op);
	virtual void Visit(LogicalDelete &op);
	virtual void Visit(LogicalFilter &op);
	virtual void Visit(LogicalGet &op);
	virtual void Visit(LogicalJoin &op);
	virtual void Visit(LogicalLimit &op);
	virtual void Visit(LogicalOrder &op);
	virtual void Visit(LogicalProjection &op);
	virtual void Visit(LogicalInsert &op);
	virtual void Visit(LogicalCopy &op);
	virtual void Visit(LogicalExplain &op);
	virtual void Visit(LogicalUnion &op);
	virtual void Visit(LogicalExcept &op);
	virtual void Visit(LogicalIntersect &op);
	virtual void Visit(LogicalUpdate &op);
	virtual void Visit(LogicalTableFunction &expr);
	virtual void Visit(LogicalPruneColumns &expr);

	virtual unique_ptr<Expression> Visit(SubqueryExpression &expr);

	void Print() {
		plan->Print();
	}

	unique_ptr<PhysicalOperator> plan;

	PhysicalPlanGenerator *parent;

private:
	ClientContext &context;
};
} // namespace duckdb
