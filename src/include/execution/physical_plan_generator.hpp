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
	PhysicalPlanGenerator(ClientContext &context) : context(context) {
	}

	void CreatePlan(unique_ptr<LogicalOperator> logical);

	void VisitOperator(LogicalOperator &op) override;

protected:
	void Visit(LogicalAggregate &op);
	void Visit(LogicalChunkGet &op);
	void Visit(LogicalCreateTable &op);
	void Visit(LogicalCreateIndex &op);
	void Visit(LogicalCrossProduct &op);
	void Visit(LogicalDelete &op);
	void Visit(LogicalDistinct &op);
	void Visit(LogicalFilter &op);
	void Visit(LogicalGet &op);
	void Visit(LogicalJoin &op);
	void Visit(LogicalLimit &op);
	void Visit(LogicalOrder &op);
	void Visit(LogicalProjection &op);
	void Visit(LogicalInsert &op);
	void Visit(LogicalCopy &op);
	void Visit(LogicalExplain &op);
	void Visit(LogicalUnion &op);
	void Visit(LogicalExcept &op);
	void Visit(LogicalIntersect &op);
	void Visit(LogicalUpdate &op);
	void Visit(LogicalTableFunction &expr);
	void Visit(LogicalPruneColumns &expr);
	void Visit(LogicalWindow &expr);

	using SQLNodeVisitor::Visit;

public:
	void Print() {
		plan->Print();
	}

	unique_ptr<PhysicalOperator> plan;

private:
	ClientContext &context;

	void GenerateExceptIntersect(LogicalOperator &op, JoinType join_type);
};
} // namespace duckdb
