//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/physical_plan_generator.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/internal_types.hpp"
#include "common/printable.hpp"

#include "execution/physical_operator.hpp"

#include "planner/bindcontext.hpp"
#include "planner/logical_operator.hpp"
#include "planner/logical_operator_visitor.hpp"

namespace duckdb {
class ClientContext;

//! The physical plan generator generates a physical execution plan from a
//! logical query plan
class PhysicalPlanGenerator : public LogicalOperatorVisitor {
  public:
	PhysicalPlanGenerator(ClientContext &context,
	                      PhysicalPlanGenerator *parent = nullptr)
	    : parent(parent), context(context) {
	}
	using LogicalOperatorVisitor::Visit;

	bool CreatePlan(std::unique_ptr<LogicalOperator> logical);

	bool GetSuccess() const {
		return success;
	}
	const std::string &GetErrorMessage() const {
		return message;
	}

	virtual void Visit(LogicalAggregate &op);
	virtual void Visit(LogicalCreate &op);
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
	virtual void Visit(LogicalUpdate &op);
	virtual void Visit(LogicalTableFunction &expr);
	virtual void Visit(LogicalPruneColumns &expr);

	virtual void Visit(SubqueryExpression &expr);

	void Print() {
		plan->Print();
	}

	std::unique_ptr<PhysicalOperator> plan;

	PhysicalPlanGenerator *parent;

	bool success;
	std::string message;

  private:
	ClientContext &context;
};
} // namespace duckdb
