
#pragma once

#include <vector>

#include "common/internal_types.hpp"
#include "common/printable.hpp"

#include "execution/physical_operator.hpp"

#include "planner/bindcontext.hpp"
#include "planner/logical_operator.hpp"
#include "planner/logical_visitor.hpp"

namespace duckdb {
class PhysicalPlanGenerator : public LogicalOperatorVisitor {
  public:
	PhysicalPlanGenerator(Catalog &catalog) : catalog(catalog) {}

	bool CreatePlan(std::unique_ptr<LogicalOperator> logical,
	                std::unique_ptr<BindContext> context);

	bool GetSuccess() const { return success; }
	const std::string &GetErrorMessage() const { return message; }

	void Visit(LogicalAggregate &op);
	void Visit(LogicalDistinct &op);
	void Visit(LogicalFilter &op);
	void Visit(LogicalGet &op);
	void Visit(LogicalLimit &op);
	void Visit(LogicalOrder &op);
	void Visit(LogicalProjection &op);

	void Print() { plan->Print(); }

	std::unique_ptr<PhysicalOperator> plan;
	std::unique_ptr<BindContext> context;
	bool success;
	std::string message;

  private:
	Catalog &catalog;
};
}
