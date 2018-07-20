
#pragma once

#include <vector>

#include "common/internal_types.hpp"
#include "common/printable.hpp"

#include "execution/physicaloperator.hpp"

#include "planner/logicaloperator.hpp"

#include "planner/logical_visitor.hpp"

namespace duckdb {
class PhysicalPlanGenerator : public LogicalOperatorVisitor {
  public:
  	PhysicalPlanGenerator(Catalog& catalog) : catalog(catalog) { }

	bool CreatePlan(std::unique_ptr<LogicalOperator> logical);

	bool GetSuccess() const { return success; }
	const std::string &GetErrorMessage() const { return message; }

  	virtual void Visit(LogicalAggregate& op) override;
  	virtual void Visit(LogicalDistinct& op) override;
  	virtual void Visit(LogicalFilter& op) override;
  	virtual void Visit(LogicalGet& op) override;
  	virtual void Visit(LogicalLimit& op) override;
  	virtual void Visit(LogicalOrder& op) override;

	void Print() {
		plan->Print();
	}

	std::unique_ptr<PhysicalOperator> plan;
	bool success;
	std::string message;
  private:
	Catalog &catalog;
};
}
