
#include "execution/physicalplangenerator.hpp"

#include "execution/seqscan.hpp"

#include "planner/logicalaggregate.hpp"
#include "planner/logicaldistinct.hpp"
#include "planner/logicalfilter.hpp"
#include "planner/logicalget.hpp"
#include "planner/logicallimit.hpp"
#include "planner/logicaloperator.hpp"
#include "planner/logicalorder.hpp"

using namespace duckdb;
using namespace std;

bool PhysicalPlanGenerator::CreatePlan(unique_ptr<LogicalOperator> logical) {
	this->success = false;
	try {
		logical->Accept(this);
		if (!this->plan) {
			throw Exception("Unknown error in physical plan generation");
		}
		this->success = true;
	} catch (Exception ex) {
		this->message = ex.GetMessage();
	} catch (...) {
		this->message = "UNHANDLED EXCEPTION TYPE THROWN IN PLANNER!";
	}
	return this->success;
}

void PhysicalPlanGenerator::Visit(LogicalAggregate& op) {
	LogicalOperatorVisitor::Visit(op);
}

void PhysicalPlanGenerator::Visit(LogicalDistinct& op) {
	LogicalOperatorVisitor::Visit(op);
}

void PhysicalPlanGenerator::Visit(LogicalFilter& op) {
	LogicalOperatorVisitor::Visit(op);
}

void PhysicalPlanGenerator::Visit(LogicalGet& op) {
	LogicalOperatorVisitor::Visit(op);
}

void PhysicalPlanGenerator::Visit(LogicalLimit& op) {
	LogicalOperatorVisitor::Visit(op);
}

void PhysicalPlanGenerator::Visit(LogicalOrder& op) {
	LogicalOperatorVisitor::Visit(op);
}
