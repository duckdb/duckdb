
#include "optimizer/optimizer.hpp"
#include "optimizer/rules/rule_list.hpp"

#include "planner/operator/logical_list.hpp"

using namespace duckdb;
using namespace std;

Optimizer::Optimizer() {
	rewriter.rules.push_back(
	    make_unique_base<OptimizerRule, ConstantFoldingRule>());
}

void Optimizer::RewriteList(vector<unique_ptr<AbstractExpression>> &list) {
	for (auto i = 0; i < list.size(); i++) {
		auto new_element = rewriter.ApplyRules(move(list[i]));
		new_element->ResolveStatistics();
		list[i] = move(new_element);
	}
}

unique_ptr<LogicalOperator>
Optimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	success = false;
	try {
		plan->Accept(this);
		success = true;
		return move(plan);
	} catch (Exception ex) {
		this->message = ex.GetMessage();
	} catch (...) {
		this->message = "UNHANDLED EXCEPTION TYPE THROWN IN PLANNER!";
	}
	return nullptr;
}

void Optimizer::Visit(LogicalAggregate &child) {
	LogicalOperatorVisitor::Visit(child);
	RewriteList(child.select_list);
}

void Optimizer::Visit(LogicalFilter &child) {
	LogicalOperatorVisitor::Visit(child);
	RewriteList(child.expressions);
}

void Optimizer::Visit(LogicalOrder &order) {
	auto &list = order.description.orders;
	for (auto i = 0; i < list.size(); i++) {

		auto new_element = rewriter.ApplyRules(move(list[i].expression));
		new_element->ResolveStatistics();
		list[i].expression = move(new_element);
	}
}

void Optimizer::Visit(LogicalProjection &child) {
	LogicalOperatorVisitor::Visit(child);
	RewriteList(child.select_list);
}
