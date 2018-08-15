
#include "optimizer/optimizer.hpp"
#include "optimizer/expression_rules/rule_list.hpp"
#include "optimizer/logical_rules/rule_list.hpp"

#include "planner/operator/logical_list.hpp"

using namespace duckdb;
using namespace std;

Optimizer::Optimizer() {
	rewriter.rules.push_back(
	    make_unique_base<ExpressionRule, ConstantCastRule>());
	rewriter.rules.push_back(
	    make_unique_base<ExpressionRule, ConstantFoldingRule>());

	logical_rewriter.rules.push_back(make_unique_base<LogicalRule, CrossProductRewrite>());
}

void Optimizer::RewriteList(vector<unique_ptr<AbstractExpression>> &list) {
	for (auto i = 0; i < list.size(); i++) {
		auto new_element = rewriter.ApplyRules(move(list[i]));
		list[i] = move(new_element);
	}
}

unique_ptr<LogicalOperator>
Optimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	success = false;
	try {
		// first we optimize all the expressions
		plan->Accept(this);
		// then we optimize the logical tree
		plan = logical_rewriter.ApplyRules(move(plan));

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
		list[i].expression = move(new_element);
	}
}

void Optimizer::Visit(LogicalProjection &child) {
	LogicalOperatorVisitor::Visit(child);
	RewriteList(child.select_list);
}
