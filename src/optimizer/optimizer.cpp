
#include "optimizer/optimizer.hpp"
#include "optimizer/expression_rules/rule_list.hpp"
#include "optimizer/logical_rules/rule_list.hpp"

#include "planner/operator/logical_list.hpp"

using namespace duckdb;
using namespace std;

Optimizer::Optimizer() : success(false) {
	rewriter.rules.push_back(make_unique_base<Rule, ConstantCastRule>());
	rewriter.rules.push_back(make_unique_base<Rule, ConstantFoldingRule>());
	rewriter.rules.push_back(make_unique_base<Rule, CrossProductRewrite>());
	rewriter.rules.push_back(make_unique_base<Rule, SelectionPushdownRule>());

	//	rewriter.rules.push_back(make_unique_base<Rule,
	// SubqueryRewritingRule>());
}

unique_ptr<LogicalOperator>
Optimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	success = false;
	try {
		// then we optimize the logical tree
		plan = rewriter.ApplyRules(move(plan));
		success = true;
		return plan;
	} catch (Exception &ex) {
		this->message = ex.GetMessage();
	} catch (...) {
		this->message = "UNHANDLED EXCEPTION TYPE THROWN IN PLANNER!";
	}
	return nullptr;
}
