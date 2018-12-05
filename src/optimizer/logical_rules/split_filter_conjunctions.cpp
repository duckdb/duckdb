#include "optimizer/logical_rules/split_filter_conjunction.hpp"
#include "planner/operator/logical_filter.hpp"

using namespace duckdb;
using namespace std;

SplitFilterConjunctionRule::SplitFilterConjunctionRule() {
	root = make_unique_base<AbstractRuleNode, LogicalNodeType>(LogicalOperatorType::FILTER);
}

unique_ptr<LogicalOperator> SplitFilterConjunctionRule::Apply(Rewriter &rewriter, LogicalOperator &root,
                                                              vector<AbstractOperator> &bindings, bool &fixed_point) {
	auto filter = (LogicalFilter *)bindings[0].value.op;
	if (filter->SplitPredicates()) {
		fixed_point = false;
	}
	return nullptr;
}
