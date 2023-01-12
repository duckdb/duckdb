#include "duckdb/optimizer/unnest_rewriter.hpp"

namespace duckdb {

void UnnestRewriter::FindCandidates(unique_ptr<LogicalOperator> *op_ptr,
                                 vector<unique_ptr<LogicalOperator> *> &candidates) {
	auto op = op_ptr->get();
	// search children before adding, so the deepest candidates get added first
	for (auto &child : op->children) {
		FindCandidates(&child, candidates);
	}

	// search for projection to rewrite
	if (op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}
	// followed by a delim join
	if (op->children[0]->type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		return;
	}

	// found a delim join, rhs child must be a projection
	auto &delim_join = *op->children[0];
	if (delim_join.children[1]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}

	// rhs projection of the delim join must have the UNNEST as its child
	auto &rhs_proj = *delim_join.children[1];
	if (rhs_proj.children[0]->type == LogicalOperatorType::LOGICAL_UNNEST) {
		candidates.push_back(op_ptr);
	}
	return;
}

unique_ptr<LogicalOperator> UnnestRewriter::Optimize(unique_ptr<LogicalOperator> op) {

	vector<unique_ptr<LogicalOperator> *> candidates;
	FindCandidates(&op, candidates);


	return op;
}

} // namespace duckdb
