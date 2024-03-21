#include "duckdb/optimizer/rule/join_dependent_filter.hpp"

#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

JoinDependentFilterRule::JoinDependentFilterRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// Match on a ConjunctionExpression that has at least two ConjunctionExpressions as children
	auto op = make_uniq<ConjunctionExpressionMatcher>();
	op->matchers.push_back(make_uniq<ConjunctionExpressionMatcher>());
	op->matchers.push_back(make_uniq<ConjunctionExpressionMatcher>());
	op->policy = SetMatcher::Policy::SOME;
	root = std::move(op);
}

static inline void ExpressionReferencesMultipleTablesRec(const Expression &binding, unordered_set<idx_t> &table_idxs) {
	ExpressionIterator::EnumerateChildren(binding, [&](const Expression &child) {
		if (child.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &colref = child.Cast<BoundColumnRefExpression>();
			table_idxs.insert(colref.binding.table_index);
		} else {
			ExpressionReferencesMultipleTablesRec(child, table_idxs);
		}
	});
}

static inline bool ExpressionReferencesMultipleTables(const Expression &binding) {
	unordered_set<idx_t> table_idxs;
	ExpressionReferencesMultipleTablesRec(binding, table_idxs);
	return table_idxs.size() > 1;
}

static inline void ExtractConjunctedExpressions(Expression &binding,
                                                expression_map_t<unique_ptr<Expression>> &expressions) {
	if (binding.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    binding.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &conjunction = binding.Cast<BoundConjunctionExpression>();
		for (auto &child : conjunction.children) {
			ExtractConjunctedExpressions(*child, expressions);
		}
	} else if (binding.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comparison = binding.Cast<BoundComparisonExpression>();
		if (comparison.left->IsFoldable() == comparison.right->IsFoldable()) {
			return; // Need exactly one foldable expression
		}

		// The non-foldable expression must not reference more than one table
		auto &non_foldable = comparison.left->IsFoldable() ? *comparison.right : *comparison.left;
		if (ExpressionReferencesMultipleTables(non_foldable)) {
			return;
		}

		// If there was already an expression, AND it together
		auto &expression = expressions[non_foldable];
		expression = expression ? make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
		                                                                std::move(expression), binding.Copy())
		                        : binding.Copy();
	}
}

unique_ptr<Expression> JoinDependentFilterRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                      bool &changes_made, bool is_root) {
	// Only applies to top-level FILTER expressions
	if ((op.type != LogicalOperatorType::LOGICAL_FILTER && op.type != LogicalOperatorType::LOGICAL_ANY_JOIN) ||
	    !is_root) {
		return nullptr;
	}

	// The expression must be an OR
	auto &conjunction = bindings[0].get().Cast<BoundConjunctionExpression>();
	if (conjunction.GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
		return nullptr;
	}

	// Must have at least two join-dependent AND expressions
	auto &children = conjunction.children;
	idx_t dependent_and_count = 0;
	for (const auto &child : children) {
		if (child->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
		    child->GetExpressionType() == ExpressionType::CONJUNCTION_AND &&
		    ExpressionReferencesMultipleTables(*child)) {
			dependent_and_count++;
		}
	}
	if (dependent_and_count < 2) {
		return nullptr;
	}

	// Extract all comparison expressions between column references and constants that are AND'ed together
	auto conjuncted_expressions = make_uniq_array<expression_map_t<unique_ptr<Expression>>>(children.size());
	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		conjuncted_expressions[child_idx] = expression_map_t<unique_ptr<Expression>>();
		ExtractConjunctedExpressions(*children[child_idx], conjuncted_expressions[child_idx]);
	}

	auto derived_filter = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	for (auto &entry : conjuncted_expressions[0]) {
		auto derived_entry_filter = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
		derived_entry_filter->children.push_back(entry.second->Copy());

		bool found = true;
		for (idx_t conj_idx = 1; conj_idx < children.size(); conj_idx++) {
			auto &other_entry = conjuncted_expressions[conj_idx];
			auto other_it = other_entry.find(entry.first);
			if (other_it == other_entry.end()) {
				found = false;
				break; // Expression does not appear in every conjuncted expression, cannot derive any restriction
			}
			derived_entry_filter->children.push_back(other_it->second->Copy());
		}

		if (!found) {
			continue; // Expression must show up in every entry
		}

		derived_filter->children.push_back(std::move(derived_entry_filter));
	}

	if (derived_filter->children.empty()) {
		return nullptr; // Could not derive filters that can be pushed down
	}

	// Add the derived expression to the original expression with an AND
	auto result = make_uniq_base<Expression, BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	auto &result_conjunction = result->Cast<BoundConjunctionExpression>();
	result_conjunction.children.push_back(conjunction.Copy());

	if (derived_filter->children.size() == 1) {
		result_conjunction.children.push_back(std::move(derived_filter->children[0]));
	} else {
		result_conjunction.children.push_back(std::move(derived_filter));
	}
	return result;
}

} // namespace duckdb
