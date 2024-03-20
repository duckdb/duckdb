#include "duckdb/optimizer/rule/join_dependent_filter.hpp"

#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

namespace duckdb {

JoinDependentFilterRule::JoinDependentFilterRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// Match on a ConjunctionExpression that has two ConjunctionExpressions as children
	auto op = make_uniq<ConjunctionExpressionMatcher>();
	op->matchers.push_back(make_uniq<ConjunctionExpressionMatcher>());
	op->matchers.push_back(make_uniq<ConjunctionExpressionMatcher>());
	op->policy = SetMatcher::Policy::UNORDERED;
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
		ExtractConjunctedExpressions(*conjunction.children[0], expressions);
		ExtractConjunctedExpressions(*conjunction.children[1], expressions);
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

	// The expression must be an OR of AND expressions
	if ((bindings[0].get().GetExpressionType() != ExpressionType::CONJUNCTION_OR ||
	     bindings[1].get().GetExpressionType() != ExpressionType::CONJUNCTION_AND ||
	     bindings[2].get().GetExpressionType() != ExpressionType::CONJUNCTION_AND)) {
		return nullptr;
	}

	// Both sides of the OR must be join-dependent
	if (!ExpressionReferencesMultipleTables(bindings[1].get()) ||
	    !ExpressionReferencesMultipleTables(bindings[2].get())) {
		return nullptr;
	}

	// Extract all comparison expressions between column references and constants that are AND'ed together
	expression_map_t<unique_ptr<Expression>> lhs_expressions;
	ExtractConjunctedExpressions(bindings[1].get(), lhs_expressions);
	expression_map_t<unique_ptr<Expression>> rhs_expressions;
	ExtractConjunctedExpressions(bindings[2].get(), rhs_expressions);

	unique_ptr<Expression> derived_filter;
	for (auto &lhs_entry : lhs_expressions) {
		auto rhs_it = rhs_expressions.find(lhs_entry.first);
		if (rhs_it == rhs_expressions.end()) {
			continue; // LHS expression does not appear in RHS
		}

		auto derived_entry_filter = make_uniq<BoundConjunctionExpression>(
		    ExpressionType::CONJUNCTION_OR, lhs_entry.second->Copy(), rhs_it->second->Copy());
		if (derived_filter) {
			derived_filter = make_uniq<BoundConjunctionExpression>(
			    ExpressionType::CONJUNCTION_AND, std::move(derived_filter), std::move(derived_entry_filter));
		} else {
			derived_filter = std::move(derived_entry_filter);
		}
	}

	if (!derived_filter) {
		return nullptr; // Could not derive join-dependent filters that can be pushed down
	}

	// Add the derived expression to the original expression with an AND
	return make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, bindings[0].get().Copy(),
	                                             std::move(derived_filter));
}

} // namespace duckdb
