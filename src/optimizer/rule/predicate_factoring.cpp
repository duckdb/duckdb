#include "duckdb/optimizer/rule/predicate_factoring.hpp"

#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/column_binding_map.hpp"

namespace duckdb {

PredicateFactoringRule::PredicateFactoringRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// Match on a ConjunctionExpression that has at least one ConjunctionExpression as a child
	auto op = make_uniq<ConjunctionExpressionMatcher>();
	op->matchers.push_back(make_uniq<ConjunctionExpressionMatcher>());
	op->policy = SetMatcher::Policy::SOME;
	root = std::move(op);
}

static bool ExpressionIsDisjunction(const Expression &expr) {
	return expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	       expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR;
}

static void ExtractDisjunctedPredicates(Expression &expression, vector<reference<Expression>> &disjuncted_children) {
	if (ExpressionIsDisjunction(expression)) {
		auto &disjunction = expression.Cast<BoundConjunctionExpression>();
		for (auto &child : disjunction.GetChildren()) {
			ExtractDisjunctedPredicates(*child, disjuncted_children);
		}
	} else {
		disjuncted_children.push_back(expression);
	}
}

static bool ColumnBindingIsvalid(const ColumnBinding &column_binding) {
	return column_binding.table_index.IsValid() && column_binding.column_index.IsValid();
}

static bool GetSingleColumnBinding(const Expression &expr, ColumnBinding &column_binding) {
	if (expr.IsVolatile() || expr.CanThrow()) {
		return false;
	}

	column_binding = ColumnBinding();

	bool found_multiple = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &colref) {
		if (!ColumnBindingIsvalid(column_binding)) {
			column_binding = colref.Binding();
		} else if (column_binding != colref.Binding()) {
			found_multiple = true;
		}
	});

	return ColumnBindingIsvalid(column_binding) && !found_multiple;
}

static void ExtractDisjunctedPredicates(Expression &expression,
                                        column_binding_map_t<vector<reference<Expression>>> &binding_map);

static void ExtractConjunctedPredicates(BoundConjunctionExpression &conjunction,
                                        column_binding_map_t<vector<reference<Expression>>> &binding_map) {
	D_ASSERT(conjunction.GetExpressionType() == ExpressionType::CONJUNCTION_AND);
	for (auto &conjunction_child : conjunction.GetChildren()) {
		if (conjunction_child->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
		    conjunction_child->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
			ExtractConjunctedPredicates(conjunction_child->Cast<BoundConjunctionExpression>(), binding_map);
		} else {
			ExtractDisjunctedPredicates(*conjunction_child, binding_map);
		}
	}
}

static void ExtractDisjunctedPredicates(Expression &expression,
                                        column_binding_map_t<vector<reference<Expression>>> &binding_map) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expression.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		ExtractConjunctedPredicates(expression.Cast<BoundConjunctionExpression>(), binding_map);
	} else {
		ColumnBinding single_binding;
		if (GetSingleColumnBinding(expression, single_binding)) {
			binding_map[single_binding].push_back(expression);
		}
	}
}

//! The predicates one disjunct has on a column all hold together
static unique_ptr<Expression> CombineConjuncts(const vector<reference<Expression>> &predicates) {
	D_ASSERT(!predicates.empty());
	vector<unique_ptr<Expression>> children;
	for (auto &predicate : predicates) {
		auto copy = predicate.get().Copy();
		bool found = false;
		for (auto &child : children) {
			found = found || child->Equals(*copy);
		}
		if (!found) {
			children.push_back(std::move(copy));
		}
	}
	return BoundConjunctionExpression::Create(ExpressionType::CONJUNCTION_AND, std::move(children));
}

//! For every column that each disjunct restricts, the distinct restrictions the disjuncts place on it
static column_binding_map_t<vector<unique_ptr<Expression>>> GetDisjunctedPredicateMap(Expression &expression) {
	vector<reference<Expression>> disjuncted_children;
	ExtractDisjunctedPredicates(expression, disjuncted_children);
	D_ASSERT(disjuncted_children.size() > 1);

	column_binding_map_t<vector<unique_ptr<Expression>>> result;
	for (idx_t child_idx = 0; child_idx < disjuncted_children.size(); child_idx++) {
		auto &child = disjuncted_children[child_idx].get();
		D_ASSERT(!ExpressionIsDisjunction(child));
		column_binding_map_t<vector<reference<Expression>>> child_binding_map;
		ExtractDisjunctedPredicates(child, child_binding_map);

		if (child_idx == 0) {
			for (auto &entry : child_binding_map) {
				result[entry.first].push_back(CombineConjuncts(entry.second));
			}
			continue;
		}
		// Bindings must appear in every disjunct to be considered for predicate factoring
		for (auto it = result.begin(); it != result.end();) {
			auto child_it = child_binding_map.find(it->first);
			if (child_it == child_binding_map.end()) {
				it = result.erase(it);
				continue;
			}
			auto restriction = CombineConjuncts(child_it->second);
			bool found = false;
			for (auto &existing : it->second) {
				found = found || restriction->Equals(*existing);
			}
			if (!found) {
				it->second.push_back(std::move(restriction));
			}
			it++;
		}
	}
	return result;
}

unique_ptr<Expression> PredicateFactoringRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                     bool &changes_made, bool is_root) {
	// Only applies to top-level FILTER expressions
	if ((op.type != LogicalOperatorType::LOGICAL_FILTER && op.type != LogicalOperatorType::LOGICAL_ANY_JOIN) ||
	    !is_root) {
		return nullptr;
	}

	// The expression must be an OR TODO: could also implement some common expression extraction for AND
	if (!ExpressionIsDisjunction(bindings[0])) {
		return nullptr;
	}

	ColumnBinding column_binding;
	if (GetSingleColumnBinding(bindings[0], column_binding)) {
		return nullptr; // If it only applies to one column binding it can be pushed down already
	}

	// Extract map from single column binding to expression it is contained in
	auto binding_map = GetDisjunctedPredicateMap(bindings[0].get());
	if (binding_map.empty()) {
		return nullptr; // None qualify
	}

	unique_ptr<Expression> derived_filter;
	for (auto &entry : binding_map) {
		D_ASSERT(!entry.second.empty());

		// Create disjunction on single-column predicates
		auto column_filter =
		    BoundConjunctionExpression::Create(ExpressionType::CONJUNCTION_OR, std::move(entry.second));

		// Conjunct each single-column predicate together
		if (!derived_filter) {
			derived_filter = std::move(column_filter);
		} else {
			auto new_conjunction = make_uniq<BoundConjunctionExpression>(
			    ExpressionType::CONJUNCTION_AND, std::move(column_filter), std::move(derived_filter));
			derived_filter = std::move(new_conjunction);
		}
	}

	// Now add the derived filter as an AND to the original expression
	auto result = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, bindings[0].get().Copy(),
	                                                    std::move(derived_filter));
	return std::move(result);
}

} // namespace duckdb
