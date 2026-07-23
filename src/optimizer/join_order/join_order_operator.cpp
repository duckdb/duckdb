#include "duckdb/optimizer/join_order/join_order_operator.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/optimizer/join_order/non_inner_join_edge.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

static bool Contains(const JoinRelationSet &super, const JoinRelationSet &sub) {
	if (sub.Empty()) {
		return true;
	}
	return JoinRelationSet::IsSubset(super, sub);
}

static bool ContainedInUnion(const JoinRelationSet &required, const JoinRelationSet &left,
                             const JoinRelationSet &right) {
	for (idx_t required_idx = 0; required_idx < required.count; required_idx++) {
		auto relation = required.relations[required_idx];
		bool found = false;
		for (idx_t left_idx = 0; left_idx < left.count; left_idx++) {
			if (left.relations[left_idx] == relation) {
				found = true;
				break;
			}
		}
		if (!found) {
			for (idx_t right_idx = 0; right_idx < right.count; right_idx++) {
				if (right.relations[right_idx] == relation) {
					found = true;
					break;
				}
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

static JoinRelationSet &Intersection(JoinRelationSetManager &set_manager, const JoinRelationSet &left,
                                     const JoinRelationSet &right) {
	unordered_set<RelationIndex> result;
	for (idx_t left_idx = 0; left_idx < left.count; left_idx++) {
		for (idx_t right_idx = 0; right_idx < right.count; right_idx++) {
			if (left.relations[left_idx] == right.relations[right_idx]) {
				result.insert(left.relations[left_idx]);
				break;
			}
		}
	}
	return set_manager.GetJoinRelation(result);
}

bool JoinOrderConflictDetector::IsCommutative(JoinOrderOperatorType type) {
	return type == JoinOrderOperatorType::INNER || type == JoinOrderOperatorType::CROSS_PRODUCT;
}

static JoinOrderOperatorType AlgebraicType(JoinOrderOperatorType type) {
	// A cross product is an INNER join carrying the degenerate TRUE predicate. Section 6.2 adds an additional
	// applicability check for it, while its algebraic transformation properties remain those of INNER.
	return type == JoinOrderOperatorType::CROSS_PRODUCT ? JoinOrderOperatorType::INNER : type;
}

static bool ContainsRelation(const JoinRelationSet &relations, RelationIndex relation) {
	for (idx_t relation_idx = 0; relation_idx < relations.count; relation_idx++) {
		if (relations.relations[relation_idx] == relation) {
			return true;
		}
	}
	return false;
}

static bool ColumnBecomesNull(const BoundColumnRefExpression &column, const JoinRelationSet &null_relations,
                              const unordered_map<TableIndex, RelationIndex> &relation_mapping) {
	if (column.Depth() != 0) {
		return false;
	}
	auto entry = relation_mapping.find(column.Binding().table_index);
	return entry != relation_mapping.end() && ContainsRelation(null_relations, entry->second);
}

static bool ExpressionBecomesNull(const Expression &expression, const JoinRelationSet &null_relations,
                                  const unordered_map<TableIndex, RelationIndex> &relation_mapping) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		return ColumnBecomesNull(expression.Cast<BoundColumnRefExpression>(), null_relations, relation_mapping);
	}
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &function = expression.Cast<BoundFunctionExpression>();
		if (function.Function().GetNullHandling() == FunctionNullHandling::SPECIAL_HANDLING) {
			return false;
		}
	}
	if (!expression.PropagatesNullValues()) {
		return false;
	}
	bool child_becomes_null = false;
	ExpressionIterator::EnumerateChildren(expression, [&](const Expression &child) {
		child_becomes_null |= ExpressionBecomesNull(child, null_relations, relation_mapping);
	});
	return child_becomes_null;
}

static bool ExpressionRejectsNulls(const Expression &expression, const JoinRelationSet &null_relations,
                                   const unordered_map<TableIndex, RelationIndex> &relation_mapping) {
	if (expression.GetExpressionType() == ExpressionType::CONJUNCTION_AND ||
	    expression.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		auto &conjunction = expression.Cast<BoundConjunctionExpression>();
		if (conjunction.GetChildren().empty()) {
			return false;
		}
		auto rejects = expression.GetExpressionType() == ExpressionType::CONJUNCTION_OR;
		for (auto &child : conjunction.GetChildren()) {
			if (expression.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
				rejects |= ExpressionRejectsNulls(*child, null_relations, relation_mapping);
			} else {
				rejects &= ExpressionRejectsNulls(*child, null_relations, relation_mapping);
			}
		}
		return rejects;
	}
	if (expression.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) {
		auto &op = expression.Cast<BoundOperatorExpression>();
		return !op.GetChildren().empty() &&
		       ExpressionBecomesNull(*op.GetChildren()[0], null_relations, relation_mapping);
	}
	if (BoundComparisonExpression::IsComparison(expression)) {
		if (expression.GetExpressionType() == ExpressionType::COMPARE_DISTINCT_FROM ||
		    expression.GetExpressionType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			return false;
		}
		auto &comparison = expression.Cast<BoundFunctionExpression>();
		return ExpressionBecomesNull(BoundComparisonExpression::Left(comparison), null_relations, relation_mapping) ||
		       ExpressionBecomesNull(BoundComparisonExpression::Right(comparison), null_relations, relation_mapping);
	}
	return expression.GetReturnType().id() == LogicalTypeId::BOOLEAN &&
	       ExpressionBecomesNull(expression, null_relations, relation_mapping);
}

static bool PredicateRejectsNulls(const vector<JoinCondition> &conditions, const JoinRelationSet &null_relations,
                                  const unordered_map<TableIndex, RelationIndex> &relation_mapping) {
	for (auto &condition : conditions) {
		if (condition.IsComparison()) {
			if (condition.GetComparisonType() == ExpressionType::COMPARE_DISTINCT_FROM ||
			    condition.GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
				continue;
			}
			if (ExpressionBecomesNull(condition.GetLHS(), null_relations, relation_mapping) ||
			    ExpressionBecomesNull(condition.GetRHS(), null_relations, relation_mapping)) {
				return true;
			}
		} else if (ExpressionRejectsNulls(condition.GetJoinExpression(), null_relations, relation_mapping)) {
			return true;
		}
	}
	return false;
}

static bool IsAssociative(const JoinOrderOperator &left, const JoinOrderOperator &right,
                          const JoinRelationSet &shared_relations,
                          const unordered_map<TableIndex, RelationIndex> &relation_mapping) {
	auto left_type = AlgebraicType(left.type);
	auto right_type = AlgebraicType(right.type);
	if (left_type == JoinOrderOperatorType::INNER) {
		return true;
	}
	if (left_type == JoinOrderOperatorType::SEMI && right_type == JoinOrderOperatorType::SEMI) {
		// Table 2, footnote 1: p23 must reject NULLs on the shared input e2.
		D_ASSERT(right.non_inner_join);
		return PredicateRejectsNulls(right.non_inner_join->conditions, shared_relations, relation_mapping);
	}
	return false;
}

static bool IsLeftAssociativeCommutative(const JoinOrderOperator &left, const JoinOrderOperator &right) {
	(void)left;
	(void)right;
	// For INNER, LEFT OUTER, ANTI, and SEMI joins every left associative-commutative entry in Table 3 is true.
	return true;
}

static bool IsRightAssociativeCommutative(const JoinOrderOperator &left, const JoinOrderOperator &right) {
	return AlgebraicType(left.type) == JoinOrderOperatorType::INNER &&
	       AlgebraicType(right.type) == JoinOrderOperatorType::INNER;
}

static void AddConflictRule(JoinOrderOperator &op, JoinRelationSet &trigger, JoinRelationSet &requirement) {
	D_ASSERT(!trigger.Empty());
	D_ASSERT(!requirement.Empty());
	for (auto &rule : op.conflict_rules) {
		if (&rule.trigger.get() == &trigger && &rule.requirement.get() == &requirement) {
			return;
		}
	}
	op.conflict_rules.emplace_back(trigger, requirement);
}

static JoinRelationSet &PredicateIntersectionOrInput(JoinRelationSetManager &set_manager, JoinRelationSet &input,
                                                     JoinRelationSet &syntactic_set) {
	auto &intersection = Intersection(set_manager, input, syntactic_set);
	return intersection.Empty() ? input : intersection;
}

static void BuildConflictRules(JoinOrderOperator &op, JoinRelationSetManager &set_manager,
                               const unordered_map<TableIndex, RelationIndex> &relation_mapping) {
	op.conflict_rules.clear();
	for (auto &child_ref : op.left_operators) {
		auto &child = child_ref.get();
		if (!IsAssociative(child, op, child.right_relations, relation_mapping)) {
			auto &requirement = PredicateIntersectionOrInput(set_manager, child.left_relations, child.syntactic_set);
			AddConflictRule(op, child.right_relations, requirement);
		}
		if (!IsLeftAssociativeCommutative(child, op)) {
			auto &requirement = PredicateIntersectionOrInput(set_manager, child.right_relations, child.syntactic_set);
			AddConflictRule(op, child.left_relations, requirement);
		}
	}
	for (auto &child_ref : op.right_operators) {
		auto &child = child_ref.get();
		if (!IsAssociative(op, child, child.left_relations, relation_mapping)) {
			auto &requirement = PredicateIntersectionOrInput(set_manager, child.right_relations, child.syntactic_set);
			AddConflictRule(op, child.left_relations, requirement);
		}
		if (!IsRightAssociativeCommutative(op, child)) {
			auto &requirement = PredicateIntersectionOrInput(set_manager, child.left_relations, child.syntactic_set);
			AddConflictRule(op, child.right_relations, requirement);
		}
	}
}

static void SimplifyConflictRules(JoinOrderOperator &op, JoinRelationSetManager &set_manager) {
	reference<JoinRelationSet> total_set = op.syntactic_set;
	bool changed;
	do {
		changed = false;
		for (auto &rule : op.conflict_rules) {
			if (JoinRelationSet::Intersects(total_set, rule.trigger) && !Contains(total_set, rule.requirement)) {
				total_set = set_manager.Union(total_set, rule.requirement);
				changed = true;
			}
		}
	} while (changed);
	op.total_set = total_set;

	vector<JoinOrderConflictRule> remaining_rules;
	for (auto &rule : op.conflict_rules) {
		if (!Contains(op.total_set, rule.requirement)) {
			remaining_rules.push_back(rule);
		}
	}
	op.conflict_rules = std::move(remaining_rules);
	op.left_total_set = Intersection(set_manager, op.total_set, op.left_relations);
	op.right_total_set = Intersection(set_manager, op.total_set, op.right_relations);
}

void JoinOrderConflictDetector::Build(vector<unique_ptr<JoinOrderOperator>> &operators,
                                      JoinRelationSetManager &set_manager,
                                      const unordered_map<TableIndex, RelationIndex> &relation_mapping) {
	for (auto &op : operators) {
		D_ASSERT(!JoinRelationSet::Intersects(op->left_relations, op->right_relations));
		BuildConflictRules(*op, set_manager, relation_mapping);
		SimplifyConflictRules(*op, set_manager);
		D_ASSERT(ContainedInUnion(op->total_set, op->left_relations, op->right_relations));
		for (auto &rule : op->conflict_rules) {
			D_ASSERT(ContainedInUnion(rule.trigger, op->left_relations, op->right_relations));
			D_ASSERT(ContainedInUnion(rule.requirement, op->left_relations, op->right_relations));
		}
		if (op->type != JoinOrderOperatorType::CROSS_PRODUCT) {
			D_ASSERT(!op->left_total_set.get().Empty());
			D_ASSERT(!op->right_total_set.get().Empty());
		}
		D_ASSERT(IsApplicable(*op, op->left_relations, op->right_relations));
	}
}

static bool ObeysConflictRules(const JoinOrderOperator &op, const JoinRelationSet &left, const JoinRelationSet &right) {
	for (auto &rule : op.conflict_rules) {
		if ((JoinRelationSet::Intersects(rule.trigger, left) || JoinRelationSet::Intersects(rule.trigger, right)) &&
		    !ContainedInUnion(rule.requirement, left, right)) {
			return false;
		}
	}
	return true;
}

bool JoinOrderConflictDetector::IsApplicable(const JoinOrderOperator &op, const JoinRelationSet &left,
                                             const JoinRelationSet &right) {
	bool direct = Contains(left, op.left_total_set) && Contains(right, op.right_total_set);
	bool inverted = IsCommutative(op.type) && Contains(left, op.right_total_set) && Contains(right, op.left_total_set);
	if (op.type == JoinOrderOperatorType::CROSS_PRODUCT) {
		// Section 6.2's conservative side condition keeps explicit cross products correct even though their SES is
		// empty.
		direct = direct && JoinRelationSet::Intersects(left, op.left_relations) &&
		         JoinRelationSet::Intersects(right, op.right_relations);
		inverted = inverted && JoinRelationSet::Intersects(left, op.right_relations) &&
		           JoinRelationSet::Intersects(right, op.left_relations);
	}
	return (direct || inverted) && ObeysConflictRules(op, left, right);
}

} // namespace duckdb
