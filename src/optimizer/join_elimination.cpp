#include "duckdb/optimizer/join_elimination.hpp"

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

static void CollectExprReferences(Expression &expr, unordered_set<TableIndex> &ref_table_ids) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		ref_table_ids.insert(expr.Cast<BoundColumnRefExpression>().Binding().table_index);
	}
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](Expression &child) { CollectExprReferences(child, ref_table_ids); });
}

static void CollectReferences(LogicalOperator &op, unordered_set<TableIndex> &ref_table_ids) {
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](const unique_ptr<Expression> *expr_ptr) {
		if (expr_ptr && *expr_ptr) {
			CollectExprReferences(**expr_ptr, ref_table_ids);
		}
	});
}

//! Elimination for a specific side of the join
static bool TryEliminateSide(LogicalComparisonJoin &join, const unordered_set<TableIndex> &ref_table_ids,
                             bool outer_is_distinct, ConstraintPropagator &propagator, idx_t try_inner_idx,
                             idx_t try_outer_idx, idx_t &out_inner_idx, idx_t &out_outer_idx) {
	bool is_inner_semi_anti =
	    join.join_type == JoinType::INNER || join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI;

	auto try_inner_bindings = join.children[try_inner_idx]->GetColumnBindings();
	// Ensure join output columns only contain outer table columns
	for (auto &binding : try_inner_bindings) {
		if (ref_table_ids.find(binding.table_index) != ref_table_ids.end()) {
			return false;
		}
	}

	vector<ColumnBinding> inner_keys;
	vector<ColumnBinding> outer_keys;

	for (auto &cond : join.conditions) {
		if (!cond.IsComparison() || cond.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
			if (is_inner_semi_anti) {
				return false;
			}
			continue;
		}

		auto &inner_expr = (try_inner_idx == 0) ? cond.LeftReference() : cond.RightReference();
		auto &outer_expr = (try_inner_idx == 0) ? cond.RightReference() : cond.LeftReference();

		bool inner_is_col = inner_expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF;
		bool outer_is_col = outer_expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF;

		if (!inner_is_col || !outer_is_col) {
			if (is_inner_semi_anti) {
				return false;
			}
			continue;
		}

		inner_keys.push_back(inner_expr->Cast<BoundColumnRefExpression>().Binding());
		outer_keys.push_back(outer_expr->Cast<BoundColumnRefExpression>().Binding());
	}

	// Bail out if the inner child has a filter
	if (is_inner_semi_anti) {
		if (!inner_keys.empty()) {
			auto inner_props_it = propagator.properties_map.find(inner_keys[0].table_index);
			if (inner_props_it != propagator.properties_map.end() && inner_props_it->second.has_filter) {
				return false;
			}
		}
	}

	if (outer_is_distinct && (join.join_type == JoinType::LEFT || join.join_type == JoinType::RIGHT)) {
		out_inner_idx = try_inner_idx;
		out_outer_idx = try_outer_idx;
		return true;
	}

	if (!inner_keys.empty() && inner_keys.size() == outer_keys.size()) {
		if (join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI) {
			auto inner_props_it = propagator.properties_map.find(inner_keys[0].table_index);
			if (inner_props_it != propagator.properties_map.end() && inner_props_it->second.base_table) {
				auto inner_table_ptr = inner_props_it->second.base_table;
				Identifier inner_schema(inner_table_ptr->schema.name);
				Identifier inner_table_name = inner_table_ptr->name;
				if (propagator.IsForeignKey(outer_keys, inner_schema, inner_table_name) &&
				    propagator.IsNotNull(outer_keys)) {
					out_inner_idx = try_inner_idx;
					out_outer_idx = try_outer_idx;
					return true;
				}
			}
			return false;
		}

		if (propagator.IsKeyUnique(inner_keys)) {
			if (join.join_type == JoinType::INNER) {
				auto inner_props_it = propagator.properties_map.find(inner_keys[0].table_index);
				if (inner_props_it != propagator.properties_map.end() && inner_props_it->second.base_table) {
					auto inner_table_ptr = inner_props_it->second.base_table;
					Identifier inner_schema(inner_table_ptr->schema.name);
					Identifier inner_table_name(inner_table_ptr->name);

					bool is_fk = propagator.IsForeignKey(outer_keys, inner_schema, inner_table_name) &&
					             propagator.IsNotNull(outer_keys);

					bool is_self_join = false;
					if (!is_fk) {
						auto outer_props_it = propagator.properties_map.find(outer_keys[0].table_index);
						if (outer_props_it != propagator.properties_map.end() && outer_props_it->second.base_table) {
							if (inner_table_ptr == outer_props_it->second.base_table) {
								is_self_join = true;
							}
						}
					}

					if (is_fk || is_self_join) {
						out_inner_idx = try_inner_idx;
						out_outer_idx = try_outer_idx;
						return true;
					}
				}
			} else {
				out_inner_idx = try_inner_idx;
				out_outer_idx = try_outer_idx;
				return true;
			}
		}
	}
	return false;
}

unique_ptr<LogicalOperator> JoinElimination::Optimize(unique_ptr<LogicalOperator> op) {
	bool changed = true;
	// Keep running until no more joins can be eliminated
	while (changed) {
		changed = false;

		ConstraintPropagator propagator;
		propagator.VisitOperator(*op);

		unordered_set<TableIndex> ref_table_ids;
		op = OptimizeInternal(std::move(op), std::move(ref_table_ids), false, propagator, changed);
	}
	return op;
}

unique_ptr<LogicalOperator> JoinElimination::OptimizeInternal(unique_ptr<LogicalOperator> op,
                                                              unordered_set<TableIndex> ref_table_ids,
                                                              bool outer_is_distinct, ConstraintPropagator &propagator,
                                                              bool &changed) {
	if (op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		CollectReferences(*op, ref_table_ids);
	}

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		unordered_set<TableIndex> child_ref_table_ids = ref_table_ids;
		CollectReferences(*op, child_ref_table_ids);

		op->children[0] =
		    OptimizeInternal(std::move(op->children[0]), child_ref_table_ids, outer_is_distinct, propagator, changed);
		op->children[1] = OptimizeInternal(std::move(op->children[1]), child_ref_table_ids, false, propagator, changed);

		auto original_op_ptr = op.get();
		auto new_op = TryEliminateJoin(std::move(op), ref_table_ids, outer_is_distinct, propagator);
		if (new_op.get() != original_op_ptr) {
			changed = true;
		}
		return new_op;
	}

	// Top-down distinct context
	bool child_is_distinct = outer_is_distinct;
	if (op->type == LogicalOperatorType::LOGICAL_DISTINCT) {
		child_is_distinct = true;
	} else if (op->type != LogicalOperatorType::LOGICAL_PROJECTION && op->type != LogicalOperatorType::LOGICAL_FILTER) {
		child_is_distinct = false;
	}

	for (auto &child : op->children) {
		child = OptimizeInternal(std::move(child), ref_table_ids, child_is_distinct, propagator, changed);
	}

	return op;
}

unique_ptr<LogicalOperator> JoinElimination::TryEliminateJoin(unique_ptr<LogicalOperator> op,
                                                              const unordered_set<TableIndex> &ref_table_ids,
                                                              bool outer_is_distinct,
                                                              ConstraintPropagator &propagator) {
	auto &join = op->Cast<LogicalComparisonJoin>();

	bool is_output_unique = false;
	idx_t inner_idx = 1;
	idx_t outer_idx = 0;

	switch (join.join_type) {
	case JoinType::INNER:
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::LEFT:
		break;
	case JoinType::SINGLE:
		is_output_unique = true;
		break;
	case JoinType::RIGHT:
		inner_idx = 0;
		outer_idx = 1;
		break;
	default:
		return op;
	}

	if (join.filter_pushdown) {
		return op;
	}

	// Try to eliminate the default inner side
	if (TryEliminateSide(join, ref_table_ids, outer_is_distinct, propagator, inner_idx, outer_idx, inner_idx,
	                     outer_idx)) {
		is_output_unique = true;
	} else if (join.join_type == JoinType::INNER) {
		// If it's an INNER join and the first attempt failed, try the other side
		idx_t swapped_inner = (inner_idx == 1) ? 0 : 1;
		idx_t swapped_outer = (outer_idx == 1) ? 0 : 1;
		if (TryEliminateSide(join, ref_table_ids, outer_is_distinct, propagator, swapped_inner, swapped_outer,
		                     inner_idx, outer_idx)) {
			is_output_unique = true;
		}
	}

	if (is_output_unique) {
		// ANTI join returns an empty result if eliminated
		if (join.join_type == JoinType::ANTI) {
			auto false_filter = make_uniq<LogicalFilter>();
			false_filter->expressions.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(false)));
			false_filter->children.push_back(std::move(op->children[outer_idx]));
			return std::move(false_filter);
		}
		return std::move(op->children[outer_idx]);
	}

	return op;
}

} // namespace duckdb
