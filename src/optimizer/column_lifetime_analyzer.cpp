#include "duckdb/optimizer/column_lifetime_analyzer.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/topn_optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

//! Peek through FILTER/PROJECTION/ORDER unary chains to detect the IN (...) MARK join InClauseRewriter produces.
//! (Broad DFS is unsafe — deeper MARK joins unrelated to this FILTER must keep projection maps intact.)
static const LogicalComparisonJoin *PeelUnaryChainToMarkJoin(const LogicalOperator *op) {
	while (op && op->children.size() == 1) {
		switch (op->type) {
		case LogicalOperatorType::LOGICAL_FILTER:
		case LogicalOperatorType::LOGICAL_PROJECTION:
		case LogicalOperatorType::LOGICAL_ORDER_BY:
			op = op->children[0].get();
			continue;
		default:
			op = nullptr;
			break;
		}
	}
	if (!op || op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return nullptr;
	}
	auto &comp_join = op->Cast<LogicalComparisonJoin>();
	return comp_join.join_type == JoinType::MARK ? &comp_join : nullptr;
}

static void GenerateProjectionMapByBindings(const vector<ColumnBinding> &current_bindings,
                                            const vector<ColumnBinding> &desired_output_bindings,
                                            vector<ProjectionIndex> &projection_map) {
	projection_map.clear();
	if (desired_output_bindings.empty()) {
		return;
	}
	projection_map.reserve(desired_output_bindings.size());
	for (const auto &desired_binding : desired_output_bindings) {
		bool found = false;
		for (idx_t i = 0; i < current_bindings.size(); i++) {
			if (current_bindings[i] != desired_binding) {
				continue;
			}
			projection_map.emplace_back(i);
			found = true;
			break;
		}
		if (!found) {
			// Child rewrites can remove or replace bindings. Fallback to identity map if we cannot express this
			// mapping.
			projection_map.clear();
			return;
		}
	}
	if (projection_map.size() == current_bindings.size()) {
		projection_map.clear();
	}
}

void ColumnLifetimeAnalyzer::ExtractUnusedColumnBindings(const vector<ColumnBinding> &bindings,
                                                         column_binding_set_t &unused_bindings) {
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (column_references.find(bindings[i]) == column_references.end()) {
			unused_bindings.insert(bindings[i]);
		}
	}
}

void ColumnLifetimeAnalyzer::GenerateProjectionMap(vector<ColumnBinding> bindings,
                                                   column_binding_set_t &unused_bindings,
                                                   vector<ProjectionIndex> &projection_map) {
	projection_map.clear();
	if (unused_bindings.empty()) {
		return;
	}
	// now iterate over the result bindings of the child
	for (idx_t i = 0; i < bindings.size(); i++) {
		// if this binding does not belong to the unused bindings, add it to the projection map
		if (unused_bindings.find(bindings[i]) == unused_bindings.end()) {
			projection_map.emplace_back(i);
		}
	}
	if (projection_map.size() == bindings.size()) {
		projection_map.clear();
	}
}

void ColumnLifetimeAnalyzer::StandardVisitOperator(LogicalOperator &op) {
	VisitOperatorExpressions(op);
	VisitOperatorChildren(op);
}

void ColumnLifetimeAnalyzer::ExtractColumnBindings(const Expression &expr, vector<ColumnBinding> &bindings) {
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expr, [&](const BoundColumnRefExpression &bound_ref) { bindings.push_back(bound_ref.binding); });
}

void ColumnLifetimeAnalyzer::VisitOperator(LogicalOperator &op) {
	Verify(op);
	if (TopN::CanOptimize(op) && op.children[0]->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		// Let's not mess with this, TopN is more important than projection maps
		// TopN does not support a projection map like Order does
		VisitOperatorExpressions(op);                        // Visit LIMIT
		VisitOperatorExpressions(*op.children[0]);           // Visit ORDER
		StandardVisitOperator(*op.children[0]->children[0]); // Recurse into child of ORDER
		return;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// FIXME: groups that are not referenced can be removed from projection
		// recurse into the children of the aggregate
		ColumnLifetimeAnalyzer analyzer(optimizer, root);
		analyzer.StandardVisitOperator(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &comp_join = op.Cast<LogicalComparisonJoin>();
		if (everything_referenced) {
			break;
		}

		// FIXME: for now, we only push into the projection map for equality (hash) joins
		idx_t has_range = 0;
		bool prefer_range_joins = Settings::Get<PreferRangeJoinsSetting>(optimizer.context);
		if (!comp_join.HasEquality(has_range) || prefer_range_joins) {
			return;
		}

		VisitOperatorExpressions(comp_join);

		// Pruning LHS via Join::left_projection_map for MARK joins can desynchronize PhysicalHashJoin
		// (lhs_output_in_probe vs probe chunk layouts) relative to LOGICAL_FILTERS/InClauseRewriter above/below —
		// especially with arrow scans and layered predicates (#22274, PR22382). Child operators may still prune
		// their own subgraph; MARK join forwards the full visited LHS/RHS layouts.
		if (comp_join.join_type == JoinType::MARK) {
			StandardVisitOperator(op);
			comp_join.left_projection_map.clear();
			comp_join.right_projection_map.clear();
			return;
		}

		column_binding_set_t lhs_unused;
		column_binding_set_t rhs_unused;
		const auto lhs_bindings_before_visit = op.children[0]->GetColumnBindings();
		const auto rhs_bindings_before_visit = op.children[1]->GetColumnBindings();
		ExtractUnusedColumnBindings(lhs_bindings_before_visit, lhs_unused);
		ExtractUnusedColumnBindings(rhs_bindings_before_visit, rhs_unused);

		vector<ColumnBinding> desired_lhs_bindings;
		desired_lhs_bindings.reserve(lhs_bindings_before_visit.size());
		for (const auto &binding : lhs_bindings_before_visit) {
			if (lhs_unused.find(binding) == lhs_unused.end()) {
				desired_lhs_bindings.push_back(binding);
			}
		}
		vector<ColumnBinding> desired_rhs_bindings;
		desired_rhs_bindings.reserve(rhs_bindings_before_visit.size());
		for (const auto &binding : rhs_bindings_before_visit) {
			if (rhs_unused.find(binding) == rhs_unused.end()) {
				desired_rhs_bindings.push_back(binding);
			}
		}

		StandardVisitOperator(op);

		// Remap projection maps against post-visit child bindings — child visitation can rewrite bindings,
		// so pairing pre-visit "unused" sets with post-visit column positions is insufficient (issue #22274).
		if (op.type != LogicalOperatorType::LOGICAL_ASOF_JOIN) {
			GenerateProjectionMapByBindings(op.children[0]->GetColumnBindings(), desired_lhs_bindings,
			                                comp_join.left_projection_map);
		}
		GenerateProjectionMapByBindings(op.children[1]->GetColumnBindings(), desired_rhs_bindings,
		                                comp_join.right_projection_map);
		return;
	}
	case LogicalOperatorType::LOGICAL_INSERT:
	case LogicalOperatorType::LOGICAL_UPDATE:
	case LogicalOperatorType::LOGICAL_DELETE:
	case LogicalOperatorType::LOGICAL_MERGE_INTO:
		//! When RETURNING is used, a PROJECTION is the top level operator for INSERTS, UPDATES, and DELETES
		//! We still need to project all values from these operators so the projection
		//! on top of them can select from only the table values being inserted.
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: {
		// for set operations/materialized CTEs we don't remove anything, just recursively visit the children
		// FIXME: for UNION we can remove unreferenced columns as long as everything_referenced is false (i.e. we
		// encounter a UNION node that is not preceded by a DISTINCT)
		ColumnLifetimeAnalyzer analyzer(optimizer, root, true);
		analyzer.StandardVisitOperator(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// then recurse into the children of this projection
		ColumnLifetimeAnalyzer analyzer(optimizer, root);
		analyzer.StandardVisitOperator(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto &order = op.Cast<LogicalOrder>();
		if (everything_referenced) {
			break;
		}

		column_binding_set_t unused_bindings;
		ExtractUnusedColumnBindings(op.children[0]->GetColumnBindings(), unused_bindings);

		StandardVisitOperator(op);

		GenerateProjectionMap(op.children[0]->GetColumnBindings(), unused_bindings, order.projection_map);
		return;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		// DISTINCT ON only references the expressions specified in the target list (and optional ORDER BY),
		auto &distinct = op.Cast<LogicalDistinct>();
		if (distinct.distinct_type == DistinctType::DISTINCT_ON) {
			auto add_bindings = [&](Expression &expr) {
				vector<ColumnBinding> bindings;
				ExtractColumnBindings(expr, bindings);
				for (auto &binding : bindings) {
					column_references.insert(binding);
				}
			};
			for (auto &target : distinct.distinct_targets) {
				if (target) {
					add_bindings(*target);
				}
			}
			if (distinct.order_by) {
				for (auto &order : distinct.order_by->orders) {
					if (order.expression) {
						add_bindings(*order.expression);
					}
				}
			}
			break;
		}
		// DISTINCT without targets references the entire projection list
		everything_referenced = true;
		break;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto &filter = op.Cast<LogicalFilter>();
		if (everything_referenced) {
			break;
		}

		// FILTER has two liveness roles (#22274): (1) output columns — only bindings referenced by operators *above*
		// this filter; (2) predicate columns — needed in the child's chunk to evaluate the filter but not necessarily
		// emitted. Snapshot (1) before visiting predicates so predicate-only columns (e.g. flag in flag < 1) do not
		// leak into the filter's projection_map.
		const column_binding_set_t refs_above_filter = column_references;

		// InClauseRewriter (large constant IN lists) uses MARK + LOGICAL_CHUNK_GET and may layer a FILTER that must
		// strip mark_index from propagated bindings — e.g. conjunct-splitting places ONLY "flag < 1" above an inner
		// filter that owns IN (...). Subquery IN uses MARK with a normal RHS plan; stripping mark here breaks filters
		// or projections that still reference the mark (predicate pushdown into CTEs — see
		// test/optimizer/pushdown/no_mark_to_semi_if_mark_index_is_projected.test).
		bool handled_in_clause_filter = false;
		if (const auto *peeked_mark = PeelUnaryChainToMarkJoin(op.children[0].get());
		    peeked_mark && peeked_mark->children.size() >= 2 &&
		    peeked_mark->children[1]->type == LogicalOperatorType::LOGICAL_CHUNK_GET) {
			VisitOperatorExpressions(op);
			const auto mark_tbl = peeked_mark->mark_index;
			VisitOperatorChildren(op);

			filter.projection_map.clear();
			const auto child_bindings_after = op.children[0]->GetColumnBindings();
			for (idx_t i = 0; i < child_bindings_after.size(); i++) {
				if (child_bindings_after[i].table_index != mark_tbl) {
					filter.projection_map.emplace_back(i);
				}
			}
			if (filter.projection_map.size() == child_bindings_after.size()) {
				filter.projection_map.clear();
			}
			handled_in_clause_filter = true;
		}
		if (!handled_in_clause_filter) {
			const auto child_bindings_before = op.children[0]->GetColumnBindings();
			vector<ColumnBinding> desired_output_bindings;
			desired_output_bindings.reserve(child_bindings_before.size());
			for (const auto &binding : child_bindings_before) {
				if (refs_above_filter.find(binding) != refs_above_filter.end()) {
					desired_output_bindings.push_back(binding);
				}
			}

			VisitOperatorExpressions(op);
			VisitOperatorChildren(op);
			GenerateProjectionMapByBindings(op.children[0]->GetColumnBindings(), desired_output_bindings,
			                                filter.projection_map);
		}
		return;
	}
	default:
		break;
	}
	StandardVisitOperator(op);
}

void ColumnLifetimeAnalyzer::Verify(LogicalOperator &op) {
	if (!Settings::Get<DebugVerificationProjectionSetting>(optimizer.context)) {
		return;
	}
	if (everything_referenced) {
		return;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		AddVerificationProjection(op.children[0]);
		if (op.Cast<LogicalComparisonJoin>().join_type != JoinType::MARK) { // Can't mess up the mark_index
			AddVerificationProjection(op.children[1]);
		}
		break;
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_FILTER:
		AddVerificationProjection(op.children[0]);
		break;
	default:
		break;
	}
}

void ColumnLifetimeAnalyzer::AddVerificationProjection(unique_ptr<LogicalOperator> &child) {
	child->ResolveOperatorTypes();
	const auto child_types = child->types;
	const auto child_bindings = child->GetColumnBindings();
	const auto column_count = child_bindings.size();

	// If our child has columns [i, j], we will generate a projection like so [NULL, j, NULL, i, NULL]
	const auto projection_column_count = column_count * 2 + 1;
	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(projection_column_count);

	// First fill with all NULLs
	for (idx_t col_idx = 0; col_idx < projection_column_count; col_idx++) {
		expressions.emplace_back(make_uniq<BoundConstantExpression>(Value(LogicalType::UTINYINT)));
	}

	// Now place the "real" columns in their respective positions, while keeping track of which column becomes which
	const auto table_index = optimizer.binder.GenerateTableIndex();
	ColumnBindingReplacer replacer;
	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		const auto &old_binding = child_bindings[col_idx];
		ProjectionIndex new_col_idx(projection_column_count - 2 - col_idx * 2);
		expressions[new_col_idx] = make_uniq<BoundColumnRefExpression>(child_types[col_idx], old_binding);
		replacer.replacement_bindings.emplace_back(old_binding, ColumnBinding(table_index, new_col_idx));
	}

	// Create a projection and swap the operators accordingly
	auto projection = make_uniq<LogicalProjection>(table_index, std::move(expressions));
	if (child->has_estimated_cardinality) {
		projection->SetEstimatedCardinality(child->estimated_cardinality);
	}
	projection->children.emplace_back(std::move(child));
	child = std::move(projection);

	// Replace references to the old binding (higher up in the plan) with references to the new binding
	replacer.stop_operator = child.get();
	replacer.VisitOperator(root);

	// Add new bindings to column_references, else they are considered "unused"
	for (const auto &replacement_binding : replacer.replacement_bindings) {
		if (column_references.find(replacement_binding.old_binding) != column_references.end()) {
			column_references.insert(replacement_binding.new_binding);
		}
	}
}

unique_ptr<Expression> ColumnLifetimeAnalyzer::VisitReplace(BoundColumnRefExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	column_references.insert(expr.binding);
	return nullptr;
}

unique_ptr<Expression> ColumnLifetimeAnalyzer::VisitReplace(BoundReferenceExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	// BoundReferenceExpression should not be used here yet, they only belong in the physical plan
	throw InternalException("BoundReferenceExpression should not be used here yet!");
}

} // namespace duckdb
