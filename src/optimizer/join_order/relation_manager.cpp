#include "duckdb/optimizer/join_order/relation_manager.hpp"

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/relation_statistics_helper.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

const vector<RelationStats> RelationManager::GetRelationStats() {
	vector<RelationStats> ret;
	for (idx_t i = 0; i < relations.size(); i++) {
		ret.push_back(relations[i]->stats);
	}
	return ret;
}

vector<unique_ptr<SingleJoinRelation>> RelationManager::GetRelations() {
	return std::move(relations);
}

idx_t RelationManager::NumRelations() {
	return relations.size();
}

void RelationManager::AddAggregateOrWindowRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent,
                                                   const RelationStats &stats, LogicalOperatorType op_type) {
	auto relation = make_uniq<SingleJoinRelation>(op, parent, stats);
	auto relation_id = relations.size();

	auto op_bindings = op.GetColumnBindings();
	for (auto &binding : op_bindings) {
		if (relation_mapping.find(binding.table_index) == relation_mapping.end()) {
			relation_mapping[binding.table_index] = relation_id;
		}
	}
	relations.push_back(std::move(relation));
	op.estimated_cardinality = stats.cardinality;
	op.has_estimated_cardinality = true;
}

void RelationManager::AddRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent,
                                  const RelationStats &stats) {

	// if parent is null, then this is a root relation
	// if parent is not null, it should have multiple children
	D_ASSERT(!parent || parent->children.size() >= 2);
	auto relation = make_uniq<SingleJoinRelation>(op, parent, stats);
	auto relation_id = relations.size();

	auto table_indexes = op.GetTableIndex();
	if (table_indexes.empty()) {
		// relation represents a non-reorderable relation, most likely a join relation
		// Get the tables referenced in the non-reorderable relation and add them to the relation mapping
		// This should all table references, even if there are nested non-reorderable joins.
		unordered_set<idx_t> table_references;
		LogicalJoin::GetTableReferences(op, table_references);
		D_ASSERT(table_references.size() > 0);
		for (auto &reference : table_references) {
			D_ASSERT(relation_mapping.find(reference) == relation_mapping.end());
			relation_mapping[reference] = relation_id;
		}
	} else if (op.type == LogicalOperatorType::LOGICAL_UNNEST) {
		// logical unnest has a logical_unnest index, but other bindings can refer to
		// columns that are not unnested.
		auto bindings = op.GetColumnBindings();
		for (auto &binding : bindings) {
			relation_mapping[binding.table_index] = relation_id;
		}
	} else {
		// Relations should never return more than 1 table index
		D_ASSERT(table_indexes.size() == 1);
		idx_t table_index = table_indexes.at(0);
		D_ASSERT(relation_mapping.find(table_index) == relation_mapping.end());
		relation_mapping[table_index] = relation_id;
	}
	relations.push_back(std::move(relation));
	op.estimated_cardinality = stats.cardinality;
	op.has_estimated_cardinality = true;
}

bool RelationManager::CrossProductWithRelationAllowed(idx_t relation_id) {
	return no_cross_product_relations.find(relation_id) == no_cross_product_relations.end();
}

static bool OperatorNeedsRelation(LogicalOperatorType op_type) {
	switch (op_type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_SAMPLE:
		return true;
	default:
		return false;
	}
}

static bool OperatorIsNonReorderable(LogicalOperatorType op_type) {
	switch (op_type) {
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		return true;
	default:
		return false;
	}
}

bool ExpressionContainsColumnRef(Expression &expression) {
	if (expression.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		// Here you have a filter on a single column in a table. Return a binding for the column
		// being filtered on so the filter estimator knows what HLL count to pull
#ifdef DEBUG
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		(void)colref.depth;
		D_ASSERT(colref.depth == 0);
		D_ASSERT(colref.binding.table_index != DConstants::INVALID_INDEX);
#endif
		// map the base table index to the relation index used by the JoinOrderOptimizer
		return true;
	}
	// TODO: handle inequality filters with functions.
	auto children_ret = false;
	ExpressionIterator::EnumerateChildren(expression,
	                                      [&](Expression &expr) { children_ret = ExpressionContainsColumnRef(expr); });
	return children_ret;
}

static bool JoinIsReorderable(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		return true;
	} else if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		switch (join.join_type) {
		case JoinType::INNER:
		case JoinType::SEMI:
		case JoinType::ANTI:
			for (auto &cond : join.conditions) {
				if (ExpressionContainsColumnRef(*cond.left) && ExpressionContainsColumnRef(*cond.right)) {
					return true;
				}
			}
			return false;
		default:
			return false;
		}
	}
	return false;
}

static bool HasNonReorderableChild(LogicalOperator &op) {
	LogicalOperator *tmp = &op;
	while (tmp->children.size() == 1) {
		if (OperatorNeedsRelation(tmp->type) || OperatorIsNonReorderable(tmp->type)) {
			return true;
		}
		tmp = tmp->children[0].get();
		if (tmp->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			if (!JoinIsReorderable(*tmp)) {
				return true;
			}
		}
	}
	return tmp->children.empty();
}

static void ModifyStatsIfLimit(optional_ptr<LogicalOperator> limit_op, RelationStats &stats) {
	if (!limit_op) {
		return;
	}
	auto &limit = limit_op->Cast<LogicalLimit>();
	if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
		stats.cardinality = MinValue(limit.limit_val.GetConstantValue(), stats.cardinality);
	}
}

bool RelationManager::ExtractJoinRelations(JoinOrderOptimizer &optimizer, LogicalOperator &input_op,
                                           vector<reference<LogicalOperator>> &filter_operators,
                                           optional_ptr<LogicalOperator> parent) {
	optional_ptr<LogicalOperator> op = &input_op;
	vector<reference<LogicalOperator>> datasource_filters;
	optional_ptr<LogicalOperator> limit_op = nullptr;
	// pass through single child operators
	while (op->children.size() == 1 && !OperatorNeedsRelation(op->type)) {
		if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
			if (HasNonReorderableChild(*op)) {
				datasource_filters.push_back(*op);
			}
			filter_operators.push_back(*op);
		}
		if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
			limit_op = op;
		}
		op = op->children[0].get();
	}
	bool non_reorderable_operation = false;
	if (OperatorIsNonReorderable(op->type)) {
		// set operation, optimize separately in children
		non_reorderable_operation = true;
	}

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		if (JoinIsReorderable(*op)) {
			// extract join conditions from inner join
			filter_operators.push_back(*op);
		} else {
			non_reorderable_operation = true;
		}
	}
	if (non_reorderable_operation) {
		// we encountered a non-reordable operation (setop or non-inner join)
		// we do not reorder non-inner joins yet, however we do want to expand the potential join graph around them
		// non-inner joins are also tricky because we can't freely make conditions through them
		// e.g. suppose we have (left LEFT OUTER JOIN right WHERE right IS NOT NULL), the join can generate
		// new NULL values in the right side, so pushing this condition through the join leads to incorrect results
		// for this reason, we just start a new JoinOptimizer pass in each of the children of the join
		// stats.cardinality will be initiated to highest cardinality of the children.
		vector<RelationStats> children_stats;
		for (auto &child : op->children) {
			auto stats = RelationStats();
			auto child_optimizer = optimizer.CreateChildOptimizer();
			child = child_optimizer.Optimize(std::move(child), &stats);
			children_stats.push_back(stats);
		}

		auto combined_stats = RelationStatisticsHelper::CombineStatsOfNonReorderableOperator(*op, children_stats);
		op->SetEstimatedCardinality(combined_stats.cardinality);
		if (!datasource_filters.empty()) {
			combined_stats.cardinality = (idx_t)MaxValue(
			    double(combined_stats.cardinality) * RelationStatisticsHelper::DEFAULT_SELECTIVITY, (double)1);
		}
		AddRelation(input_op, parent, combined_stats);
		return true;
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// optimize children
		RelationStats child_stats;
		auto child_optimizer = optimizer.CreateChildOptimizer();
		op->children[0] = child_optimizer.Optimize(std::move(op->children[0]), &child_stats);
		auto &aggr = op->Cast<LogicalAggregate>();
		auto operator_stats = RelationStatisticsHelper::ExtractAggregationStats(aggr, child_stats);
		// the extracted cardinality should be set for aggregate
		aggr.SetEstimatedCardinality(operator_stats.cardinality);
		if (!datasource_filters.empty()) {
			operator_stats.cardinality = LossyNumericCast<idx_t>(static_cast<double>(operator_stats.cardinality) *
			                                                     RelationStatisticsHelper::DEFAULT_SELECTIVITY);
		}
		ModifyStatsIfLimit(limit_op.get(), child_stats);
		AddAggregateOrWindowRelation(input_op, parent, operator_stats, op->type);
		return true;
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		// optimize children
		RelationStats child_stats;
		auto child_optimizer = optimizer.CreateChildOptimizer();
		op->children[0] = child_optimizer.Optimize(std::move(op->children[0]), &child_stats);
		auto &window = op->Cast<LogicalWindow>();
		auto operator_stats = RelationStatisticsHelper::ExtractWindowStats(window, child_stats);
		// the extracted cardinality should be set for window
		window.SetEstimatedCardinality(operator_stats.cardinality);
		if (!datasource_filters.empty()) {
			operator_stats.cardinality = LossyNumericCast<idx_t>(static_cast<double>(operator_stats.cardinality) *
			                                                     RelationStatisticsHelper::DEFAULT_SELECTIVITY);
		}
		ModifyStatsIfLimit(limit_op.get(), child_stats);
		AddAggregateOrWindowRelation(input_op, parent, operator_stats, op->type);
		return true;
	}
	case LogicalOperatorType::LOGICAL_UNNEST: {
		// optimize children of unnest
		RelationStats child_stats;
		auto child_optimizer = optimizer.CreateChildOptimizer();
		op->children[0] = child_optimizer.Optimize(std::move(op->children[0]), &child_stats);
		// the extracted cardinality should be set for window
		if (!datasource_filters.empty()) {
			child_stats.cardinality = LossyNumericCast<idx_t>(static_cast<double>(child_stats.cardinality) *
			                                                  RelationStatisticsHelper::DEFAULT_SELECTIVITY);
		}
		ModifyStatsIfLimit(limit_op.get(), child_stats);
		AddRelation(input_op, parent, child_stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op->Cast<LogicalComparisonJoin>();
		// Adding relations of the left side to the current join order optimizer
		bool can_reorder_left = ExtractJoinRelations(optimizer, *op->children[0], filter_operators, op);
		bool can_reorder_right = true;
		// For semi & anti joins, you only reorder relations in the left side of the join.
		// We do not want to reorder a relation A into the right side because then all column bindings A from A will be
		// lost after the semi or anti join

		// We cannot reorder a relation B out of the right side because any filter/join in the right side
		// between a relation B and another RHS relation will be invalid. The semi join will remove
		// all right column bindings,

		// So we treat the right side of left join as its own relation so no relations
		// are pushed into the right side, or taken out of the right side.
		if (join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI) {
			RelationStats child_stats;
			// optimize the child and copy the stats
			auto child_optimizer = optimizer.CreateChildOptimizer();
			op->children[1] = child_optimizer.Optimize(std::move(op->children[1]), &child_stats);
			AddRelation(*op->children[1], op, child_stats);
			// remember that if a cross product needs to be forced, it cannot be forced
			// across the children of a semi or anti join
			no_cross_product_relations.insert(relations.size() - 1);
			auto right_child_bindings = op->children[1]->GetColumnBindings();
			for (auto &bindings : right_child_bindings) {
				relation_mapping[bindings.table_index] = relations.size() - 1;
			}
		} else {
			can_reorder_right = ExtractJoinRelations(optimizer, *op->children[1], filter_operators, op);
		}
		return can_reorder_left && can_reorder_right;
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		bool can_reorder_left = ExtractJoinRelations(optimizer, *op->children[0], filter_operators, op);
		bool can_reorder_right = ExtractJoinRelations(optimizer, *op->children[1], filter_operators, op);
		return can_reorder_left && can_reorder_right;
	}
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN: {
		auto &dummy_scan = op->Cast<LogicalDummyScan>();
		auto stats = RelationStatisticsHelper::ExtractDummyScanStats(dummy_scan, context);
		AddRelation(input_op, parent, stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		// base table scan, add to set of relations.
		// create empty stats for dummy scan or logical expression get
		auto &expression_get = op->Cast<LogicalExpressionGet>();
		auto stats = RelationStatisticsHelper::ExtractExpressionGetStats(expression_get, context);
		AddRelation(input_op, parent, stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_GET: {
		// TODO: Get stats from a logical GET
		auto &get = op->Cast<LogicalGet>();
		auto stats = RelationStatisticsHelper::ExtractGetStats(get, context);
		// if there is another logical filter that could not be pushed down into the
		// table scan, apply another selectivity.
		get.SetEstimatedCardinality(stats.cardinality);
		if (!datasource_filters.empty()) {
			stats.cardinality =
			    (idx_t)MaxValue(double(stats.cardinality) * RelationStatisticsHelper::DEFAULT_SELECTIVITY, (double)1);
		}
		ModifyStatsIfLimit(limit_op.get(), stats);
		AddRelation(input_op, parent, stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		RelationStats child_stats;
		// optimize the child and copy the stats
		auto child_optimizer = optimizer.CreateChildOptimizer();
		op->children[0] = child_optimizer.Optimize(std::move(op->children[0]), &child_stats);
		auto &proj = op->Cast<LogicalProjection>();
		// Projection can create columns so we need to add them here
		auto proj_stats = RelationStatisticsHelper::ExtractProjectionStats(proj, child_stats);
		proj.SetEstimatedCardinality(proj_stats.cardinality);
		ModifyStatsIfLimit(limit_op.get(), proj_stats);
		AddRelation(input_op, parent, proj_stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT: {
		// optimize the child and copy the stats
		auto &empty_result = op->Cast<LogicalEmptyResult>();
		// Projection can create columns so we need to add them here
		auto stats = RelationStatisticsHelper::ExtractEmptyResultStats(empty_result);
		empty_result.SetEstimatedCardinality(stats.cardinality);
		AddRelation(input_op, parent, stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE: {
		RelationStats lhs_stats;
		// optimize the lhs child and copy the stats
		auto lhs_optimizer = optimizer.CreateChildOptimizer();
		op->children[0] = lhs_optimizer.Optimize(std::move(op->children[0]), &lhs_stats);
		// optimize the rhs child
		auto rhs_optimizer = optimizer.CreateChildOptimizer();
		auto table_index = op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE
		                       ? op->Cast<LogicalMaterializedCTE>().table_index
		                       : op->Cast<LogicalRecursiveCTE>().table_index;
		rhs_optimizer.AddMaterializedCTEStats(table_index, std::move(lhs_stats));
		op->children[1] = rhs_optimizer.Optimize(std::move(op->children[1]));
		return false;
	}
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		auto &cte_ref = op->Cast<LogicalCTERef>();
		if (cte_ref.materialized_cte != CTEMaterialize::CTE_MATERIALIZE_ALWAYS) {
			return false;
		}
		auto cte_stats = optimizer.GetMaterializedCTEStats(cte_ref.cte_index);
		cte_ref.SetEstimatedCardinality(cte_stats.cardinality);
		AddRelation(input_op, parent, cte_stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		auto &delim_join = op->Cast<LogicalComparisonJoin>();

		// optimize LHS (duplicate-eliminated) child
		RelationStats lhs_stats;
		auto lhs_optimizer = optimizer.CreateChildOptimizer();
		op->children[0] = lhs_optimizer.Optimize(std::move(op->children[0]), &lhs_stats);

		// create dummy aggregation for the duplicate elimination
		auto dummy_aggr = make_uniq<LogicalAggregate>(DConstants::INVALID_INDEX - 1, DConstants::INVALID_INDEX,
		                                              vector<unique_ptr<Expression>>());
		for (auto &delim_col : delim_join.duplicate_eliminated_columns) {
			dummy_aggr->groups.push_back(delim_col->Copy());
		}
		auto lhs_delim_stats = RelationStatisticsHelper::ExtractAggregationStats(*dummy_aggr, lhs_stats);

		// optimize the other child, which will now have access to the stats
		RelationStats rhs_stats;
		auto rhs_optimizer = optimizer.CreateChildOptimizer();
		rhs_optimizer.AddDelimScanStats(lhs_delim_stats);
		op->children[1] = rhs_optimizer.Optimize(std::move(op->children[1]), rhs_stats);

		return false;
	}
	case LogicalOperatorType::LOGICAL_DELIM_GET: {
		// Used to not be possible to reorder these. We added reordering (without stats) before,
		// but ran into terrible join orders (see internal issue #596), so we removed it again
		// We now have proper statistics for DelimGets, and get an even better query plan for #596
		auto delim_scan_stats = optimizer.GetDelimScanStats();
		op->SetEstimatedCardinality(delim_scan_stats.cardinality);
		AddAggregateOrWindowRelation(input_op, parent, delim_scan_stats, op->type);
		return true;
	}
	default:
		return false;
	}
}

bool RelationManager::ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings) {
	if (expression.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);
		D_ASSERT(colref.binding.table_index != DConstants::INVALID_INDEX);
		// map the base table index to the relation index used by the JoinOrderOptimizer
		if (expression.GetAlias() == "SUBQUERY" &&
		    relation_mapping.find(colref.binding.table_index) == relation_mapping.end()) {
			// most likely a BoundSubqueryExpression that was created from an uncorrelated subquery
			// Here we return true and don't fill the bindings, the expression can be reordered.
			// A filter will be created using this expression, and pushed back on top of the parent
			// operator during plan reconstruction
			return true;
		}
		if (relation_mapping.find(colref.binding.table_index) != relation_mapping.end()) {
			bindings.insert(relation_mapping[colref.binding.table_index]);
		}
	}
	if (expression.GetExpressionType() == ExpressionType::BOUND_REF) {
		// bound expression
		bindings.clear();
		return false;
	}
	D_ASSERT(expression.GetExpressionType() != ExpressionType::SUBQUERY);
	bool can_reorder = true;
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &expr) {
		if (!ExtractBindings(expr, bindings)) {
			can_reorder = false;
			return;
		}
	});
	return can_reorder;
}

vector<unique_ptr<FilterInfo>> RelationManager::ExtractEdges(LogicalOperator &op,
                                                             vector<reference<LogicalOperator>> &filter_operators,
                                                             JoinRelationSetManager &set_manager) {
	// now that we know we are going to perform join ordering we actually extract the filters, eliminating duplicate
	// filters in the process
	vector<unique_ptr<FilterInfo>> filters_and_bindings;
	expression_set_t filter_set;
	for (auto &filter_op : filter_operators) {
		auto &f_op = filter_op.get();
		if (f_op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		    f_op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
			auto &join = f_op.Cast<LogicalComparisonJoin>();
			D_ASSERT(join.expressions.empty());
			if (join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI) {

				auto conjunction_expression = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
				// create a conjunction expression for the semi join.
				// It's possible multiple LHS relations have a condition in
				// this semi join. Suppose we have ((A ⨝ B) ⋉ C). (example in test_4950.test)
				// If the semi join condition has A.x = C.y AND B.x = C.z then we need to prevent a reordering
				// that looks like ((A ⋉ C) ⨝ B)), since all columns from C will be lost after it joins with A,
				// and the condition B.x = C.z will no longer be possible.
				// if we make a conjunction expressions and populate the left set and right set with all
				// the relations from the conditions in the conjunction expression, we can prevent invalid
				// reordering.
				for (auto &cond : join.conditions) {
					auto comparison = make_uniq<BoundComparisonExpression>(cond.comparison, std::move(cond.left),
					                                                       std::move(cond.right));
					conjunction_expression->children.push_back(std::move(comparison));
				}

				// create the filter info so all required LHS relations are present when reconstructing the
				// join
				optional_ptr<JoinRelationSet> left_set;
				optional_ptr<JoinRelationSet> right_set;
				optional_ptr<JoinRelationSet> full_set;
				// here we create a left_set that unions all relations from the left side of
				// every expression and a right_set that unions all relations frmo the right side of a
				// every expression (although this should always be 1).
				for (auto &bound_expr : conjunction_expression->children) {
					D_ASSERT(bound_expr->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
					auto &comp = bound_expr->Cast<BoundComparisonExpression>();
					unordered_set<idx_t> right_bindings, left_bindings;
					ExtractBindings(*comp.right, right_bindings);
					ExtractBindings(*comp.left, left_bindings);

					if (!left_set) {
						left_set = set_manager.GetJoinRelation(left_bindings);
					} else {
						left_set = set_manager.Union(set_manager.GetJoinRelation(left_bindings), *left_set);
					}
					if (!right_set) {
						right_set = set_manager.GetJoinRelation(right_bindings);
					} else {
						right_set = set_manager.Union(set_manager.GetJoinRelation(right_bindings), *right_set);
					}
				}
				full_set = set_manager.Union(*left_set, *right_set);
				D_ASSERT(left_set && left_set->count > 0);
				D_ASSERT(right_set && right_set->count == 1);
				D_ASSERT(full_set && full_set->count > 0);

				// now we push the conjunction expressions
				// In QueryGraphManager::GenerateJoins we extract each condition again and create a standalone join
				// condition.
				auto filter_info = make_uniq<FilterInfo>(std::move(conjunction_expression), *full_set,
				                                         filters_and_bindings.size(), join.join_type);
				filter_info->SetLeftSet(left_set);
				filter_info->SetRightSet(right_set);

				filters_and_bindings.push_back(std::move(filter_info));
			} else {
				// can extract every inner join condition individually.
				for (auto &cond : join.conditions) {
					auto comparison = make_uniq<BoundComparisonExpression>(cond.comparison, std::move(cond.left),
					                                                       std::move(cond.right));
					if (filter_set.find(*comparison) == filter_set.end()) {
						filter_set.insert(*comparison);
						unordered_set<idx_t> bindings;
						ExtractBindings(*comparison, bindings);
						auto &set = set_manager.GetJoinRelation(bindings);
						auto filter_info = make_uniq<FilterInfo>(std::move(comparison), set,
						                                         filters_and_bindings.size(), join.join_type);
						filters_and_bindings.push_back(std::move(filter_info));
					}
				}
			}
			join.conditions.clear();
		} else {
			vector<unique_ptr<Expression>> leftover_expressions;
			for (auto &expression : f_op.expressions) {
				if (filter_set.find(*expression) == filter_set.end()) {
					filter_set.insert(*expression);
					unordered_set<idx_t> bindings;
					ExtractBindings(*expression, bindings);
					if (bindings.empty()) {
						// the filter is on a column that is not in our relational map. (example: limit_rownum)
						// in this case we do not create a FilterInfo for it. (duckdb-internal/#1493)s
						leftover_expressions.push_back(std::move(expression));
						continue;
					}
					auto &set = set_manager.GetJoinRelation(bindings);
					auto filter_info = make_uniq<FilterInfo>(std::move(expression), set, filters_and_bindings.size());
					filters_and_bindings.push_back(std::move(filter_info));
				}
			}
			f_op.expressions = std::move(leftover_expressions);
		}
	}

	return filters_and_bindings;
}

// LCOV_EXCL_START

void RelationManager::PrintRelationStats() {
#ifdef DEBUG
	string to_print;
	for (idx_t i = 0; i < relations.size(); i++) {
		auto &relation = relations.at(i);
		auto &stats = relation->stats;
		D_ASSERT(stats.column_names.size() == stats.column_distinct_count.size());
		for (idx_t i = 0; i < stats.column_names.size(); i++) {
			to_print = stats.column_names.at(i) + " has estimated distinct count " +
			           to_string(stats.column_distinct_count.at(i).distinct_count);
			Printer::Print(to_print);
		}
		to_print = stats.table_name + " has estimated cardinality " + to_string(stats.cardinality);
		to_print += " and relation id " + to_string(i) + "\n";
		Printer::Print(to_print);
	}
#endif
}

// LCOV_EXCL_STOP

} // namespace duckdb
