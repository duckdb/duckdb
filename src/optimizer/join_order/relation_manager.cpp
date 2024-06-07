#include "duckdb/optimizer/join_order/relation_manager.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/relation_statistics_helper.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/list.hpp"
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
	} else {
		// Relations should never return more than 1 table index
		D_ASSERT(table_indexes.size() == 1);
		idx_t table_index = table_indexes.at(0);
		D_ASSERT(relation_mapping.find(table_index) == relation_mapping.end());
		relation_mapping[table_index] = relation_id;
	}
	relations.push_back(std::move(relation));
}

static bool OperatorNeedsRelation(LogicalOperatorType op_type) {
	switch (op_type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_WINDOW:
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
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		return true;
	default:
		return false;
	}
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
			return true;
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

bool RelationManager::ExtractJoinRelations(LogicalOperator &input_op,
                                           vector<reference<LogicalOperator>> &filter_operators,
                                           optional_ptr<LogicalOperator> parent) {
	LogicalOperator *op = &input_op;
	vector<reference<LogicalOperator>> datasource_filters;
	// pass through single child operators
	while (op->children.size() == 1 && !OperatorNeedsRelation(op->type)) {
		if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
			if (HasNonReorderableChild(*op)) {
				datasource_filters.push_back(*op);
			}
			filter_operators.push_back(*op);
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
			JoinOrderOptimizer optimizer(context);
			child = optimizer.Optimize(std::move(child), &stats);
			children_stats.push_back(stats);
		}

		auto combined_stats = RelationStatisticsHelper::CombineStatsOfNonReorderableOperator(*op, children_stats);
		if (!datasource_filters.empty()) {
			combined_stats.cardinality =
			    (idx_t)MaxValue(combined_stats.cardinality * RelationStatisticsHelper::DEFAULT_SELECTIVITY, (double)1);
		}
		AddRelation(input_op, parent, combined_stats);
		return true;
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// optimize children
		RelationStats child_stats;
		JoinOrderOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(std::move(op->children[0]), &child_stats);
		auto &aggr = op->Cast<LogicalAggregate>();
		auto operator_stats = RelationStatisticsHelper::ExtractAggregationStats(aggr, child_stats);
		if (!datasource_filters.empty()) {
			operator_stats.cardinality = NumericCast<idx_t>(static_cast<double>(operator_stats.cardinality) *
			                                                RelationStatisticsHelper::DEFAULT_SELECTIVITY);
		}
		AddAggregateOrWindowRelation(input_op, parent, operator_stats, op->type);
		return true;
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		// optimize children
		RelationStats child_stats;
		JoinOrderOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(std::move(op->children[0]), &child_stats);
		auto &window = op->Cast<LogicalWindow>();
		auto operator_stats = RelationStatisticsHelper::ExtractWindowStats(window, child_stats);
		if (!datasource_filters.empty()) {
			operator_stats.cardinality = NumericCast<idx_t>(static_cast<double>(operator_stats.cardinality) *
			                                                RelationStatisticsHelper::DEFAULT_SELECTIVITY);
		}
		AddAggregateOrWindowRelation(input_op, parent, operator_stats, op->type);
		return true;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		// Adding relations to the current join order optimizer
		bool can_reorder_left = ExtractJoinRelations(*op->children[0], filter_operators, op);
		bool can_reorder_right = ExtractJoinRelations(*op->children[1], filter_operators, op);
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
		if (!datasource_filters.empty()) {
			stats.cardinality =
			    (idx_t)MaxValue(stats.cardinality * RelationStatisticsHelper::DEFAULT_SELECTIVITY, (double)1);
		}
		AddRelation(input_op, parent, stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_DELIM_GET: {
		//      Removed until we can extract better stats from delim gets. See #596
		//		auto &delim_get = op->Cast<LogicalDelimGet>();
		//		auto stats = RelationStatisticsHelper::ExtractDelimGetStats(delim_get, context);
		//		AddRelation(input_op, parent, stats);
		return false;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto child_stats = RelationStats();
		// optimize the child and copy the stats
		JoinOrderOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(std::move(op->children[0]), &child_stats);
		auto &proj = op->Cast<LogicalProjection>();
		// Projection can create columns so we need to add them here
		auto proj_stats = RelationStatisticsHelper::ExtractProjectionStats(proj, child_stats);
		AddRelation(input_op, parent, proj_stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT: {
		// optimize the child and copy the stats
		auto &empty_result = op->Cast<LogicalEmptyResult>();
		// Projection can create columns so we need to add them here
		auto stats = RelationStatisticsHelper::ExtractEmptyResultStats(empty_result);
		AddRelation(input_op, parent, stats);
		return true;
	}
	default:
		return false;
	}
}

bool RelationManager::ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);
		D_ASSERT(colref.binding.table_index != DConstants::INVALID_INDEX);
		// map the base table index to the relation index used by the JoinOrderOptimizer
		if (expression.alias == "SUBQUERY" &&
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
	if (expression.type == ExpressionType::BOUND_REF) {
		// bound expression
		bindings.clear();
		return false;
	}
	D_ASSERT(expression.type != ExpressionType::SUBQUERY);
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
			for (auto &cond : join.conditions) {
				auto comparison =
				    make_uniq<BoundComparisonExpression>(cond.comparison, std::move(cond.left), std::move(cond.right));
				if (filter_set.find(*comparison) == filter_set.end()) {
					filter_set.insert(*comparison);
					unordered_set<idx_t> bindings;
					ExtractBindings(*comparison, bindings);
					auto &set = set_manager.GetJoinRelation(bindings);
					auto filter_info =
					    make_uniq<FilterInfo>(std::move(comparison), set, filters_and_bindings.size(), join.join_type);
					filters_and_bindings.push_back(std::move(filter_info));
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
