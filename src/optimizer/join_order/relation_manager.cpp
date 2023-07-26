#include "duckdb/optimizer/join_order/relation_manager.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/optimizer/join_order/statistics_extractor.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/assert.hpp"

#include "iostream"

namespace duckdb {


const vector<RelationStats> RelationManager::GetRelationStats() {
	vector<RelationStats> ret;
	for (idx_t i = 0; i < relations.size(); i++) {
		ret.push_back(relations[i]->stats);
	}
	return ret;
}

vector<unique_ptr<SingleJoinRelation>> RelationManager::GetRelations() {
//	return vector<unique_ptr<SingleJoinRelation>>();
	return std::move(relations);
}

idx_t RelationManager::NumRelations() {
	return relations.size();
}

struct DistinctCount;

void RelationManager::AddAggregateRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent, RelationStats stats) {
	// we have an aggregate operator we are returning that this can be reordered, but we are only returning one relation.
	D_ASSERT(op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY);
	auto relation = make_uniq<SingleJoinRelation>(op, parent, stats);
	auto relation_id = relations.size();

	auto table_indexes = op.GetTableIndex();
	for (auto &index : table_indexes) {
		D_ASSERT(relation_mapping.find(index) == relation_mapping.end());
		relation_mapping[index] = relation_id;
	}
	relations.push_back(std::move(relation));
}

void RelationManager::AddRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent, RelationStats stats) {

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
	// Add binding information from the nonreorderable join to this relation.
	//	auto relation_name = GetRelationName(op);
}

struct ExpressionBinding {
	bool found_expression = false;
	ColumnBinding child_binding;
	bool expression_is_constant = false;
};

static ExpressionBinding GetChildColumnBinding(Expression *expr) {
	auto ret = ExpressionBinding();
	switch (expr->expression_class) {
	case ExpressionClass::BOUND_FUNCTION: {
		// TODO: Other expression classes that can have 0 children?
		auto &func = expr->Cast<BoundFunctionExpression>();
		// no children some sort of gen_random_uuid() or equivalent.
		if (func.children.size() == 0) {
			ret.found_expression = true;
			ret.expression_is_constant = true;
			return ret;
		}
		break;
	}
	case ExpressionClass::BOUND_COLUMN_REF: {
		ret.found_expression = true;
		auto &new_col_ref = expr->Cast<BoundColumnRefExpression>();
		ret.child_binding = ColumnBinding(new_col_ref.binding.table_index, new_col_ref.binding.column_index);
		return ret;
	}
	case ExpressionClass::BOUND_LAMBDA_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_DEFAULT:
	case ExpressionClass::BOUND_PARAMETER:
	case ExpressionClass::BOUND_REF:
		ret.found_expression = true;
		ret.expression_is_constant = true;
		return ret;
	default:
		break;
	}
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		auto recursive_result = GetChildColumnBinding(child.get());
		if (recursive_result.found_expression) {
			ret = recursive_result;
		}
	});
	// we didn't find a Bound Column Ref
	return ret;
}


bool RelationManager::ExtractJoinRelations(LogicalOperator &input_op,
	                                             vector<reference<LogicalOperator>> &filter_operators,
	                                             optional_ptr<LogicalOperator> parent) {
	LogicalOperator *op = &input_op;
	// pass through single child operators
	while (op->children.size() == 1 &&
		   (op->type != LogicalOperatorType::LOGICAL_PROJECTION &&
			op->type != LogicalOperatorType::LOGICAL_EXPRESSION_GET && op->type != LogicalOperatorType::LOGICAL_GET)) {
		if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
			// extract join conditions from filter
			filter_operators.push_back(*op);
		}
		if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY ||
			op->type == LogicalOperatorType::LOGICAL_WINDOW) {
			// don't push filters through projection or aggregate and group by
			JoinOrderOptimizer optimizer(context);
			RelationStats child_stats;
			op->children[0] = optimizer.Optimize(std::move(op->children[0]), &child_stats);
			// TODO: create new stats object based on the stats of the child.
			//  look into the distinct count and the operators group by index.
			// might have to add
			AddAggregateRelation(input_op, parent, child_stats);
			// TODO: technially you should be able to reorder aggregates.
			//  i.e. an inner join between an aggregate operation and another operation is allowed
			//  but to avoid some headaches, return false here.
			return false;
		}
		op = op->children[0].get();
	}
	bool non_reorderable_operation = false;
	if (op->type == LogicalOperatorType::LOGICAL_UNION || op->type == LogicalOperatorType::LOGICAL_EXCEPT ||
		op->type == LogicalOperatorType::LOGICAL_INTERSECT || op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
		op->type == LogicalOperatorType::LOGICAL_ANY_JOIN || op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
		// set operation, optimize separately in children
		non_reorderable_operation = true;
	}

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		if (join.join_type == JoinType::INNER) {
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
		auto stats = RelationStats();
		// stats.cardinality will be initiated to highest cardinality of the children.
		stats.cardinality = 0;
		for (auto &child : op->children) {
			JoinOrderOptimizer optimizer(context);
			// use the same stats, distinct counts are pushed at
			child = optimizer.Optimize(std::move(child), &stats);
		}
		// TODO: update stats.cardinality to predict the cardinality of
		//  what this non-reorderable operation will be. (if
		AddRelation(input_op, parent, stats);
		return true;
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		// Adding relations to the current join order optimizer
		bool can_reorder_left = ExtractJoinRelations(*op->children[0], filter_operators, op);
		bool can_reorder_right = ExtractJoinRelations(*op->children[1], filter_operators, op);
		return can_reorder_left && can_reorder_right;
	}
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		// base table scan, add to set of relations.
		// create empty stats for dummy scan or logical expression get
		auto stats = RelationStats();
		idx_t card = op->EstimateCardinality(context);
		stats.cardinality = card;
		for (auto &binding : op->GetColumnBindings()) {
			stats.column_distinct_count.push_back(DistinctCount({card, false}));
			stats.column_names.push_back("dummy_scan_column");
		}
		stats.filter_strength = 1;
		stats.stats_initialized = true;
		stats.table_name = "dummy scan/expression get";
		AddRelation(input_op, parent, stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_GET: {
		// TODO: Get stats from a logical GET
		auto &get = op->Cast<LogicalGet>();
		auto stats = StatisticsExtractor::ExtractOperatorStats(get, context);

		AddRelation(input_op, parent, stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_DELIM_GET: {
		auto &get = op->Cast<LogicalDelimGet>();
		RelationStats stats;
		stats.table_name = get.GetName();
		idx_t card = get.EstimateCardinality(context);
		stats.cardinality = card;
		for (auto &binding : get.GetColumnBindings()) {
			stats.column_distinct_count.push_back(DistinctCount({1, false}));
			stats.column_names.push_back("column" + to_string(binding.column_index));
		}

		AddRelation(input_op, parent, stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto stats = RelationStats();
		// optimize the child
		JoinOrderOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(std::move(op->children[0]), &stats);

		// Projection can create columns so we need to add them here
		auto proj_stats = RelationStats();
		proj_stats.cardinality = stats.cardinality;
		proj_stats.table_name = op->GetName();
		for (auto &expr : op->expressions) {
			proj_stats.column_names.push_back(expr->GetName());
			auto res = GetChildColumnBinding(expr.get());
			D_ASSERT(res.found_expression);
			if (res.expression_is_constant) {
				proj_stats.column_distinct_count.push_back(DistinctCount({1, true}));
			} else {
				auto column_index = res.child_binding.column_index;
				if (column_index >= stats.column_distinct_count.size() && expr->ToString() == "count_star()") {
					// only one value for a count star
					proj_stats.column_distinct_count.push_back(DistinctCount({1, true}));
				} else {
					// TODO: add this back in
//					D_ASSERT(column_index < stats.column_distinct_count.size());
					if (column_index < stats.column_distinct_count.size()) {
						proj_stats.column_distinct_count.push_back(stats.column_distinct_count.at(column_index));
					} else {
						proj_stats.column_distinct_count.push_back(DistinctCount({proj_stats.cardinality, false}));
					}
				}
			}
		}
		proj_stats.stats_initialized = true;
		proj_stats.filter_strength = 1;
		AddRelation(input_op, parent, proj_stats);
		return true;
	}
	default:
		return false;
	}
}

//! Extract the set of relations referred to inside an expression
bool RelationManager::ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);
		D_ASSERT(colref.binding.table_index != DConstants::INVALID_INDEX);
		// map the base table index to the relation index used by the JoinOrderOptimizer
		// TODO: what is the relation mapping and why is it important?
		D_ASSERT(relation_mapping.find(colref.binding.table_index) != relation_mapping.end());
		//		auto catalog_table = relation_manager.relation_mapping[colref.binding.table_index];
		//		auto column_index = colref.binding.column_index;
		//		cardinality_estimator.AddColumnToRelationMap(catalog_table, column_index);
		bindings.insert(relation_mapping[colref.binding.table_index]);
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
			D_ASSERT(join.join_type == JoinType::INNER);
			D_ASSERT(join.expressions.empty());
			for (auto &cond : join.conditions) {
				auto comparison =
				    make_uniq<BoundComparisonExpression>(cond.comparison, std::move(cond.left), std::move(cond.right));
				if (filter_set.find(*comparison) == filter_set.end()) {
					filter_set.insert(*comparison);
					unordered_set<idx_t> bindings;
					ExtractBindings(*comparison, bindings);
					auto set = set_manager.GetJoinRelation(bindings);
					auto filter_info = make_uniq<FilterInfo>(std::move(comparison), set, filters_and_bindings.size());
					filters_and_bindings.push_back(std::move(filter_info));
				}
			}
			join.conditions.clear();
		} else {
			for (auto &expression : f_op.expressions) {
				if (filter_set.find(*expression) == filter_set.end()) {
					filter_set.insert(*expression);
					unordered_set<idx_t> bindings;
					ExtractBindings(*expression, bindings);
					auto set = set_manager.GetJoinRelation(bindings);
					auto filter_info = make_uniq<FilterInfo>(std::move(expression), set, filters_and_bindings.size());
					filters_and_bindings.push_back(std::move(filter_info));
				}
			}
			f_op.expressions.clear();
		}
	}

	return filters_and_bindings;
}

void RelationManager::PrintRelationStats() {
	for(auto &relation : relations) {
		auto &stats = relation->stats;
		D_ASSERT(stats.column_names.size() == stats.column_distinct_count.size());
		for (idx_t i = 0; i < stats.column_names.size(); i++) {
			std::cout << stats.column_names.at(i) << " has estimated distinct count " << stats.column_distinct_count.at(i).distinct_count << std::endl;
		}
		std::cout << "table has cardinality " << stats.cardinality << std::endl;
	}
}

} // namespace duckdb
