#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/cost_model.hpp"
#include "duckdb/optimizer/join_order/plan_enumerator.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/common/queue.hpp"

#include <algorithm>
#include <cmath>

namespace std {

//! A JoinNode is defined by the relations it joins.
template <>
struct hash<duckdb::JoinNode> {
	inline string operator()(const duckdb::JoinNode &join_node) const {
		return join_node.set.ToString();
	}
};
} // namespace std

namespace duckdb {

//! Returns true if A and B are disjoint, false otherwise
template <class T>
static bool Disjoint(const unordered_set<T> &a, const unordered_set<T> &b) {
	return std::all_of(a.begin(), a.end(), [&b](typename std::unordered_set<T>::const_reference entry) {
		return b.find(entry) == b.end();
	});
}

//! Extract the set of relations referred to inside an expression
bool JoinOrderOptimizer::ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);
		D_ASSERT(colref.binding.table_index != DConstants::INVALID_INDEX);
		// map the base table index to the relation index used by the JoinOrderOptimizer
		D_ASSERT(relation_mapping.find(colref.binding.table_index) != relation_mapping.end());
		auto catalog_table = relation_mapping[colref.binding.table_index];
		auto column_index = colref.binding.column_index;
		cardinality_estimator.AddColumnToRelationMap(catalog_table, column_index);
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

void JoinOrderOptimizer::GetColumnBinding(Expression &expression, ColumnBinding &binding) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		// Here you have a filter on a single column in a table. Return a binding for the column
		// being filtered on so the filter estimator knows what HLL count to pull
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);
		D_ASSERT(colref.binding.table_index != DConstants::INVALID_INDEX);
		// map the base table index to the relation index used by the JoinOrderOptimizer
		D_ASSERT(relation_mapping.find(colref.binding.table_index) != relation_mapping.end());
		binding = ColumnBinding(relation_mapping[colref.binding.table_index], colref.binding.column_index);
	}
	// TODO: handle inequality filters with functions.
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &expr) { GetColumnBinding(expr, binding); });
}

static unique_ptr<LogicalOperator> PushFilter(unique_ptr<LogicalOperator> node, unique_ptr<Expression> expr) {
	// push an expression into a filter
	// first check if we have any filter to push it into
	if (node->type != LogicalOperatorType::LOGICAL_FILTER) {
		// we don't, we need to create one
		auto filter = make_uniq<LogicalFilter>();
		filter->children.push_back(std::move(node));
		node = std::move(filter);
	}
	// push the filter into the LogicalFilter
	D_ASSERT(node->type == LogicalOperatorType::LOGICAL_FILTER);
	auto &filter = node->Cast<LogicalFilter>();
	filter.expressions.push_back(std::move(expr));
	return node;
}

bool JoinOrderOptimizer::ExtractJoinRelations(LogicalOperator &input_op,
                                              vector<reference<LogicalOperator>> &filter_operators,
                                              optional_ptr<LogicalOperator> parent) {
	LogicalOperator *op = &input_op;
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
			op->children[0] = optimizer.Optimize(std::move(op->children[0]));
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
			// non-inner join, not reorderable yet
			non_reorderable_operation = true;
			if (join.join_type == JoinType::LEFT && join.right_projection_map.empty()) {
				// for left joins; if the RHS cardinality is significantly larger than the LHS (2x)
				// we convert to doing a RIGHT OUTER JOIN
				// FIXME: for now we don't swap if the right_projection_map is not empty
				// this can be fixed once we implement the left_projection_map properly...
				auto lhs_cardinality = join.children[0]->EstimateCardinality(context);
				auto rhs_cardinality = join.children[1]->EstimateCardinality(context);
				if (rhs_cardinality > lhs_cardinality * 2) {
					join.join_type = JoinType::RIGHT;
					std::swap(join.children[0], join.children[1]);
					for (auto &cond : join.conditions) {
						std::swap(cond.left, cond.right);
						cond.comparison = FlipComparisonExpression(cond.comparison);
					}
				}
			}
		}
	}
	if (op->type == LogicalOperatorType::LOGICAL_ANY_JOIN && non_reorderable_operation) {
		auto &join = op->Cast<LogicalAnyJoin>();
		if (join.join_type == JoinType::LEFT && join.right_projection_map.empty()) {
			auto lhs_cardinality = join.children[0]->EstimateCardinality(context);
			auto rhs_cardinality = join.children[1]->EstimateCardinality(context);
			if (rhs_cardinality > lhs_cardinality * 2) {
				join.join_type = JoinType::RIGHT;
				std::swap(join.children[0], join.children[1]);
			}
		}
	}

	if (non_reorderable_operation) {
		// we encountered a non-reordable operation (setop or non-inner join)
		// we do not reorder non-inner joins yet, however we do want to expand the potential join graph around them
		// non-inner joins are also tricky because we can't freely make conditions through them
		// e.g. suppose we have (left LEFT OUTER JOIN right WHERE right IS NOT NULL), the join can generate
		// new NULL values in the right side, so pushing this condition through the join leads to incorrect results
		// for this reason, we just start a new JoinOptimizer pass in each of the children of the join

		// Keep track of all filter bindings the new join order optimizer makes
		vector<column_binding_map_t<ColumnBinding>> child_binding_maps;
		idx_t child_bindings_it = 0;
		for (auto &child : op->children) {
			child_binding_maps.emplace_back();
			JoinOrderOptimizer optimizer(context);
			child = optimizer.Optimize(std::move(child));
			// save the relation bindings from the optimized child. These later all get added to the
			// parent cardinality_estimator relation column binding map.
			optimizer.cardinality_estimator.CopyRelationMap(child_binding_maps.at(child_bindings_it));
			child_bindings_it += 1;
		}
		// after this we want to treat this node as one  "end node" (like e.g. a base relation)
		// however the join refers to multiple base relations
		// enumerate all base relations obtained from this join and add them to the relation mapping
		// also, we have to resolve the join conditions for the joins here
		// get the left and right bindings
		unordered_set<idx_t> bindings;
		LogicalJoin::GetTableReferences(*op, bindings);
		// now create the relation that refers to all these bindings
		auto relation = make_uniq<SingleJoinRelation>(input_op, parent);
		auto relation_id = relations.size();
		// Add binding information from the nonreorderable join to this relation.
		for (idx_t it : bindings) {
			cardinality_estimator.MergeBindings(it, relation_id, child_binding_maps);
			relation_mapping[it] = relation_id;
		}
		relations.push_back(std::move(relation));
		return true;
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		// inner join or cross product
		bool can_reorder_left = ExtractJoinRelations(*op->children[0], filter_operators, op);
		bool can_reorder_right = ExtractJoinRelations(*op->children[1], filter_operators, op);
		return can_reorder_left && can_reorder_right;
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		// base table scan, add to set of relations
		auto &get = op->Cast<LogicalExpressionGet>();
		auto relation = make_uniq<SingleJoinRelation>(input_op, parent);
		//! make sure the optimizer has knowledge of the exact column bindings as well.
		relation_mapping[get.table_index] = relations.size();
		relations.push_back(std::move(relation));
		return true;
	}
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN: {
		// table function call, add to set of relations
		auto &dummy_scan = op->Cast<LogicalDummyScan>();
		auto relation = make_uniq<SingleJoinRelation>(input_op, parent);
		relation_mapping[dummy_scan.table_index] = relations.size();
		relations.push_back(std::move(relation));
		return true;
	}
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto table_index = op->GetTableIndex()[0];
		auto relation = make_uniq<SingleJoinRelation>(input_op, parent);
		auto relation_id = relations.size();

		// If the children are empty, operator can't ge a logical get.
		if (op->children.empty() && op->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op->Cast<LogicalGet>();
			cardinality_estimator.AddRelationColumnMapping(get, relation_id);
			relation_mapping[table_index] = relation_id;
			relations.push_back(std::move(relation));
			return true;
		}

		// we run the join order optimizer within the subquery as well
		JoinOrderOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(std::move(op->children[0]));
		// push one child column binding map back.
		vector<column_binding_map_t<ColumnBinding>> child_binding_maps;
		child_binding_maps.emplace_back();
		optimizer.cardinality_estimator.CopyRelationMap(child_binding_maps.at(0));
		// This logical projection/get may sit on top of a logical comparison join that has been pushed down
		// we want to copy the binding info of both tables
		relation_mapping[table_index] = relation_id;
		for (auto &binding_info : child_binding_maps.at(0)) {
			cardinality_estimator.AddRelationToColumnMapping(
			    ColumnBinding(table_index, binding_info.first.column_index), binding_info.second);
			cardinality_estimator.AddColumnToRelationMap(binding_info.second.table_index,
			                                             binding_info.second.column_index);
		}
		relations.push_back(std::move(relation));
		return true;
	}
	default:
		return false;
	}
}


static unique_ptr<LogicalOperator> ExtractJoinRelation(SingleJoinRelation &rel) {
	auto &children = rel.parent->children;
	for (idx_t i = 0; i < children.size(); i++) {
		if (children[i].get() == &rel.op) {
			// found it! take ownership o/**/f it from the parent
			auto result = std::move(children[i]);
			children.erase(children.begin() + i);
			return result;
		}
	}
	throw Exception("Could not find relation in parent node (?)");
}

GenerateJoinRelation JoinOrderOptimizer::GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations,
                                                       JoinNode &node) {
	optional_ptr<JoinRelationSet> left_node;
	optional_ptr<JoinRelationSet> right_node;
	optional_ptr<JoinRelationSet> result_relation;
	unique_ptr<LogicalOperator> result_operator;
	if (node.left && node.right && node.info) {
		// generate the left and right children
		auto left = GenerateJoins(extracted_relations, *node.left);
		auto right = GenerateJoins(extracted_relations, *node.right);

		if (node.info->filters.empty()) {
			// no filters, create a cross product
			result_operator = LogicalCrossProduct::Create(std::move(left.op), std::move(right.op));
		} else {
			// we have filters, create a join node
			auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
			join->children.push_back(std::move(left.op));
			join->children.push_back(std::move(right.op));
			// set the join conditions from the join node
			for (auto &filter_ref : node.info->filters) {
				auto &f = filter_ref.get();
				// extract the filter from the operator it originally belonged to
				D_ASSERT(filters[f.filter_index]);
				auto condition = std::move(filters[f.filter_index]);
				// now create the actual join condition
				D_ASSERT((JoinRelationSet::IsSubset(left.set, *f.left_set) &&
				          JoinRelationSet::IsSubset(right.set, *f.right_set)) ||
				         (JoinRelationSet::IsSubset(left.set, *f.right_set) &&
				          JoinRelationSet::IsSubset(right.set, *f.left_set)));
				JoinCondition cond;
				D_ASSERT(condition->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
				auto &comparison = condition->Cast<BoundComparisonExpression>();
				// we need to figure out which side is which by looking at the relations available to us
				bool invert = !JoinRelationSet::IsSubset(left.set, *f.left_set);
				cond.left = !invert ? std::move(comparison.left) : std::move(comparison.right);
				cond.right = !invert ? std::move(comparison.right) : std::move(comparison.left);
				cond.comparison = condition->type;

				if (invert) {
					// reverse comparison expression if we reverse the order of the children
					cond.comparison = FlipComparisonExpression(cond.comparison);
				}
				join->conditions.push_back(std::move(cond));
			}
			D_ASSERT(!join->conditions.empty());
			result_operator = std::move(join);
		}
		left_node = &left.set;
		right_node = &right.set;
		right_node = &right.set;
		result_relation = &set_manager.Union(*left_node, *right_node);
	} else {
		// base node, get the entry from the list of extracted relations
		D_ASSERT(node.set.count == 1);
		D_ASSERT(extracted_relations[node.set.relations[0]]);
		result_relation = &node.set;
		result_operator = std::move(extracted_relations[node.set.relations[0]]);
	}
	result_operator->estimated_props = node.estimated_props->Copy();
	result_operator->estimated_cardinality = result_operator->estimated_props->GetCardinality<idx_t>();
	result_operator->has_estimated_cardinality = true;
	if (result_operator->type == LogicalOperatorType::LOGICAL_FILTER &&
	    result_operator->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
		// FILTER on top of GET, add estimated properties to both
		auto &filter_props = *result_operator->estimated_props;
		auto &child_operator = *result_operator->children[0];
		child_operator.estimated_props = make_uniq<EstimatedProperties>(filter_props.GetCardinality<double>() /
		                                                                    CardinalityEstimator::DEFAULT_SELECTIVITY,
		                                                                filter_props.GetCost<double>());
		child_operator.estimated_cardinality = child_operator.estimated_props->GetCardinality<idx_t>();
		child_operator.has_estimated_cardinality = true;
	}
	// check if we should do a pushdown on this node
	// basically, any remaining filter that is a subset of the current relation will no longer be used in joins
	// hence we should push it here
	for (auto &filter_info : filter_infos) {
		// check if the filter has already been extracted
		auto &info = *filter_info;
		if (filters[info.filter_index]) {
			// now check if the filter is a subset of the current relation
			// note that infos with an empty relation set are a special case and we do not push them down
			if (info.set.count > 0 && JoinRelationSet::IsSubset(*result_relation, info.set)) {
				auto filter = std::move(filters[info.filter_index]);
				// if it is, we can push the filter
				// we can push it either into a join or as a filter
				// check if we are in a join or in a base table
				if (!left_node || !info.left_set) {
					// base table or non-comparison expression, push it as a filter
					result_operator = PushFilter(std::move(result_operator), std::move(filter));
					continue;
				}
				// the node below us is a join or cross product and the expression is a comparison
				// check if the nodes can be split up into left/right
				bool found_subset = false;
				bool invert = false;
				if (JoinRelationSet::IsSubset(*left_node, *info.left_set) &&
				    JoinRelationSet::IsSubset(*right_node, *info.right_set)) {
					found_subset = true;
				} else if (JoinRelationSet::IsSubset(*right_node, *info.left_set) &&
				           JoinRelationSet::IsSubset(*left_node, *info.right_set)) {
					invert = true;
					found_subset = true;
				}
				if (!found_subset) {
					// could not be split up into left/right
					result_operator = PushFilter(std::move(result_operator), std::move(filter));
					continue;
				}
				// create the join condition
				JoinCondition cond;
				D_ASSERT(filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
				auto &comparison = filter->Cast<BoundComparisonExpression>();
				// we need to figure out which side is which by looking at the relations available to us
				cond.left = !invert ? std::move(comparison.left) : std::move(comparison.right);
				cond.right = !invert ? std::move(comparison.right) : std::move(comparison.left);
				cond.comparison = comparison.type;
				if (invert) {
					// reverse comparison expression if we reverse the order of the children
					cond.comparison = FlipComparisonExpression(comparison.type);
				}
				// now find the join to push it into
				auto node = result_operator.get();
				if (node->type == LogicalOperatorType::LOGICAL_FILTER) {
					node = node->children[0].get();
				}
				if (node->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
					// turn into comparison join
					auto comp_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
					comp_join->children.push_back(std::move(node->children[0]));
					comp_join->children.push_back(std::move(node->children[1]));
					comp_join->conditions.push_back(std::move(cond));
					if (node == result_operator.get()) {
						result_operator = std::move(comp_join);
					} else {
						D_ASSERT(result_operator->type == LogicalOperatorType::LOGICAL_FILTER);
						result_operator->children[0] = std::move(comp_join);
					}
				} else {
					D_ASSERT(node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
					         node->type == LogicalOperatorType::LOGICAL_ASOF_JOIN);
					auto &comp_join = node->Cast<LogicalComparisonJoin>();
					comp_join.conditions.push_back(std::move(cond));
				}
			}
		}
	}
	return GenerateJoinRelation(*result_relation, std::move(result_operator));
}

unique_ptr<LogicalOperator> JoinOrderOptimizer::RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode &node) {
	// now we have to rewrite the plan
	bool root_is_join = plan->children.size() > 1;

	// first we will extract all relations from the main plan
	vector<unique_ptr<LogicalOperator>> extracted_relations;
	extracted_relations.reserve(relations.size());
	for (auto &relation : relations) {
		extracted_relations.push_back(ExtractJoinRelation(*relation));
	}

	// now we generate the actual joins
	auto join_tree = GenerateJoins(extracted_relations, node);
	// perform the final pushdown of remaining filters
	for (auto &filter : filters) {
		// check if the filter has already been extracted
		if (filter) {
			// if not we need to push it
			join_tree.op = PushFilter(std::move(join_tree.op), std::move(filter));
		}
	}

	// find the first join in the relation to know where to place this node
	if (root_is_join) {
		// first node is the join, return it immediately
		return std::move(join_tree.op);
	}
	D_ASSERT(plan->children.size() == 1);
	// have to move up through the relations
	auto op = plan.get();
	auto parent = plan.get();
	while (op->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT &&
	       op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	       op->type != LogicalOperatorType::LOGICAL_ASOF_JOIN) {
		D_ASSERT(op->children.size() == 1);
		parent = op;
		op = op->children[0].get();
	}
	// have to replace at this node
	parent->children[0] = std::move(join_tree.op);
	return plan;
}

// the join ordering is pretty much a straight implementation of the paper "Dynamic Programming Strikes Back" by Guido
// Moerkotte and Thomas Neumannn, see that paper for additional info/documentation bonus slides:
// https://db.in.tum.de/teaching/ws1415/queryopt/chapter3.pdf?lang=de
// FIXME: incorporate cardinality estimation into the plans, possibly by pushing samples?
unique_ptr<LogicalOperator> JoinOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {

	// make sure query graph manager
	D_ASSERT(!query_graph_manager.HasQueryGraph());
//	D_ASSERT(filters.empty() && relations.empty()); // assert that the JoinOrderOptimizer has not been used before
	LogicalOperator *op = plan.get();

	// extract the relations that go into the hyper graph.
	// We optimize and non-reorderable relations we come across.
	query_graph_manager.Build(op);

	if (query_graph_manager.relation_manager.NumRelations() <= 1) {
		// at most one relation, nothing to reorder
		return plan;
	}

	//! QUERY GRAPH NOW HAS FILTERS AND RELATIONS KEWL

	auto cost_model = CostModel(query_graph_manager);

	// Initialize a plan enumerator
	auto plan_enumerator = PlanEnumerator(query_graph_manager, cost_model);

	// now we perform the actual dynamic programming to compute the final result
	auto final_plan = plan_enumerator.SolveJoinOrder();

	// now reconstruct a logical plan
	auto new_logical_plan = query_graph_manager.Reconstruct(final_plan);

	// TODO: swap left and right joins based on statistics.

	return std::move(new_logical_plan);
}

} // namespace duckdb
