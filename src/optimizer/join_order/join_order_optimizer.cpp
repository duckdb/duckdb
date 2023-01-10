#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <algorithm>

namespace std {

//! A JoinNode is defined by the relations it joins.
template <>
struct hash<duckdb::JoinNode> {
	inline string operator()(const duckdb::JoinNode &join_node) const {
		return join_node.set->ToString();
	}
};
} // namespace std

namespace duckdb {

//! Returns true if A and B are disjoint, false otherwise
template <class T>
static bool Disjoint(unordered_set<T> &a, unordered_set<T> &b) {
	for (auto &entry : a) {
		if (b.find(entry) != b.end()) {
			return false;
		}
	}
	return true;
}

//! Extract the set of relations referred to inside an expression
bool JoinOrderOptimizer::ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)expression;
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
		auto &colref = (BoundColumnRefExpression &)expression;
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
		auto filter = make_unique<LogicalFilter>();
		filter->children.push_back(std::move(node));
		node = std::move(filter);
	}
	// push the filter into the LogicalFilter
	D_ASSERT(node->type == LogicalOperatorType::LOGICAL_FILTER);
	auto filter = (LogicalFilter *)node.get();
	filter->expressions.push_back(std::move(expr));
	return node;
}

bool JoinOrderOptimizer::ExtractJoinRelations(LogicalOperator &input_op, vector<LogicalOperator *> &filter_operators,
                                              LogicalOperator *parent) {
	LogicalOperator *op = &input_op;
	while (op->children.size() == 1 && (op->type != LogicalOperatorType::LOGICAL_PROJECTION &&
	                                    op->type != LogicalOperatorType::LOGICAL_EXPRESSION_GET)) {
		if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
			// extract join conditions from filter
			filter_operators.push_back(op);
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
	    op->type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
		// set operation, optimize separately in children
		non_reorderable_operation = true;
	}

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = (LogicalComparisonJoin &)*op;
		if (join.join_type == JoinType::INNER) {
			// extract join conditions from inner join
			filter_operators.push_back(op);
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
						cond.comparison = FlipComparisionExpression(cond.comparison);
					}
				}
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

		// Keep track of all of the filter bindings the new join order optimizer makes
		vector<column_binding_map_t<ColumnBinding>> child_binding_maps;
		idx_t child_bindings_it = 0;
		for (auto &child : op->children) {
			child_binding_maps.emplace_back(column_binding_map_t<ColumnBinding>());
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
		auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
		auto relation_id = relations.size();
		// Add binding information from the nonreorderable join to this relation.
		for (idx_t it : bindings) {
			cardinality_estimator.MergeBindings(it, relation_id, child_binding_maps);
			relation_mapping[it] = relation_id;
		}
		relations.push_back(std::move(relation));
		return true;
	}
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		// inner join or cross product
		bool can_reorder_left = ExtractJoinRelations(*op->children[0], filter_operators, op);
		bool can_reorder_right = ExtractJoinRelations(*op->children[1], filter_operators, op);
		return can_reorder_left && can_reorder_right;
	} else if (op->type == LogicalOperatorType::LOGICAL_GET) {
		// base table scan, add to set of relations
		auto get = (LogicalGet *)op;
		auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
		idx_t relation_id = relations.size();
		//! make sure the optimizer has knowledge of the exact column bindings as well.
		auto table_index = get->table_index;
		relation_mapping[table_index] = relation_id;
		cardinality_estimator.AddRelationColumnMapping(get, relation_id);
		relations.push_back(std::move(relation));
		return true;
	} else if (op->type == LogicalOperatorType::LOGICAL_EXPRESSION_GET) {
		// base table scan, add to set of relations
		auto get = (LogicalExpressionGet *)op;
		auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
		idx_t relation_id = relations.size();
		//! make sure the optimizer has knowledge of the exact column bindings as well.
		auto table_index = get->table_index;
		relation_mapping[table_index] = relation_id;
		relations.push_back(std::move(relation));
		return true;
	} else if (op->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		// table function call, add to set of relations
		auto dummy_scan = (LogicalDummyScan *)op;
		auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
		relation_mapping[dummy_scan->table_index] = relations.size();
		relations.push_back(std::move(relation));
		return true;
	} else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto proj = (LogicalProjection *)op;
		// we run the join order optimizer witin the subquery as well
		JoinOrderOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(std::move(op->children[0]));
		// projection, add to the set of relations
		auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
		relation_mapping[proj->table_index] = relations.size();
		relations.push_back(std::move(relation));
		return true;
	}
	return false;
}

//! Update the exclusion set with all entries in the subgraph
static void UpdateExclusionSet(JoinRelationSet *node, unordered_set<idx_t> &exclusion_set) {
	for (idx_t i = 0; i < node->count; i++) {
		exclusion_set.insert(node->relations[i]);
	}
}

//! Create a new JoinTree node by joining together two previous JoinTree nodes
unique_ptr<JoinNode> JoinOrderOptimizer::CreateJoinTree(JoinRelationSet *set,
                                                        const vector<NeighborInfo *> &possible_connections,
                                                        JoinNode *left, JoinNode *right) {
	// for the hash join we want the right side (build side) to have the smallest cardinality
	// also just a heuristic but for now...
	// FIXME: we should probably actually benchmark that as well
	// FIXME: should consider different join algorithms, should we pick a join algorithm here as well? (probably)
	double expected_cardinality;
	NeighborInfo *best_connection = nullptr;
	auto plan = plans.find(set);
	// if we have already calculated an expected cardinality for this set,
	// just re-use that cardinality
	if (left->GetCardinality<double>() < right->GetCardinality<double>()) {
		return CreateJoinTree(set, possible_connections, right, left);
	}
	if (plan != plans.end()) {
		if (!plan->second) {
			throw InternalException("No plan: internal error in join order optimizer");
		}
		expected_cardinality = plan->second->GetCardinality<double>();
		best_connection = possible_connections.back();
	} else if (possible_connections.empty()) {
		// cross product
		expected_cardinality = cardinality_estimator.EstimateCrossProduct(left, right);
	} else {
		// normal join, expect foreign key join
		expected_cardinality = cardinality_estimator.EstimateCardinalityWithSet(set);
		best_connection = possible_connections.back();
	}

	auto cost = CardinalityEstimator::ComputeCost(left, right, expected_cardinality);
	auto result = make_unique<JoinNode>(set, best_connection, left, right, expected_cardinality, cost);
	D_ASSERT(cost >= expected_cardinality);
	return result;
}

bool JoinOrderOptimizer::NodeInFullPlan(JoinNode *node) {
	return join_nodes_in_full_plan.find(node->set->ToString()) != join_nodes_in_full_plan.end();
}

void JoinOrderOptimizer::UpdateJoinNodesInFullPlan(JoinNode *node) {
	if (!node) {
		return;
	}
	if (node->set->count == relations.size()) {
		join_nodes_in_full_plan.clear();
	}
	if (node->set->count < relations.size()) {
		join_nodes_in_full_plan.insert(node->set->ToString());
	}
	UpdateJoinNodesInFullPlan(node->left);
	UpdateJoinNodesInFullPlan(node->right);
}

JoinNode *JoinOrderOptimizer::EmitPair(JoinRelationSet *left, JoinRelationSet *right,
                                       const vector<NeighborInfo *> &info) {
	// get the left and right join plans
	auto &left_plan = plans[left];
	auto &right_plan = plans[right];
	if (!left_plan || !right_plan) {
		throw InternalException("No left or right plan: internal error in join order optimizer");
	}
	auto new_set = set_manager.Union(left, right);
	// create the join tree based on combining the two plans
	auto new_plan = CreateJoinTree(new_set, info, left_plan.get(), right_plan.get());
	// check if this plan is the optimal plan we found for this set of relations
	auto entry = plans.find(new_set);
	if (entry == plans.end() || new_plan->GetCost() < entry->second->GetCost()) {
		// the plan is the optimal plan, move it into the dynamic programming tree
		auto result = new_plan.get();

		//! make sure plans are symmetric for cardinality estimation
		if (entry != plans.end()) {
			cardinality_estimator.VerifySymmetry(result, entry->second.get());
		}
		if (full_plan_found &&
		    join_nodes_in_full_plan.find(new_plan->set->ToString()) != join_nodes_in_full_plan.end()) {
			must_update_full_plan = true;
		}
		if (new_set->count == relations.size()) {
			full_plan_found = true;
			// If we find a full plan, we need to keep track of which nodes are in the full plan.
			// It's possible the DP algorithm updates one of these nodes, then goes on to solve
			// the order approximately. In the approximate algorithm, it's not guaranteed that the
			// node references are updated. If the original full plan is determined to still have
			// the lowest cost, it's possible to get use-after-free errors.
			// If we know a node in the full plan is updated, we can prevent ourselves from exiting the
			// DP algorithm until the last plan updated is a full plan
			UpdateJoinNodesInFullPlan(result);
			if (must_update_full_plan) {
				must_update_full_plan = false;
			}
		}

		D_ASSERT(new_plan);
		plans[new_set] = std::move(new_plan);
		return result;
	}
	return entry->second.get();
}

bool JoinOrderOptimizer::TryEmitPair(JoinRelationSet *left, JoinRelationSet *right,
                                     const vector<NeighborInfo *> &info) {
	pairs++;
	// If a full plan is created, it's possible a node in the plan gets updated. When this happens, make sure you keep
	// emitting pairs until you emit another final plan. Another final plan is guaranteed to be produced because of
	// our symmetry guarantees.
	if (pairs >= 10000 && !must_update_full_plan) {
		// when the amount of pairs gets too large we exit the dynamic programming and resort to a greedy algorithm
		// FIXME: simple heuristic currently
		// at 10K pairs stop searching exactly and switch to heuristic
		return false;
	}
	EmitPair(left, right, info);
	return true;
}

bool JoinOrderOptimizer::EmitCSG(JoinRelationSet *node) {
	if (node->count == relations.size()) {
		return true;
	}
	// create the exclusion set as everything inside the subgraph AND anything with members BELOW it
	unordered_set<idx_t> exclusion_set;
	for (idx_t i = 0; i < node->relations[0]; i++) {
		exclusion_set.insert(i);
	}
	UpdateExclusionSet(node, exclusion_set);
	// find the neighbors given this exclusion set
	auto neighbors = query_graph.GetNeighbors(node, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}

	//! Neighbors should be reversed when iterating over them.
	std::sort(neighbors.begin(), neighbors.end(), std::greater_equal<idx_t>());
	for (idx_t i = 0; i < neighbors.size() - 1; i++) {
		D_ASSERT(neighbors[i] >= neighbors[i + 1]);
	}
	for (auto neighbor : neighbors) {
		// since the GetNeighbors only returns the smallest element in a list, the entry might not be connected to
		// (only!) this neighbor,  hence we have to do a connectedness check before we can emit it
		auto neighbor_relation = set_manager.GetJoinRelation(neighbor);
		auto connections = query_graph.GetConnections(node, neighbor_relation);
		if (!connections.empty()) {
			if (!TryEmitPair(node, neighbor_relation, connections)) {
				return false;
			}
		}
		if (!EnumerateCmpRecursive(node, neighbor_relation, exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool JoinOrderOptimizer::EnumerateCmpRecursive(JoinRelationSet *left, JoinRelationSet *right,
                                               unordered_set<idx_t> exclusion_set) {
	// get the neighbors of the second relation under the exclusion set
	auto neighbors = query_graph.GetNeighbors(right, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}
	vector<JoinRelationSet *> union_sets;
	union_sets.resize(neighbors.size());
	for (idx_t i = 0; i < neighbors.size(); i++) {
		auto neighbor = set_manager.GetJoinRelation(neighbors[i]);
		// emit the combinations of this node and its neighbors
		auto combined_set = set_manager.Union(right, neighbor);
		if (combined_set->count > right->count && plans.find(combined_set) != plans.end()) {
			auto connections = query_graph.GetConnections(left, combined_set);
			if (!connections.empty()) {
				if (!TryEmitPair(left, combined_set, connections)) {
					return false;
				}
			}
		}
		union_sets[i] = combined_set;
	}
	// recursively enumerate the sets
	unordered_set<idx_t> new_exclusion_set = exclusion_set;
	for (idx_t i = 0; i < neighbors.size(); i++) {
		// updated the set of excluded entries with this neighbor
		new_exclusion_set.insert(neighbors[i]);
		if (!EnumerateCmpRecursive(left, union_sets[i], new_exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool JoinOrderOptimizer::EnumerateCSGRecursive(JoinRelationSet *node, unordered_set<idx_t> &exclusion_set) {
	// find neighbors of S under the exclusion set
	auto neighbors = query_graph.GetNeighbors(node, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}
	vector<JoinRelationSet *> union_sets;
	union_sets.resize(neighbors.size());
	for (idx_t i = 0; i < neighbors.size(); i++) {
		auto neighbor = set_manager.GetJoinRelation(neighbors[i]);
		// emit the combinations of this node and its neighbors
		auto new_set = set_manager.Union(node, neighbor);
		if (new_set->count > node->count && plans.find(new_set) != plans.end()) {
			if (!EmitCSG(new_set)) {
				return false;
			}
		}
		union_sets[i] = new_set;
	}
	// recursively enumerate the sets
	unordered_set<idx_t> new_exclusion_set = exclusion_set;
	for (idx_t i = 0; i < neighbors.size(); i++) {
		// Reset the exclusion set so that the algorithm considers all combinations
		// of the exclusion_set with a subset of neighbors.
		new_exclusion_set = exclusion_set;
		new_exclusion_set.insert(neighbors[i]);
		// updated the set of excluded entries with this neighbor
		if (!EnumerateCSGRecursive(union_sets[i], new_exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool JoinOrderOptimizer::SolveJoinOrderExactly() {
	// now we perform the actual dynamic programming to compute the final result
	// we enumerate over all the possible pairs in the neighborhood
	for (idx_t i = relations.size(); i > 0; i--) {
		// for every node in the set, we consider it as the start node once
		auto start_node = set_manager.GetJoinRelation(i - 1);
		// emit the start node
		if (!EmitCSG(start_node)) {
			return false;
		}
		// initialize the set of exclusion_set as all the nodes with a number below this
		unordered_set<idx_t> exclusion_set;
		for (idx_t j = 0; j < i - 1; j++) {
			exclusion_set.insert(j);
		}
		// then we recursively search for neighbors that do not belong to the banned entries
		if (!EnumerateCSGRecursive(start_node, exclusion_set)) {
			return false;
		}
	}
	return true;
}

static vector<unordered_set<idx_t>> AddSuperSets(vector<unordered_set<idx_t>> current,
                                                 const vector<idx_t> &all_neighbors) {
	vector<unordered_set<idx_t>> ret;
	for (auto &neighbor : all_neighbors) {
		for (auto &neighbor_set : current) {
			auto max_val = std::max_element(neighbor_set.begin(), neighbor_set.end());
			if (*max_val >= neighbor) {
				continue;
			}
			if (neighbor_set.count(neighbor) == 0) {
				unordered_set<idx_t> new_set;
				for (auto &n : neighbor_set) {
					new_set.insert(n);
				}
				new_set.insert(neighbor);
				ret.push_back(new_set);
			}
		}
	}
	return ret;
}

// works by first creating all sets with cardinality 1
// then iterates over each previously created group of subsets and will only add a neighbor if the neighbor
// is greater than all relations in the set.
static vector<unordered_set<idx_t>> GetAllNeighborSets(JoinRelationSet *new_set, unordered_set<idx_t> &exclusion_set,
                                                       vector<idx_t> neighbors) {
	vector<unordered_set<idx_t>> ret;
	sort(neighbors.begin(), neighbors.end());
	vector<unordered_set<idx_t>> added;
	for (auto &neighbor : neighbors) {
		added.push_back(unordered_set<idx_t>({neighbor}));
		ret.push_back(unordered_set<idx_t>({neighbor}));
	}
	do {
		added = AddSuperSets(added, neighbors);
		for (auto &d : added) {
			ret.push_back(d);
		}
	} while (!added.empty());
#if DEBUG
	// drive by test to make sure we have an accurate amount of
	// subsets, and that each neighbor is in a correct amount
	// of those subsets.
	D_ASSERT(ret.size() == pow(2, neighbors.size()) - 1);
	for (auto &n : neighbors) {
		idx_t count = 0;
		for (auto &set : ret) {
			if (set.count(n) >= 1) {
				count += 1;
			}
		}
		D_ASSERT(count == pow(2, neighbors.size() - 1));
	}
#endif
	return ret;
}

void JoinOrderOptimizer::UpdateDPTree(JoinNode *new_plan) {
	if (!NodeInFullPlan(new_plan)) {
		// if the new node is not in the full plan, feel free to return
		// because you won't be updating the full plan.
		return;
	}
	auto new_set = new_plan->set;
	// now update every plan that uses this plan
	unordered_set<idx_t> exclusion_set;
	for (idx_t i = 0; i < new_set->count; i++) {
		exclusion_set.insert(new_set->relations[i]);
	}
	auto neighbors = query_graph.GetNeighbors(new_set, exclusion_set);
	auto all_neighbors = GetAllNeighborSets(new_set, exclusion_set, neighbors);
	for (auto neighbor : all_neighbors) {
		auto neighbor_relation = set_manager.GetJoinRelation(neighbor);
		auto combined_set = set_manager.Union(new_set, neighbor_relation);

		auto combined_set_plan = plans.find(combined_set);
		if (combined_set_plan == plans.end()) {
			continue;
		}

		double combined_set_plan_cost = combined_set_plan->second->GetCost();
		auto connections = query_graph.GetConnections(new_set, neighbor_relation);
		// recurse and update up the tree if the combined set produces a plan with a lower cost
		// only recurse on neighbor relations that have plans.
		auto right_plan = plans.find(neighbor_relation);
		if (right_plan == plans.end()) {
			continue;
		}
		auto updated_plan = EmitPair(new_set, neighbor_relation, connections);
		// <= because the child node has already been replaced. You need to
		// replace the parent node as well in this case
		if (updated_plan->GetCost() < combined_set_plan_cost) {
			UpdateDPTree(updated_plan);
		}
	}
}

void JoinOrderOptimizer::SolveJoinOrderApproximately() {
	// at this point, we exited the dynamic programming but did not compute the final join order because it took too
	// long instead, we use a greedy heuristic to obtain a join ordering now we use Greedy Operator Ordering to
	// construct the result tree first we start out with all the base relations (the to-be-joined relations)
	vector<JoinRelationSet *> join_relations; // T in the paper
	for (idx_t i = 0; i < relations.size(); i++) {
		join_relations.push_back(set_manager.GetJoinRelation(i));
	}
	while (join_relations.size() > 1) {
		// now in every step of the algorithm, we greedily pick the join between the to-be-joined relations that has the
		// smallest cost. This is O(r^2) per step, and every step will reduce the total amount of relations to-be-joined
		// by 1, so the total cost is O(r^3) in the amount of relations
		idx_t best_left = 0, best_right = 0;
		JoinNode *best_connection = nullptr;
		for (idx_t i = 0; i < join_relations.size(); i++) {
			auto left = join_relations[i];
			for (idx_t j = i + 1; j < join_relations.size(); j++) {
				auto right = join_relations[j];
				// check if we can connect these two relations
				auto connection = query_graph.GetConnections(left, right);
				if (!connection.empty()) {
					// we can check the cost of this connection
					auto node = EmitPair(left, right, connection);

					// update the DP tree in case a plan created by the DP algorithm uses the node
					// that was potentially just updated by EmitPair. You will get a use-after-free
					// error if future plans rely on the old node that was just replaced.
					// if node in FullPath, then updateDP tree.
					UpdateDPTree(node);

					if (!best_connection || node->GetCost() < best_connection->GetCost()) {
						// best pair found so far
						best_connection = node;
						best_left = i;
						best_right = j;
					}
				}
			}
		}
		if (!best_connection) {
			// could not find a connection, but we were not done with finding a completed plan
			// we have to add a cross product; we add it between the two smallest relations
			JoinNode *smallest_plans[2] = {nullptr};
			idx_t smallest_index[2];
			for (idx_t i = 0; i < join_relations.size(); i++) {
				// get the plan for this relation
				auto current_plan = plans[join_relations[i]].get();
				// check if the cardinality is smaller than the smallest two found so far
				for (idx_t j = 0; j < 2; j++) {
					if (!smallest_plans[j] ||
					    smallest_plans[j]->GetCardinality<double>() > current_plan->GetCardinality<double>()) {
						smallest_plans[j] = current_plan;
						smallest_index[j] = i;
						break;
					}
				}
			}
			if (!smallest_plans[0] || !smallest_plans[1]) {
				throw InternalException("Internal error in join order optimizer");
			}
			D_ASSERT(smallest_plans[0] && smallest_plans[1]);
			D_ASSERT(smallest_index[0] != smallest_index[1]);
			auto left = smallest_plans[0]->set;
			auto right = smallest_plans[1]->set;
			// create a cross product edge (i.e. edge with empty filter) between these two sets in the query graph
			query_graph.CreateEdge(left, right, nullptr);
			// now emit the pair and continue with the algorithm
			auto connections = query_graph.GetConnections(left, right);
			D_ASSERT(!connections.empty());

			best_connection = EmitPair(left, right, connections);
			best_left = smallest_index[0];
			best_right = smallest_index[1];

			UpdateDPTree(best_connection);
			// the code below assumes best_right > best_left
			if (best_left > best_right) {
				std::swap(best_left, best_right);
			}
		}
		// now update the to-be-checked pairs
		// remove left and right, and add the combination

		// important to erase the biggest element first
		// if we erase the smallest element first the index of the biggest element changes
		D_ASSERT(best_right > best_left);
		join_relations.erase(join_relations.begin() + best_right);
		join_relations.erase(join_relations.begin() + best_left);
		join_relations.push_back(best_connection->set);
	}
}

void JoinOrderOptimizer::SolveJoinOrder() {
	// first try to solve the join order exactly
	if (!SolveJoinOrderExactly()) {
		// otherwise, if that times out we resort to a greedy algorithm
		SolveJoinOrderApproximately();
	}
}

void JoinOrderOptimizer::GenerateCrossProducts() {
	// generate a set of cross products to combine the currently available plans into a full join plan
	// we create edges between every relation with a high cost
	for (idx_t i = 0; i < relations.size(); i++) {
		auto left = set_manager.GetJoinRelation(i);
		for (idx_t j = 0; j < relations.size(); j++) {
			if (i != j) {
				auto right = set_manager.GetJoinRelation(j);
				query_graph.CreateEdge(left, right, nullptr);
				query_graph.CreateEdge(right, left, nullptr);
			}
		}
	}
}

static unique_ptr<LogicalOperator> ExtractJoinRelation(SingleJoinRelation &rel) {
	auto &children = rel.parent->children;
	for (idx_t i = 0; i < children.size(); i++) {
		if (children[i].get() == rel.op) {
			// found it! take ownership of it from the parent
			auto result = std::move(children[i]);
			children.erase(children.begin() + i);
			return result;
		}
	}
	throw Exception("Could not find relation in parent node (?)");
}

pair<JoinRelationSet *, unique_ptr<LogicalOperator>>
JoinOrderOptimizer::GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations, JoinNode *node) {
	JoinRelationSet *left_node = nullptr, *right_node = nullptr;
	JoinRelationSet *result_relation;
	unique_ptr<LogicalOperator> result_operator;
	if (node->left && node->right) {
		// generate the left and right children
		auto left = GenerateJoins(extracted_relations, node->left);
		auto right = GenerateJoins(extracted_relations, node->right);

		if (node->info->filters.empty()) {
			// no filters, create a cross product
			result_operator = LogicalCrossProduct::Create(std::move(left.second), std::move(right.second));
		} else {
			// we have filters, create a join node
			auto join = make_unique<LogicalComparisonJoin>(JoinType::INNER);
			join->children.push_back(std::move(left.second));
			join->children.push_back(std::move(right.second));
			// set the join conditions from the join node
			for (auto &f : node->info->filters) {
				// extract the filter from the operator it originally belonged to
				D_ASSERT(filters[f->filter_index]);
				auto condition = std::move(filters[f->filter_index]);
				// now create the actual join condition
				D_ASSERT((JoinRelationSet::IsSubset(left.first, f->left_set) &&
				          JoinRelationSet::IsSubset(right.first, f->right_set)) ||
				         (JoinRelationSet::IsSubset(left.first, f->right_set) &&
				          JoinRelationSet::IsSubset(right.first, f->left_set)));
				JoinCondition cond;
				D_ASSERT(condition->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
				auto &comparison = (BoundComparisonExpression &)*condition;
				// we need to figure out which side is which by looking at the relations available to us
				bool invert = !JoinRelationSet::IsSubset(left.first, f->left_set);
				cond.left = !invert ? std::move(comparison.left) : std::move(comparison.right);
				cond.right = !invert ? std::move(comparison.right) : std::move(comparison.left);
				cond.comparison = condition->type;

				if (invert) {
					// reverse comparison expression if we reverse the order of the children
					cond.comparison = FlipComparisionExpression(cond.comparison);
				}
				join->conditions.push_back(std::move(cond));
			}
			D_ASSERT(!join->conditions.empty());
			result_operator = std::move(join);
		}
		left_node = left.first;
		right_node = right.first;
		right_node = right.first;
		result_relation = set_manager.Union(left_node, right_node);
	} else {
		// base node, get the entry from the list of extracted relations
		D_ASSERT(node->set->count == 1);
		D_ASSERT(extracted_relations[node->set->relations[0]]);
		result_relation = node->set;
		result_operator = std::move(extracted_relations[node->set->relations[0]]);
	}
	result_operator->estimated_cardinality = node->GetCardinality<idx_t>();
	result_operator->has_estimated_cardinality = true;
	result_operator->estimated_props = node->estimated_props->Copy();
	// check if we should do a pushdown on this node
	// basically, any remaining filter that is a subset of the current relation will no longer be used in joins
	// hence we should push it here
	for (auto &filter_info : filter_infos) {
		// check if the filter has already been extracted
		auto info = filter_info.get();
		if (filters[info->filter_index]) {
			// now check if the filter is a subset of the current relation
			// note that infos with an empty relation set are a special case and we do not push them down
			if (info->set->count > 0 && JoinRelationSet::IsSubset(result_relation, info->set)) {
				auto filter = std::move(filters[info->filter_index]);
				// if it is, we can push the filter
				// we can push it either into a join or as a filter
				// check if we are in a join or in a base table
				if (!left_node || !info->left_set) {
					// base table or non-comparison expression, push it as a filter
					result_operator = PushFilter(std::move(result_operator), std::move(filter));
					continue;
				}
				// the node below us is a join or cross product and the expression is a comparison
				// check if the nodes can be split up into left/right
				bool found_subset = false;
				bool invert = false;
				if (JoinRelationSet::IsSubset(left_node, info->left_set) &&
				    JoinRelationSet::IsSubset(right_node, info->right_set)) {
					found_subset = true;
				} else if (JoinRelationSet::IsSubset(right_node, info->left_set) &&
				           JoinRelationSet::IsSubset(left_node, info->right_set)) {
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
				auto &comparison = (BoundComparisonExpression &)*filter;
				// we need to figure out which side is which by looking at the relations available to us
				cond.left = !invert ? std::move(comparison.left) : std::move(comparison.right);
				cond.right = !invert ? std::move(comparison.right) : std::move(comparison.left);
				cond.comparison = comparison.type;
				if (invert) {
					// reverse comparison expression if we reverse the order of the children
					cond.comparison = FlipComparisionExpression(comparison.type);
				}
				// now find the join to push it into
				auto node = result_operator.get();
				if (node->type == LogicalOperatorType::LOGICAL_FILTER) {
					node = node->children[0].get();
				}
				if (node->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
					// turn into comparison join
					auto comp_join = make_unique<LogicalComparisonJoin>(JoinType::INNER);
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
					D_ASSERT(node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
					auto &comp_join = (LogicalComparisonJoin &)*node;
					comp_join.conditions.push_back(std::move(cond));
				}
			}
		}
	}
	return make_pair(result_relation, std::move(result_operator));
}

unique_ptr<LogicalOperator> JoinOrderOptimizer::RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode *node) {
	// now we have to rewrite the plan
	bool root_is_join = plan->children.size() > 1;

	// first we will extract all relations from the main plan
	vector<unique_ptr<LogicalOperator>> extracted_relations;
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
			join_tree.second = PushFilter(std::move(join_tree.second), std::move(filter));
		}
	}

	// find the first join in the relation to know where to place this node
	if (root_is_join) {
		// first node is the join, return it immediately
		return std::move(join_tree.second);
	}
	D_ASSERT(plan->children.size() == 1);
	// have to move up through the relations
	auto op = plan.get();
	auto parent = plan.get();
	while (op->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT &&
	       op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		D_ASSERT(op->children.size() == 1);
		parent = op;
		op = op->children[0].get();
	}
	// have to replace at this node
	parent->children[0] = std::move(join_tree.second);
	return plan;
}

// the join ordering is pretty much a straight implementation of the paper "Dynamic Programming Strikes Back" by Guido
// Moerkotte and Thomas Neumannn, see that paper for additional info/documentation bonus slides:
// https://db.in.tum.de/teaching/ws1415/queryopt/chapter3.pdf?lang=de
// FIXME: incorporate cardinality estimation into the plans, possibly by pushing samples?
unique_ptr<LogicalOperator> JoinOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	D_ASSERT(filters.empty() && relations.empty()); // assert that the JoinOrderOptimizer has not been used before
	LogicalOperator *op = plan.get();
	// now we optimize the current plan
	// we skip past until we find the first projection, we do this because the HAVING clause inserts a Filter AFTER the
	// group by and this filter cannot be reordered
	// extract a list of all relations that have to be joined together
	// and a list of all conditions that is applied to them
	vector<LogicalOperator *> filter_operators;
	if (!ExtractJoinRelations(*op, filter_operators)) {
		// do not support reordering this type of plan
		return plan;
	}
	if (relations.size() <= 1) {
		// at most one relation, nothing to reorder
		return plan;
	}
	// now that we know we are going to perform join ordering we actually extract the filters, eliminating duplicate
	// filters in the process
	expression_set_t filter_set;
	for (auto &op : filter_operators) {
		if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = (LogicalComparisonJoin &)*op;
			D_ASSERT(join.join_type == JoinType::INNER);
			D_ASSERT(join.expressions.empty());
			for (auto &cond : join.conditions) {
				auto comparison =
				    make_unique<BoundComparisonExpression>(cond.comparison, std::move(cond.left), std::move(cond.right));
				if (filter_set.find(comparison.get()) == filter_set.end()) {
					filter_set.insert(comparison.get());
					filters.push_back(std::move(comparison));
				}
			}
			join.conditions.clear();
		} else {
			for (auto &expression : op->expressions) {
				if (filter_set.find(expression.get()) == filter_set.end()) {
					filter_set.insert(expression.get());
					filters.push_back(std::move(expression));
				}
			}
			op->expressions.clear();
		}
	}
	// create potential edges from the comparisons
	for (idx_t i = 0; i < filters.size(); i++) {
		auto &filter = filters[i];
		auto info = make_unique<FilterInfo>();
		auto filter_info = info.get();
		filter_infos.push_back(std::move(info));
		// first extract the relation set for the entire filter
		unordered_set<idx_t> bindings;
		ExtractBindings(*filter, bindings);
		filter_info->set = set_manager.GetJoinRelation(bindings);
		filter_info->filter_index = i;
		// now check if it can be used as a join predicate
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			auto comparison = (BoundComparisonExpression *)filter.get();
			// extract the bindings that are required for the left and right side of the comparison
			unordered_set<idx_t> left_bindings, right_bindings;
			ExtractBindings(*comparison->left, left_bindings);
			ExtractBindings(*comparison->right, right_bindings);
			GetColumnBinding(*comparison->left, filter_info->left_binding);
			GetColumnBinding(*comparison->right, filter_info->right_binding);
			if (!left_bindings.empty() && !right_bindings.empty()) {
				// both the left and the right side have bindings
				// first create the relation sets, if they do not exist
				filter_info->left_set = set_manager.GetJoinRelation(left_bindings);
				filter_info->right_set = set_manager.GetJoinRelation(right_bindings);
				// we can only create a meaningful edge if the sets are not exactly the same
				if (filter_info->left_set != filter_info->right_set) {
					// check if the sets are disjoint
					if (Disjoint(left_bindings, right_bindings)) {
						// they are disjoint, we only need to create one set of edges in the join graph
						query_graph.CreateEdge(filter_info->left_set, filter_info->right_set, filter_info);
						query_graph.CreateEdge(filter_info->right_set, filter_info->left_set, filter_info);
					} else {
						continue;
					}
					continue;
				}
			}
		}
	}
	// now use dynamic programming to figure out the optimal join order
	// First we initialize each of the single-node plans with themselves and with their cardinalities these are the leaf
	// nodes of the join tree NOTE: we can just use pointers to JoinRelationSet* here because the GetJoinRelation
	// function ensures that a unique combination of relations will have a unique JoinRelationSet object.
	vector<NodeOp> nodes_ops;
	for (idx_t i = 0; i < relations.size(); i++) {
		auto &rel = *relations[i];
		auto node = set_manager.GetJoinRelation(i);
		nodes_ops.emplace_back(NodeOp(make_unique<JoinNode>(node, 0), rel.op));
	}

	cardinality_estimator.InitCardinalityEstimatorProps(&nodes_ops, &filter_infos);

	for (auto &node_op : nodes_ops) {
		D_ASSERT(node_op.node);
		plans[node_op.node->set] = std::move(node_op.node);
	}
	// now we perform the actual dynamic programming to compute the final result
	SolveJoinOrder();
	// now the optimal join path should have been found
	// get it from the node
	unordered_set<idx_t> bindings;
	for (idx_t i = 0; i < relations.size(); i++) {
		bindings.insert(i);
	}
	auto total_relation = set_manager.GetJoinRelation(bindings);
	auto final_plan = plans.find(total_relation);
	if (final_plan == plans.end()) {
		// could not find the final plan
		// this should only happen in case the sets are actually disjunct
		// in this case we need to generate cross product to connect the disjoint sets
		if (context.config.force_no_cross_product) {
			throw InvalidInputException(
			    "Query requires a cross-product, but 'force_no_cross_product' PRAGMA is enabled");
		}
		GenerateCrossProducts();
		//! solve the join order again
		SolveJoinOrder();
		// now we can obtain the final plan!
		final_plan = plans.find(total_relation);
		D_ASSERT(final_plan != plans.end());
	}
	// now perform the actual reordering
	return RewritePlan(std::move(plan), final_plan->second.get());
}

} // namespace duckdb
