#include "optimizer/join_order_optimizer.hpp"
#include "planner/operator/list.hpp"
#include "parser/expression/list.hpp"

using namespace duckdb;
using namespace std;

using JoinNode = JoinOrderOptimizer::JoinNode;

//! Returns true if A and B are disjoint, false otherwise
template<class T>
static bool Disjoint(unordered_set<T>& a, unordered_set<T>& b) {
	for(auto &entry : a) {
		if (b.find(entry) != b.end()) {
			return false;
		}
	}
	return true;
}

unique_ptr<Expression> JoinOrderOptimizer::Visit(SubqueryExpression &subquery) {
	// we perform join reordering within the subquery expression
	JoinOrderOptimizer optimizer;
	subquery.op = optimizer.Optimize(move(subquery.op));
	return nullptr;
}

//! Extract the set of relations referred to inside an expression
bool JoinOrderOptimizer::ExtractBindings(Expression &expression, unordered_set<size_t> &bindings) {
	if (expression.type == ExpressionType::COLUMN_REF) {
		auto &colref = (ColumnRefExpression&) expression;
		if (colref.depth > 0) {
			// correlated column reference, we don't allow this to be reshuffled inside the subquery
			// we clear any currently made bindings
			bindings.clear();
			return false;
		}
		if (colref.index != (size_t) -1) {
			// column reference has already been bound, don't use it for reordering
			bindings.clear();
			return false;
		}
		assert(colref.binding.table_index != (size_t) -1);
		// map the base table index to the relation index used by the JoinOrderOptimizer
		assert(relation_mapping.find(colref.binding.table_index) != relation_mapping.end());
		bindings.insert(relation_mapping[colref.binding.table_index]);
	}
	if (expression.type == ExpressionType::SELECT_SUBQUERY) {
		auto &subquery = (SubqueryExpression&) expression;
		if (subquery.is_correlated) {
			// we don't allow correlated subqueries to be reordered
			// FIXME: we could extract all the correlated table_indexes referenced inside the subquery here
			bindings.clear();
			return false;
		}
	}
	for(auto &child : expression.children) {
		if (!ExtractBindings(*child, bindings)) {
			return false;
		}
	}
	return true;
}

static void ExtractFilters(LogicalOperator *op, vector<unique_ptr<FilterInfo>>& filters) {
	for(size_t i = 0; i < op->expressions.size(); i++) {
		auto info = make_unique<FilterInfo>();
		info->filter = op->expressions[i].get();
		info->parent = op;
		filters.push_back(move(info));
	}
}

static void GetTableReferences(LogicalOperator* op, unordered_set<size_t>& bindings) {
	if (op->type == LogicalOperatorType::GET) {
		auto get = (LogicalGet*) op;
		bindings.insert(get->table_index);
	} else if (op->type == LogicalOperatorType::SUBQUERY) {
		auto subquery = (LogicalSubquery*) op;
		bindings.insert(subquery->table_index);
	} else if (op->type == LogicalOperatorType::TABLE_FUNCTION) {
		auto table_function = (LogicalTableFunction*) op;
		bindings.insert(table_function->table_index);
	} else {
		// iterate over the children
		for(auto &child : op->children) {
			GetTableReferences(child.get(), bindings);
		}
	}
}

static unique_ptr<LogicalOperator> PushFilter(unique_ptr<LogicalOperator> node, unique_ptr<Expression> expr) {
	// check if we already have a logical filter
	if (node->type != LogicalOperatorType::FILTER) {
		// we don't, we need to create one
		auto filter = make_unique<LogicalFilter>();
		filter->children.push_back(move(node));
		node = move(filter);
	}
	// push the filter into the LogicalFilter
	assert(node->type == LogicalOperatorType::FILTER);
	auto filter = (LogicalFilter*) node.get();
	filter->expressions.push_back(move(expr));
	return node;
}

static JoinSide CombineJoinSide(JoinSide left, JoinSide right) {
	if (left == JoinSide::NONE) {
		return right;
	}
	if (right == JoinSide::NONE) {
		return left;
	}
	if (left != right) {
		return JoinSide::BOTH;
	}
	return left;
}

static JoinSide GetJoinSide(Expression &expression, unordered_set<size_t> &left_bindings, unordered_set<size_t> &right_bindings) {
	if (expression.type == ExpressionType::COLUMN_REF) {
		auto &colref = (ColumnRefExpression&) expression;
		if (colref.depth > 0) {
			// correlated column reference, we can't join on this
			return JoinSide::BOTH;
		}
		if (colref.index != (size_t) -1) {
			// column reference has already been bound, don't use it for reordering
			return JoinSide::NONE;
		}
		if (left_bindings.find(colref.binding.table_index) != left_bindings.end()) {
			// column references table on left side
			assert(right_bindings.find(colref.binding.table_index) == right_bindings.end());
			return JoinSide::LEFT;
		} else {
			// column references table on right side
			assert(right_bindings.find(colref.binding.table_index) != right_bindings.end());
			return JoinSide::RIGHT;
		}
	}
	if (expression.type == ExpressionType::SELECT_SUBQUERY) {
		return JoinSide::BOTH;
	}
	JoinSide join_side = JoinSide::NONE;
	for(auto &child : expression.children) {
		auto child_side = GetJoinSide(*child, left_bindings, right_bindings);
		join_side = CombineJoinSide(child_side, join_side);
	}
	return join_side;
}

static unique_ptr<LogicalOperator> CreateJoinCondition(unique_ptr<LogicalOperator> op, LogicalJoin &join, unique_ptr<Expression> expr, unordered_set<size_t>& left_bindings, unordered_set<size_t>& right_bindings) {
	if (expr->type >= ExpressionType::COMPARE_EQUAL &&
		expr->type <= ExpressionType::COMPARE_NOTLIKE) {
		// comparison
		auto left_side = GetJoinSide(*expr->children[0], left_bindings, right_bindings);
		auto right_side = GetJoinSide(*expr->children[1], left_bindings, right_bindings);
		auto total_side = CombineJoinSide(left_side, right_side);
		if (total_side != JoinSide::BOTH) {
			// join condition does not reference both sides, add it as filter under the join
			int push_side = total_side == JoinSide::LEFT ? 0 : 1;
			join.children[push_side] = PushFilter(move(join.children[push_side]), move(expr));
		} else {
			// assert both
			assert(left_side != JoinSide::BOTH && right_side != JoinSide::BOTH);
			JoinCondition condition;
			condition.comparison = expr->type;
			int left_side = 0;
			if (left_side == JoinSide::RIGHT) {
				// left = right, right = left, flip the comparison symbol and reverse sides
				left_side = 1;
				condition.comparison = ComparisonExpression::FlipComparisionExpression(expr->type);
			}
			condition.left = move(expr->children[left_side]);
			condition.right = move(expr->children[1 - left_side]);
			join.conditions.push_back(move(condition));
		}
	} else if (expr->type == ExpressionType::OPERATOR_NOT) {
		assert(expr->children.size() == 1);
		ExpressionType child_type = expr->children[0]->GetExpressionType();

		if (child_type < ExpressionType::COMPARE_EQUAL ||
			child_type > ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
			throw Exception("ON NOT only supports comparision operators");
		}
		// switcheroo the child condition
		// our join needs to compare explicit left and right sides. So we
		// invert the condition to express NOT, this way we can still use
		// equi-joins
		expr->children[0]->type = ComparisonExpression::NegateComparisionExpression(child_type);
		return CreateJoinCondition(move(op), join, move(expr->children[0]), left_bindings, right_bindings);
	} else {
		// unrecognized type for join condition
		// push as filter under the join
		op = PushFilter(move(op), move(expr));
	}
	return op;
}

//! Resolve join conditions for non-inner joins
unique_ptr<LogicalOperator> JoinOrderOptimizer::ResolveJoinConditions(unique_ptr<LogicalOperator> op) {
	// first resolve the join conditions of any children
	for(size_t i = 0; i < op->children.size(); i++) {
		op->children[i] = ResolveJoinConditions(move(op->children[i]));
	}
	if (op->type == LogicalOperatorType::JOIN) {
		LogicalJoin &join = (LogicalJoin&) *op;
		if (join.type != JoinType::INNER) {
			// non-inner join, turn expressions into proper join conditions
			unordered_set<size_t> left_bindings, right_bindings;
			GetTableReferences(join.children[0].get(), left_bindings);
			GetTableReferences(join.children[1].get(), right_bindings);
			// now for each expression turn it into a proper JoinCondition
			for(size_t i = 0; i < join.expressions.size(); i++) {
				op = CreateJoinCondition(move(op), join, move(join.expressions[i]), left_bindings, right_bindings);
			}
			join.expressions.clear();
		}
	}
	return op;
}

bool JoinOrderOptimizer::ExtractJoinRelations(LogicalOperator &input_op, vector<unique_ptr<FilterInfo>>& filters, LogicalOperator *parent) {
	LogicalOperator *op = &input_op;
	while(op->children.size() == 1 && op->type != LogicalOperatorType::SUBQUERY) {
		if (op->type == LogicalOperatorType::FILTER) {
			// extract join conditions from filter
			ExtractFilters(op, filters);
		}
		op = op->children[0].get();
	}
	
	if (op->type == LogicalOperatorType::JOIN) {
		LogicalJoin *join = (LogicalJoin*) op;
		if (join->type != JoinType::INNER) {
			// non-inner join
			// we do not reorder non-inner joins yet, however we do want to expand the potential join graph around them
			// non-inner joins are also tricky because we can't freely make conditions through them
			// e.g. suppose we have (left LEFT OUTER JOIN right WHERE right IS NOT NULL), the join can generate
			// new NULL values in the right side, so pushing this condition through the join leads to incorrect results
			// for this reason, we just start a new JoinOptimizer pass in each of the children of the join
			JoinOrderOptimizer optimizer_left, optimizer_right;
			join->children[0] = optimizer_left.Optimize(move(join->children[0]));
			join->children[1] = optimizer_right.Optimize(move(join->children[1]));
			// after this we want to treat this node as one  "end node" (like e.g. a base relation)
			// however the join refers to multiple base relations
			// enumerate all base relations obtained from this join and add them to the relation mapping
			// also, we have to resolve the join conditions for the joins here
			// get the left and right bindings
			unordered_set<size_t> bindings;
			GetTableReferences(join, bindings);
			// now create the relation that refers to all these bindings
			auto relation = make_unique<Relation>(&input_op, parent);
			for(size_t it : bindings) {
				relation_mapping[it] = relations.size();
			}
			relations.push_back(move(relation));
			return true;
		} else {
			// extract join conditions from inner join
			ExtractFilters(op, filters);
		}
	}
	if (op->type == LogicalOperatorType::JOIN ||
	    op->type == LogicalOperatorType::CROSS_PRODUCT) {
		// inner join or cross product
		if (!ExtractJoinRelations(*op->children[0], filters, op)) {
			return false;
		}
		if (!ExtractJoinRelations(*op->children[1], filters, op)) {
			return false;
		}
		return true;
	} else if (op->type == LogicalOperatorType::GET) {
		// base table scan, add to set of relations
		auto get = (LogicalGet*) op;
		auto relation = make_unique<Relation>(&input_op, parent);
		relation_mapping[get->table_index] = relations.size();
		relations.push_back(move(relation));
		return true;
	} else if (op->type == LogicalOperatorType::SUBQUERY) {
		auto subquery = (LogicalSubquery*) op;
		assert(op->children.size() == 1);
		// we run the join order optimizer witin the subquery as well
		JoinOrderOptimizer optimizer;
		op->children[0] = optimizer.Optimize(move(op->children[0]));
		// now we add the subquery to the set of relations
		auto relation = make_unique<Relation>(&input_op, parent);
		relation_mapping[subquery->table_index] = relations.size();
		relations.push_back(move(relation));
		return true;
	} else if (op->type == LogicalOperatorType::TABLE_FUNCTION) {
		// table function call, add to set of relations
		auto table_function = (LogicalTableFunction*) op;
		auto relation = make_unique<Relation>(&input_op, parent);
		relation_mapping[table_function->table_index] = relations.size();
		relations.push_back(move(relation));
		return true;
	}
	return false;
}


void JoinOrderOptimizer::AddPushdownFilter(RelationSet *set, FilterInfo* filter) {
	// look it up in the tree
	FilterNode* info = &pushdown_filters;
	for(size_t i = 0; i < set->count; i++) {
		auto entry = info->children.find(set->relations[i]);
		if (entry == info->children.end()) {
			// node not found, create it
			auto insert_it = info->children.insert(make_pair(set->relations[i], make_unique<FilterNode>()));
			entry = insert_it.first;
		}
		// move to the next node
		info = entry->second.get();
	}
	info->filters.push_back(filter);
}

void JoinOrderOptimizer::EnumeratePushdownFilters(RelationSet *node, function<bool(FilterInfo*)> callback) {
	for(size_t j = 0; j < node->count; j++) {
		FilterNode *info = &pushdown_filters;
		for(size_t i = j; i < node->count; i++) {
			auto entry = info->children.find(node->relations[i]);
			if (entry == info->children.end()) {
				// node not found
				break;
			}
			info = entry->second.get();
			// check if any subset of the other set is in this sets neighbors
			for(size_t k = 0; k < info->filters.size(); k++) {
				if (callback(info->filters[k])) {
					// remove the filter from the set of filters
					info->filters.erase(info->filters.begin() + k);
					k--;
				}
			}
		}
	}
}

//! Update the exclusion set with all entries in the subgraph
static void UpdateExclusionSet(RelationSet *node, unordered_set<size_t> &exclusion_set) {
	for(size_t i = 0; i < node->count; i++) {
		exclusion_set.insert(node->relations[i]);
	}
}

//! Create a new JoinTree node by joining together two previous JoinTree nodes
static unique_ptr<JoinNode> CreateJoinTree(RelationSet *set, NeighborInfo *info, JoinNode *left, JoinNode *right) {
	// for the hash join we want the right side (build side) to have the smallest cardinality
	// also just a heuristic but for now...
	// FIXME: we should probably actually benchmark that as well
	// FIXME: should consider different join algorithms, should we pick a join algorithm here as well? (probably)
	if (left->cardinality < right->cardinality) {
		return CreateJoinTree(set, info, right, left);
	}
	// the expected cardinality is the max of the child cardinalities
	// FIXME: we should obviously use better cardinality estimation here
	// but for now we just assume foreign key joins only
	size_t expected_cardinality;
	if (info->filters.size() == 0) {
		// cross product
		expected_cardinality = left->cardinality * right->cardinality;
	} else {
		// normal join, expect foreign key join
		expected_cardinality = std::max(left->cardinality, right->cardinality);
	}
	// cost is expected_cardinality plus the cost of the previous plans
	size_t cost = expected_cardinality + left->cost + right->cost;
	return make_unique<JoinNode>(set, info, left, right, expected_cardinality, cost);
}

void JoinOrderOptimizer::EmitPair(RelationSet *left, RelationSet *right, NeighborInfo *info) {
	// get the left and right join plans
	auto &left_plan = plans[left];
	auto &right_plan = plans[right];
	auto new_set = set_manager.Union(left, right);
	// create the join tree based on combining the two plans
	auto new_plan = CreateJoinTree(new_set, info, left_plan.get(), right_plan.get());
	// check if this plan is the optimal plan we found for this set of relations
	auto entry = plans.find(new_set);
	if (entry == plans.end() || new_plan->cost < entry->second->cost) {
		plans[new_set] = move(new_plan);
	}
}

void JoinOrderOptimizer::EmitCSG(RelationSet *node) {
	// create the exclusion set as everything inside the subgraph AND anything with members BELOW it
	unordered_set<size_t> exclusion_set;
	for(size_t i = 0; i < node->relations[0]; i++) {
		exclusion_set.insert(i);
	}
	UpdateExclusionSet(node, exclusion_set);
	// find the neighbors given this exclusion set
	auto neighbors = query_graph.GetNeighbors(node, exclusion_set);
	if (neighbors.size() == 0) {
		return;
	}
	// we iterate over the neighbors ordered by their first node
	sort(neighbors.begin(), neighbors.end());
	for(auto neighbor : neighbors) {
		// since the GetNeighbors only returns the smallest element in a list, the entry might not be connected to (only!) this neighbor,  hence we have to do a connectedness check before we can emit it
		auto neighbor_relation = set_manager.GetRelation(neighbor);
		auto connection = query_graph.GetConnection(node, neighbor_relation);
		if (connection) {
			EmitPair(node, neighbor_relation, connection);
		}
		EnumerateCmpRecursive(node, neighbor_relation, exclusion_set);
	}
}

void JoinOrderOptimizer::EnumerateCmpRecursive(RelationSet *left, RelationSet *right, unordered_set<size_t> exclusion_set) {
	// get the neighbors of the second relation under the exclusion set
	auto neighbors = query_graph.GetNeighbors(right, exclusion_set);
	if (neighbors.size() == 0) {
		return;
	}
	vector<RelationSet*> union_sets;
	union_sets.resize(neighbors.size());
	for(size_t i = 0; i < neighbors.size(); i++) {
		auto neighbor = set_manager.GetRelation(neighbors[i]);
		// emit the combinations of this node and its neighbors
		auto combined_set = set_manager.Union(right, neighbor);
		if (plans.find(combined_set) != plans.end()) {
			auto connection = query_graph.GetConnection(left, combined_set);
			if (connection) {
				EmitPair(left, combined_set, connection);
			}
		}
		union_sets[i] = combined_set;
	}
	// recursively enumerate the sets
	for(size_t i = 0; i < neighbors.size(); i++) {
		// updated the set of excluded entries with this neighbor
		unordered_set<size_t> new_exclusion_set = exclusion_set;
		new_exclusion_set.insert(neighbors[i]);
		EnumerateCmpRecursive(left, union_sets[i], new_exclusion_set);
	}
}

void JoinOrderOptimizer::EnumerateCSGRecursive(RelationSet *node, unordered_set<size_t> &exclusion_set) {
	// find neighbors of S under the exlusion set
	auto neighbors = query_graph.GetNeighbors(node, exclusion_set);
	if (neighbors.size() == 0) {
		return;
	}
	// now first emit the connected subgraphs of the neighbors
	vector<RelationSet*> union_sets;
	union_sets.resize(neighbors.size());
	for(size_t i = 0; i < neighbors.size(); i++) {
		auto neighbor = set_manager.GetRelation(neighbors[i]);
		// emit the combinations of this node and its neighbors
		auto new_set = set_manager.Union(node, neighbor);
		if (plans.find(new_set) != plans.end()) {
			EmitCSG(new_set);
		}
		union_sets[i] = new_set;
	}
	// recursively enumerate the sets
	for(size_t i = 0; i < neighbors.size(); i++) {
		// updated the set of excluded entries with this neighbor
		unordered_set<size_t> new_exclusion_set = exclusion_set;
		new_exclusion_set.insert(neighbors[i]);
		EnumerateCSGRecursive(union_sets[i] , new_exclusion_set);
	}
}

void JoinOrderOptimizer::SolveJoinOrder() {
	// now we perform the actual dynamic programming to compute the final result
	// we enumerate over all the possible pairs in the neighborhood
	for(size_t i = relations.size(); i > 0; i--) {
		// for every node in the set, we consider it as the start node once
		auto start_node = set_manager.GetRelation(i - 1);
		// emit the start node
		EmitCSG(start_node);
		// initialize the set of exclusion_set as all the nodes with a number below this
		unordered_set<size_t> exclusion_set;
		for(size_t j = 0; j < i - 1; j++) {
			exclusion_set.insert(j);
		}
		// then we recursively search for neighbors that do not belong to the banned entries
		EnumerateCSGRecursive(start_node, exclusion_set);
	}
}

void JoinOrderOptimizer::GenerateCrossProducts() {
	// generate a set of cross products to combine the currently available plans into a full join plan
	// we create edges between every relation with a high cost
	for(size_t i = 0; i < relations.size(); i++) {
		auto left = set_manager.GetRelation(i);
		for(size_t j = 0; j < relations.size(); j++) {
			if (i != j) {
				auto right =  set_manager.GetRelation(j);
				query_graph.CreateEdge(left, right, nullptr);
				query_graph.CreateEdge(right, left, nullptr);
			}
		}
	}
}

static unique_ptr<LogicalOperator> ExtractRelation(Relation &rel) {
	auto &children = rel.parent->children;
	for(size_t i = 0; i < children.size(); i++) {
		if (children[i].get() == rel.op) {
			// found it! take ownership of it from the parent
			auto result = move(children[i]);
			children.erase(children.begin() + i);
			return result;
		}
	}
	throw Exception("Could not find relation in parent node (?)");
}

static unique_ptr<Expression> ExtractFilter(FilterInfo *info) {
	auto &expressions = info->parent->expressions;
	for(size_t i = 0; i < expressions.size(); i++) {
		if (expressions[i].get() == info->filter) {
			auto result = move(expressions[i]);
			expressions.erase(expressions.begin() + i);
			return result;
		}
	}
	throw Exception("Could not find expression in parent node (?)");
}

pair<RelationSet*, unique_ptr<LogicalOperator>> JoinOrderOptimizer::GenerateJoins(vector<unique_ptr<LogicalOperator>>& extracted_relations, JoinNode* node) {
	RelationSet *result_relation;
	unique_ptr<LogicalOperator> result_operator;
	if (node->left && node->right) {
		// generate the left and right children
		auto left = GenerateJoins(extracted_relations, node->left);
		auto right = GenerateJoins(extracted_relations, node->right);
		
		if (node->info->filters.size() == 0) {
			// no filters, create a cross product
			auto join = make_unique<LogicalCrossProduct>();
			join->children.push_back(move(left.second));
			join->children.push_back(move(right.second));
			result_operator = move(join);
		} else {
			// we have filters, create a join node
			auto join = make_unique<LogicalJoin>(JoinType::INNER);
			join->children.push_back(move(left.second));
			join->children.push_back(move(right.second));
			// set the join conditions from the join node
			for(auto &f : node->info->filters) {
				// extract the filter from the operator it originally belonged to
				auto condition = ExtractFilter(f);
				// now create the actual join condition
				assert((RelationSet::IsSubset(left.first, f->left_set)  && RelationSet::IsSubset(right.first, f->right_set)) || 
					(RelationSet::IsSubset(left.first, f->right_set) && RelationSet::IsSubset(right.first, f->left_set))) ;
				JoinCondition cond;
				// we need to figure out which side is which by looking at the relations available to us
				int left_child = RelationSet::IsSubset(left.first, f->left_set) ? 0 : 1;
				int right_child = 1 - left_child;
				cond.left = move(condition->children[left_child]);
				cond.right = move(condition->children[right_child]);
				cond.comparison = condition->type;
				join->conditions.push_back(move(cond));
			}
			assert(join->conditions.size() > 0);
			result_operator = move(join);
		}
		result_relation = set_manager.Union(left.first, right.first);
	} else {
		// base node, get the entry from the list of extracted relations
		assert(node->set->count == 1);
		assert(extracted_relations[node->set->relations[0]]);
		result_relation = node->set;
		result_operator = move(extracted_relations[node->set->relations[0]]);
	}
	// check if we can pushdown any filters to here
	EnumeratePushdownFilters(result_relation, [&](FilterInfo *info) -> bool {
		// found a relation to pushdown!
		result_operator = PushFilter(move(result_operator), ExtractFilter(info));
		// successfully pushed down, remove it from consideration for future nodes
		return true;
	});
	return make_pair(result_relation, move(result_operator));
}

unique_ptr<LogicalOperator> JoinOrderOptimizer::RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode* node) {
	// now we have to rewrite the plan
	// first we will extract all relations from the main plan
	vector<unique_ptr<LogicalOperator>> extracted_relations;
	for(size_t i = 0; i < relations.size(); i++) {
		extracted_relations.push_back(ExtractRelation(*relations[i]));
	}
	// now we generate the actual joins
	auto join_tree = GenerateJoins(extracted_relations, node);
	// push any "remaining" filters into the base relation (filters with an empty RelationSet, these are filters that cannot be pushed down)
	for(auto filter : pushdown_filters.filters) {
		join_tree.second = PushFilter(move(join_tree.second), ExtractFilter(filter));
	}
	// find the first join in the relation to know where to place this node
	if (plan->children.size() > 1) {
		// first node is the join, return it immediately
		return move(join_tree.second);
	}
	assert(plan->children.size() == 1);
	// have to move up through the relations
	auto op = plan.get();
	auto parent = plan.get();
	while(op->type != LogicalOperatorType::CROSS_PRODUCT && 
	      op->type != LogicalOperatorType::JOIN) {
		assert(op->children.size() == 1);
		parent = op;
		op = op->children[0].get();
	}
	// have to replace at this node
	parent->children[0] = move(join_tree.second);
	return plan;
}

// the join ordering is pretty much a straight implementation of the paper "Dynamic Programming Strikes Back" by Guido Moerkotte and Thomas Neumannn, see that paper for additional info/documentation
// bonus slides: https://db.in.tum.de/teaching/ws1415/queryopt/chapter3.pdf?lang=de
// FIXME: incorporate cardinality estimation into the plans, possibly by pushing samples?
unique_ptr<LogicalOperator> JoinOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	vector<unique_ptr<FilterInfo>> filters;
	// first we visit the plan in order to optimize subqueries
	plan->Accept(this);
	// now we optimize the current plan
	// first resolve join conditions for non-inner joins
	plan = ResolveJoinConditions(move(plan));
	// first we skip past until we find the first projection, we do this because the HAVING clause inserts a Filter AFTER the group by
	// and this filter cannot be reordered
	LogicalOperator *op = plan.get();
	while(!IsProjection(op->type)) {
		if (op->children.size() != 1) {
			// no projection found in plan
			return plan;
		}
		op = op->children[0].get();
	}
	// extract a list of all relations that have to be joined together
	// and a list of all conditions that is applied to them
	if (!ExtractJoinRelations(*op, filters)) {
		// do not support reordering this type of plan
		return plan;
	}
	if (relations.size() <= 1) {
		// at most one relation, nothing to reorder
		return plan;
	}
	// create potential edges from the comparisons
	for(size_t i = 0; i < filters.size(); i++) {
		auto &filter = filters[i];
		if (filter->filter->GetExpressionClass() == ExpressionClass::COMPARISON) {
			auto comparison = (ComparisonExpression*) filter->filter;
			// extract the bindings that are required for the left and right side of the comparison
			unordered_set<size_t> left_bindings, right_bindings;
			ExtractBindings(*comparison->children[0], left_bindings);
			ExtractBindings(*comparison->children[1], right_bindings);
			if (left_bindings.size() > 0 && right_bindings.size() > 0) {
				// both the left and the right side have bindings
				// first create the relation sets, if they do not exist
				filter->left_set  =  set_manager.GetRelation(left_bindings);
				filter->right_set =  set_manager.GetRelation(right_bindings);
				// we can only create a meaningful edge if the sets are not exactly the same
				if (filter->left_set != filter->right_set) {
					// check if the sets are disjoint
					if (Disjoint(left_bindings, right_bindings)) {
						// they are disjoint, we only need to create one set of edges in the join graph
						query_graph.CreateEdge(filter->left_set, filter->right_set, filter.get());
						query_graph.CreateEdge(filter->right_set, filter->left_set, filter.get());
					} else {
						// the sets are not disjoint, we create two sets of edges
						auto left_difference  = set_manager.Difference(filter->left_set, filter->right_set);
						auto right_difference = set_manager.Difference(filter->right_set, filter->left_set);
						// -> LEFT <-> RIGHT \ LEFT
						query_graph.CreateEdge(filter->left_set, right_difference, filter.get());
						query_graph.CreateEdge(right_difference, filter->left_set, filter.get());
						// -> RIGHT <-> LEFT \ RIGHT
						query_graph.CreateEdge(left_difference, filter->right_set, filter.get());
						query_graph.CreateEdge(filter->right_set, left_difference, filter.get());
					}
					continue;
				}
			}
		}
		// this filter condition could not be turned into an edge because either (1) it was not a comparison, (2) it was not comparing different relations
		// in this case, we might still be able to push it down
		// get the set of bindings referenced in the expression and create a RelationSet
		unordered_set<size_t> bindings;
		ExtractBindings(*filter->filter, bindings);
		auto relation = set_manager.GetRelation(bindings);
		AddPushdownFilter(relation, filter.get());
	}
	// now use dynamic programming to figure out the optimal join order
	// note: we can just use pointers to RelationSet* here because the CreateRelation/set_manager.GetRelation function ensures that a unique combination of relations will have a unique RelationSet object
	// initialize each of the single-node plans with themselves and with their cardinalities
	// these are the leaf nodes of the join tree
	for(size_t i = 0; i < relations.size(); i++) {
		auto &rel = *relations[i];
		auto node = set_manager.GetRelation(i);
		plans[node] = make_unique<JoinNode>(node, rel.op->EstimateCardinality());
	}
	// now we perform the actual dynamic programming to compute the final result
	SolveJoinOrder();
	// now the optimal join path should have been found
	// get it from the node
	unordered_set<size_t> bindings;
	for(size_t i = 0; i < relations.size(); i++) {
		bindings.insert(i);
	}
	auto total_relation = set_manager.GetRelation(bindings);
	auto final_plan = plans.find(total_relation);
	if (final_plan == plans.end()) {
		// could not find the final plan
		// this should only happen in case the sets are actually disjunct
		// in this case we need to generate cross product to connect the disjoint sets
		GenerateCrossProducts();
		//! solve the join order again
		SolveJoinOrder();
		// now we can obtain the final plan!
		final_plan = plans.find(total_relation);
		assert(final_plan != plans.end());
	}
	// now perform the actual reordering
	return RewritePlan(move(plan), final_plan->second.get());
}
