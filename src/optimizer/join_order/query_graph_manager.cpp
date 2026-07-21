#include "duckdb/optimizer/join_order/query_graph_manager.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/optimizer/join_order/join_relation_set.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

GenerateJoinRelation::GenerateJoinRelation(optional_ptr<JoinRelationSet> set, unique_ptr<LogicalOperator> op_p)
    : set(set), op(std::move(op_p)) {
}

QueryGraphManager::QueryGraphManager(ClientContext &context) : context(context), relation_manager(context) {
}

static bool NonInnerRelationSetsIntersect(const JoinRelationSet &left, const JoinRelationSet &right) {
	for (idx_t left_idx = 0; left_idx < left.count; left_idx++) {
		for (idx_t right_idx = 0; right_idx < right.count; right_idx++) {
			if (left.relations[left_idx] == right.relations[right_idx]) {
				return true;
			}
		}
	}
	return false;
}

void QueryGraphManager::ResolveNonInnerJoinPrerequisites() {
	struct Prerequisite {
		idx_t edge_index;
		JoinSide consumer_side;
	};
	vector<vector<Prerequisite>> direct_prerequisites(non_inner_joins.size());
	for (idx_t consumer_idx = 0; consumer_idx < non_inner_joins.size(); consumer_idx++) {
		auto &consumer = *non_inner_joins[consumer_idx];
		if (consumer.predicate_left_set.get().Empty() || consumer.predicate_right_set.get().Empty()) {
			throw InternalException("Non-inner join %llu has an empty predicate endpoint", consumer.index);
		}
		if (NonInnerRelationSetsIntersect(consumer.predicate_left_set, consumer.predicate_right_set)) {
			throw InternalException("Non-inner join %llu has overlapping predicate endpoints", consumer.index);
		}
		for (idx_t producer_idx = 0; producer_idx < non_inner_joins.size(); producer_idx++) {
			if (consumer_idx == producer_idx) {
				continue;
			}
			auto &producer = *non_inner_joins[producer_idx];
			if (producer.join_type != JoinType::LEFT) {
				continue;
			}
			auto depends_on_left =
			    NonInnerRelationSetsIntersect(consumer.predicate_left_set, producer.predicate_right_set);
			auto depends_on_right =
			    NonInnerRelationSetsIntersect(consumer.predicate_right_set, producer.predicate_right_set);
			if (depends_on_left && depends_on_right) {
				throw InternalException("Non-inner join %llu depends on both sides of non-inner join %llu",
				                        consumer.index, producer.index);
			}
			if (depends_on_left || depends_on_right) {
				direct_prerequisites[consumer_idx].push_back(
				    {producer_idx, depends_on_left ? JoinSide::LEFT : JoinSide::RIGHT});
			}
		}
	}

	vector<uint8_t> resolve_state(non_inner_joins.size(), 0);
	std::function<void(idx_t)> resolve_edge = [&](idx_t edge_idx) {
		if (resolve_state[edge_idx] == 2) {
			return;
		}
		if (resolve_state[edge_idx] == 1) {
			throw InternalException("Non-inner join prerequisite cycle involving edge %llu", edge_idx);
		}
		resolve_state[edge_idx] = 1;
		auto &edge = *non_inner_joins[edge_idx];
		unordered_set<idx_t> prerequisite_ids;
		for (auto &prerequisite : direct_prerequisites[edge_idx]) {
			resolve_edge(prerequisite.edge_index);
			auto &producer = *non_inner_joins[prerequisite.edge_index];
			auto &producer_input = set_manager.Union(producer.required_left_set, producer.required_right_set);
			if (prerequisite.consumer_side == JoinSide::LEFT) {
				edge.required_left_set = set_manager.Union(edge.required_left_set, producer_input);
			} else {
				D_ASSERT(prerequisite.consumer_side == JoinSide::RIGHT);
				edge.required_right_set = set_manager.Union(edge.required_right_set, producer_input);
			}
			prerequisite_ids.insert(producer.index);
			prerequisite_ids.insert(producer.prerequisite_edges.begin(), producer.prerequisite_edges.end());
		}
		edge.prerequisite_edges.insert(edge.prerequisite_edges.end(), prerequisite_ids.begin(), prerequisite_ids.end());
		std::sort(edge.prerequisite_edges.begin(), edge.prerequisite_edges.end());
		if (NonInnerRelationSetsIntersect(edge.required_left_set, edge.required_right_set)) {
			throw InternalException("Non-inner join %llu has overlapping required input sets", edge.index);
		}
		resolve_state[edge_idx] = 2;
	};
	for (idx_t edge_idx = 0; edge_idx < non_inner_joins.size(); edge_idx++) {
		resolve_edge(edge_idx);
	}
}

void QueryGraphManager::BuildPredicateModel() {
	predicate_model.Clear();

	column_binding_map_t<idx_t> binding_to_component;
	vector<idx_t> parents;

	auto get_component = [&](const ColumnBinding &binding) -> idx_t {
		auto entry = binding_to_component.find(binding);
		if (entry != binding_to_component.end()) {
			return entry->second;
		}
		auto component = parents.size();
		parents.push_back(component);
		binding_to_component[binding] = component;
		return component;
	};

	auto find_root = [&](idx_t component) -> idx_t {
		while (parents[component] != component) {
			parents[component] = parents[parents[component]];
			component = parents[component];
		}
		return component;
	};

	// First union all normalized equality predicates. Do not assign edge ids in this pass,
	// because a later predicate can merge two previously separate components.
	for (auto &filter : filters_and_bindings) {
		filter->edge_equivalence_index = optional_idx();
		auto predicate_class = JoinOrderUtil::ClassifyJoinPredicate(*filter);
		ColumnBinding left_equality_binding;
		ColumnBinding right_equality_binding;
		if (BoundComparisonExpression::IsComparison(*filter->filter)) {
			auto &comparison = filter->filter->Cast<BoundFunctionExpression>();
			GetEquivalenceBinding(BoundComparisonExpression::Left(comparison), left_equality_binding);
			GetEquivalenceBinding(BoundComparisonExpression::Right(comparison), right_equality_binding);
		}
		auto &predicate =
		    predicate_model.RegisterPredicate(*filter, predicate_class, left_equality_binding, right_equality_binding);
		if (!predicate.CanBuildEqualityClosure()) {
			continue;
		}

		auto left_root = find_root(get_component(predicate.GetEqualityBinding(true)));
		auto right_root = find_root(get_component(predicate.GetEqualityBinding(false)));
		if (left_root != right_root) {
			parents[MaxValue(left_root, right_root)] = MinValue(left_root, right_root);
		}
	}

	// Then assign stable final ids to every equality edge using the final roots.
	unordered_map<idx_t, idx_t> root_to_equivalence_id;
	vector<vector<reference<JoinPredicate>>> equality_class_predicates;
	for (auto predicate_ref : predicate_model.GetPredicates()) {
		auto &predicate = predicate_ref.get();
		if (!predicate.CanBuildEqualityClosure()) {
			continue;
		}
		auto root = find_root(binding_to_component[predicate.GetEqualityBinding(true)]);
		auto entry = root_to_equivalence_id.find(root);
		if (entry == root_to_equivalence_id.end()) {
			entry = root_to_equivalence_id.insert(make_pair(root, root_to_equivalence_id.size())).first;
			equality_class_predicates.emplace_back();
		}
		predicate.SetEqualityClassIndex(optional_idx(entry->second));
		equality_class_predicates[entry->second].push_back(predicate);
	}

	for (idx_t equality_class_index = 0; equality_class_index < equality_class_predicates.size();
	     equality_class_index++) {
		JoinEqualityClass equality_class;
		equality_class.index = equality_class_index;
		for (auto predicate_ref : equality_class_predicates[equality_class_index]) {
			auto &predicate = predicate_ref.get();
			auto &left_binding = predicate.GetEqualityBinding(true);
			auto &right_binding = predicate.GetEqualityBinding(false);
			auto left_relation = JoinOrderUtil::GetBindingRelation(left_binding);
			auto right_relation = JoinOrderUtil::GetBindingRelation(right_binding);
			equality_class.columns.insert(left_binding);
			equality_class.columns.insert(right_binding);
			equality_class.relations.insert(left_relation);
			equality_class.relations.insert(right_relation);
			equality_class.edges.emplace_back(predicate, left_relation, right_relation, left_binding, right_binding);
		}
		predicate_model.AddEqualityClass(std::move(equality_class));
	}

	for (auto &equality_class : predicate_model.GetEqualityClasses()) {
		for (auto &edge : equality_class.edges) {
			auto &left = set_manager.GetJoinRelation(edge.left_relation);
			auto &right = set_manager.GetJoinRelation(edge.right_relation);
			auto &pair = set_manager.Union(left, right);
			auto first_binding = edge.left_binding;
			auto second_binding = edge.right_binding;
			if (pair.relations[0] != edge.left_relation) {
				D_ASSERT(pair.relations[0] == edge.right_relation);
				first_binding = edge.right_binding;
				second_binding = edge.left_binding;
			}
			predicate_model.AddDirectEqualityPairClass(pair, equality_class.index, first_binding, second_binding);
		}
	}
}

bool QueryGraphManager::Build(JoinOrderOptimizer &optimizer, LogicalOperator &op) {
	// have the relation manager extract the join relations and create a reference list of all the
	// filter operators.
	auto can_reorder = relation_manager.ExtractJoinRelations(optimizer, op, filter_operators);
	auto num_relations = relation_manager.NumRelations();
	if (num_relations <= 1 || !can_reorder) {
		// nothing to optimize/reorder
		return false;
	}
	// extract the edges of the hypergraph, creating a list of filters and their associated bindings.
	auto extraction = relation_manager.ExtractEdges(op, filter_operators, set_manager);
	filters_and_bindings = std::move(extraction.filters);
	non_inner_joins = std::move(extraction.non_inner_edges);
	ResolveNonInnerJoinPrerequisites();
	for (auto &non_inner_join : non_inner_joins) {
		for (auto predicate_index : non_inner_join->costing_predicate_indices) {
			auto inserted = non_inner_costing_predicates.insert(predicate_index);
			if (!inserted.second) {
				throw InternalException("Join-order predicate %llu belongs to multiple non-inner joins",
				                        predicate_index);
			}
		}
	}
	// Populate left/right endpoints and stats bindings before building the predicate model.
	BindFilterEndpoints();
	// Build the predicate model after BindFilterEndpoints so that left_binding/right_binding are populated.
	BuildPredicateModel();
	// Create query_graph hyper edges from the normalized predicate model.
	CreateHyperGraphEdges();
	return true;
}

const JoinPredicateModel &QueryGraphManager::GetPredicateModel() const {
	return predicate_model;
}

void QueryGraphManager::GetColumnBinding(const Expression &root_expr, ColumnBinding &binding) {
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    root_expr, [&](const BoundColumnRefExpression &colref) {
		    D_ASSERT(colref.Depth() == 0);
		    D_ASSERT(colref.Binding().table_index.IsValid());
		    // map the base table index to the relation index used by the JoinOrderOptimizer
		    D_ASSERT(relation_manager.relation_mapping.find(colref.Binding().table_index) !=
		             relation_manager.relation_mapping.end());
		    binding = ColumnBinding(TableIndex(relation_manager.relation_mapping[colref.Binding().table_index].index),
		                            colref.Binding().column_index);
	    });
}

void QueryGraphManager::GetEquivalenceBinding(const Expression &expression, ColumnBinding &binding) {
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.Depth() == 0);
		if (!colref.Binding().table_index.IsValid()) {
			return;
		}
		auto entry = relation_manager.relation_mapping.find(colref.Binding().table_index);
		D_ASSERT(entry != relation_manager.relation_mapping.end());
		binding = ColumnBinding(TableIndex(entry->second.index), colref.Binding().column_index);
		return;
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast = expression.Cast<BoundCastExpression>();
		if (cast.IsTryCast() || !BoundCastExpression::CastIsInvertible(cast.source_type(), cast.GetReturnType())) {
			return;
		}
		GetEquivalenceBinding(cast.Child(), binding);
		return;
	}
	default:
		return;
	}
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

static bool RelationSetsEqual(JoinRelationSet &left, JoinRelationSet &right) {
	return JoinRelationSet::IsSubset(left, right) && JoinRelationSet::IsSubset(right, left);
}

void QueryGraphManager::BindFilterEndpoints() {
	for (auto &filter_info : filters_and_bindings) {
		if (filter_info->must_remain_filter) {
			D_ASSERT(filter_info->from_logical_filter);
			D_ASSERT(!filter_info->left_set);
			D_ASSERT(!filter_info->right_set);
			continue;
		}
		auto &filter = filter_info->filter;
		// now check if it can be used as a join predicate
		if (BoundComparisonExpression::IsComparison(*filter)) {
			auto &comparison = filter->Cast<BoundFunctionExpression>();
			auto &left = BoundComparisonExpression::Left(comparison);
			auto &right = BoundComparisonExpression::Right(comparison);
			// extract the bindings that are required for the left and right side of the comparison
			unordered_set<RelationIndex> left_bindings, right_bindings;
			relation_manager.ExtractBindings(left, left_bindings);
			relation_manager.ExtractBindings(right, right_bindings);
			GetColumnBinding(left, filter_info->left_binding);
			GetColumnBinding(right, filter_info->right_binding);
			if (!left_bindings.empty() && !right_bindings.empty()) {
				// both the left and the right side have bindings
				// first create the relation sets, if they do not exist
				if (!filter_info->left_set) {
					filter_info->left_set = &set_manager.GetJoinRelation(left_bindings);
				}
				if (!filter_info->right_set) {
					filter_info->right_set = &set_manager.GetJoinRelation(right_bindings);
				}
				D_ASSERT(filter_info->left_set && filter_info->right_set);
				auto &condition_set = set_manager.Union(*filter_info->left_set, *filter_info->right_set);
				if (!RelationSetsEqual(filter_info->set.get(), condition_set)) {
					filter_info->SetLeftSet(nullptr);
					filter_info->SetRightSet(nullptr);
				}
			}
		} else if (filter->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
			auto &conjunction = filter->Cast<BoundConjunctionExpression>();
			if (conjunction.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
				continue;
			}
			if (filter_info->join_type == JoinType::INNER || filter_info->join_type == JoinType::INVALID) {
				continue;
			}
			unordered_set<RelationIndex> left_bindings, right_bindings;
			D_ASSERT(filter_info->left_set);
			D_ASSERT(filter_info->right_set);
			D_ASSERT(filter_info->join_type == JoinType::SEMI || filter_info->join_type == JoinType::ANTI);
			for (auto &child_comp : conjunction.GetChildren()) {
				if (!BoundComparisonExpression::IsComparison(*child_comp)) {
					continue;
				}
				auto &comparison = child_comp->Cast<BoundFunctionExpression>();
				auto &left = BoundComparisonExpression::Left(comparison);
				auto &right = BoundComparisonExpression::Right(comparison);
				// extract the bindings that are required for the left and right side of the comparison
				relation_manager.ExtractBindings(left, left_bindings);
				relation_manager.ExtractBindings(right, right_bindings);
				if (!filter_info->left_binding.table_index.IsValid() &&
				    !filter_info->left_binding.column_index.IsValid()) {
					GetColumnBinding(left, filter_info->left_binding);
				}
				if (!filter_info->right_binding.table_index.IsValid() &&
				    !filter_info->right_binding.column_index.IsValid()) {
					GetColumnBinding(right, filter_info->right_binding);
				}
			}
			if (!left_bindings.empty() && !right_bindings.empty()) {
				D_ASSERT(filter_info->left_set && filter_info->right_set);
			}
		}
	}
}

void QueryGraphManager::CreateHyperGraphEdges() {
	for (auto predicate_ref : predicate_model.GetGraphPredicates()) {
		auto &predicate = predicate_ref.get();
		D_ASSERT(predicate.GetLeftSetOptional() && predicate.GetRightSetOptional());
		if (non_inner_costing_predicates.count(predicate.GetIndex())) {
			continue;
		}
		query_graph.CreateEdge(predicate.GetLeftSet(), predicate.GetRightSet(), predicate);
		query_graph.CreateEdge(predicate.GetRightSet(), predicate.GetLeftSet(), predicate);
	}

	vector<optional_ptr<JoinPredicate>> predicates_by_index(filters_and_bindings.size(), nullptr);
	for (auto predicate_ref : predicate_model.GetPredicates()) {
		auto &predicate = predicate_ref.get();
		D_ASSERT(predicate.GetIndex() < predicates_by_index.size());
		predicates_by_index[predicate.GetIndex()] = predicate;
	}
	for (auto &non_inner_join_ptr : non_inner_joins) {
		auto &non_inner_join = *non_inner_join_ptr;
		for (auto predicate_index : non_inner_join.costing_predicate_indices) {
			auto predicate = predicates_by_index[predicate_index];
			if (!predicate || !predicate->GetLeftSetOptional() || !predicate->GetRightSetOptional()) {
				throw InternalException("Non-inner join %llu has a cost predicate without graph endpoints",
				                        non_inner_join.index);
			}
			query_graph.CreateEdge(non_inner_join.required_left_set, non_inner_join.required_right_set, *predicate,
			                       non_inner_join);
			query_graph.CreateEdge(non_inner_join.required_right_set, non_inner_join.required_left_set, *predicate,
			                       non_inner_join);
		}
	}
}

static unique_ptr<LogicalOperator> ExtractJoinRelation(unique_ptr<SingleJoinRelation> &rel) {
	auto &children = rel->parent->children;
	for (idx_t i = 0; i < children.size(); i++) {
		if (children[i].get() == &rel->op) {
			// found it! take ownership o/**/f it from the parent
			auto result = std::move(children[i]);
			children.erase_at(i);
			return result;
		}
	}
	throw InternalException("Could not find relation in parent node (?)");
}

unique_ptr<LogicalOperator> QueryGraphManager::Reconstruct(unique_ptr<LogicalOperator> plan) {
	// now we have to rewrite the plan
	bool root_is_join = plan->children.size() > 1;

	unordered_set<RelationIndex> bindings;
	for (idx_t i = 0; i < relation_manager.NumRelations(); i++) {
		bindings.emplace(i);
	}
	auto &total_relation = set_manager.GetJoinRelation(bindings);

	// first we will extract all relations from the main plan
	vector<unique_ptr<LogicalOperator>> extracted_relations;
	extracted_relations.reserve(relation_manager.NumRelations());
	for (auto &relation : relation_manager.GetRelations()) {
		extracted_relations.push_back(ExtractJoinRelation(relation));
	}

	// now we generate the actual joins
	auto join_tree = GenerateJoins(extracted_relations, total_relation);
	for (auto &non_inner_join : non_inner_joins) {
		if (!reconstructed_non_inner_joins.count(non_inner_join->index)) {
			throw InternalException("Join-order optimizer did not reconstruct non-inner join %llu",
			                        non_inner_join->index);
		}
	}

	// perform the final pushdown of remaining filters
	for (auto &filter : filters_and_bindings) {
		if (non_inner_costing_predicates.count(filter->filter_index)) {
			if (filter->filter) {
				throw InternalException("Cost predicate %llu escaped non-inner join reconstruction",
				                        filter->filter_index);
			}
			continue;
		}
		// check if the filter has already been extracted
		if (filter->filter) {
			// if not we need to push it
			join_tree.op = PushFilter(std::move(join_tree.op), std::move(filter->filter));
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

static JoinCondition MaybeInvertConditions(unique_ptr<Expression> condition, bool invert) {
	auto &comparison = condition->Cast<BoundFunctionExpression>();
	auto &left_ref = BoundComparisonExpression::LeftMutable(comparison);
	auto &right_ref = BoundComparisonExpression::RightMutable(comparison);
	unique_ptr<Expression> left, right;
	if (!invert) {
		left = std::move(left_ref);
		right = std::move(right_ref);
	} else {
		left = std::move(right_ref);
		right = std::move(left_ref);
	}
	auto comp_type = condition->GetExpressionType();
	if (invert) {
		// reverse comparison expression if we reverse the order of the children
		comp_type = FlipComparisonExpression(comp_type);
	}
	return JoinCondition(std::move(left), std::move(right), comp_type);
}

static bool ShouldInvertJoinCondition(JoinRelationSetManager &set_manager, GenerateJoinRelation &left,
                                      GenerateJoinRelation &right, FilterInfo &filter, bool fallback_invert) {
	if (!filter.left_binding.table_index.IsValid()) {
		return fallback_invert;
	}
	auto &condition_left_set = set_manager.GetJoinRelation(RelationIndex(filter.left_binding.table_index.index));
	if (JoinRelationSet::IsSubset(*right.set, condition_left_set)) {
		return true;
	}
	if (JoinRelationSet::IsSubset(*left.set, condition_left_set)) {
		return false;
	}
	return fallback_invert;
}

GenerateJoinRelation QueryGraphManager::GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations,
                                                      JoinRelationSet &set) {
	optional_ptr<JoinRelationSet> left_node;
	optional_ptr<JoinRelationSet> right_node;
	optional_ptr<JoinRelationSet> result_relation;
	unique_ptr<LogicalOperator> result_operator;
	unordered_set<idx_t> subtree_edges;

	auto dp_entry = plans->find(set);
	if (dp_entry == plans->end()) {
		throw InternalException("Join Order Optimizer Error: No full plan was created");
	}
	auto &node = dp_entry->second;
	if (!dp_entry->second->is_leaf) {
		// generate the left and right children
		auto left = GenerateJoins(extracted_relations, node->left_set);
		auto right = GenerateJoins(extracted_relations, node->right_set);
		subtree_edges = left.non_inner_edges;
		subtree_edges.insert(right.non_inner_edges.begin(), right.non_inner_edges.end());
		auto non_inner_join = dp_entry->second->info->non_inner_join;
		if (non_inner_join) {
			auto direct = JoinRelationSet::IsSubset(*left.set, non_inner_join->required_left_set) &&
			              JoinRelationSet::IsSubset(*right.set, non_inner_join->required_right_set);
			auto inverted = JoinRelationSet::IsSubset(*left.set, non_inner_join->required_right_set) &&
			                JoinRelationSet::IsSubset(*right.set, non_inner_join->required_left_set);
			if (direct == inverted) {
				throw InternalException("Could not orient non-inner join %llu in reconstructed join tree",
				                        non_inner_join->index);
			}
			if (inverted) {
				std::swap(left, right);
			}
			for (auto prerequisite : non_inner_join->prerequisite_edges) {
				if (!subtree_edges.count(prerequisite)) {
					throw InternalException("Non-inner join %llu is missing prerequisite %llu from its input subtrees",
					                        non_inner_join->index, prerequisite);
				}
			}
			if (!reconstructed_non_inner_joins.insert(non_inner_join->index).second ||
			    !subtree_edges.insert(non_inner_join->index).second) {
				throw InternalException("Non-inner join %llu was reconstructed more than once", non_inner_join->index);
			}

			auto join = make_uniq<LogicalComparisonJoin>(non_inner_join->join_type);
			join->children.push_back(std::move(left.op));
			join->children.push_back(std::move(right.op));
			join->conditions = std::move(non_inner_join->conditions);
			if (join->conditions.empty()) {
				throw InternalException("Non-inner join %llu has no conditions", non_inner_join->index);
			}
			for (auto predicate_index : non_inner_join->costing_predicate_indices) {
				auto &filter = filters_and_bindings.at(predicate_index);
				if (!filter->filter) {
					throw InternalException("Cost predicate %llu was consumed before non-inner join %llu",
					                        predicate_index, non_inner_join->index);
				}
				filter->filter.reset();
			}
			result_operator = std::move(join);
		} else if (dp_entry->second->info->predicates.empty()) {
			// no filters, create a cross product
			auto cardinality = left.op->estimated_cardinality * right.op->estimated_cardinality;
			result_operator = LogicalCrossProduct::Create(std::move(left.op), std::move(right.op));
			result_operator->SetEstimatedCardinality(cardinality);
		} else {
			// we have filters, create a join node
			auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
			// Here we optimize build side probe side. Our build side is the right side
			// So the right plans should have lower cardinalities.
			join->children.push_back(std::move(left.op));
			join->children.push_back(std::move(right.op));

			// set the join conditions from the join node
			for (auto predicate_ref : node->info->predicates) {
				auto f = &predicate_ref.get().GetFilter();
				// extract the filter from the operator it originally belonged to
				auto &filter_and_binding = filters_and_bindings.at(f->filter_index);
				D_ASSERT(filter_and_binding->filter);
				// now create the actual join condition
				D_ASSERT((JoinRelationSet::IsSubset(*left.set, *f->left_set) &&
				          JoinRelationSet::IsSubset(*right.set, *f->right_set)) ||
				         (JoinRelationSet::IsSubset(*left.set, *f->right_set) &&
				          JoinRelationSet::IsSubset(*right.set, *f->left_set)));

				bool invert_children = !JoinRelationSet::IsSubset(*left.set, *f->left_set);

				// If the left and right set are inverted for LEFT/SEMI/ANTI joins then swap them back
				// and set invert = false. This is to preserve left/rightedness of relations
				if (invert_children && (f->join_type == JoinType::LEFT || f->join_type == JoinType::SEMI ||
				                        f->join_type == JoinType::ANTI)) {
					std::swap(join->children[0], join->children[1]);
					std::swap(left, right);
					invert_children = false;
				}
				auto condition = std::move(filter_and_binding->filter);
				if (BoundComparisonExpression::IsComparison(*condition)) {
					auto invert = ShouldInvertJoinCondition(set_manager, left, right, *f, invert_children);
					auto cond = MaybeInvertConditions(std::move(condition), invert);
					join->conditions.push_back(std::move(cond));
				} else if (condition->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
					auto &conjunction = condition->Cast<BoundConjunctionExpression>();
					for (auto &child : conjunction.GetChildrenMutable()) {
						D_ASSERT(BoundComparisonExpression::IsComparison(*child));
						auto cond = MaybeInvertConditions(std::move(child), invert_children);
						join->conditions.push_back(std::move(cond));
					}
				}
			}
			D_ASSERT(!join->conditions.empty());
			result_operator = std::move(join);
		}
		left_node = left.set;
		right_node = right.set;
		result_relation = &set_manager.Union(*left.set, *right.set);
	} else {
		// base node, get the entry from the list of extracted relations
		D_ASSERT(node->set.count == 1);
		D_ASSERT(extracted_relations[node->set.relations[0].index]);
		result_relation = &node->set;
		result_operator = std::move(extracted_relations[result_relation->relations[0].index]);
	}
	// TODO: this is where estimated properties start coming into play.
	//  when creating the result operator, we should ask the cost model and cardinality estimator what
	//  the cost and cardinality are
	result_operator->estimated_cardinality = node->cardinality;
	result_operator->has_estimated_cardinality = true;

	// Collect unused residual predicates that belong to this join. Semantic non-inner joins own their complete
	// condition directly, so only ordinary (notably INNER) residual predicates reach this path.
	vector<unique_ptr<Expression>> unused_residual_predicates;
	for (auto &filter_info : filters_and_bindings) {
		if (filter_info->from_residual_predicate && filters_and_bindings[filter_info->filter_index]->filter &&
		    filter_info->set.get().count > 0 && JoinRelationSet::IsSubset(*result_relation, filter_info->set)) {
			unused_residual_predicates.push_back(std::move(filters_and_bindings[filter_info->filter_index]->filter));
		}
	}
	if (!unused_residual_predicates.empty()) {
		unique_ptr<Expression> combined = std::move(unused_residual_predicates[0]);
		for (idx_t i = 1; i < unused_residual_predicates.size(); i++) {
			combined = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(combined),
			                                                 std::move(unused_residual_predicates[i]));
		}
		if (result_operator->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			result_operator->Cast<LogicalComparisonJoin>().conditions.emplace_back(std::move(combined));
		} else {
			result_operator = PushFilter(std::move(result_operator), std::move(combined));
		}
	}

	// check if we should do a pushdown on this node
	// basically, any remaining filter that is a subset of the current relation will no longer be used in joins
	// hence we should push it here
	for (auto &filter_info : filters_and_bindings) {
		// check if the filter has already been extracted
		auto &info = *filter_info;
		if (filters_and_bindings[info.filter_index]->filter) {
			if (info.from_residual_predicate) {
				continue;
			}
			// now check if the filter is a subset of the current relation
			// note that infos with an empty relation set are a special case and we do not push them down
			if (info.join_type == JoinType::LEFT) {
				// any left join is most definitely a filter that joins two relations,
				// so do not push the filter preemptively here
				continue;
			}
			if (info.set.get().count > 0 && JoinRelationSet::IsSubset(*result_relation, info.set)) {
				auto &filter_and_binding = filters_and_bindings[info.filter_index];
				auto filter = std::move(filter_and_binding->filter);
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
				D_ASSERT(BoundComparisonExpression::IsComparison(*filter));
				auto &comparison = filter->Cast<BoundFunctionExpression>();
				auto &left_ref = BoundComparisonExpression::LeftMutable(comparison);
				auto &right_ref = BoundComparisonExpression::RightMutable(comparison);
				// we need to figure out which side is which by looking at the relations available to us
				unique_ptr<Expression> left, right;
				if (!invert) {
					left = std::move(left_ref);
					right = std::move(right_ref);
				} else {
					left = std::move(right_ref);
					right = std::move(left_ref);
				}
				auto comp_type = comparison.GetExpressionType();
				if (invert) {
					// reverse comparison expression if we reverse the order of the children
					comp_type = FlipComparisonExpression(comp_type);
				}
				JoinCondition cond(std::move(left), std::move(right), comp_type);
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
	auto result = GenerateJoinRelation(result_relation, std::move(result_operator));
	if (!node->is_leaf) {
		result.non_inner_edges = std::move(subtree_edges);
	}
	return result;
}

const QueryGraphEdges &QueryGraphManager::GetQueryGraphEdges() const {
	return query_graph;
}

void QueryGraphManager::CreateQueryGraphCrossProduct(JoinRelationSet &left, JoinRelationSet &right) {
	query_graph.CreateEdge(left, right, nullptr);
	query_graph.CreateEdge(right, left, nullptr);
}

} // namespace duckdb
