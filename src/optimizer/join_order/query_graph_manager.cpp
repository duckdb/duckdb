#include "duckdb/optimizer/join_order/query_graph_manager.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

//! Returns true if A and B are disjoint, false otherwise
template <class T>
static bool Disjoint(const unordered_set<T> &a, const unordered_set<T> &b) {
	return std::all_of(a.begin(), a.end(), [&b](typename std::unordered_set<T>::const_reference entry) {
		return b.find(entry) == b.end();
	});
}

void QueryGraphManager::MarkEdgeEquivalences() {
	// Assign edge_equivalence_index to INNER equality join filters using union-find over column bindings.
	// All filters in the same transitive equality closure receive the same index, regardless of the
	// order they appear in filters_and_bindings. This allows skipping redundant edges during
	// plan reconstruction (GenerateJoins) and cardinality estimation (GetDenominator).
	//
	// Only INNER equality filters that connect two different relation sets are considered.
	// Non-equality and non-INNER filters keep the default invalid edge_equivalence_index.
	//
	// Union-find invariant: on merge, the smaller (older) component ID is inherited by both sides,
	// so all transitively connected bindings always share the same component ID.

	// Maps each column binding to its component ID.
	column_binding_map_t<idx_t> binding_to_component;
	idx_t next_component_id = 0;

	auto find_component = [&](const ColumnBinding &binding) -> optional_idx {
		auto it = binding_to_component.find(binding);
		if (it == binding_to_component.end()) {
			return optional_idx();
		}
		return optional_idx(it->second);
	};

	auto merge_components = [&](idx_t comp_a, idx_t comp_b) -> idx_t {
		// Inherit the smaller (older) component ID so all members of both components get the same ID.
		idx_t keep = MinValue(comp_a, comp_b);
		idx_t remove = MaxValue(comp_a, comp_b);
		if (keep == remove) {
			return keep;
		}
		for (auto &entry : binding_to_component) {
			if (entry.second == remove) {
				entry.second = keep;
			}
		}
		return keep;
	};

	for (auto &filter : filters_and_bindings) {
		if (filter->join_type != JoinType::INNER) {
			continue;
		}
		if (!filter->filter || filter->filter->GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
			continue;
		}
		if (!filter->left_binding.table_index.IsValid() || !filter->right_binding.table_index.IsValid()) {
			continue;
		}
		if (filter->left_set == filter->right_set) {
			continue;
		}

		auto left_comp = find_component(filter->left_binding);
		auto right_comp = find_component(filter->right_binding);

		idx_t component_id;
		if (!left_comp.IsValid() && !right_comp.IsValid()) {
			// Both new: create a fresh component.
			component_id = next_component_id++;
			binding_to_component[filter->left_binding] = component_id;
			binding_to_component[filter->right_binding] = component_id;
		} else if (!left_comp.IsValid()) {
			component_id = right_comp.GetIndex();
			binding_to_component[filter->left_binding] = component_id;
		} else if (!right_comp.IsValid()) {
			component_id = left_comp.GetIndex();
			binding_to_component[filter->right_binding] = component_id;
		} else {
			// Both already tracked — merge the two components.
			component_id = merge_components(left_comp.GetIndex(), right_comp.GetIndex());
		}
		filter->edge_equivalence_index = optional_idx(component_id);
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
	filters_and_bindings = relation_manager.ExtractEdges(op, filter_operators, set_manager);
	// Mark INNER equality filters with equivalence group indices for redundancy detection.
	MarkEdgeEquivalences();
	// Create the query_graph hyper edges
	CreateHyperGraphEdges();
	return true;
}

const vector<unique_ptr<FilterInfo>> &QueryGraphManager::GetFilterBindings() const {
	return filters_and_bindings;
}

void QueryGraphManager::GetColumnBinding(Expression &root_expr, ColumnBinding &binding) {
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    root_expr, [&](const BoundColumnRefExpression &colref) {
		    D_ASSERT(colref.depth == 0);
		    D_ASSERT(colref.binding.table_index.IsValid());
		    // map the base table index to the relation index used by the JoinOrderOptimizer
		    D_ASSERT(relation_manager.relation_mapping.find(colref.binding.table_index) !=
		             relation_manager.relation_mapping.end());
		    binding = ColumnBinding(TableIndex(relation_manager.relation_mapping[colref.binding.table_index].index),
		                            colref.binding.column_index);
	    });
}

void FilterInfo::SetLeftSet(optional_ptr<JoinRelationSet> left_set_new) {
	left_set = left_set_new;
}

void FilterInfo::SetRightSet(optional_ptr<JoinRelationSet> right_set_new) {
	right_set = right_set_new;
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

void QueryGraphManager::CreateHyperGraphEdges() {
	// create potential edges from the comparisons
	for (auto &filter_info : filters_and_bindings) {
		auto &filter = filter_info->filter;
		// now check if it can be used as a join predicate
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			auto &comparison = filter->Cast<BoundComparisonExpression>();
			// extract the bindings that are required for the left and right side of the comparison
			unordered_set<RelationIndex> left_bindings, right_bindings;
			relation_manager.ExtractBindings(*comparison.left, left_bindings);
			relation_manager.ExtractBindings(*comparison.right, right_bindings);
			GetColumnBinding(*comparison.left, filter_info->left_binding);
			GetColumnBinding(*comparison.right, filter_info->right_binding);
			if (!left_bindings.empty() && !right_bindings.empty()) {
				// both the left and the right side have bindings
				// first create the relation sets, if they do not exist
				if (!filter_info->left_set) {
					filter_info->left_set = &set_manager.GetJoinRelation(left_bindings);
				}
				if (!filter_info->right_set) {
					filter_info->right_set = &set_manager.GetJoinRelation(right_bindings);
				}
				// we can only create a meaningful edge if the sets are not exactly the same
				if (filter_info->left_set != filter_info->right_set) {
					// check if the sets are disjoint
					if (Disjoint(left_bindings, right_bindings)) {
						// they are disjoint, we only need to create one set of edges in the join graph
						query_graph.CreateEdge(*filter_info->left_set, *filter_info->right_set, filter_info);
						query_graph.CreateEdge(*filter_info->right_set, *filter_info->left_set, filter_info);
					}
				}
			}
		} else if (filter->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
			auto &conjunction = filter->Cast<BoundConjunctionExpression>();
			if (conjunction.GetExpressionType() == ExpressionType::CONJUNCTION_OR ||
			    filter_info->join_type == JoinType::INNER || filter_info->join_type == JoinType::INVALID) {
				// Currently we do not interpret Conjunction expressions as INNER joins
				// for hyper graph edges. These are most likely OR conjunctions, and
				// will be pushed down into a join later in the optimizer.
				// Conjunction filters are mostly to help plan semi and anti joins at the moment.
				continue;
			}
			unordered_set<RelationIndex> left_bindings, right_bindings;
			D_ASSERT(filter_info->left_set);
			D_ASSERT(filter_info->right_set);
			D_ASSERT(filter_info->join_type == JoinType::SEMI || filter_info->join_type == JoinType::ANTI);
			for (auto &child_comp : conjunction.children) {
				if (child_comp->GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) {
					continue;
				}
				auto &comparison = child_comp->Cast<BoundComparisonExpression>();
				// extract the bindings that are required for the left and right side of the comparison
				relation_manager.ExtractBindings(*comparison.left, left_bindings);
				relation_manager.ExtractBindings(*comparison.right, right_bindings);
				if (!filter_info->left_binding.table_index.IsValid() &&
				    !filter_info->left_binding.column_index.IsValid()) {
					GetColumnBinding(*comparison.left, filter_info->left_binding);
				}
				if (!filter_info->right_binding.table_index.IsValid() &&
				    !filter_info->right_binding.column_index.IsValid()) {
					GetColumnBinding(*comparison.right, filter_info->right_binding);
				}
			}
			if (!left_bindings.empty() && !right_bindings.empty()) {
				// we can only create a meaningful edge if the sets are not exactly the same
				if (filter_info->left_set != filter_info->right_set) {
					// check if the sets are disjoint
					if (Disjoint(left_bindings, right_bindings)) {
						// they are disjoint, we only need to create one set of edges in the join graph
						query_graph.CreateEdge(*filter_info->left_set, *filter_info->right_set, filter_info);
						query_graph.CreateEdge(*filter_info->right_set, *filter_info->left_set, filter_info);
					}
				}
			}
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
	unordered_set<idx_t> applied_equivalence_groups;
	auto join_tree = GenerateJoins(extracted_relations, total_relation, applied_equivalence_groups);

	// perform the final pushdown of remaining filters
	for (auto &filter : filters_and_bindings) {
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
	auto &comparison = condition->Cast<BoundComparisonExpression>();
	auto left = !invert ? std::move(comparison.left) : std::move(comparison.right);
	auto right = !invert ? std::move(comparison.right) : std::move(comparison.left);
	auto comp_type = condition->GetExpressionType();
	if (invert) {
		// reverse comparison expression if we reverse the order of the children
		comp_type = FlipComparisonExpression(comp_type);
	}
	return JoinCondition(std::move(left), std::move(right), comp_type);
}

GenerateJoinRelation QueryGraphManager::GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations,
                                                      JoinRelationSet &set,
                                                      unordered_set<idx_t> &applied_equivalence_groups) {
	optional_ptr<JoinRelationSet> left_node;
	optional_ptr<JoinRelationSet> right_node;
	optional_ptr<JoinRelationSet> result_relation;
	unique_ptr<LogicalOperator> result_operator;

	auto dp_entry = plans->find(set);
	if (dp_entry == plans->end()) {
		throw InternalException("Join Order Optimizer Error: No full plan was created");
	}
	auto &node = dp_entry->second;
	if (!dp_entry->second->is_leaf) {
		// generate the left and right children
		auto left = GenerateJoins(extracted_relations, node->left_set, applied_equivalence_groups);
		auto right = GenerateJoins(extracted_relations, node->right_set, applied_equivalence_groups);
		if (dp_entry->second->info->filters.empty()) {
			// no filters, create a cross product
			auto cardinality = left.op->estimated_cardinality * right.op->estimated_cardinality;
			result_operator = LogicalCrossProduct::Create(std::move(left.op), std::move(right.op));
			result_operator->SetEstimatedCardinality(cardinality);
		} else {
			// we have filters, create a join node
			// Prefer non-INNER join types (LEFT/SEMI/ANTI) since WHERE clause filters default
			// to INNER but should not override the actual join semantics of the edge.
			auto chosen_filter = node->info->filters.at(0);
			for (idx_t i = 0; i < node->info->filters.size(); i++) {
				auto filter_join_type = node->info->filters.at(i)->join_type;
				if (filter_join_type != JoinType::INNER) {
					chosen_filter = node->info->filters.at(i);
					break;
				}
			}
			auto join = make_uniq<LogicalComparisonJoin>(chosen_filter->join_type);
			// Here we optimize build side probe side. Our build side is the right side
			// So the right plans should have lower cardinalities.
			join->children.push_back(std::move(left.op));
			join->children.push_back(std::move(right.op));

			// set the join conditions from the join node (source 1)
			for (auto &filter_ref : node->info->filters) {
				auto f = filter_ref.get();
				// extract the filter from the operator it originally belonged to
				D_ASSERT(filters_and_bindings[f->filter_index]->filter);
				auto &filter_and_binding = filters_and_bindings.at(f->filter_index);
				auto condition = std::move(filter_and_binding->filter);
				// now create the actual join condition
				D_ASSERT((JoinRelationSet::IsSubset(*left.set, *f->left_set) &&
				          JoinRelationSet::IsSubset(*right.set, *f->right_set)) ||
				         (JoinRelationSet::IsSubset(*left.set, *f->right_set) &&
				          JoinRelationSet::IsSubset(*right.set, *f->left_set)));

				bool invert = !JoinRelationSet::IsSubset(*left.set, *f->left_set);

				// If the left and right set are inverted for LEFT/SEMI/ANTI joins then swap them back
				// and set invert = false. This is to preserve left/rightedness of relations
				if (invert && (f->join_type == JoinType::LEFT || f->join_type == JoinType::SEMI ||
				               f->join_type == JoinType::ANTI)) {
					std::swap(join->children[0], join->children[1]);
					std::swap(left, right);
					invert = false;
				}

				if (condition->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
					auto cond = MaybeInvertConditions(std::move(condition), invert);
					join->conditions.push_back(std::move(cond));
				} else if (condition->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
					auto &conjunction = condition->Cast<BoundConjunctionExpression>();
					for (auto &child : conjunction.children) {
						D_ASSERT(child->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
						auto cond = MaybeInvertConditions(std::move(child), invert);
						join->conditions.push_back(std::move(cond));
					}
				}
				// Record the equivalence group so source 2 can skip transitively redundant filters.
				if (f->edge_equivalence_index.IsValid()) {
					applied_equivalence_groups.insert(f->edge_equivalence_index.GetIndex());
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

	// collect unused residual predicates that belong to THIS join
	vector<unique_ptr<Expression>> unused_residual_predicates;
	for (auto &filter_info : filters_and_bindings) {
		if (filter_info->from_residual_predicate && filters_and_bindings[filter_info->filter_index]->filter) {
			if (filter_info->set.get().count > 0 && JoinRelationSet::IsSubset(*result_relation, filter_info->set)) {
				unused_residual_predicates.push_back(
				    std::move(filters_and_bindings[filter_info->filter_index]->filter));
			}
		}
	}

	if (!unused_residual_predicates.empty()) {
		unique_ptr<Expression> combined = std::move(unused_residual_predicates[0]);
		for (idx_t i = 1; i < unused_residual_predicates.size(); i++) {
			combined = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(combined),
			                                                 std::move(unused_residual_predicates[i]));
		}

		if (result_operator->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			// attach to join's predicate field
			auto &comp_join = result_operator->Cast<LogicalComparisonJoin>();
			comp_join.conditions.emplace_back(std::move(combined));
		} else {
			// push as filter
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
			// skip filters from residual predicates
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
			// Skip filters whose equivalence group was already applied as a primary join condition
			// (source 1) somewhere in the join tree — they are transitively redundant.
			if (info.edge_equivalence_index.IsValid() &&
			    applied_equivalence_groups.count(info.edge_equivalence_index.GetIndex())) {
				continue;
			}
			if (info.set.get().count > 0 && JoinRelationSet::IsSubset(*result_relation, info.set)) {
				auto &filter_and_binding = filters_and_bindings[info.filter_index];
				auto filter = std::move(filter_and_binding->filter);
				// Record this equivalence group as applied so later pushdowns don't add it again.
				if (info.edge_equivalence_index.IsValid()) {
					applied_equivalence_groups.insert(info.edge_equivalence_index.GetIndex());
				}
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
				D_ASSERT(filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
				auto &comparison = filter->Cast<BoundComparisonExpression>();
				// we need to figure out which side is which by looking at the relations available to us
				auto left = !invert ? std::move(comparison.left) : std::move(comparison.right);
				auto right = !invert ? std::move(comparison.right) : std::move(comparison.left);
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
