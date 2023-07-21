#include "duckdb/optimizer/join_order/query_graph_manager.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/planner/operator/list.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

void QueryGraphManager::Build(LogicalOperator *op) {
	vector<reference<LogicalOperator>> filter_operators;
	ExtractJoinRelations(*op, filter_operators, nullptr);
	if (relation_manager.NumRelations() <= 1) {
		// nothing to optimize/reorder
		return;
	}
	ExtractEdges(*op, filter_operators);
	D_ASSERT(!query_graph.ToString().empty());
}


//! Extract the set of relations referred to inside an expression
bool QueryGraphManager::ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);
		D_ASSERT(colref.binding.table_index != DConstants::INVALID_INDEX);
		// map the base table index to the relation index used by the JoinOrderOptimizer
		// TODO: what is the relation mapping and why is it important?
		D_ASSERT(relation_manager.relation_mapping.find(colref.binding.table_index) != relation_manager.relation_mapping.end());
		auto catalog_table = relation_manager.relation_mapping[colref.binding.table_index];
		auto column_index = colref.binding.column_index;
//		cardinality_estimator.AddColumnToRelationMap(catalog_table, column_index);
		bindings.insert(relation_manager.relation_mapping[colref.binding.table_index]);
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


static RelationStats GetStats(LogicalOperator &op) {
	return RelationStats();
}

bool QueryGraphManager::ExtractJoinRelations(LogicalOperator &input_op,
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
		for (auto &child : op->children) {
			JoinOrderOptimizer optimizer(context);
			child = optimizer.Optimize(std::move(child));
			// now that we have optimized the child, we get the statistics of the child
			// and merge them together (i.e merge left and right stats)/
			auto extra_stats = GetStats(*child);
			merge_stats(stats, extra_stats);
		}
		relation_manager.AddRelation(input_op, parent, stats);
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
		relation_manager.AddRelation(input_op, parent, stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_GET:
	// FIXME: See if we can get rid of NOP() projection first, so we can reorder more join.
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		if (op->children.empty() && op->type == LogicalOperatorType::LOGICAL_GET) {
			// TODO: Get stats from a logical GET
			auto stats = RelationStats();
			relation_manager.AddRelation(input_op, parent, stats);
			return true;
		}
		JoinOrderOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(std::move(op->children[0]));
		// now that we have optimized the children, we can get the statistics and add them to the relation
		auto stats = GetStats(*op->children[0]);
		// have to be careful here. projections can sit on joins and have more columns than
		// the original logical get underneath. For this reason we need to copy some bindings from
		// the optimizer just declared, so we know what columns map to
		relation_manager.AddRelation(input_op, parent, stats);
		return true;
	}
	default:
		return false;
	}
}


void QueryGraphManager::GetColumnBinding(Expression &expression, ColumnBinding &binding) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		// Here you have a filter on a single column in a table. Return a binding for the column
		// being filtered on so the filter estimator knows what HLL count to pull
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);
		D_ASSERT(colref.binding.table_index != DConstants::INVALID_INDEX);
		// map the base table index to the relation index used by the JoinOrderOptimizer
		D_ASSERT(relation_manager.relation_mapping.find(colref.binding.table_index) != relation_manager.relation_mapping.end());
		binding = ColumnBinding(relation_manager.relation_mapping[colref.binding.table_index], colref.binding.column_index);
	}
	// TODO: handle inequality filters with functions.
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &expr) { GetColumnBinding(expr, binding); });
}


const vector<unique_ptr<FilterInfo>> QueryGraphManager::GetFilterBindings() {
	return filters_and_bindings;
}

bool QueryGraphManager::ExtractEdges(LogicalOperator &op, vector<reference<LogicalOperator>> &filter_operators) {
	// now that we know we are going to perform join ordering we actually extract the filters, eliminating duplicate
	// filters in the process

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
					auto &set = set_manager.GetJoinRelation(bindings);
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
					auto &set = set_manager.GetJoinRelation(bindings);
					auto filter_info = make_uniq<FilterInfo>(std::move(expression), set, filters_and_bindings.size());
					filters_and_bindings.push_back(std::move(filter_info));
				}
			}
			f_op.expressions.clear();
		}
	}

	// create potential edges from the comparisons
	for (auto &filter_info: filters_and_bindings) {
		auto &filter = filter_info->filter;
		// now check if it can be used as a join predicate
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			auto &comparison = filter->Cast<BoundComparisonExpression>();
			// extract the bindings that are required for the left and right side of the comparison
			unordered_set<idx_t> left_bindings, right_bindings;
			ExtractBindings(*comparison.left, left_bindings);
			ExtractBindings(*comparison.right, right_bindings);
			GetColumnBinding(*comparison.left, filter_info->left_binding);
			GetColumnBinding(*comparison.right, filter_info->right_binding);
			if (!left_bindings.empty() && !right_bindings.empty()) {
				// both the left and the right side have bindings
				// first create the relation sets, if they do not exist
				filter_info->left_set = &set_manager.GetJoinRelation(left_bindings);
				filter_info->right_set = &set_manager.GetJoinRelation(right_bindings);
				// we can only create a meaningful edge if the sets are not exactly the same
				if (filter_info->left_set != filter_info->right_set) {
					// check if the sets are disjoint
					if (Disjoint(left_bindings, right_bindings)) {
						// they are disjoint, we only need to create one set of edges in the join graph
						query_graph.CreateEdge(*filter_info->left_set, *filter_info->right_set, filter_info);
						query_graph.CreateEdge(*filter_info->right_set, *filter_info->left_set, filter_info);
					} else {
						continue;
					}
					continue;
				}
			}
		}
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

unique_ptr<LogicalOperator> QueryGraphManager::Reconstruct(unique_ptr<LogicalOperator> plan, JoinNode &node) {
    return RewritePlan(std::move(plan), node);
}

GenerateJoinRelation QueryGraphManager::GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations,
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
				D_ASSERT(filters_and_bindings[f.filter_index]);
				auto filter_and_binding = filters_and_bindings.at(f.filter_index);
				auto condition = std::move(filter_and_binding);
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
		result_relation = set_manager.Union(*left_node, *right_node);
	} else {
		// base node, get the entry from the list of extracted relations
		D_ASSERT(node.set.count == 1);
		D_ASSERT(extracted_relations[node.set.relations[0]]);
		result_relation = &node.set;
		result_operator = std::move(extracted_relations[node.set.relations[0]]);
	}
	// TODO: this is where estimated properties start coming into play.
	//  when creating the result operator, we should ask the cost model and cardinality estimator what
	//  the cost and cardinality are
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
	for (auto &filter_info : filters_and_bindings) {
		// check if the filter has already been extracted
		auto &info = *filter_info;
		if (filters_and_bindings[info.filter_index]) {
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

unique_ptr<LogicalOperator> QueryGraphManager::RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode &node) {
	// now we have to rewrite the plan
	bool root_is_join = plan->children.size() > 1;

	// first we will extract all relations from the main plan
	vector<unique_ptr<LogicalOperator>> extracted_relations;
	extracted_relations.reserve(relation_manager.NumRelations());
	for (auto &relation : relation_manager.GetRelations()) {
		extracted_relations.push_back(ExtractJoinRelation(relation));
	}

	// now we generate the actual joins
	auto join_tree = GenerateJoins(extracted_relations, node);
	// perform the final pushdown of remaining filters
	for (auto &filter : filters_and_bindings) {
		// check if the filter has already been extracted
		if (filter) {
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


} // namespace duckdb
