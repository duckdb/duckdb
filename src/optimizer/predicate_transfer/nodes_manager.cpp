#include "duckdb/optimizer/predicate_transfer/nodes_manager.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/optimizer/predicate_transfer/predicate_transfer_optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include <pthread.h>

namespace duckdb {
void NodesManager::Reset() {
	can_add_mark = true;
	nodes.clear();
	duplicate_nodes.clear();
	sorted_nodes.clear();
	rename_cols.clear();
}

idx_t NodesManager::NumNodes() {
	return nodes.size();
}

idx_t NodesManager::GetScalarTableIndex(LogicalOperator *op) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		return op->GetTableIndex()[0];
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		if (op->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
			return op->children[0]->Cast<LogicalGet>().GetTableIndex()[0];
		}
		if (op->children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			// For In-Clause optimization
			return op->children[0]->children[0]->Cast<LogicalGet>().GetTableIndex()[0];
		}
		if (op->children[0]->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			return op->children[0]->Cast<LogicalAggregate>().GetTableIndex()[0];
		}
		return op->Cast<LogicalGet>().GetTableIndex()[0];
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		return op->GetTableIndex()[1];
	}
	default: {
		return -1;
	}
	}
}

ColumnBinding NodesManager::FindRename(ColumnBinding col) {
	auto itr = rename_cols.find(col);
	ColumnBinding res = col;
	if (itr != rename_cols.end()) {
		res = itr->second;
		for (auto cur_itr = rename_cols.find(res); cur_itr != rename_cols.end(); cur_itr = rename_cols.find(res)) {
			res = cur_itr->second;
		}
	}
	return res;
}

void NodesManager::AddNode(LogicalOperator *op) {
	op->estimated_cardinality = op->EstimateCardinality(context);

	idx_t table_idx = GetScalarTableIndex(op);
	if (table_idx != -1 && nodes.find(table_idx) == nodes.end()) {
		nodes[table_idx] = op;
	}
}

void NodesManager::SortNodes() {
	sorted_nodes.clear();
	for (auto &node : nodes) {
		sorted_nodes.emplace_back(node.second);
	}
	sort(sorted_nodes.begin(), sorted_nodes.end(),
	     [&](LogicalOperator *a, LogicalOperator *b) { return a->estimated_cardinality < b->estimated_cardinality; });
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

void NodesManager::EraseNode(idx_t key) {
	nodes.erase(key);
}

void NodesManager::ExtractNodes(LogicalOperator &plan, vector<reference<LogicalOperator>> &joins) {
	LogicalOperator *op = &plan;

	while (op->children.size() == 1 && !OperatorNeedsRelation(op->type)) {
		if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
			LogicalOperator *child = op->children[0].get();
			if (child->type == LogicalOperatorType::LOGICAL_GET) {
				AddNode(op);
				return;
			}
			if (op->expressions[0]->type == ExpressionType::OPERATOR_NOT &&
			    op->expressions[0]->expression_class == ExpressionClass::BOUND_OPERATOR &&
			    child->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
				can_add_mark = false;
			} else {
				can_add_mark = true;
			}
			ExtractNodes(*child, joins);
			return;
		}
		op = op->children[0].get();
	}

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		if ((join.join_type == JoinType::INNER || join.join_type == JoinType::LEFT ||
		     join.join_type == JoinType::RIGHT || join.join_type == JoinType::SEMI ||
		     join.join_type == JoinType::RIGHT_SEMI || (join.join_type == JoinType::MARK && can_add_mark)) &&
		    std::any_of(join.conditions.begin(), join.conditions.end(), [](const auto &jc) {
			    return jc.comparison == ExpressionType::COMPARE_EQUAL &&
			           jc.left->type == ExpressionType::BOUND_COLUMN_REF &&
			           jc.right->type == ExpressionType::BOUND_COLUMN_REF;
		    })) {
			joins.push_back(*op);
		}
		can_add_mark = true;
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &agg = op->Cast<LogicalAggregate>();
		if (agg.groups.empty() && agg.grouping_sets.size() <= 1) {
			ExtractNodes(*op->children[0], joins);
			AddNode(op);
		} else {
			for (size_t i = 0; i < agg.groups.size(); i++) {
				if (agg.groups[i]->type == ExpressionType::BOUND_COLUMN_REF) {
					auto &colref = agg.groups[i]->Cast<BoundColumnRefExpression>();
					rename_cols.insert({agg.GetColumnBindings()[i], colref.binding});
				}
			}
			ExtractNodes(*op->children[0], joins);
		}
		return;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		for (size_t i = 0; i < op->expressions.size(); i++) {
			if (op->expressions[i]->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &colref = op->expressions[i]->Cast<BoundColumnRefExpression>();
				rename_cols.insert({op->GetColumnBindings()[i], colref.binding});
			}
		}
		ExtractNodes(*op->children[0], joins);
		return;
	}
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
		AddNode(op);
		ExtractNodes(*op->children[0], joins);
		ExtractNodes(*op->children[1], joins);
		return;
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		AddNode(op);
		return;
	default:
		for (auto &child : op->children) {
			ExtractNodes(*child, joins);
		}
	}
}
} // namespace duckdb