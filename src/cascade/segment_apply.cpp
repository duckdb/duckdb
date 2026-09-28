#include "duckdb/cascade/segment_apply.hpp"

#include <algorithm>

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

//! Follow a binding down to the base-table column it comes from, through the projections
//! and GroupBy keys that merely rename it. A computed column, and an aggregate's own
//! result, have no base column and stop the trace - which is what keeps the equality in
//! the join predicate honest about comparing two instances of the same *table* column.
static bool TraceToBaseColumn(LogicalOperator &op, const ColumnBinding &binding, string &table, idx_t &column) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		auto bindings = get.GetColumnBindings();
		for (idx_t i = 0; i < bindings.size(); i++) {
			if (bindings[i] != binding) {
				continue;
			}
			auto entry = get.GetTable();
			table = entry ? entry->name.GetIdentifierName() : get.GetName();
			// A scan may project a subset of the table's columns, so the scan's output
			// position and the table's column are not the same thing.
			auto &column_ids = get.GetColumnIds();
			column = i < column_ids.size() ? column_ids[i].GetPrimaryIndex() : i;
			return true;
		}
		return false;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		for (idx_t i = 0; i < projection.expressions.size(); i++) {
			if (ColumnBinding(projection.table_index, ProjectionIndex(i)) != binding) {
				continue;
			}
			auto &expr = *projection.expressions[i];
			if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				return false;
			}
			return TraceToBaseColumn(*projection.children[0], expr.Cast<BoundColumnRefExpression>().Binding(), table,
			                         column);
		}
		return false;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggregate = op.Cast<LogicalAggregate>();
		for (idx_t i = 0; i < aggregate.groups.size(); i++) {
			if (ColumnBinding(aggregate.group_index, ProjectionIndex(i)) != binding) {
				continue;
			}
			auto &expr = *aggregate.groups[i];
			if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				return false;
			}
			return TraceToBaseColumn(*aggregate.children[0], expr.Cast<BoundColumnRefExpression>().Binding(), table,
			                         column);
		}
		// an aggregate's own result is not a column of any table
		return false;
	}
	default:
		break;
	}
	// A filter, an order by, a limit ... pass their child's bindings through unchanged,
	// and a join exposes both of its inputs - so the binding names whichever child
	// produces it. That child is the one to follow, which is what lets the aggregated
	// side of a two-instance join be recognized when it sits inside another join.
	for (auto &child : op.children) {
		for (auto &candidate : child->GetColumnBindings()) {
			if (candidate == binding) {
				return TraceToBaseColumn(*child, binding, table, column);
			}
		}
	}
	return false;
}

static string ColumnName(LogicalOperator &op, const string &table, idx_t column) {
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return table + ".#" + to_string(column);
	}
	auto entry = op.Cast<LogicalGet>().GetTable();
	if (!entry || column >= entry->GetColumns().PhysicalColumnCount()) {
		return table + ".#" + to_string(column);
	}
	return table + "." + entry->GetColumns().GetColumn(PhysicalIndex(column)).Name();
}

static void CollectSegmentingColumns(LogicalOperator &op, vector<string> &found) {
	if ((op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	     op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) &&
	    op.children.size() == 2) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &condition : join.conditions) {
			if (!condition.IsComparison() ||
			    condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
				continue;
			}
			auto &lhs = condition.GetLHS();
			auto &rhs = condition.GetRHS();
			if (lhs.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
			    rhs.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				// An equality between arbitrary expressions is not necessarily an
				// equality of two instances of the same column.
				continue;
			}
			auto &left_ref = lhs.Cast<BoundColumnRefExpression>();
			auto &right_ref = rhs.Cast<BoundColumnRefExpression>();
			string left_table;
			string right_table;
			idx_t left_column = 0;
			idx_t right_column = 0;
			if (!TraceToBaseColumn(*op.children[0], left_ref.Binding(), left_table, left_column)) {
				continue;
			}
			if (!TraceToBaseColumn(*op.children[1], right_ref.Binding(), right_table, right_column)) {
				continue;
			}
			// The same column of the same table on both sides: rows whose value differs
			// can never match, so the column partitions the relation.
			if (left_table != right_table || left_column != right_column) {
				continue;
			}
			auto name = ColumnName(*op.children[0], left_table, left_column);
			bool already = false;
			for (auto &existing : found) {
				if (existing == name) {
					already = true;
					break;
				}
			}
			if (!already) {
				found.push_back(std::move(name));
			}
		}
	}
	for (auto &child : op.children) {
		CollectSegmentingColumns(*child, found);
	}
}

vector<string> DescribeSegmentApplyAlternatives(LogicalOperator &plan) {
	vector<string> found;
	CollectSegmentingColumns(plan, found);
	sort(found.begin(), found.end());
	return found;
}

} // namespace duckdb
