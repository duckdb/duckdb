#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

static void ReplaceSetOpBindings(vector<ColumnBinding> &bindings, Filter &filter, Expression &expr,
                                 LogicalSetOperation &setop) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.binding.table_index == setop.table_index);
		D_ASSERT(colref.depth == 0);

		// rewrite the binding by looking into the bound_tables list of the subquery
		colref.binding = bindings[colref.binding.column_index];
		filter.bindings.insert(colref.binding.table_index);
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](Expression &child) { ReplaceSetOpBindings(bindings, filter, child, setop); });
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownSetOperation(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_UNION || op->type == LogicalOperatorType::LOGICAL_EXCEPT ||
	         op->type == LogicalOperatorType::LOGICAL_INTERSECT);
	auto &setop = op->Cast<LogicalSetOperation>();

	D_ASSERT(op->children.size() == 2);
	auto left_bindings = op->children[0]->GetColumnBindings();
	auto right_bindings = op->children[1]->GetColumnBindings();
	if (left_bindings.size() != right_bindings.size()) {
		throw InternalException("Filter pushdown - set operation LHS and RHS have incompatible counts");
	}

	// pushdown into set operation, we can duplicate the condition and pushdown the expressions into both sides
	FilterPushdown left_pushdown(optimizer, convert_mark_joins), right_pushdown(optimizer, convert_mark_joins);
	for (idx_t i = 0; i < filters.size(); i++) {
		// first create a copy of the filter
		auto right_filter = make_uniq<Filter>();
		right_filter->filter = filters[i]->filter->Copy();

		// in the original filter, rewrite references to the result of the union into references to the left_index
		ReplaceSetOpBindings(left_bindings, *filters[i], *filters[i]->filter, setop);
		// in the copied filter, rewrite references to the result of the union into references to the right_index
		ReplaceSetOpBindings(right_bindings, *right_filter, *right_filter->filter, setop);

		// extract bindings again
		filters[i]->ExtractBindings();
		right_filter->ExtractBindings();

		// move the filters into the child pushdown nodes
		left_pushdown.filters.push_back(std::move(filters[i]));
		right_pushdown.filters.push_back(std::move(right_filter));
	}

	op->children[0] = left_pushdown.Rewrite(std::move(op->children[0]));
	op->children[1] = right_pushdown.Rewrite(std::move(op->children[1]));

	bool left_empty = op->children[0]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT;
	bool right_empty = op->children[1]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT;
	if (left_empty && right_empty) {
		// both empty: return empty result
		return make_uniq<LogicalEmptyResult>(std::move(op));
	}
	if (left_empty && setop.setop_all) {
		// left child is empty result
		switch (op->type) {
		case LogicalOperatorType::LOGICAL_UNION:
			if (op->children[1]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				// union with empty left side: return right child
				auto &projection = op->children[1]->Cast<LogicalProjection>();
				projection.table_index = setop.table_index;
				return std::move(op->children[1]);
			}
			break;
		case LogicalOperatorType::LOGICAL_EXCEPT:
			// except: if left child is empty, return empty result
		case LogicalOperatorType::LOGICAL_INTERSECT:
			// intersect: if any child is empty, return empty result itself
			return make_uniq<LogicalEmptyResult>(std::move(op));
		default:
			throw InternalException("Unsupported set operation");
		}
	} else if (right_empty && setop.setop_all) {
		// right child is empty result
		switch (op->type) {
		case LogicalOperatorType::LOGICAL_UNION:
		case LogicalOperatorType::LOGICAL_EXCEPT:
			if (op->children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				// union or except with empty right child: return left child
				auto &projection = op->children[0]->Cast<LogicalProjection>();
				projection.table_index = setop.table_index;
				return std::move(op->children[0]);
			}
			break;
		case LogicalOperatorType::LOGICAL_INTERSECT:
			// intersect: if any child is empty, return empty result itself
			return make_uniq<LogicalEmptyResult>(std::move(op));
		default:
			throw InternalException("Unsupported set operation");
		}
	}
	return op;
}

} // namespace duckdb
