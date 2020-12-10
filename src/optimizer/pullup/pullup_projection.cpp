#include "duckdb/optimizer/filter_pullup.hpp"
// #include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"

namespace duckdb {
using namespace std;

static Expression *GetColumnRefExpression(Expression &expr) {
        if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
            return &expr;
        }
        ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { return GetColumnRefExpression(child); });
        return &expr;
}

static bool GenerateBinding(LogicalProjection &proj, BoundColumnRefExpression &colref, ColumnBinding &binding) {
        D_ASSERT(colref.depth == 0);
        int column_index = -1;
        // find the corresponding column index in the projection
        for(idx_t proj_idx=0; proj_idx < proj.expressions.size(); proj_idx++) {
            auto proj_colref = GetColumnRefExpression(*proj.expressions[proj_idx]);
            if (proj_colref->type == ExpressionType::BOUND_COLUMN_REF) {
                // auto proj_colref = (BoundColumnRefExpression *)proj.expressions[proj_idx].get();
                if(colref.Equals(proj_colref)) {
                    column_index = proj_idx;
                    break;
                }
            }
        }
        // Case the filter column is not projected, returns false
        if(column_index == -1) {
            return false;
        }
        binding.table_index = proj.table_index;
        binding.column_index = column_index;
        return true;
}

static bool ReplaceFilterBindings(LogicalProjection &proj, Expression &expr) {
    // we do not use ExpressionIterator here because we need to check if the filtered column is being projected,
    // otherwise we should avoid the filter to be pulled up by returning false
    if(expr.expression_class == ExpressionClass::BOUND_COMPARISON) {
        auto &comp_expr = (BoundComparisonExpression &)expr;
        unique_ptr<BoundColumnRefExpression> left_expr, right_expr;
        if(comp_expr.left->type == ExpressionType::BOUND_COLUMN_REF) {
            auto &colref = (BoundColumnRefExpression &)*comp_expr.left;
            ColumnBinding binding;
            if(GenerateBinding(proj, colref, binding) == false) {
                // the filtered column is not projected, this filter doesn't need to be pulled up
                return false;
            }
            left_expr = make_unique<BoundColumnRefExpression>(colref.alias, colref.return_type, binding, colref.depth);
        }
        if(comp_expr.right->type == ExpressionType::BOUND_COLUMN_REF) {
            auto &colref = (BoundColumnRefExpression &)*comp_expr.right;
            ColumnBinding binding;
            if(GenerateBinding(proj, colref, binding) == false) {
                // the filtered column is not projected, this filter doesn't need to be pulled up
                return false;
            }
            right_expr = make_unique<BoundColumnRefExpression>(colref.alias, colref.return_type, binding, colref.depth);
        }
        if(left_expr) {
            comp_expr.left = move(left_expr);
        }
        if(right_expr) {
            comp_expr.right = move(right_expr);
        }
    }
    return true;
}

unique_ptr<LogicalOperator> FilterPullup::PullupProjection(unique_ptr<LogicalOperator> op) {
    D_ASSERT(op->type == LogicalOperatorType::LOGICAL_PROJECTION);
    if(root_pullup_node_ptr == nullptr) {
        root_pullup_node_ptr = op.get();
    }
    op->children[0] = Rewrite(move(op->children[0]));
    if(root_pullup_node_ptr == op.get() && filters_pullup.size() > 0) {
        return FixParenthood(move(op), move(filters_pullup[0]));
        // return FinishPullup(move(op));
    } else if(filters_pullup.size() > 0) {
        auto &proj = (LogicalProjection &)*op;
        auto column_bindings = op->GetColumnBindings();
        for (idx_t f_idx = 0; f_idx < filters_pullup.size(); ++f_idx) {
            bool to_reverse = false;
            for(idx_t expr_idx=0; expr_idx < filters_pullup[f_idx]->expressions.size(); ++expr_idx) {
                auto &expr_filter =  (Expression &)*filters_pullup[f_idx]->expressions[expr_idx];
                to_reverse = !ReplaceFilterBindings(proj, expr_filter);
            }
            if(to_reverse) {
                // revert filter pullup
                filters_pullup[f_idx]->children.push_back(move(op->children[0]));
                op->children[0] = move(filters_pullup[f_idx]);
                filters_pullup.erase(filters_pullup.begin() + f_idx);
            }
        }
    }
    return op;
}

} // namespace duckdb
