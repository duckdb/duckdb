#include "duckdb/optimizer/column_binding_replacer.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"

namespace duckdb {

ReplacementBinding::ReplacementBinding(ColumnBinding old_binding, ColumnBinding new_binding)
    : old_binding(old_binding), new_binding(new_binding), replace_type(false) {
}

ReplacementBinding::ReplacementBinding(ColumnBinding old_binding, ColumnBinding new_binding, LogicalType new_type)
    : old_binding(old_binding), new_binding(new_binding), replace_type(true), new_type(std::move(new_type)) {
}

ColumnBindingReplacer::ColumnBindingReplacer() {
}

void ColumnBindingReplacer::AddReplacement(ColumnBinding old_binding, ColumnBinding new_binding) {
	if (old_binding != new_binding) {
		replacement_bindings.emplace_back(old_binding, new_binding);
	}
}

void ColumnBindingReplacer::AddReplacements(const vector<ColumnBinding> &old_bindings,
                                            const vector<ColumnBinding> &new_bindings) {
	if (old_bindings.size() != new_bindings.size()) {
		throw InternalException("Column binding replacement lists must have the same size");
	}
	replacement_bindings.reserve(replacement_bindings.size() + old_bindings.size());
	for (idx_t i = 0; i < old_bindings.size(); i++) {
		AddReplacement(old_bindings[i], new_bindings[i]);
	}
}

static void ReplaceCorrelatedColumns(CorrelatedColumns &columns,
                                     const vector<ReplacementBinding> &replacement_bindings) {
	for (auto &column : columns) {
		for (auto &replacement : replacement_bindings) {
			if (column.binding != replacement.old_binding) {
				continue;
			}
			column.binding = replacement.new_binding;
			if (replacement.replace_type) {
				column.type = replacement.new_type;
			}
		}
	}
}

void ColumnBindingReplacer::VisitOperator(LogicalOperator &op) {
	if (stop_operator && stop_operator.get() == &op) {
		return;
	}
	VisitOperatorChildren(op);
	VisitOperatorBindings(op);
}

void ColumnBindingReplacer::VisitOperatorBindings(LogicalOperator &op) {
	VisitOperatorExpressions(op);
}

unique_ptr<Expression> ColumnBindingReplacer::VisitReplace(BoundColumnRefExpression &expr,
                                                           unique_ptr<Expression> *expr_ptr) {
	for (auto &replacement : replacement_bindings) {
		if (expr.Binding() != replacement.old_binding) {
			continue;
		}
		expr.BindingMutable() = replacement.new_binding;
		if (replacement.replace_type) {
			expr.SetReturnType(replacement.new_type);
		}
	}
	return nullptr;
}

void CorrelatedColumnBindingReplacer::VisitOperatorBindings(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
		auto &dependent_join = op.Cast<LogicalDependentJoin>();
		ReplaceCorrelatedColumns(dependent_join.correlated_columns, replacement_bindings);
		break;
	}
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		ReplaceCorrelatedColumns(op.Cast<LogicalCTE>().correlated_columns, replacement_bindings);
		break;
	default:
		break;
	}
	ColumnBindingReplacer::VisitOperatorBindings(op);
}

unique_ptr<Expression> CorrelatedColumnBindingReplacer::VisitReplace(BoundSubqueryExpression &expr,
                                                                     unique_ptr<Expression> *expr_ptr) {
	ReplaceCorrelatedColumns(expr.GetBinder()->correlated_columns, replacement_bindings);
	if (expr.SubqueryMutable().plan) {
		VisitOperator(*expr.SubqueryMutable().plan);
	}
	return nullptr;
}

} // namespace duckdb
