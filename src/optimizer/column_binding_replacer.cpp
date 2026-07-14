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

void ColumnBindingReplacer::AddReplacement(ColumnBinding old_binding, ColumnBinding new_binding,
                                           const LogicalType &new_type) {
	replacement_bindings.emplace_back(old_binding, new_binding, new_type);
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

static const ReplacementBinding *FindReplacement(const ColumnBinding &binding,
                                                 const vector<ReplacementBinding> &replacement_bindings) {
	for (auto &replacement : replacement_bindings) {
		if (binding == replacement.old_binding) {
			return &replacement;
		}
	}
	return nullptr;
}

bool ColumnBindingReplacer::ReplaceBinding(ColumnBinding &binding,
                                           const vector<ReplacementBinding> &replacement_bindings) {
	auto replacement = FindReplacement(binding, replacement_bindings);
	if (!replacement) {
		return false;
	}
	binding = replacement->new_binding;
	return true;
}

void ColumnBindingReplacer::ReplaceBindings(vector<ColumnBinding> &bindings,
                                            const vector<ReplacementBinding> &replacement_bindings) {
	for (auto &binding : bindings) {
		ReplaceBinding(binding, replacement_bindings);
	}
}

static void ReplaceCorrelatedColumns(CorrelatedColumns &columns,
                                     const vector<ReplacementBinding> &replacement_bindings) {
	for (auto &column : columns) {
		auto replacement = FindReplacement(column.binding, replacement_bindings);
		if (!replacement) {
			continue;
		}
		column.binding = replacement->new_binding;
		if (replacement->replace_type) {
			column.type = replacement->new_type;
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

void ColumnBindingReplacer::VisitExpression(unique_ptr<Expression> *expression) {
	LogicalOperatorVisitor::VisitExpression(expression);
}

unique_ptr<Expression> ColumnBindingReplacer::VisitReplace(BoundColumnRefExpression &expr,
                                                           unique_ptr<Expression> *expr_ptr) {
	auto replacement = FindReplacement(expr.Binding(), replacement_bindings);
	if (!replacement) {
		return nullptr;
	}
	expr.BindingMutable() = replacement->new_binding;
	if (replacement->replace_type) {
		expr.SetReturnType(replacement->new_type);
	}
	return nullptr;
}

unique_ptr<Expression> ColumnBindingReplacer::VisitReplace(BoundSubqueryExpression &expr,
                                                           unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

void CorrelatedColumnBindingReplacer::VisitOperator(LogicalOperator &op) {
	if (stop_operator && stop_operator.get() == &op) {
		return;
	}
	VisitOperatorChildren(op);
	VisitOperatorBindings(op);
}

void CorrelatedColumnBindingReplacer::VisitOperatorBindings(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
		ReplaceCorrelatedColumns(op.Cast<LogicalDependentJoin>().correlated_columns, replacement_bindings);
		break;
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
