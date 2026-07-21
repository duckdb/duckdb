#include "duckdb/optimizer/column_binding_replacer.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/column_binding_map.hpp"

namespace duckdb {

ReplacementBinding::ReplacementBinding(ColumnBinding old_binding, ColumnBinding new_binding)
    : old_binding(old_binding), new_binding(new_binding), replace_type(false) {
}

ReplacementBinding::ReplacementBinding(ColumnBinding old_binding, ColumnBinding new_binding, LogicalType new_type)
    : old_binding(old_binding), new_binding(new_binding), replace_type(true), new_type(std::move(new_type)) {
}

ReplacementBinding BindingReplacementMap::ResolveReplacement(ColumnBinding binding) const {
	ReplacementBinding result(binding, binding);
	column_binding_set_t visited;
	while (visited.insert(result.new_binding).second) {
		optional_ptr<const ReplacementBinding> replacement;
		for (auto &entry : replacement_bindings) {
			if (entry.old_binding == result.new_binding) {
				replacement = entry;
				break;
			}
		}
		if (!replacement) {
			return result;
		}
		result.new_binding = replacement->new_binding;
		if (replacement->replace_type) {
			result.replace_type = true;
			result.new_type = replacement->new_type;
		}
	}
	throw InternalException("Cyclic column binding replacements");
}

ColumnBinding BindingReplacementMap::Resolve(ColumnBinding binding) const {
	return ResolveReplacement(binding).new_binding;
}

void BindingReplacementMap::Add(ColumnBinding old_binding, ColumnBinding new_binding) {
	Add(ReplacementBinding(old_binding, new_binding));
}

bool BindingReplacementMap::TryAdd(const ReplacementBinding &replacement) {
	auto resolved_new = ResolveReplacement(replacement.new_binding);
	if (!resolved_new.replace_type && replacement.replace_type) {
		resolved_new.replace_type = true;
		resolved_new.new_type = replacement.new_type;
	}
	if (replacement.old_binding == resolved_new.new_binding) {
		if (replacement.old_binding != replacement.new_binding) {
			return false;
		}
		return true;
	}
	for (auto &existing : replacement_bindings) {
		if (existing.old_binding != replacement.old_binding) {
			continue;
		}
		auto resolved_existing = ResolveReplacement(existing.old_binding);
		if (resolved_existing.new_binding != resolved_new.new_binding ||
		    (resolved_existing.replace_type && resolved_new.replace_type &&
		     resolved_existing.new_type != resolved_new.new_type)) {
			return false;
		}
		if (!resolved_existing.replace_type && resolved_new.replace_type) {
			existing.replace_type = true;
			existing.new_type = resolved_new.new_type;
		}
		return true;
	}
	if (resolved_new.replace_type) {
		replacement_bindings.emplace_back(replacement.old_binding, resolved_new.new_binding, resolved_new.new_type);
	} else {
		replacement_bindings.emplace_back(replacement.old_binding, resolved_new.new_binding);
	}
	return true;
}

void BindingReplacementMap::Add(const ReplacementBinding &replacement) {
	if (!TryAdd(replacement)) {
		throw InternalException("Conflicting or cyclic column binding replacement for %s",
		                        replacement.old_binding.ToString());
	}
}

void BindingReplacementMap::Merge(const BindingReplacementMap &replacements) {
	Merge(replacements.replacement_bindings);
}

void BindingReplacementMap::Merge(const vector<ReplacementBinding> &replacements) {
	for (auto &replacement : replacements) {
		Add(replacement);
	}
}

void BindingReplacementMap::AddTo(ColumnBindingReplacer &replacer) const {
	for (auto &replacement : replacement_bindings) {
		auto resolved = ResolveReplacement(replacement.old_binding);
		if (resolved.replace_type) {
			replacer.replacement_bindings.emplace_back(replacement.old_binding, resolved.new_binding,
			                                           resolved.new_type);
		} else {
			replacer.replacement_bindings.emplace_back(replacement.old_binding, resolved.new_binding);
		}
	}
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

void ColumnBindingRewrite::RemapProjectionMapStrict(vector<ProjectionIndex> &projection_map,
                                                    const vector<ColumnBinding> &child_bindings_before,
                                                    const vector<ColumnBinding> &child_bindings_after) {
	vector<ColumnBinding> selected_bindings;
	if (projection_map.empty()) {
		selected_bindings = child_bindings_before;
	} else {
		selected_bindings.reserve(projection_map.size());
		for (auto projection_index : projection_map) {
			if (projection_index.GetIndex() >= child_bindings_before.size()) {
				throw InternalException("Projection map references column %llu in a child with %llu columns",
				                        projection_index.GetIndex(), child_bindings_before.size());
			}
			selected_bindings.push_back(child_bindings_before[projection_index.GetIndex()]);
		}
	}
	if (selected_bindings == child_bindings_after) {
		projection_map.clear();
		return;
	}
	vector<ProjectionIndex> new_projection_map;
	new_projection_map.reserve(selected_bindings.size());
	for (auto &binding : selected_bindings) {
		auto entry = std::find(child_bindings_after.begin(), child_bindings_after.end(), binding);
		if (entry == child_bindings_after.end()) {
			throw InternalException("Binding rewrite lost projected child binding %s (selected %s, child output %s)",
			                        binding.ToString(), LogicalOperator::ColumnBindingsToString(selected_bindings),
			                        LogicalOperator::ColumnBindingsToString(child_bindings_after));
		}
		new_projection_map.emplace_back(NumericCast<idx_t>(entry - child_bindings_after.begin()));
	}
	projection_map = std::move(new_projection_map);
}

void ColumnBindingRewrite::ApplyToOperatorBindings(LogicalOperator &op, const BindingReplacementMap &replacements) {
	if (replacements.Empty()) {
		return;
	}
	CorrelatedColumnBindingReplacer replacer;
	replacements.AddTo(replacer);
	replacer.VisitOperatorBindings(op);
}

void ColumnBindingRewrite::FinalizeOperator(ClientContext &context, unique_ptr<LogicalOperator> &op) {
	LogicalComparisonJoin::FinalizeBindingRewrite(context, op);
}

bool ColumnBindingRewrite::ApplyToChild(ClientContext &context, unique_ptr<LogicalOperator> &op, idx_t child_index,
                                        vector<ColumnBinding> old_child_bindings,
                                        const BindingReplacementMap &replacements) {
	if (child_index >= op->children.size()) {
		throw InternalException("Binding rewrite child index %llu out of range", child_index);
	}
	for (auto &binding : old_child_bindings) {
		binding = replacements.Resolve(binding);
	}
	auto new_child_bindings = op->children[child_index]->GetColumnBindings();
	auto layout_changed = old_child_bindings != new_child_bindings;
	auto projection_map = LogicalOperatorVisitor::GetProjectionMap(*op, child_index);
	if (projection_map) {
		RemapProjectionMapStrict(*projection_map, old_child_bindings, new_child_bindings);
	}
	if (replacements.Empty() && !layout_changed) {
		return false;
	}
	if (!replacements.Empty()) {
		CorrelatedColumnBindingReplacer replacer;
		replacements.AddTo(replacer);
		if (op->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN && child_index == 0) {
			replacer.stop_operator = *op->children[child_index];
			replacer.VisitOperator(*op);
		} else {
			replacer.VisitOperatorBindings(*op);
		}
	}
	FinalizeOperator(context, op);
	return true;
}

} // namespace duckdb
