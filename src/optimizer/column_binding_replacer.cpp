#include "duckdb/optimizer/column_binding_replacer.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/column_binding_map.hpp"

namespace duckdb {

ReplacementBinding::ReplacementBinding(ColumnBinding old_binding, ColumnBinding new_binding)
    : old_binding(old_binding), new_binding(new_binding), replace_type(false) {
}

ReplacementBinding::ReplacementBinding(ColumnBinding old_binding, ColumnBinding new_binding, LogicalType new_type)
    : old_binding(old_binding), new_binding(new_binding), replace_type(true), new_type(std::move(new_type)) {
}

ReplacementBinding BindingReplacementGraph::ResolveReplacement(ColumnBinding binding) const {
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

ColumnBinding BindingReplacementGraph::Resolve(ColumnBinding binding) const {
	return ResolveReplacement(binding).new_binding;
}

void BindingReplacementGraph::Add(ColumnBinding old_binding, ColumnBinding new_binding) {
	Add(ReplacementBinding(old_binding, new_binding));
}

bool BindingReplacementGraph::TryAdd(const ReplacementBinding &replacement) {
	auto resolved_new = ResolveReplacement(replacement.new_binding);
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
		if (existing.new_binding != replacement.new_binding ||
		    (existing.replace_type && replacement.replace_type && existing.new_type != replacement.new_type)) {
			return false;
		}
		if (!existing.replace_type && replacement.replace_type) {
			existing.replace_type = true;
			existing.new_type = replacement.new_type;
		}
		return true;
	}
	if (replacement.replace_type) {
		replacement_bindings.emplace_back(replacement.old_binding, replacement.new_binding, replacement.new_type);
	} else {
		replacement_bindings.emplace_back(replacement.old_binding, replacement.new_binding);
	}
	return true;
}

void BindingReplacementGraph::Add(const ReplacementBinding &replacement) {
	if (!TryAdd(replacement)) {
		throw InternalException("Conflicting or cyclic column binding replacement for %s",
		                        replacement.old_binding.ToString());
	}
}

void BindingReplacementGraph::Merge(const BindingReplacementGraph &replacements) {
	for (auto &replacement : replacements.replacement_bindings) {
		Add(replacement);
	}
}

void BindingReplacementGraph::AddTo(ColumnBindingReplacer &replacer) const {
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

void ColumnBindingReplacer::AddReplacements(const vector<ColumnBinding> &old_bindings,
                                            const vector<ColumnBinding> &new_bindings) {
	if (old_bindings.size() != new_bindings.size()) {
		throw InternalException("Column binding replacement lists must have the same size");
	}
	replacement_bindings.reserve(replacement_bindings.size() + old_bindings.size());
	for (idx_t i = 0; i < old_bindings.size(); i++) {
		if (old_bindings[i] != new_bindings[i]) {
			replacement_bindings.emplace_back(old_bindings[i], new_bindings[i]);
		}
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

void ColumnBindingRewrite::ApplyToOperatorBindings(LogicalOperator &op, const BindingReplacementGraph &replacements) {
	if (replacements.Empty()) {
		return;
	}
	CorrelatedColumnBindingReplacer replacer;
	replacements.AddTo(replacer);
	replacer.VisitOperatorBindings(op);
}

static optional_ptr<const ReplacementBinding> FindReplacement(const BindingReplacementGraph &replacements,
                                                              ColumnBinding binding) {
	for (auto &replacement : replacements) {
		if (replacement.old_binding == binding) {
			return replacement;
		}
	}
	return nullptr;
}

static bool TryResolveToOutput(ColumnBinding binding, const column_binding_set_t &new_bindings,
                               const BindingReplacementGraph &replacements, ReplacementBinding &result) {
	result = ReplacementBinding(binding, binding);
	column_binding_set_t visited;
	while (new_bindings.find(result.new_binding) == new_bindings.end()) {
		if (!visited.insert(result.new_binding).second) {
			throw InternalException("Cyclic column binding replacements");
		}
		auto next = FindReplacement(replacements, result.new_binding);
		if (!next) {
			return false;
		}
		result.new_binding = next->new_binding;
		if (next->replace_type) {
			result.replace_type = true;
			result.new_type = next->new_type;
		}
	}
	return true;
}

static vector<ReplacementBinding> ScopeToOutput(const vector<ColumnBinding> &new_output,
                                                const BindingReplacementGraph &replacements) {
	column_binding_set_t new_bindings(new_output.begin(), new_output.end());
	vector<ReplacementBinding> result;
	for (auto &replacement : replacements) {
		if (new_bindings.find(replacement.old_binding) != new_bindings.end()) {
			continue;
		}

		ReplacementBinding output_replacement(replacement.old_binding, replacement.old_binding);
		if (!TryResolveToOutput(replacement.old_binding, new_bindings, replacements, output_replacement)) {
			continue;
		}
		result.push_back(std::move(output_replacement));
	}
	return result;
}

void ColumnBindingRewrite::ValidateOutput(const vector<ColumnBinding> &old_output,
                                          const vector<ColumnBinding> &new_output,
                                          const BindingReplacementGraph &replacements) {
	column_binding_set_t new_bindings(new_output.begin(), new_output.end());
	for (auto &binding : old_output) {
		ReplacementBinding resolved(binding, binding);
		if (!TryResolveToOutput(binding, new_bindings, replacements, resolved)) {
			throw InternalException("Binding rewrite lost output binding %s (resolved to %s, output %s)",
			                        binding.ToString(), resolved.new_binding.ToString(),
			                        LogicalOperator::ColumnBindingsToString(new_output));
		}
	}
}

static ReplacementBinding ResolveBoundaryReplacement(ColumnBinding binding,
                                                     const vector<ReplacementBinding> &replacements) {
	for (auto &replacement : replacements) {
		if (replacement.old_binding == binding) {
			return replacement;
		}
	}
	return ReplacementBinding(binding, binding);
}

void ColumnBindingRewrite::ApplyToChild(unique_ptr<LogicalOperator> &op, idx_t child_index,
                                        vector<ColumnBinding> old_child_bindings,
                                        const BindingReplacementGraph &replacements) {
	if (child_index >= op->children.size()) {
		throw InternalException("Binding rewrite child index %llu out of range", child_index);
	}
	auto new_child_bindings = op->children[child_index]->GetColumnBindings();
	auto boundary_replacements = ScopeToOutput(new_child_bindings, replacements);
	column_binding_set_t new_bindings(new_child_bindings.begin(), new_child_bindings.end());
	for (auto &binding : old_child_bindings) {
		if (new_bindings.find(binding) == new_bindings.end() && FindReplacement(replacements, binding)) {
			ReplacementBinding resolved(binding, binding);
			if (!TryResolveToOutput(binding, new_bindings, replacements, resolved)) {
				throw InternalException("Binding rewrite moved child binding %s outside rewritten child output %s",
				                        binding.ToString(),
				                        LogicalOperator::ColumnBindingsToString(new_child_bindings));
			}
		}
		binding = ResolveBoundaryReplacement(binding, boundary_replacements).new_binding;
	}
	if (op->HasProjectionMap()) {
		auto projection_map = LogicalOperatorVisitor::GetProjectionMap(*op, child_index);
		D_ASSERT(projection_map);
		RemapProjectionMapStrict(*projection_map, old_child_bindings, new_child_bindings);
	}
	if (boundary_replacements.empty()) {
		return;
	}
	CorrelatedColumnBindingReplacer replacer;
	replacer.replacement_bindings = std::move(boundary_replacements);
	if (op->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN && child_index == 0) {
		replacer.stop_operator = *op->children[child_index];
		replacer.VisitOperator(*op);
	} else {
		replacer.VisitOperatorBindings(*op);
	}
}

} // namespace duckdb
