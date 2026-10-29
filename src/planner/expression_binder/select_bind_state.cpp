#include "duckdb/planner/expression_binder/select_bind_state.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

static optional_ptr<const Expression> GetRegisteredExpression(const Expression &expr, const BoundSelectNode &node) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}
	auto &binding = expr.Cast<BoundColumnRefExpression>().Binding();
	if (binding.table_index == node.group_index) {
		return node.groups.group_expressions[binding.column_index].get();
	}
	if (binding.table_index == node.aggregate_index) {
		return node.aggregates[binding.column_index].get();
	}
	if (binding.table_index == node.window_index) {
		return node.windows[binding.column_index].get();
	}
	for (auto &unnests : {reference<const BoundUnnestMap>(node.unnests.SelectList()),
	                      reference<const BoundUnnestMap>(node.unnests.GroupBy())}) {
		for (auto &entry : unnests.get()) {
			if (binding.table_index == entry.second.index) {
				return entry.second.expressions[binding.column_index].get();
			}
		}
	}
	return nullptr;
}

bool CaseExpressionMap::HasVolatileSource(const Expression &expr, const BoundSelectNode &node) {
	if (expr.IsVolatile()) {
		return true;
	}
	auto source = GetRegisteredExpression(expr, node);
	if (source) {
		return HasVolatileSource(*source, node);
	}
	bool is_volatile = false;
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { is_volatile |= HasVolatileSource(child, node); });
	return is_volatile;
}

bool CaseExpressionMap::IsCompatible(const Expression &expr, const BoundSelectNode &node) {
	auto source = GetRegisteredExpression(expr, node);
	if (source) {
		return IsCompatible(*source, node);
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CASE) {
		auto &checks = expr.Cast<BoundCaseExpression>().CaseChecks();
		// Normalization repeats the comparison operands, but preserves result-arm evaluation.
		if (checks.size() > 1) {
			for (auto &check : checks) {
				if (check.when_expr->GetExpressionType() == ExpressionType::COMPARE_EQUAL) {
					auto &comparison = check.when_expr->Cast<BoundFunctionExpression>();
					if (HasVolatileSource(BoundComparisonExpression::Left(comparison), node)) {
						return false;
					}
				}
			}
		}
	} else if (expr.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY ||
	           (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
	            expr.Cast<BoundFunctionExpression>().Function().HasBindLambdaCallback())) {
		// Keep opaque query/lambda bodies conservative, including volatile CASE lowered to invoke.
		if (HasVolatileSource(expr, node)) {
			return false;
		}
	}
	bool compatible = true;
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](const Expression &child) { compatible &= IsCompatible(child, node); });
	return compatible;
}

void CaseExpressionMap::Add(const ParsedExpression &expr, idx_t index, bool check_ambiguity) {
	if (CaseExpression::ContainsLambdaReference(expr)) {
		return;
	}
	auto normalized = CaseExpression::ContainsSimpleCase(expr) ? CaseExpression::Normalize(expr) : nullptr;
	auto entry = entries.emplace(normalized ? *normalized : expr, index);
	if (entry.second && normalized) {
		expressions.push_back(std::move(normalized));
	} else if (check_ambiguity && entry.first->second != index) {
		entry.first->second = DConstants::INVALID_INDEX;
	}
}

optional_ptr<const idx_t> CaseExpressionMap::Find(const ParsedExpression &expr) const {
	if (entries.empty() || CaseExpression::ContainsLambdaReference(expr)) {
		return nullptr;
	}
	auto entry = entries.find(expr);
	if (entry != entries.end()) {
		return &entry->second;
	}
	if (!CaseExpression::ContainsSimpleCase(expr)) {
		return nullptr;
	}
	auto normalized = CaseExpression::Normalize(expr);
	entry = entries.find(*normalized);
	return entry == entries.end() ? nullptr : &entry->second;
}

void SelectBindState::RegisterCaseGroup(const ParsedExpression &expr, ProjectionIndex index, bool case_compatible) {
	if (case_compatible) {
		case_group_map.Add(expr, index);
	}
}

ProjectionIndex SelectBindState::TryBindCaseGroup(const ParsedExpression &expr) const {
	auto index = case_group_map.Find(expr);
	return index ? ProjectionIndex(*index) : ProjectionIndex();
}

void SelectBindState::SetExpressionIsCaseCompatible(idx_t index) {
	case_compatible_expressions.insert(index);
}

bool SelectBindState::ExpressionIsCaseCompatible(idx_t index) const {
	return case_compatible_expressions.find(index) != case_compatible_expressions.end();
}

unique_ptr<ParsedExpression> SelectBindState::BindAlias(idx_t index) {
	if (volatile_expressions.find(index) != volatile_expressions.end()) {
		throw BinderException("Alias %s referenced - but the expression has side "
		                      "effects. This is not yet supported.",
		                      original_expressions[index]->GetAlias());
	}
	referenced_aliases.insert(index);
	return original_expressions[index]->Copy();
}

void SelectBindState::SetExpressionIsVolatile(idx_t index) {
	// check if this expression has been referenced before
	if (referenced_aliases.find(index) != referenced_aliases.end()) {
		throw BinderException("Alias %s referenced - but the expression has side "
		                      "effects. This is not yet supported.",
		                      original_expressions[index]->GetAlias());
	}
	volatile_expressions.insert(index);
}

void SelectBindState::SetExpressionHasSubquery(idx_t index) {
	subquery_expressions.insert(index);
}

bool SelectBindState::AliasHasSubquery(idx_t index) const {
	return subquery_expressions.find(index) != subquery_expressions.end();
}

void SelectBindState::AddExpandedColumn(idx_t expand_count) {
	if (expanded_column_indices.empty()) {
		expanded_column_indices.emplace_back(0);
	}
	expanded_column_indices.push_back(ProjectionIndex(expanded_column_indices.back() + expand_count));
}

void SelectBindState::AddRegularColumn() {
	AddExpandedColumn(1);
}

ProjectionIndex SelectBindState::GetFinalIndex(idx_t index) const {
	if (index >= expanded_column_indices.size()) {
		return ProjectionIndex(index);
	}
	return expanded_column_indices[index];
}

} // namespace duckdb
