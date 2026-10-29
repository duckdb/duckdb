#include "duckdb/planner/expression_binder/select_bind_state.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/common/exception/binder_exception.hpp"

namespace duckdb {

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

void SelectBindState::AddCaseGroup(const ParsedExpression &expr, ProjectionIndex index) {
	case_group_map.Add(expr, index);
}

ProjectionIndex SelectBindState::TryBindCaseGroup(const ParsedExpression &expr) const {
	auto index = case_group_map.Find(expr);
	return index ? ProjectionIndex(*index) : ProjectionIndex();
}

bool SelectBindState::ExpressionIsVolatile(idx_t index) const {
	return volatile_expressions.find(index) != volatile_expressions.end();
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
