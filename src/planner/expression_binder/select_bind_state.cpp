#include "duckdb/planner/expression_binder/select_bind_state.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/common/exception/binder_exception.hpp"

namespace duckdb {

static void NormalizeCaseGroups(unique_ptr<ParsedExpression> &expr) {
	if (expr->GetExpressionClass() == ExpressionClass::CASE) {
		auto &case_expr = expr->Cast<CaseExpression>();
		if (case_expr.CaseOperand()) {
			expr = case_expr.GetLegacyCaseExpression();
		}
	}
	for (auto &child : expr->ChildrenMutable()) {
		NormalizeCaseGroups(child);
	}
}

static bool ContainsLambdaReference(const ParsedExpression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::LAMBDA_REF) {
		return true;
	}
	for (auto &child : expr.Children()) {
		if (ContainsLambdaReference(child)) {
			return true;
		}
	}
	return false;
}

static bool ContainsSimpleCase(const ParsedExpression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::CASE && expr.Cast<CaseExpression>().CaseOperand()) {
		return true;
	}
	for (auto &child : expr.Children()) {
		if (ContainsSimpleCase(child)) {
			return true;
		}
	}
	return false;
}

void SelectBindState::AddCaseGroup(const ParsedExpression &expr, ProjectionIndex index) {
	if (!ContainsSimpleCase(expr)) {
		case_group_map.emplace(expr, index);
		return;
	}
	auto normalized = expr.Copy();
	NormalizeCaseGroups(normalized);
	case_group_map.emplace(*normalized, index);
	case_group_expressions.push_back(std::move(normalized));
}

ProjectionIndex SelectBindState::TryBindCaseGroup(const ParsedExpression &expr) const {
	if (case_group_map.empty() || ContainsLambdaReference(expr)) {
		return ProjectionIndex();
	}
	auto entry = case_group_map.find(expr);
	if (entry != case_group_map.end()) {
		return entry->second;
	}
	if (!ContainsSimpleCase(expr)) {
		return ProjectionIndex();
	}
	auto normalized = expr.Copy();
	NormalizeCaseGroups(normalized);
	entry = case_group_map.find(*normalized);
	return entry == case_group_map.end() ? ProjectionIndex() : entry->second;
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
