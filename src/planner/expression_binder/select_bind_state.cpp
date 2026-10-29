#include "duckdb/planner/expression_binder/select_bind_state.hpp"
#include "duckdb/common/exception/binder_exception.hpp"

namespace duckdb {

SelectBindState::SelectBindState(ExpressionMatchPolicy projection_policy)
    : projection_index(projection_policy, projection_policy),
      group_index(ExpressionMatchPolicy::REPLACE, ExpressionMatchPolicy::FIRST) {
}

void SelectBindState::AddProjection(Binder &binder, ParsedExpression &expr) {
	auto snapshot = ExpressionMatchDescriptor::CreateSnapshot(binder, expr);
	projection_index.AddExact(*snapshot, projections.size());
	projections.push_back({std::move(snapshot), nullptr});
}

void SelectBindState::CompleteProjection(idx_t index) {
	// ORDER BY can append expressions that are not exported as original projections.
	if (index >= projections.size()) {
		return;
	}
	auto &projection = projections[index];
	D_ASSERT(projection.snapshot);
	projection.descriptor = make_shared_ptr<ExpressionMatchDescriptor>(std::move(projection.snapshot));
}

idx_t SelectBindState::ProjectionCount() const {
	return projections.size();
}

const ParsedExpression &SelectBindState::ProjectionExpression(idx_t index) const {
	auto &projection = projections[index];
	return projection.descriptor ? projection.descriptor->OriginalExpression() : *projection.snapshot;
}

shared_ptr<const ExpressionMatchDescriptor> SelectBindState::ProjectionDescriptor(idx_t index) const {
	D_ASSERT(projections[index].descriptor);
	return projections[index].descriptor;
}

void SelectBindState::AddGroup(shared_ptr<const ExpressionMatchDescriptor> expression, ProjectionIndex index) {
	if (groups.size() == index.GetIndex()) {
		groups.push_back(expression);
	}
	D_ASSERT(index.GetIndex() < groups.size());
	group_index.Add(std::move(expression), index);
}

void SelectBindState::ApplyGroupBinding(const ParsedExpression &expr, ProjectionIndex index) const {
	groups[index]->ApplyBindingFacts(expr);
}

ProjectionIndex SelectBindState::FindGroup(const ParsedExpression &expr) const {
	auto match = group_index.Find(expr);
	D_ASSERT(!match.IsAmbiguous());
	return match.IsValid() ? ProjectionIndex(match.GetIndex()) : ProjectionIndex();
}

unique_ptr<ParsedExpression> SelectBindState::BindAlias(idx_t index) {
	if (volatile_expressions.find(index) != volatile_expressions.end()) {
		throw BinderException("Alias %s referenced - but the expression has side "
		                      "effects. This is not yet supported.",
		                      ProjectionExpression(index).GetAlias());
	}
	referenced_aliases.insert(index);
	return ProjectionExpression(index).Copy();
}

void SelectBindState::SetExpressionIsVolatile(idx_t index) {
	// check if this expression has been referenced before
	if (referenced_aliases.find(index) != referenced_aliases.end()) {
		throw BinderException("Alias %s referenced - but the expression has side "
		                      "effects. This is not yet supported.",
		                      ProjectionExpression(index).GetAlias());
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
