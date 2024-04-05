#include "duckdb/planner/expression_binder/select_bind_state.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> SelectBindState::BindAlias(idx_t index) {
	if (volatile_expressions.find(index) != volatile_expressions.end()) {
		throw BinderException("Alias \"%s\" referenced - but the expression has side "
		                      "effects. This is not yet supported.",
		                      original_expressions[index]->alias);
	}
	referenced_aliases.insert(index);
	return original_expressions[index]->Copy();
}

void SelectBindState::SetExpressionIsVolatile(idx_t index) {
	// check if this expression has been referenced before
	if (referenced_aliases.find(index) != referenced_aliases.end()) {
		throw BinderException("Alias \"%s\" referenced - but the expression has side "
		                      "effects. This is not yet supported.",
		                      original_expressions[index]->alias);
	}
	volatile_expressions.insert(index);
}

void SelectBindState::SetExpressionHasSubquery(idx_t index) {
	subquery_expressions.insert(index);
}

bool SelectBindState::AliasHasSubquery(idx_t index) {
	return subquery_expressions.find(index) != subquery_expressions.end();
}

void SelectBindState::AddExpandedColumn(idx_t expand_count) {

}

void SelectBindState::AddRegularColumn() {

}

} // namespace duckdb
