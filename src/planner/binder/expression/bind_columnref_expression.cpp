#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {
using namespace std;

BindResult ExpressionBinder::BindExpression(ColumnRefExpression &colref, idx_t depth) {
	D_ASSERT(!colref.column_name.empty());
	// individual column reference
	// resolve to either a base table or a subquery expression
	if (colref.table_name.empty()) {
		// no table name: find a binding that contains this
		if (binder.macro_binding != nullptr && binder.macro_binding->HasMatchingBinding(colref.column_name)) {
			// priority to macro parameter bindings TODO: throw a warning when this name conflicts
			colref.table_name = binder.macro_binding->alias;
		} else {
			colref.table_name = binder.bind_context.GetMatchingBinding(colref.column_name);
		}
		if (colref.table_name.empty()) {
			auto similar_bindings = binder.bind_context.GetSimilarBindings(colref.column_name);
			string candidate_str = StringUtil::CandidatesMessage(similar_bindings, "Candidate bindings");
			return BindResult(
			    binder.FormatError(colref, StringUtil::Format("Referenced column \"%s\" not found in FROM clause!%s",
			                                                  colref.column_name.c_str(), candidate_str)));
		}
	}
	// if it was a macro parameter, let macro_binding bind it to the argument
	BindResult result = binder.macro_binding != nullptr && colref.table_name == binder.macro_binding->alias
	                        ? binder.macro_binding->Bind(colref, depth)
	                        : binder.bind_context.BindColumn(colref, depth);
	if (!result.HasError()) {
		bound_columns = true;
	} else {
		result.error = binder.FormatError(colref, result.error);
	}
	return result;
}

} // namespace duckdb
