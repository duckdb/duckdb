#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/tableref/explain_ref.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

BoundStatement Binder::Bind(ExplainRef &ref) {
	// EXPLAIN output is rendered during planning and must reflect the current parameter values.
	SetAlwaysRequireRebind();
	auto child_binder = Binder::CreateBinder(context, this);
	auto select = make_uniq<SelectStatement>();
	select->node = ref.query->Copy();
	ExplainStatement explain(std::move(select), ExplainType::EXPLAIN_STANDARD, ProfilerPrintFormat(ref.format));
	auto result = child_binder->Bind(explain);
	if (!child_binder->correlated_columns.empty()) {
		throw BinderException("Correlated EXPLAIN subqueries are not supported");
	}
	bind_context.AddGenericBinding(result.plan->GetRootIndex(), "__explain", result.names, result.types);
	return result;
}

} // namespace duckdb
