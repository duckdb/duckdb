#include "duckdb/main/relation/explain_relation.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

ExplainRelation::ExplainRelation(shared_ptr<Relation> child_p, ExplainType type)
    : Relation(child_p->context, RelationType::EXPLAIN_RELATION), child(std::move(child_p)), type(type) {
	context.GetContext()->TryBindRelation(*this, this->columns);
}

BoundStatement ExplainRelation::Bind(Binder &binder) {
	auto select = make_uniq<SelectStatement>();
	select->node = child->GetQueryNode();
	ExplainStatement explain(std::move(select), type);
	return binder.Bind(explain.Cast<SQLStatement>());
}

const vector<ColumnDefinition> &ExplainRelation::Columns() {
	return columns;
}

string ExplainRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Explain\n";
	return str + child->ToString(depth + 1);
}

} // namespace duckdb
