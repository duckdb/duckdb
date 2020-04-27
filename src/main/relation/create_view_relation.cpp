#include "duckdb/main/relation/create_view_relation.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

CreateViewRelation::CreateViewRelation(shared_ptr<Relation> child_p, string view_name, bool replace)
    : Relation(child_p->context, RelationType::CREATE_VIEW_RELATION), child(move(child_p)), view_name(move(view_name)),
      replace(replace) {
	context.TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> CreateViewRelation::GetQueryNode() {
	throw InternalException("Cannot create a query node from a CreateViewRelation!");
}

BoundStatement CreateViewRelation::Bind(Binder &binder) {
	CreateStatement stmt;
	auto info = make_unique<CreateViewInfo>();
	info->query = child->GetQueryNode();
	info->view_name = view_name;
	info->on_conflict = replace ? OnCreateConflict::REPLACE : OnCreateConflict::ERROR;
	stmt.info = move(info);
	return binder.Bind((SQLStatement &)stmt);
}

const vector<ColumnDefinition> &CreateViewRelation::Columns() {
	return columns;
}

string CreateViewRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Create View\n";
	return str + child->ToString(depth + 1);
}

} // namespace duckdb
