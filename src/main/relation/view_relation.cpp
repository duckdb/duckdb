#include "duckdb/main/relation/view_relation.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

ViewRelation::ViewRelation(const shared_ptr<ClientContext> &context, string schema_name_p, string view_name_p)
    : Relation(context, RelationType::VIEW_RELATION), schema_name(std::move(schema_name_p)),
      view_name(std::move(view_name_p)) {
	context->TryBindRelation(*this, this->columns);
}

ViewRelation::ViewRelation(const shared_ptr<ClientContext> &context, unique_ptr<TableRef> ref, const string &view_name)
    : Relation(context, RelationType::VIEW_RELATION), view_name(view_name), premade_tableref(std::move(ref)) {
	context->TryBindRelation(*this, this->columns);
	premade_tableref->alias = view_name;
}

unique_ptr<QueryNode> ViewRelation::GetQueryNode() {
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = GetTableRef();
	return std::move(result);
}

unique_ptr<TableRef> ViewRelation::GetTableRef() {
	if (premade_tableref) {
		return premade_tableref->Copy();
	}
	auto table_ref = make_uniq<BaseTableRef>();
	table_ref->schema_name = schema_name;
	table_ref->table_name = view_name;
	return std::move(table_ref);
}

string ViewRelation::GetAlias() {
	D_ASSERT(!view_name.empty());
	return view_name;
}

const vector<ColumnDefinition> &ViewRelation::Columns() {
	return columns;
}

string ViewRelation::ToString(idx_t depth) {
	return RenderWhitespace(depth) + "View [" + view_name + "]";
}

} // namespace duckdb
