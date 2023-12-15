#include "duckdb/main/relation/create_table_relation.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

CreateTableRelation::CreateTableRelation(shared_ptr<Relation> child_p, string schema_name, string table_name)
    : Relation(child_p->context, RelationType::CREATE_TABLE_RELATION), child(std::move(child_p)),
      schema_name(std::move(schema_name)), table_name(std::move(table_name)) {
	context.GetContext()->TryBindRelation(*this, this->columns);
}

BoundStatement CreateTableRelation::Bind(Binder &binder) {
	auto select = make_uniq<SelectStatement>();
	select->node = child->GetQueryNode();

	CreateStatement stmt;
	auto info = make_uniq<CreateTableInfo>();
	info->schema = schema_name;
	info->table = table_name;
	info->query = std::move(select);
	info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
	stmt.info = std::move(info);
	return binder.Bind(stmt.Cast<SQLStatement>());
}

const vector<ColumnDefinition> &CreateTableRelation::Columns() {
	return columns;
}

string CreateTableRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Create Table\n";
	return str + child->ToString(depth + 1);
}

} // namespace duckdb
