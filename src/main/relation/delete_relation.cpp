#include "duckdb/main/relation/delete_relation.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

DeleteRelation::DeleteRelation(ClientContextWrapper &context, unique_ptr<ParsedExpression> condition_p,
                               string schema_name_p, string table_name_p)
    : Relation(context, RelationType::DELETE_RELATION), condition(std::move(condition_p)),
      schema_name(std::move(schema_name_p)), table_name(std::move(table_name_p)) {
	context.GetContext()->TryBindRelation(*this, this->columns);
}

BoundStatement DeleteRelation::Bind(Binder &binder) {
	auto basetable = make_uniq<BaseTableRef>();
	basetable->schema_name = schema_name;
	basetable->table_name = table_name;

	DeleteStatement stmt;
	stmt.condition = condition ? condition->Copy() : nullptr;
	stmt.table = std::move(basetable);
	return binder.Bind(stmt.Cast<SQLStatement>());
}

const vector<ColumnDefinition> &DeleteRelation::Columns() {
	return columns;
}

string DeleteRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "DELETE FROM " + table_name;
	if (condition) {
		str += " WHERE " + condition->ToString();
	}
	return str;
}

} // namespace duckdb
