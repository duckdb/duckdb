#include "duckdb/main/relation/write_csv_relation.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

WriteCSVRelation::WriteCSVRelation(shared_ptr<Relation> child_p, string csv_file_p,
                                   case_insensitive_map_t<vector<Value>> options_p)
    : Relation(child_p->context, RelationType::WRITE_CSV_RELATION), child(std::move(child_p)),
      csv_file(std::move(csv_file_p)), options(std::move(options_p)) {
	TryBindRelation(columns);
}

BoundStatement WriteCSVRelation::Bind(Binder &binder) {
	CopyStatement copy;
	auto info = make_uniq<CopyInfo>();
	info->select_statement = child->GetQueryNode();
	info->is_from = false;
	info->file_path = csv_file;
	info->format = "csv";
	info->options = options;
	copy.info = std::move(info);
	return binder.Bind(copy.Cast<SQLStatement>());
}

const vector<ColumnDefinition> &WriteCSVRelation::Columns() {
	return columns;
}

string WriteCSVRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Write To CSV [" + csv_file + "]\n";
	return str + child->ToString(depth + 1);
}

} // namespace duckdb
