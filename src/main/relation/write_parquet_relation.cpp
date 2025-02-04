#include "duckdb/main/relation/write_parquet_relation.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

WriteParquetRelation::WriteParquetRelation(shared_ptr<Relation> child_p, string parquet_file_p,
                                           case_insensitive_map_t<vector<Value>> options_p)
    : Relation(child_p->context, RelationType::WRITE_PARQUET_RELATION), child(std::move(child_p)),
      parquet_file(std::move(parquet_file_p)), options(std::move(options_p)) {
	TryBindRelation(columns);
}

BoundStatement WriteParquetRelation::Bind(Binder &binder) {
	CopyStatement copy;
	auto info = make_uniq<CopyInfo>();
	info->select_statement = child->GetQueryNode();
	info->is_from = false;
	info->file_path = parquet_file;
	info->format = "parquet";
	info->options = options;
	copy.info = std::move(info);
	return binder.Bind(copy.Cast<SQLStatement>());
}

const vector<ColumnDefinition> &WriteParquetRelation::Columns() {
	return columns;
}

string WriteParquetRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Write To Parquet [" + parquet_file + "]\n";
	return str + child->ToString(depth + 1);
}

} // namespace duckdb
