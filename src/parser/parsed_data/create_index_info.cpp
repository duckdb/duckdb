#include "duckdb/parser/parsed_data/create_index_info.hpp"

namespace duckdb {

unique_ptr<CreateInfo> CreateIndexInfo::Copy() const {
	auto result = make_uniq<CreateIndexInfo>();
	CopyProperties(*result);

	result->index_type = index_type;
	result->index_name = index_name;
	result->constraint_type = constraint_type;
	result->table = table;
	for (auto &expr : expressions) {
		result->expressions.push_back(expr->Copy());
	}
	for (auto &expr : parsed_expressions) {
		result->parsed_expressions.push_back(expr->Copy());
	}

	result->scan_types = scan_types;
	result->names = names;
	result->column_ids = column_ids;
	result->options = options;
	return std::move(result);
}

} // namespace duckdb
