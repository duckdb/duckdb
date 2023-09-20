#include "duckdb/parser/parsed_data/create_index_info.hpp"

namespace duckdb {

unique_ptr<CreateInfo> CreateIndexInfo::Copy() const {
	auto result = make_uniq<CreateIndexInfo>();
	CopyProperties(*result);

	result->table = table;
	result->name = name;

	result->options = options;

	result->index_type = index_type;
	result->index_constraint_type = index_constraint_type;
	result->column_ids = column_ids;

	for (auto &expr : expressions) {
		result->expressions.push_back(expr->Copy());
	}
	for (auto &expr : parsed_expressions) {
		result->parsed_expressions.push_back(expr->Copy());
	}

	result->scan_types = scan_types;
	result->names = names;

	return std::move(result);
}

} // namespace duckdb
