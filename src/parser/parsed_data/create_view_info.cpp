#include "duckdb/parser/parsed_data/create_view_info.hpp"

namespace duckdb {
unique_ptr<CreateInfo> CreateViewInfo::Copy() const {
	auto result = make_unique<CreateViewInfo>(schema, view_name);
	CopyProperties(*result);
	result->aliases = aliases;
	result->types = types;
	result->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
	return move(result);
}
} // namespace duckdb
