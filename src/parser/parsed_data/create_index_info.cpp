#include "duckdb/parser/parsed_data/create_index_info.hpp"

namespace duckdb {

CreateIndexInfo::CreateIndexInfo() : CreateInfo(CatalogType::INDEX_ENTRY) {
}

CreateIndexInfo::CreateIndexInfo(const duckdb::CreateIndexInfo &info)
    : CreateInfo(CatalogType::INDEX_ENTRY), table(info.table), index_name(info.index_name), options(info.options),
      index_type(info.index_type), constraint_type(info.constraint_type), column_ids(info.column_ids),
      scan_types(info.scan_types), names(info.names) {
}

unique_ptr<CreateInfo> CreateIndexInfo::Copy() const {

	auto result = make_uniq<CreateIndexInfo>(*this);
	CopyProperties(*result);

	for (auto &expr : expressions) {
		result->expressions.push_back(expr->Copy());
	}
	for (auto &expr : parsed_expressions) {
		result->parsed_expressions.push_back(expr->Copy());
	}

	return std::move(result);
}

} // namespace duckdb
