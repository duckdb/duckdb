#include "duckdb/parser/parsed_data/create_index_type_info.hpp"

namespace duckdb {

CreateIndexTypeInfo::CreateIndexTypeInfo(string name_p, string schema)
    : CreateInfo(CatalogType::INDEX_TYPE_ENTRY, std::move(schema)), name(std::move(name_p)) {
	internal = true;
}

unique_ptr<CreateInfo> CreateIndexTypeInfo::Copy() const {

	auto result = make_uniq<CreateIndexTypeInfo>(*this);
	CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
