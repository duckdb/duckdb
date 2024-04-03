#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CreateSequenceInfo::CreateSequenceInfo()
    : CreateInfo(CatalogType::SEQUENCE_ENTRY, INVALID_SCHEMA), name(string()), usage_count(0), increment(1),
      min_value(1), max_value(NumericLimits<int64_t>::Maximum()), start_value(1), cycle(false) {
}

unique_ptr<CreateInfo> CreateSequenceInfo::Copy() const {
	auto result = make_uniq<CreateSequenceInfo>();
	CopyProperties(*result);
	result->name = name;
	result->schema = schema;
	result->usage_count = usage_count;
	result->increment = increment;
	result->min_value = min_value;
	result->max_value = max_value;
	result->start_value = start_value;
	result->cycle = cycle;
	return std::move(result);
}

} // namespace duckdb
