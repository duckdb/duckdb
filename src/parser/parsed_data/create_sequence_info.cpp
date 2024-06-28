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

string CreateSequenceInfo::ToString() const {
	std::stringstream ss;
	ss << "CREATE";
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		ss << " OR REPLACE";
	}
	if (temporary) {
		ss << " TEMPORARY";
	}
	ss << " SEQUENCE ";
	if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		ss << " IF NOT EXISTS ";
	}
	ss << QualifierToString(temporary ? "" : catalog, schema, name);
	ss << " INCREMENT BY " << increment;
	ss << " MINVALUE " << min_value;
	ss << " MAXVALUE " << max_value;
	ss << " START " << start_value;
	ss << " " << (cycle ? "CYCLE" : "NO CYCLE") << ";";
	return ss.str();
}

} // namespace duckdb
