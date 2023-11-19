#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/catalog/dependency_manager.hpp"

#include <algorithm>
#include <sstream>

namespace duckdb {

SequenceCatalogEntry::SequenceCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateSequenceInfo &info)
    : StandardEntry(CatalogType::SEQUENCE_ENTRY, schema, catalog, info.name), usage_count(info.usage_count),
      counter(info.start_value), increment(info.increment), start_value(info.start_value), min_value(info.min_value),
      max_value(info.max_value), cycle(info.cycle) {
	this->temporary = info.temporary;
}

unique_ptr<CreateInfo> SequenceCatalogEntry::GetInfo() const {
	auto result = make_uniq<CreateSequenceInfo>();
	result->schema = schema.name;
	result->name = name;
	result->usage_count = usage_count;
	result->increment = increment;
	result->min_value = min_value;
	result->max_value = max_value;
	result->start_value = counter;
	result->cycle = cycle;
	return std::move(result);
}

string SequenceCatalogEntry::ToSQL() const {
	std::stringstream ss;
	ss << "CREATE SEQUENCE ";
	ss << name;
	ss << " INCREMENT BY " << increment;
	ss << " MINVALUE " << min_value;
	ss << " MAXVALUE " << max_value;
	ss << " START " << counter;
	ss << " " << (cycle ? "CYCLE" : "NO CYCLE") << ";";
	return ss.str();
}
} // namespace duckdb
