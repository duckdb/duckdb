#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/catalog/dependency_manager.hpp"

#include <algorithm>
#include <sstream>

namespace duckdb {

SequenceCatalogEntry::SequenceCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateSequenceInfo *info)
    : StandardEntry(CatalogType::SEQUENCE_ENTRY, schema, catalog, info->name), usage_count(info->usage_count),
      counter(info->start_value), increment(info->increment), start_value(info->start_value),
      min_value(info->min_value), max_value(info->max_value), cycle(info->cycle) {
	this->temporary = info->temporary;
}

void SequenceCatalogEntry::Serialize(Serializer &serializer) {
	FieldWriter writer(serializer);
	writer.WriteString(schema->name);
	writer.WriteString(name);
	writer.WriteField<uint64_t>(usage_count);
	writer.WriteField<int64_t>(increment);
	writer.WriteField<int64_t>(min_value);
	writer.WriteField<int64_t>(max_value);
	writer.WriteField<int64_t>(counter);
	writer.WriteField<bool>(cycle);
	writer.Finalize();
}

unique_ptr<CreateSequenceInfo> SequenceCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateSequenceInfo>();

	FieldReader reader(source);
	info->schema = reader.ReadRequired<string>();
	info->name = reader.ReadRequired<string>();
	info->usage_count = reader.ReadRequired<uint64_t>();
	info->increment = reader.ReadRequired<int64_t>();
	info->min_value = reader.ReadRequired<int64_t>();
	info->max_value = reader.ReadRequired<int64_t>();
	info->start_value = reader.ReadRequired<int64_t>();
	info->cycle = reader.ReadRequired<bool>();
	reader.Finalize();

	return info;
}

string SequenceCatalogEntry::ToSQL() {
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
