#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

#include <algorithm>
#include <sstream>

namespace duckdb {

SequenceData::SequenceData(CreateSequenceInfo &info)
    : usage_count(info.usage_count), counter(info.start_value), last_value(info.start_value), increment(info.increment),
      start_value(info.start_value), min_value(info.min_value), max_value(info.max_value), cycle(info.cycle) {
}

SequenceCatalogEntry::SequenceCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateSequenceInfo &info)
    : StandardEntry(CatalogType::SEQUENCE_ENTRY, schema, catalog, info.name), data(info) {
	this->temporary = info.temporary;
	this->comment = info.comment;
	this->tags = info.tags;
}

unique_ptr<CatalogEntry> SequenceCatalogEntry::Copy(ClientContext &context) const {
	auto info_copy = GetInfo();
	auto &cast_info = info_copy->Cast<CreateSequenceInfo>();

	auto result = make_uniq<SequenceCatalogEntry>(catalog, schema, cast_info);
	result->data = GetData();

	return std::move(result);
}

SequenceData SequenceCatalogEntry::GetData() const {
	lock_guard<mutex> seqlock(lock);
	return data;
}

int64_t SequenceCatalogEntry::CurrentValue() {
	lock_guard<mutex> seqlock(lock);
	int64_t result;
	if (data.usage_count == 0u) {
		throw SequenceException("currval: sequence is not yet defined in this session");
	}
	result = data.last_value;
	return result;
}

int64_t SequenceCatalogEntry::NextValue(DuckTransaction &transaction) {
	lock_guard<mutex> seqlock(lock);
	int64_t result;
	result = data.counter;
	bool overflow = !TryAddOperator::Operation(data.counter, data.increment, data.counter);
	if (data.cycle) {
		if (overflow) {
			data.counter = data.increment < 0 ? data.max_value : data.min_value;
		} else if (data.counter < data.min_value) {
			data.counter = data.max_value;
		} else if (data.counter > data.max_value) {
			data.counter = data.min_value;
		}
	} else {
		if (result < data.min_value || (overflow && data.increment < 0)) {
			throw SequenceException("nextval: reached minimum value of sequence \"%s\" (%lld)", name, data.min_value);
		}
		if (result > data.max_value || overflow) {
			throw SequenceException("nextval: reached maximum value of sequence \"%s\" (%lld)", name, data.max_value);
		}
	}
	data.last_value = result;
	data.usage_count++;
	if (!temporary) {
		transaction.PushSequenceUsage(*this, data);
	}
	return result;
}

void SequenceCatalogEntry::ReplayValue(uint64_t v_usage_count, int64_t v_counter) {
	if (v_usage_count > data.usage_count) {
		data.usage_count = v_usage_count;
		data.counter = v_counter;
	}
}

unique_ptr<CreateInfo> SequenceCatalogEntry::GetInfo() const {
	auto seq_data = GetData();

	auto result = make_uniq<CreateSequenceInfo>();
	result->catalog = catalog.GetName();
	result->schema = schema.name;
	result->name = name;
	result->usage_count = seq_data.usage_count;
	result->increment = seq_data.increment;
	result->min_value = seq_data.min_value;
	result->max_value = seq_data.max_value;
	result->start_value = seq_data.counter;
	result->cycle = seq_data.cycle;
	result->dependencies = dependencies;
	result->comment = comment;
	result->tags = tags;
	return std::move(result);
}

string SequenceCatalogEntry::ToSQL() const {
	auto seq_data = GetData();

	std::stringstream ss;
	ss << "CREATE SEQUENCE ";
	ss << name;
	ss << " INCREMENT BY " << seq_data.increment;
	ss << " MINVALUE " << seq_data.min_value;
	ss << " MAXVALUE " << seq_data.max_value;
	ss << " START " << seq_data.counter;
	ss << " " << (seq_data.cycle ? "CYCLE" : "NO CYCLE") << ";";
	return ss.str();
}
} // namespace duckdb
