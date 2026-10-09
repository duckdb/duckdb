#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

#include <algorithm>
#include <sstream>

namespace duckdb {

constexpr const char *SequenceCatalogEntry::Name;

SequenceData::SequenceData(CreateSequenceInfo &info)
    : usage_count(info.usage_count), counter(info.start_value), last_value(info.last_value), increment(info.increment),
      start_value(info.start_value), min_value(info.min_value), max_value(info.max_value), cycle(info.cycle),
      exhausted(info.exhausted) {
}

SequenceCatalogEntry::SequenceCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateSequenceInfo &info)
    : StandardEntry(CatalogType::SEQUENCE_ENTRY, schema, catalog, info.GetSequenceName()), data(info) {
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
	if (!data.last_value) {
		throw SequenceException("currval: sequence is not yet defined in this session");
	}
	result = data.last_value.value();
	return result;
}

int64_t SequenceCatalogEntry::NextValue(DuckTransaction &transaction) {
	lock_guard<mutex> seqlock(lock);
	int64_t result;
	result = data.counter;
	int64_t next_counter;
	bool overflow = !TryAddOperator::Operation(data.counter, data.increment, next_counter);
	if (data.cycle) {
		if (overflow) {
			next_counter = data.increment < 0 ? data.max_value : data.min_value;
		} else if (next_counter < data.min_value) {
			next_counter = data.max_value;
		} else if (next_counter > data.max_value) {
			next_counter = data.min_value;
		}
	} else {
		// storage before v2.0 cannot persist exhausted, so stop one value early there
		bool exhausted = data.exhausted || (overflow && !CanPersistExhausted());
		if (result < data.min_value || (exhausted && data.increment < 0)) {
			throw SequenceException("nextval: reached minimum value of sequence %s (%lld)", name, data.min_value);
		}
		if (result > data.max_value || exhausted) {
			throw SequenceException("nextval: reached maximum value of sequence \"%s\" (%lld)", name, data.max_value);
		}
		if (overflow) {
			// the next value does not fit in int64, so mark the sequence as exhausted
			next_counter = result;
			data.exhausted = true;
		}
	}
	data.counter = next_counter;
	data.last_value = result;
	data.usage_count++;
	if (!temporary) {
		transaction.PushSequenceUsage(*this, data);
	}
	return result;
}

bool SequenceCatalogEntry::CanPersistExhausted() const {
	if (temporary) {
		return true;
	}
	auto &storage = catalog.GetAttached().GetStorageManager();
	return storage.GetStorageVersion() >= StorageVersion::V2_0_0;
}

int64_t SequenceCatalogEntry::SetValue(DuckTransaction &transaction, int64_t value, bool is_called) {
	{
		lock_guard<mutex> seqlock(lock);
		if (value < data.min_value || value > data.max_value) {
			throw SequenceException("setval: value %lld is out of bounds for sequence %s (%lld..%lld)", value, name,
			                        data.min_value, data.max_value);
		}

		data.exhausted = false;
		data.counter = value;
		if (!is_called) {
			data.usage_count++;
			if (!temporary) {
				transaction.PushSequenceUsage(*this, data);
			}
			return value;
		}
	}

	// is_called: behave as if nextval() was just invoked and returned `value`.
	return NextValue(transaction);
}

void SequenceCatalogEntry::ReplayValue(uint64_t v_usage_count, int64_t v_counter, optional<int64_t> last_value,
                                       bool exhausted) {
	if (v_usage_count > data.usage_count) {
		data.usage_count = v_usage_count;
		data.counter = v_counter;
		data.last_value = last_value;
		data.exhausted = exhausted;
	}
}

unique_ptr<CreateInfo> SequenceCatalogEntry::GetInfo() const {
	auto seq_data = GetData();

	auto result = make_uniq<CreateSequenceInfo>();
	result->SetQualifiedName(schema.GetQualifiedName(name));
	result->usage_count = seq_data.usage_count;
	result->increment = seq_data.increment;
	result->min_value = seq_data.min_value;
	result->max_value = seq_data.max_value;
	result->start_value = seq_data.counter;
	result->cycle = seq_data.cycle;
	result->last_value = seq_data.last_value;
	result->dependencies = dependencies;
	result->comment = comment;
	result->tags = tags;
	result->exhausted = seq_data.exhausted;
	return std::move(result);
}

string SequenceCatalogEntry::ToSQL() const {
	auto seq_data = GetData();

	duckdb::stringstream ss;
	ss << "CREATE SEQUENCE ";
	ss << name.GetIdentifierName();
	ss << " INCREMENT BY " << seq_data.increment;
	ss << " MINVALUE " << seq_data.min_value;
	ss << " MAXVALUE " << seq_data.max_value;
	ss << " START " << seq_data.counter;
	ss << " " << (seq_data.cycle ? "CYCLE" : "NO CYCLE") << ";";
	return ss.str();
}
} // namespace duckdb
