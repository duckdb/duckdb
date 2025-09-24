#include "duckdb/parser/tableref/column_data_ref.hpp"
#include "duckdb/common/string_util.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

ColumnDataRef::ColumnDataRef(optionally_owned_ptr<ColumnDataCollection> collection_p, vector<string> expected_names)
    : TableRef(TableReferenceType::COLUMN_DATA), expected_names(std::move(expected_names)),
      collection(std::move(collection_p)) {
}

ColumnDataRef::ColumnDataRef(shared_ptr<ManagedQueryResult> managed_result_p, vector<string> expected_names)
    : TableRef(TableReferenceType::COLUMN_DATA), expected_names(std::move(expected_names)),
      collection(managed_result_p->Collection()), managed_result(std::move(managed_result_p)) {
}

optionally_owned_ptr<ColumnDataCollection> &ColumnDataRef::Collection() {
	if (managed_result) {
		// This checks if it's still valid
		managed_result->Collection();
	}
	return collection;
}

const optionally_owned_ptr<ColumnDataCollection> &ColumnDataRef::Collection() const {
	if (managed_result) {
		// This checks if it's still valid
		managed_result->Collection();
	}
	return collection;
}

string ColumnDataRef::ToString() const {
	auto result = Collection()->ToString();
	return BaseToString(result, expected_names);
}

bool ColumnDataRef::Equals(const TableRef &other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<ColumnDataRef>();
	auto expected_types = Collection()->Types();
	auto other_expected_types = other.Collection()->Types();
	if (expected_types.size() != other_expected_types.size()) {
		return false;
	}
	if (expected_names.size() != other.expected_names.size()) {
		return false;
	}
	D_ASSERT(expected_types.size() == expected_names.size());
	for (idx_t i = 0; i < expected_types.size(); i++) {
		auto &this_type = expected_types[i];
		auto &other_type = other_expected_types[i];

		auto &this_name = expected_names[i];
		auto &other_name = other.expected_names[i];

		if (this_type != other_type) {
			return false;
		}
		if (!StringUtil::CIEquals(this_name, other_name)) {
			return false;
		}
	}
	string unused;
	if (!ColumnDataCollection::ResultEquals(*Collection(), *other.Collection(), unused, true)) {
		return false;
	}
	return true;
}

optionally_owned_ptr<ColumnDataCollection> CopyCollection(optionally_owned_ptr<ColumnDataCollection> &collection) {
	auto &unique = collection.get_owned_unique();
	if (unique) {
		// uniquely owned - need to copy over all the data and make a new collection
		auto new_collection = make_uniq<ColumnDataCollection>(collection->GetAllocator(), collection->Types());
		for (auto &chunk : collection->Chunks()) {
			new_collection->Append(chunk);
		}
		return std::move(new_collection);
	}
	auto &shared = collection.get_owned_shared();
	if (shared) {
		// shared ptr - we can directly reference it
		return shared;
	}
	// unowned collection - just return the raw reference
	return collection.get();
}

unique_ptr<TableRef> ColumnDataRef::Copy() {
	unique_ptr<ColumnDataRef> result;
	if (managed_result) {
		result = make_uniq<ColumnDataRef>(managed_result, expected_names);
	} else {
		auto copied_collection = CopyCollection(collection);
		result = make_uniq<ColumnDataRef>(std::move(copied_collection), expected_names);
	}
	CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
