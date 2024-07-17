#include "duckdb/parser/tableref/column_data_ref.hpp"
#include "duckdb/common/string_util.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

string ColumnDataRef::ToString() const {
	auto result = collection->ToString();
	return BaseToString(result, expected_names);
}

bool ColumnDataRef::Equals(const TableRef &other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<ColumnDataRef>();
	auto expected_types = collection->Types();
	auto other_expected_types = other.collection->Types();
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
	if (!ColumnDataCollection::ResultEquals(*collection, *other.collection, unused, true)) {
		return false;
	}
	return true;
}

unique_ptr<TableRef> ColumnDataRef::Copy() {
	auto result = make_uniq<ColumnDataRef>(collection, expected_names);
	CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
