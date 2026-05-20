#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

template <>
void Serializer::WriteValue(const vector<bool> &vec) {
	auto count = vec.size();
	OnListBegin(count);
	for (auto item : vec) {
		WriteValue(item);
	}
	OnListEnd();
}

template <>
void Serializer::WritePropertyWithDefault<Value>(const field_id_t field_id, const char *tag, const Value &value,
                                                 const Value &default_value) {
	// If current value is default, don't write it
	if (!options.serialize_default_values && ValueOperations::NotDistinctFrom(value, default_value)) {
		OnOptionalPropertyBegin(field_id, tag, false);
		OnOptionalPropertyEnd(false);
		return;
	}
	OnOptionalPropertyBegin(field_id, tag, true);
	WriteValue(value);
	OnOptionalPropertyEnd(true);
}

template <>
void Serializer::WritePropertyWithDefault<CommonTableExpressionMap>(const field_id_t field_id, const char *tag,
                                                                    const CommonTableExpressionMap &value,
                                                                    const CommonTableExpressionMap &default_value) {
	if (!options.serialize_default_values && value.map.empty() && default_value.map.empty()) {
		OnOptionalPropertyBegin(field_id, tag, false);
		OnOptionalPropertyEnd(false);
		return;
	}
	OnOptionalPropertyBegin(field_id, tag, true);
	WriteValue(value);
	OnOptionalPropertyEnd(true);
}

void Serializer::List::WriteElement(data_ptr_t ptr, idx_t size) {
	serializer.WriteDataPtr(ptr, size);
}

} // namespace duckdb
