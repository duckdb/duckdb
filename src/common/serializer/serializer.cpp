#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

SerializationOptions::SerializationOptions(const SerializationCompatibility &serialization_compat) {
	serialization_compatibility = serialization_compat;
}

SerializationOptions::SerializationOptions(const AttachedDatabase &db) {
	serialization_compatibility = SerializationCompatibility::FromIndex(db.GetCompatibilityVersion());
}

SerializationOptions::SerializationOptions(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	serialization_compatibility =
	    SerializationCompatibility::FromIndex(config.options.serialization_compatibility.serialization_version);
}

SerializationOptions SerializationOptions::DefaultOldestSupported() {
	SerializationOptions res(SerializationCompatibility::Default());
	return res;
}

SerializationOptions SerializationOptions::Latest() {
	SerializationOptions res(SerializationCompatibility::Latest());
	return res;
}

SerializationOptions SerializationOptions::From(const AttachedDatabase &db) {
	SerializationOptions res(db);
	return res;
}

SerializationOptions SerializationOptions::From(const ClientContext &context) {
	SerializationOptions res(context);
	return res;
}

SerializationOptions SerializationOptions::From(const SerializationCompatibility &serialization_compat) {
	return SerializationOptions {serialization_compat};
}

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
                                                 const Value &&default_value) {
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

void Serializer::List::WriteElement(data_ptr_t ptr, idx_t size) {
	serializer.WriteDataPtr(ptr, size);
}

} // namespace duckdb
