#include "duckdb/common/serializer/binary_serializer.hpp"

#ifdef DEBUG
#include "duckdb/common/string_util.hpp"
#endif

namespace duckdb {

void BinarySerializer::OnPropertyBegin(const field_id_t field_id, const char *tag) {
	// Just write the field id straight up
	Write<field_id_t>(field_id);
#ifdef DEBUG
	// First of check that we are inside an object
	if (debug_stack.empty()) {
		throw InternalException("OnPropertyBegin called outside of object");
	}

	// Check that the tag is unique
	auto &state = debug_stack.back();
	auto &seen_field_ids = state.seen_field_ids;
	auto &seen_field_tags = state.seen_field_tags;
	auto &seen_fields = state.seen_fields;

	if (seen_field_ids.find(field_id) != seen_field_ids.end() || seen_field_tags.find(tag) != seen_field_tags.end()) {
		string all_fields;
		for (auto &field : seen_fields) {
			all_fields += StringUtil::Format("\"%s\":%d ", field.first, field.second);
		}
		throw InternalException("Duplicate field id/tag in field: \"%s\":%d, other fields: %s", tag, field_id,
		                        all_fields);
	}

	seen_field_ids.insert(field_id);
	seen_field_tags.insert(tag);
	seen_fields.emplace_back(tag, field_id);
#else
	(void)tag;
#endif
}

void BinarySerializer::OnPropertyEnd() {
	// Nothing to do here
}

void BinarySerializer::OnOptionalPropertyBegin(const field_id_t field_id, const char *tag, bool present) {
	// Dont write anything at all if the property is not present
	if (present) {
		OnPropertyBegin(field_id, tag);
	}
}

void BinarySerializer::OnOptionalPropertyEnd(bool present) {
	// Nothing to do here
}

//-------------------------------------------------------------------------
// Nested Type Hooks
//-------------------------------------------------------------------------
void BinarySerializer::OnObjectBegin() {
#ifdef DEBUG
	debug_stack.emplace_back();
#endif
}

void BinarySerializer::OnObjectEnd() {
#ifdef DEBUG
	debug_stack.pop_back();
#endif
	// Write object terminator
	Write<field_id_t>(MESSAGE_TERMINATOR_FIELD_ID);
}

void BinarySerializer::OnListBegin(idx_t count) {
	VarIntEncode(count);
}

void BinarySerializer::OnListEnd() {
}

void BinarySerializer::OnNullableBegin(bool present) {
	WriteValue(present);
}

void BinarySerializer::OnNullableEnd() {
}

//-------------------------------------------------------------------------
// Primitive Types
//-------------------------------------------------------------------------
void BinarySerializer::WriteNull() {
	// This should never be called, optional writes should be handled by OnOptionalBegin
}

void BinarySerializer::WriteValue(bool value) {
	Write<uint8_t>(value);
}

void BinarySerializer::WriteValue(uint8_t value) {
	VarIntEncode(value);
}

void BinarySerializer::WriteValue(char value) {
	Write(value);
}

void BinarySerializer::WriteValue(int8_t value) {
	VarIntEncode(value);
}

void BinarySerializer::WriteValue(uint16_t value) {
	VarIntEncode(value);
}

void BinarySerializer::WriteValue(int16_t value) {
	VarIntEncode(value);
}

void BinarySerializer::WriteValue(uint32_t value) {
	VarIntEncode(value);
}

void BinarySerializer::WriteValue(int32_t value) {
	VarIntEncode(value);
}

void BinarySerializer::WriteValue(uint64_t value) {
	VarIntEncode(value);
}

void BinarySerializer::WriteValue(int64_t value) {
	VarIntEncode(value);
}

void BinarySerializer::WriteValue(hugeint_t value) {
	VarIntEncode(value.upper);
	VarIntEncode(value.lower);
}

void BinarySerializer::WriteValue(float value) {
	Write(value);
}

void BinarySerializer::WriteValue(double value) {
	Write(value);
}

void BinarySerializer::WriteValue(const string &value) {
	uint32_t len = value.length();
	VarIntEncode(len);
	WriteData(value.c_str(), len);
}

void BinarySerializer::WriteValue(const string_t value) {
	uint32_t len = value.GetSize();
	VarIntEncode(len);
	WriteData(value.GetDataUnsafe(), len);
}

void BinarySerializer::WriteValue(const char *value) {
	uint32_t len = strlen(value);
	VarIntEncode(len);
	WriteData(value, len);
}

void BinarySerializer::WriteDataPtr(const_data_ptr_t ptr, idx_t count) {
	VarIntEncode(static_cast<uint64_t>(count));
	WriteData(ptr, count);
}

} // namespace duckdb
