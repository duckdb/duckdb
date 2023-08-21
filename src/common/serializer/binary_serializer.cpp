#include "duckdb/common/serializer/binary_serializer.hpp"

#ifdef DEBUG
#include "duckdb/common/string_util.hpp"
#endif

namespace duckdb {

void BinarySerializer::SetTag(const field_id_t field_id, const char *tag) {
	current_field_id = field_id;
	current_tag = tag;
	// Increment the number of fields
	stack.back().field_count++;

#ifdef DEBUG

	// Check that the tag is unique
	auto &seen_fields = stack.back().seen_fields;

	for (auto &seen_field : seen_fields) {
		if (seen_field.first == tag || seen_field.second == field_id) {
			string all_fields;
			for (auto &field : seen_fields) {
				all_fields += StringUtil::Format("\"%s\":%d ", field.first, field.second);
			}
			throw SerializationException("Duplicate field id/tag in field: \"%s\":%d, other fields: %s", tag, field_id,
			                             all_fields);
		}
	}

	seen_fields.emplace_back(tag, field_id);

#endif
}

//===--------------------------------------------------------------------===//
// Nested types
//===--------------------------------------------------------------------===//

// We serialize optional values as a message with a "present" flag, followed by the value.
void BinarySerializer::OnOptionalBegin(bool present) {
	OnObjectBegin();
	SetTag(0, "is_not_null");
	WriteValue(present);
	if (present) {
		SetTag(1, "value");
	}
}

void BinarySerializer::OnOptionalEnd(bool present) {
	OnObjectEnd();
}

void BinarySerializer::OnListBegin(idx_t count) {
	Write(count);
}

void BinarySerializer::OnListEnd(idx_t count) {
}

// Serialize maps as arrays of objects with "key" and "value" properties.
void BinarySerializer::OnMapBegin(idx_t count) {
	Write(count);
}

void BinarySerializer::OnMapEntryBegin() {
}

void BinarySerializer::OnMapKeyBegin() {
}

void BinarySerializer::OnMapValueBegin() {
}

void BinarySerializer::OnMapEntryEnd() {
}

void BinarySerializer::OnMapEnd(idx_t count) {
}

void BinarySerializer::OnObjectBegin() {
	stack.push_back(State({0, 0, data.size()}));
	// Store the field id
	Write<uint32_t>(current_field_id);
	Write<uint8_t>(static_cast<uint8_t>(BinaryMessageKind::VARIABLE_LEN));
	// Store the offset so we can patch the field count and size later
	Write<uint64_t>(0); // Placeholder for the size
}

void BinarySerializer::OnObjectEnd() {
	auto &frame = stack.back();
	auto size = frame.size;
	// Patch the field count and size
	auto ptr = &data[frame.offset];
	ptr += sizeof(uint32_t); // Skip the field id
	ptr += sizeof(uint8_t);  // Skip the message kind
	Store<uint64_t>(size, ptr);
	stack.pop_back();

	// Add the size to the parent frame
	if (!stack.empty()) {
		stack.back().size += size;
	}
}

void BinarySerializer::OnPairBegin() {
}

void BinarySerializer::OnPairKeyBegin() {
}

void BinarySerializer::OnPairValueBegin() {
}

void BinarySerializer::OnPairEnd() {
}

//===--------------------------------------------------------------------===//
// Primitive types
//===--------------------------------------------------------------------===//
void BinarySerializer::WriteNull() {
	// This should never be called, optional writes should be handled by OnOptionalBegin
}

void BinarySerializer::WriteValue(bool value) {
	WriteField(current_field_id, BinaryMessageKind::FIXED_8);
	Write(static_cast<uint8_t>(value));
}

void BinarySerializer::WriteValue(uint8_t value) {
	WriteField(current_field_id, BinaryMessageKind::FIXED_8);
	Write(value);
}

void BinarySerializer::WriteValue(int8_t value) {
	WriteField(current_field_id, BinaryMessageKind::FIXED_8);
	Write(value);
}

void BinarySerializer::WriteValue(uint16_t value) {
	WriteField(current_field_id, BinaryMessageKind::FIXED_16);
	Write(value);
}

void BinarySerializer::WriteValue(int16_t value) {
	WriteField(current_field_id, BinaryMessageKind::FIXED_16);
	Write(value);
}

void BinarySerializer::WriteValue(uint32_t value) {
	WriteField(current_field_id, BinaryMessageKind::FIXED_32);
	Write(value);
}

void BinarySerializer::WriteValue(int32_t value) {
	WriteField(current_field_id, BinaryMessageKind::FIXED_32);
	Write(value);
}

void BinarySerializer::WriteValue(uint64_t value) {
	WriteField(current_field_id, BinaryMessageKind::FIXED_64);
	Write(value);
}

void BinarySerializer::WriteValue(int64_t value) {
	WriteField(current_field_id, BinaryMessageKind::FIXED_64);
	Write(value);
}

void BinarySerializer::WriteValue(hugeint_t value) {
	WriteField(current_field_id, BinaryMessageKind::VARIABLE_LEN);
	Write(static_cast<uint64_t>(sizeof(hugeint_t)));
	Write(value);
}

void BinarySerializer::WriteValue(float value) {
	WriteField(current_field_id, BinaryMessageKind::FIXED_32);
	Write(value);
}

void BinarySerializer::WriteValue(double value) {
	WriteField(current_field_id, BinaryMessageKind::FIXED_64);
	Write(value);
}

void BinarySerializer::WriteValue(const string &value) {
	WriteField(current_field_id, BinaryMessageKind::VARIABLE_LEN);
	uint64_t len = value.length();
	Write(len);
	WriteDataInternal(value.c_str(), len);
}

void BinarySerializer::WriteValue(const string_t value) {
	WriteField(current_field_id, BinaryMessageKind::VARIABLE_LEN);
	uint64_t len = value.GetSize();
	Write(len);
	WriteDataInternal(value.GetDataUnsafe(), len);
}

void BinarySerializer::WriteValue(const char *value) {
	WriteField(current_field_id, BinaryMessageKind::VARIABLE_LEN);
	uint64_t len = strlen(value);
	Write(len);
	WriteDataInternal(value, len);
}

void BinarySerializer::WriteDataPtr(const_data_ptr_t ptr, idx_t count) {
	WriteField(current_field_id, BinaryMessageKind::VARIABLE_LEN);
	Write(static_cast<uint64_t>(count));
	WriteDataInternal(ptr, count);
}

} // namespace duckdb
