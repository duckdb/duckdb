#include "duckdb/common/serializer/binary_deserializer.hpp"

namespace duckdb {

void BinaryDeserializer::SetTag(const field_id_t field_id, const char *tag) {
	current_field_id = field_id;
	current_tag = tag;
}

bool BinaryDeserializer::HasTag(const field_id_t field_id, const char *tag) {
	// Double check that there's space left in the buffer,
	// we might try to read an optional field at the end
	if (ptr + sizeof(field_id_t) > end_ptr) {
		return false;
	}

	// Check that we dont try to read outside the object
	auto object_start = stack.back().start_offset;
	auto object_end = object_start + stack.back().expected_size;
	if (ptr + sizeof(field_id_t) > object_end) {
		return false;
	}

	auto next_field_id = ReadPrimitive<field_id_t>();
	ptr -= sizeof(field_id_t);
	return next_field_id == field_id;
}

//===--------------------------------------------------------------------===//
// Nested Types Hooks
//===--------------------------------------------------------------------===//
void BinaryDeserializer::OnObjectBegin() {
	auto start_offset = ptr;
	auto expected_field_id = ReadPrimitive<field_id_t>();
	auto expected_field_type = ReadPrimitive<uint8_t>();
	auto expected_size = ReadPrimitive<uint64_t>();
	if (expected_field_id != current_field_id) {
		throw SerializationException("Expected field id %d, but got %d", current_field_id, expected_field_id);
	}
	if (expected_field_type != static_cast<uint8_t>(BinaryFieldType::VARIABLE_LEN)) {
		throw SerializationException("Expected variable length field type, but got %d", expected_field_type);
	}
	stack.emplace_back(start_offset, expected_size, expected_field_id);
}

void BinaryDeserializer::OnObjectEnd() {
	stack.pop_back();
}

idx_t BinaryDeserializer::OnListBegin() {
	return ReadPrimitive<idx_t>();
}

void BinaryDeserializer::OnListEnd() {
}

// Deserialize maps as [ { key: ..., value: ... } ]
idx_t BinaryDeserializer::OnMapBegin() {
	return ReadPrimitive<idx_t>();
}

void BinaryDeserializer::OnMapEntryBegin() {
}

void BinaryDeserializer::OnMapKeyBegin() {
}

void BinaryDeserializer::OnMapValueBegin() {
}

void BinaryDeserializer::OnMapEntryEnd() {
}

void BinaryDeserializer::OnMapEnd() {
}

void BinaryDeserializer::OnPairBegin() {
}

void BinaryDeserializer::OnPairKeyBegin() {
}

void BinaryDeserializer::OnPairValueBegin() {
}

void BinaryDeserializer::OnPairEnd() {
}

bool BinaryDeserializer::OnOptionalBegin() {
	OnObjectBegin();
	SetTag(0, "is_not_null");
	auto present = ReadBool();
	if (present) {
		SetTag(1, "value");
	}
	return present;
}

void BinaryDeserializer::OnOptionalEnd() {
	OnObjectEnd();
}

//===--------------------------------------------------------------------===//
// Primitive Types
//===--------------------------------------------------------------------===//
bool BinaryDeserializer::ReadBool() {
	ReadField(current_field_id, BinaryFieldType::FIXED_8);
	return ReadPrimitive<uint8_t>();
}

int8_t BinaryDeserializer::ReadSignedInt8() {
	ReadField(current_field_id, BinaryFieldType::FIXED_8);
	return ReadPrimitive<int8_t>();
}

uint8_t BinaryDeserializer::ReadUnsignedInt8() {
	ReadField(current_field_id, BinaryFieldType::FIXED_8);
	return ReadPrimitive<uint8_t>();
}

int16_t BinaryDeserializer::ReadSignedInt16() {
	ReadField(current_field_id, BinaryFieldType::FIXED_16);
	return ReadPrimitive<int16_t>();
}

uint16_t BinaryDeserializer::ReadUnsignedInt16() {
	ReadField(current_field_id, BinaryFieldType::FIXED_16);
	return ReadPrimitive<uint16_t>();
}

int32_t BinaryDeserializer::ReadSignedInt32() {
	ReadField(current_field_id, BinaryFieldType::FIXED_32);
	return ReadPrimitive<int32_t>();
}

uint32_t BinaryDeserializer::ReadUnsignedInt32() {
	ReadField(current_field_id, BinaryFieldType::FIXED_32);
	return ReadPrimitive<uint32_t>();
}

int64_t BinaryDeserializer::ReadSignedInt64() {
	ReadField(current_field_id, BinaryFieldType::FIXED_64);
	return ReadPrimitive<int64_t>();
}

uint64_t BinaryDeserializer::ReadUnsignedInt64() {
	ReadField(current_field_id, BinaryFieldType::FIXED_64);
	return ReadPrimitive<uint64_t>();
}

float BinaryDeserializer::ReadFloat() {
	ReadField(current_field_id, BinaryFieldType::FIXED_32);
	return ReadPrimitive<float>();
}

double BinaryDeserializer::ReadDouble() {
	ReadField(current_field_id, BinaryFieldType::FIXED_64);
	return ReadPrimitive<double>();
}

string BinaryDeserializer::ReadString() {
	ReadField(current_field_id, BinaryFieldType::VARIABLE_LEN);
	auto len = ReadPrimitive<uint64_t>();
	if (len == 0) {
		return string();
	}
	auto buffer = make_unsafe_uniq_array<data_t>(len);
	ReadData(buffer.get(), len);
	return string(const_char_ptr_cast(buffer.get()), len);
}

hugeint_t BinaryDeserializer::ReadHugeInt() {
	ReadField(current_field_id, BinaryFieldType::VARIABLE_LEN);
	ReadPrimitive<uint64_t>();
	return ReadPrimitive<hugeint_t>();
}

void BinaryDeserializer::ReadDataPtr(data_ptr_t &ptr_p, idx_t count) {
	ReadField(current_field_id, BinaryFieldType::VARIABLE_LEN);
	auto len = ReadPrimitive<uint64_t>();
	if (len != count) {
		throw SerializationException("Tried to read blob of %d size, but only %d elements are available", count, len);
	}
	ReadData(ptr_p, count);
}

} // namespace duckdb
