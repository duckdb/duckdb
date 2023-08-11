#include "duckdb/common/serializer/binary_deserializer.hpp"

namespace duckdb {

void BinaryDeserializer::SetTag(const field_id_t field_id, const char *tag) {
	current_field_id = field_id;
	current_tag = tag;
	stack.back().read_field_count++;
	if (stack.back().read_field_count > stack.back().expected_field_count) {
		throw SerializationException("Attempting to read a required field, but field is missing");
	}
}

//===--------------------------------------------------------------------===//
// Nested Types Hooks
//===--------------------------------------------------------------------===//
void BinaryDeserializer::OnObjectBegin() {
	auto expected_field_id = ReadPrimitive<field_id_t>();
	auto expected_field_count = ReadPrimitive<uint32_t>();
	auto expected_size = ReadPrimitive<uint64_t>();
	D_ASSERT(expected_field_count > 0);
	D_ASSERT(expected_size > 0);
	D_ASSERT(expected_field_id == current_field_id);
	stack.emplace_back(expected_field_count, expected_size, expected_field_id);
}

void BinaryDeserializer::OnObjectEnd() {
	auto &frame = stack.back();
	if (frame.read_field_count < frame.expected_field_count) {
		throw SerializationException("Not all fields were read. This file might have been written with a newer version "
		                             "of DuckDB and is incompatible with this version of DuckDB.");
	}
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
	return ReadPrimitive<bool>();
}

//===--------------------------------------------------------------------===//
// Primitive Types
//===--------------------------------------------------------------------===//
bool BinaryDeserializer::ReadBool() {
	return ReadPrimitive<bool>();
}

int8_t BinaryDeserializer::ReadSignedInt8() {
	return ReadPrimitive<int8_t>();
}

uint8_t BinaryDeserializer::ReadUnsignedInt8() {
	return ReadPrimitive<uint8_t>();
}

int16_t BinaryDeserializer::ReadSignedInt16() {
	return ReadPrimitive<int16_t>();
}

uint16_t BinaryDeserializer::ReadUnsignedInt16() {
	return ReadPrimitive<uint16_t>();
}

int32_t BinaryDeserializer::ReadSignedInt32() {
	return ReadPrimitive<int32_t>();
}

uint32_t BinaryDeserializer::ReadUnsignedInt32() {
	return ReadPrimitive<uint32_t>();
}

int64_t BinaryDeserializer::ReadSignedInt64() {
	return ReadPrimitive<int64_t>();
}

uint64_t BinaryDeserializer::ReadUnsignedInt64() {
	return ReadPrimitive<uint64_t>();
}

float BinaryDeserializer::ReadFloat() {
	return ReadPrimitive<float>();
}

double BinaryDeserializer::ReadDouble() {
	return ReadPrimitive<double>();
}

string BinaryDeserializer::ReadString() {
	uint32_t size = ReadPrimitive<uint32_t>();
	if (size == 0) {
		return string();
	}
	auto buffer = make_unsafe_uniq_array<data_t>(size);
	ReadData(buffer.get(), size);
	return string(const_char_ptr_cast(buffer.get()), size);
}

interval_t BinaryDeserializer::ReadInterval() {
	return ReadPrimitive<interval_t>();
}

hugeint_t BinaryDeserializer::ReadHugeInt() {
	return ReadPrimitive<hugeint_t>();
}

void BinaryDeserializer::ReadDataPtr(data_ptr_t &ptr, idx_t count) {
	ReadData(ptr, count);
}

} // namespace duckdb
