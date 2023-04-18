#include "duckdb/common/serializer/binary_deserializer.hpp"

namespace duckdb {

void BinaryDeserializer::SetTag(const char *tag) {
	current_tag = tag;
	stack.back().read_field_count++;
}

//===--------------------------------------------------------------------===//
// Nested Types Hooks
//===--------------------------------------------------------------------===//
void BinaryDeserializer::OnObjectBegin() {
	auto expected_field_count = reader.Read<uint32_t>();
	auto expected_size = reader.Read<uint64_t>();
	D_ASSERT(expected_field_count > 0);
	D_ASSERT(expected_size > 0);

	stack.emplace_back(expected_field_count, expected_size);
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
	return reader.Read<idx_t>();
}

void BinaryDeserializer::OnListEnd() {
}

// Deserialize maps as [ { key: ..., value: ... } ]
idx_t BinaryDeserializer::OnMapBegin() {
	return reader.Read<idx_t>();
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
	return reader.Read<bool>();
}

//===--------------------------------------------------------------------===//
// Primitive Types
//===--------------------------------------------------------------------===//
bool BinaryDeserializer::ReadBool() {
	return reader.Read<bool>();
}

int8_t BinaryDeserializer::ReadSignedInt8() {
	return reader.Read<int8_t>();
}

uint8_t BinaryDeserializer::ReadUnsignedInt8() {
	return reader.Read<uint8_t>();
}

int16_t BinaryDeserializer::ReadSignedInt16() {
	return reader.Read<int16_t>();
}

uint16_t BinaryDeserializer::ReadUnsignedInt16() {
	return reader.Read<uint16_t>();
}

int32_t BinaryDeserializer::ReadSignedInt32() {
	return reader.Read<int32_t>();
}

uint32_t BinaryDeserializer::ReadUnsignedInt32() {
	return reader.Read<uint32_t>();
}

int64_t BinaryDeserializer::ReadSignedInt64() {
	return reader.Read<int64_t>();
}

uint64_t BinaryDeserializer::ReadUnsignedInt64() {
	return reader.Read<uint64_t>();
}

float BinaryDeserializer::ReadFloat() {
	return reader.Read<float>();
}

double BinaryDeserializer::ReadDouble() {
	return reader.Read<double>();
}

string BinaryDeserializer::ReadString() {
	return reader.Read<string>();
}

interval_t BinaryDeserializer::ReadInterval() {
	return reader.Read<interval_t>();
}

hugeint_t BinaryDeserializer::ReadHugeInt() {
	return reader.Read<hugeint_t>();
}

void BinaryDeserializer::ReadDataPtr(data_ptr_t &ptr, idx_t count) {
	reader.ReadData(ptr, count);
}

} // namespace duckdb
