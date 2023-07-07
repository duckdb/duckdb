#include "duckdb/common/serializer/binary_serializer.hpp"

namespace duckdb {

void BinarySerializer::SetTag(const char *tag) {
	current_tag = tag;

	// Increment the number of fields
	stack.back().field_count++;
}

//===--------------------------------------------------------------------===//
// Nested types
//===--------------------------------------------------------------------===//
void BinarySerializer::OnOptionalBegin(bool present) {
	Write(present);
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
	Write<uint32_t>(0); // Placeholder for the field count
	Write<uint64_t>(0); // Placeholder for the size
}

void BinarySerializer::OnObjectEnd() {
	auto &frame = stack.back();
	// Patch the field count and size
	auto message_start = &data[frame.offset];
	Store<uint32_t>(frame.field_count, message_start);
	Store<uint64_t>(frame.size, message_start + sizeof(uint32_t));
	stack.pop_back();
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

void BinarySerializer::WriteValue(uint8_t value) {
	Write(value);
}

void BinarySerializer::WriteValue(int8_t value) {
	Write(value);
}

void BinarySerializer::WriteValue(uint16_t value) {
	Write(value);
}

void BinarySerializer::WriteValue(int16_t value) {
	Write(value);
}

void BinarySerializer::WriteValue(uint32_t value) {
	Write(value);
}

void BinarySerializer::WriteValue(int32_t value) {
	Write(value);
}

void BinarySerializer::WriteValue(uint64_t value) {
	Write(value);
}

void BinarySerializer::WriteValue(int64_t value) {
	Write(value);
}

void BinarySerializer::WriteValue(hugeint_t value) {
	Write(value);
}

void BinarySerializer::WriteValue(float value) {
	Write(value);
}

void BinarySerializer::WriteValue(double value) {
	Write(value);
}

void BinarySerializer::WriteValue(interval_t value) {
	Write(value);
}

void BinarySerializer::WriteValue(const string &value) {
	auto len = value.length();
	Write<uint32_t>((uint32_t)len);
	if (len > 0) {
		WriteData(value.c_str(), len);
	}
}

void BinarySerializer::WriteValue(const string_t value) {
	auto len = value.GetSize();
	Write<uint32_t>((uint32_t)len);
	if (len > 0) {
		WriteData(value.GetDataUnsafe(), len);
	}
}

void BinarySerializer::WriteValue(const char *value) {
	auto len = strlen(value);
	Write<uint32_t>((uint32_t)len);
	if (len > 0) {
		WriteData(value, len);
	}
}

void BinarySerializer::WriteValue(bool value) {
	Write(value);
}

void BinarySerializer::WriteDataPtr(const_data_ptr_t ptr, idx_t count) {
	WriteData(ptr, count);
}

} // namespace duckdb
