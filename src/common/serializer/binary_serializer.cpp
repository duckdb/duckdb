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
	stack.back().buffer.Write(present);
}

void BinarySerializer::OnListBegin(idx_t count) {
	stack.back().buffer.Write(count);
}

void BinarySerializer::OnListEnd(idx_t count) {
}

// Serialize maps as arrays of objects with "key" and "value" properties.
void BinarySerializer::OnMapBegin(idx_t count) {
	stack.back().buffer.Write(count);
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
	stack.push_back(StackFrame());
}

void BinarySerializer::OnObjectEnd() {
	D_ASSERT(stack.size() > 1); // we should never pop the root frame

	auto &parent = stack[stack.size() - 2];
	auto &frame = stack.back();
	parent.buffer.Write(frame.field_count);
	parent.buffer.Write(frame.buffer.blob.size);
	parent.buffer.WriteData(frame.buffer.blob.data.get(), frame.buffer.blob.size);

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
	stack.back().buffer.Write(value);
}

void BinarySerializer::WriteValue(int8_t value) {
	stack.back().buffer.Write(value);
}

void BinarySerializer::WriteValue(uint16_t value) {
	stack.back().buffer.Write(value);
}

void BinarySerializer::WriteValue(int16_t value) {
	stack.back().buffer.Write(value);
}

void BinarySerializer::WriteValue(uint32_t value) {
	stack.back().buffer.Write(value);
}

void BinarySerializer::WriteValue(int32_t value) {
	stack.back().buffer.Write(value);
}

void BinarySerializer::WriteValue(uint64_t value) {
	stack.back().buffer.Write(value);
}

void BinarySerializer::WriteValue(int64_t value) {
	stack.back().buffer.Write(value);
}

void BinarySerializer::WriteValue(hugeint_t value) {
	stack.back().buffer.Write(value);
}

void BinarySerializer::WriteValue(float value) {
	stack.back().buffer.Write(value);
}

void BinarySerializer::WriteValue(double value) {
	stack.back().buffer.Write(value);
}

void BinarySerializer::WriteValue(interval_t value) {
	stack.back().buffer.Write(value);
}

void BinarySerializer::WriteValue(const string &value) {
	stack.back().buffer.WriteString(value);
}

void BinarySerializer::WriteValue(const string_t value) {
	stack.back().buffer.WriteStringLen((const unsigned char *)(value.GetDataUnsafe()), value.GetSize());
}

void BinarySerializer::WriteValue(const char *value) {
	stack.back().buffer.WriteString(value);
}

void BinarySerializer::WriteValue(bool value) {
	stack.back().buffer.Write(value);
}

void BinarySerializer::WriteDataPtr(const_data_ptr_t ptr, idx_t count) {
	stack.back().buffer.WriteData(ptr, count);
}

} // namespace duckdb
