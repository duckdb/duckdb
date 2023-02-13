#include "duckdb/common/serializer/binary_serializer.hpp"

namespace duckdb {

void BinarySerializer::WriteTag(const char *tag) {

	printf("%s", tag);
	trace.push_back(tag);
	// We ignore the tag, but record that we wrote a field at the current depth
	GetCurrent().AddField();
}
void BinarySerializer::WriteNull() {
	GetCurrent().Write<ptrdiff_t>(0); // ??
}

void BinarySerializer::BeginWriteList(idx_t count) {
	GetCurrent().Write((uint32_t)count);
}

void BinarySerializer::BeginWriteMap(idx_t count) {
	GetCurrent().Write((uint32_t)count);
}

void BinarySerializer::BeginWriteObject() {
	// Push a new object to the stack
	stack.emplace_back();
}

void BinarySerializer::EndWriteObject() {

	// Pop the current object
	auto inner = std::move(stack.back());
	stack.pop_back();

	// Write the field count, size and data in the parent object
	auto &outer = stack.back();
	outer.Write<uint32_t>(inner.field_count);
	outer.Write<uint64_t>(inner.writer->blob.size);
	outer.writer->WriteData(inner.writer->blob.data.get(), inner.writer->blob.size);
}

void BinarySerializer::WriteValue(uint8_t value) {
	GetCurrent().writer->Write(value);
}

void BinarySerializer::WriteValue(int8_t value) {
	GetCurrent().writer->Write(value);
}

void BinarySerializer::WriteValue(const string &value) {
	auto len = value.size();
	GetCurrent().writer->Write<uint32_t>((uint32_t)len);
	if(len > 0) {
		GetCurrent().writer->WriteData((const_data_ptr_t)value.c_str(), value.size());
	}
}

void BinarySerializer::WriteValue(const string_t value) {
	auto len = value.GetSize();
	GetCurrent().writer->Write<uint32_t>((uint32_t)len);
	if(len > 0) {
		GetCurrent().writer->WriteData((const_data_ptr_t)value.GetDataUnsafe(), len);
	}
}

void BinarySerializer::WriteValue(const char *value) {
	auto len = strlen(value);
	GetCurrent().writer->Write<uint32_t>((uint32_t)len);
	if(len > 0) {
		GetCurrent().writer->WriteData((const_data_ptr_t)value, len);
	}
}

void BinarySerializer::WriteValue(uint64_t value) {
	GetCurrent().writer->Write(value);
}

void BinarySerializer::WriteValue(uint16_t value) {
	GetCurrent().writer->Write(value);
}

void BinarySerializer::WriteValue(int16_t value) {
	GetCurrent().writer->Write(value);
}

void BinarySerializer::WriteValue(uint32_t value) {
	GetCurrent().writer->Write(value);
}

void BinarySerializer::WriteValue(int32_t value) {
	GetCurrent().writer->Write(value);
}

void BinarySerializer::WriteValue(int64_t value) {
	GetCurrent().writer->Write(value);
}

void BinarySerializer::WriteValue(hugeint_t value) {
	GetCurrent().writer->Write(value);
}

void BinarySerializer::WriteValue(float value) {
	GetCurrent().writer->Write(value);
}

void BinarySerializer::WriteValue(double value) {
	GetCurrent().writer->Write(value);
}

void BinarySerializer::WriteValue(interval_t value) {
	GetCurrent().writer->Write(value);
}

void BinarySerializer::WriteValue(bool value) {
	GetCurrent().writer->Write(value);
}

BinaryData BinarySerializer::GetData() {
	D_ASSERT(stack.size() == 1);
	return stack.back().writer->GetData();
}

} // namespace duckdb
