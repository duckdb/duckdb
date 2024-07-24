#include "duckdb/common/serializer/binary_deserializer.hpp"

namespace duckdb {

//-------------------------------------------------------------------------
// Nested Type Hooks
//-------------------------------------------------------------------------
void BinaryDeserializer::OnPropertyBegin(const field_id_t field_id, const char *) {
	auto field = NextField();
	if (field != field_id) {
		throw SerializationException("Failed to deserialize: field id mismatch, expected: %d, got: %d", field_id,
		                             field);
	}
}

void BinaryDeserializer::OnPropertyEnd() {
}

bool BinaryDeserializer::OnOptionalPropertyBegin(const field_id_t field_id, const char *s) {
	auto next_field = PeekField();
	auto present = next_field == field_id;
	if (present) {
		ConsumeField();
	}
	return present;
}

void BinaryDeserializer::OnOptionalPropertyEnd(bool present) {
}

void BinaryDeserializer::OnObjectBegin() {
	nesting_level++;
}

void BinaryDeserializer::OnObjectEnd() {
	auto next_field = NextField();
	if (next_field != MESSAGE_TERMINATOR_FIELD_ID) {
		throw SerializationException("Failed to deserialize: expected end of object, but found field id: %d",
		                             next_field);
	}
	nesting_level--;
}

idx_t BinaryDeserializer::OnListBegin() {
	return VarIntDecode<idx_t>();
}

void BinaryDeserializer::OnListEnd() {
}

bool BinaryDeserializer::OnNullableBegin() {
	return ReadBool();
}

void BinaryDeserializer::OnNullableEnd() {
}

//-------------------------------------------------------------------------
// Primitive Types
//-------------------------------------------------------------------------
bool BinaryDeserializer::ReadBool() {
	return static_cast<bool>(ReadPrimitive<uint8_t>());
}

char BinaryDeserializer::ReadChar() {
	return ReadPrimitive<char>();
}

int8_t BinaryDeserializer::ReadSignedInt8() {
	return VarIntDecode<int8_t>();
}

uint8_t BinaryDeserializer::ReadUnsignedInt8() {
	return VarIntDecode<uint8_t>();
}

int16_t BinaryDeserializer::ReadSignedInt16() {
	return VarIntDecode<int16_t>();
}

uint16_t BinaryDeserializer::ReadUnsignedInt16() {
	return VarIntDecode<uint16_t>();
}

int32_t BinaryDeserializer::ReadSignedInt32() {
	return VarIntDecode<int32_t>();
}

uint32_t BinaryDeserializer::ReadUnsignedInt32() {
	return VarIntDecode<uint32_t>();
}

int64_t BinaryDeserializer::ReadSignedInt64() {
	return VarIntDecode<int64_t>();
}

uint64_t BinaryDeserializer::ReadUnsignedInt64() {
	return VarIntDecode<uint64_t>();
}

float BinaryDeserializer::ReadFloat() {
	auto value = ReadPrimitive<float>();
	return value;
}

double BinaryDeserializer::ReadDouble() {
	auto value = ReadPrimitive<double>();
	return value;
}

string BinaryDeserializer::ReadString() {
	auto len = VarIntDecode<uint32_t>();
	if (len == 0) {
		return string();
	}
	auto buffer = make_unsafe_uniq_array_uninitialized<data_t>(len);
	ReadData(buffer.get(), len);
	return string(const_char_ptr_cast(buffer.get()), len);
}

hugeint_t BinaryDeserializer::ReadHugeInt() {
	auto upper = VarIntDecode<int64_t>();
	auto lower = VarIntDecode<uint64_t>();
	return hugeint_t(upper, lower);
}

uhugeint_t BinaryDeserializer::ReadUhugeInt() {
	auto upper = VarIntDecode<uint64_t>();
	auto lower = VarIntDecode<uint64_t>();
	return uhugeint_t(upper, lower);
}

void BinaryDeserializer::ReadDataPtr(data_ptr_t &ptr_p, idx_t count) {
	auto len = VarIntDecode<uint64_t>();
	if (len != count) {
		throw SerializationException("Tried to read blob of %d size, but only %d elements are available", count, len);
	}
	ReadData(ptr_p, count);
}

} // namespace duckdb
