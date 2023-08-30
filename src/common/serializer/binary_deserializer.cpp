#include "duckdb/common/serializer/binary_deserializer.hpp"

namespace duckdb {

//-------------------------------------------------------------------------
// Nested Type Hooks
//-------------------------------------------------------------------------
void BinaryDeserializer::OnPropertyBegin(const field_id_t field_id, const char *) {
	if (current_field != field_id) {
		throw InternalException("Failed to deserialize: field id mismatch, expected: %d, got: %d", field_id,
		                        current_field);
	}
}

void BinaryDeserializer::OnPropertyEnd() {
	// Move to the next field, unless we are at the terminator on level 0 (always the last field ID written)
	// If we are at nesting level 0 and read a terminator, we are done
	if (current_field != MESSAGE_TERMINATOR_FIELD_ID || nesting_level != 0) {
		current_field = ReadPrimitive<field_id_t>();
	}
}

bool BinaryDeserializer::OnOptionalPropertyBegin(const field_id_t field_id, const char *s) {
	if (current_field != field_id) {
		return false;
	}
	return true;
}

void BinaryDeserializer::OnOptionalPropertyEnd(bool present) {
	// If the property was present, (and we presumably consumed it)
	// move to the next field, unless we are at the terminator on level 0
	if (present && (current_field != MESSAGE_TERMINATOR_FIELD_ID || nesting_level != 0)) {
		current_field = ReadPrimitive<field_id_t>();
	}
}

void BinaryDeserializer::OnObjectBegin() {
	nesting_level++;
}

void BinaryDeserializer::OnObjectEnd() {
	if (current_field != MESSAGE_TERMINATOR_FIELD_ID) {
		throw InternalException("Failed to deserialize: expected terminator, got: %d", current_field);
	}
	nesting_level--;

	if (nesting_level > 0) {
		// Move to the next field
		current_field = ReadPrimitive<field_id_t>();
	}
}

idx_t BinaryDeserializer::OnListBegin() {
	auto count = ReadPrimitive<idx_t>();
	return count;
}

void BinaryDeserializer::OnListEnd() {
}

bool BinaryDeserializer::OnNullableBegin() {
	auto present = ReadPrimitive<bool>();
	return present;
}

void BinaryDeserializer::OnNullableEnd() {
}

//-------------------------------------------------------------------------
// Primitive Types
//-------------------------------------------------------------------------
bool BinaryDeserializer::ReadBool() {
	auto value = ReadPrimitive<uint8_t>();
	return value;
}

int8_t BinaryDeserializer::ReadSignedInt8() {
	auto value = ReadPrimitive<int8_t>();
	return value;
}

uint8_t BinaryDeserializer::ReadUnsignedInt8() {
	auto value = ReadPrimitive<uint8_t>();
	return value;
}

int16_t BinaryDeserializer::ReadSignedInt16() {
	auto value = ReadPrimitive<int16_t>();
	return value;
}

uint16_t BinaryDeserializer::ReadUnsignedInt16() {
	auto value = ReadPrimitive<uint16_t>();
	return value;
}

int32_t BinaryDeserializer::ReadSignedInt32() {
	auto value = ReadPrimitive<int32_t>();
	return value;
}

uint32_t BinaryDeserializer::ReadUnsignedInt32() {
	auto value = ReadPrimitive<uint32_t>();
	return value;
}

int64_t BinaryDeserializer::ReadSignedInt64() {
	auto value = ReadPrimitive<int64_t>();
	return value;
}

uint64_t BinaryDeserializer::ReadUnsignedInt64() {
	auto value = ReadPrimitive<uint64_t>();
	return value;
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
	auto len = ReadPrimitive<uint32_t>();
	if (len == 0) {
		return string();
	}
	auto buffer = make_unsafe_uniq_array<data_t>(len);
	ReadData(buffer.get(), len);
	return string(const_char_ptr_cast(buffer.get()), len);
}

hugeint_t BinaryDeserializer::ReadHugeInt() {
	auto upper = ReadPrimitive<int64_t>();
	auto lower = ReadPrimitive<uint64_t>();
	return hugeint_t(upper, lower);
}

void BinaryDeserializer::ReadDataPtr(data_ptr_t &ptr_p, idx_t count) {
	auto len = ReadPrimitive<uint64_t>();
	if (len != count) {
		throw SerializationException("Tried to read blob of %d size, but only %d elements are available", count, len);
	}
	ReadData(ptr_p, count);
}

} // namespace duckdb
