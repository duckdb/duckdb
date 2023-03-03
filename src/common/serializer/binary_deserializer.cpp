#pragma once

#include "duckdb/common/serializer/binary_deserializer.hpp"

namespace duckdb {


void SetTag(const char* tag) {
    // Ignore, the binary deserializer reads everything in-order anyway
	(void)tag;
};

//===--------------------------------------------------------------------===//
// Nested Types Hooks
//===--------------------------------------------------------------------===//
void BinaryDeserializer::OnObjectBegin() { }
void BinaryDeserializer::OnObjectEnd() { }
idx_t BinaryDeserializer::OnListBegin() { return 0; }
void BinaryDeserializer::OnListEnd() { }
idx_t BinaryDeserializer::OnMapBegin() { return 0; };
void BinaryDeserializer::OnMapEnd() { };
bool BinaryDeserializer::OnOptionalBegin() { return false; }

//===--------------------------------------------------------------------===//
// Primitive Types
//===--------------------------------------------------------------------===//
bool BinaryDeserializer::ReadBool() {
	return false;
}

int8_t BinaryDeserializer::ReadSignedInt8() {
	return 0;
}

uint8_t BinaryDeserializer::ReadUnsignedInt8() {
	return 0;
}

int16_t BinaryDeserializer::ReadSignedInt16() {
	return 0;
}

uint16_t BinaryDeserializer::ReadUnsignedInt16() {
	return 0;
}

int32_t BinaryDeserializer::ReadSignedInt32() {
	return 0;
}

uint32_t BinaryDeserializer::ReadUnsignedInt32() {
	return 0;
}

int64_t BinaryDeserializer::ReadSignedInt64() {
	return 0;
}

uint64_t BinaryDeserializer::ReadUnsignedInt64() {
	return 0;
}

float BinaryDeserializer::ReadFloat() {
	return 0;
}

double BinaryDeserializer::ReadDouble() {
	return 0;
}

string BinaryDeserializer::ReadString() {
	return "";
}

interval_t BinaryDeserializer::ReadInterval() {
	return interval_t();
}

}