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
idx_t BinaryDeserializer::BeginReadList() { return 0; }
void BinaryDeserializer::EndReadList() { }

//===--------------------------------------------------------------------===//
// Primitive Types
//===--------------------------------------------------------------------===//
bool BinaryDeserializer::ReadBool() {

}

int32_t BinaryDeserializer::ReadSignedInt32() {

}

uint32_t BinaryDeserializer::ReadUnsignedInt32() {

}

float BinaryDeserializer::ReadFloat() {

}

double BinaryDeserializer::ReadDouble() {

}

string BinaryDeserializer::ReadString() {

}

}