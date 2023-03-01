#pragma once

#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

class BinaryDeserializer : public FormatDeserializer {
private:
	//===--------------------------------------------------------------------===//
	// Nested Types Hooks
	//===--------------------------------------------------------------------===//
	idx_t BeginReadList() final;
	void EndReadList() final;

	//===--------------------------------------------------------------------===//
	// Primitive Types
	//===--------------------------------------------------------------------===//
	bool ReadBool() final;
	int32_t ReadSignedInt32() final;
	uint32_t ReadUnsignedInt32() final;
	float ReadFloat() final;
	double ReadDouble() final;
	string ReadString() final;
};

}