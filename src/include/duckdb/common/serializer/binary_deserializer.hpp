#pragma once
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

class BinaryDeserializer : public FormatDeserializer {
public:
	explicit BinaryDeserializer(Deserializer &reader) : reader(reader), stack({{0, 0}}) {
		deserialize_enum_from_string = false;
	}

private:
	struct StackFrame {
		uint32_t expected_field_count;
		idx_t expected_size;
		uint32_t read_field_count;
		StackFrame(uint32_t expected_field_count, idx_t expected_size)
		    : expected_field_count(expected_field_count), expected_size(expected_size), read_field_count(0) {
		}
	};

	const char *current_tag = nullptr;
	Deserializer &reader;
	vector<StackFrame> stack;

	// Set the 'tag' of the property to read
	void SetTag(const char *tag) final;

	//===--------------------------------------------------------------------===//
	// Nested Types Hooks
	//===--------------------------------------------------------------------===//
	void OnObjectBegin() final;
	void OnObjectEnd() final;
	idx_t OnListBegin() final;
	void OnListEnd() final;
	idx_t OnMapBegin() final;
	void OnMapEnd() final;
	void OnMapEntryBegin() final;
	void OnMapEntryEnd() final;
	void OnMapKeyBegin() final;
	void OnMapValueBegin() final;
	bool OnOptionalBegin() final;

	void OnPairBegin() final;
	void OnPairKeyBegin() final;
	void OnPairValueBegin() final;
	void OnPairEnd() final;

	//===--------------------------------------------------------------------===//
	// Primitive Types
	//===--------------------------------------------------------------------===//
	bool ReadBool() final;
	int8_t ReadSignedInt8() final;
	uint8_t ReadUnsignedInt8() final;
	int16_t ReadSignedInt16() final;
	uint16_t ReadUnsignedInt16() final;
	int32_t ReadSignedInt32() final;
	uint32_t ReadUnsignedInt32() final;
	int64_t ReadSignedInt64() final;
	uint64_t ReadUnsignedInt64() final;
	float ReadFloat() final;
	double ReadDouble() final;
	string ReadString() final;
	interval_t ReadInterval() final;
	hugeint_t ReadHugeInt() final;
	void ReadDataPtr(data_ptr_t &ptr, idx_t count) final;
};

} // namespace duckdb
