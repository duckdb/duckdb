#pragma once

#include "duckdb/common/serializer/format_serializer.hpp"

namespace duckdb {

struct BinarySerializer : public FormatSerializer {
public:
	explicit BinarySerializer() {
		stack.push_back(State());
		serialize_enum_as_string = false;
	}

private:
	struct State {
		uint32_t field_count;
		BufferedSerializer buffer;
	};

	const char *current_tag;
	vector<State> stack;

public:
	uint32_t GetRootFieldCount() {
		return stack.back().field_count;
	}
	data_ptr_t GetRootBlobData() {
		return stack.back().buffer.blob.data.get();
	}
	idx_t GetRootBlobSize() {
		return stack.back().buffer.blob.size;
	}

	void SetTag(const char *tag) final;

	//===--------------------------------------------------------------------===//
	// Nested Types Hooks
	//===--------------------------------------------------------------------===//
	void OnOptionalBegin(bool present) final;
	void OnListBegin(idx_t count) final;
	void OnListEnd(idx_t count) final;
	void OnMapBegin(idx_t count) final;
	void OnMapEntryBegin() final;
	void OnMapEntryEnd() final;
	void OnMapKeyBegin() final;
	void OnMapValueBegin() final;
	void OnMapEnd(idx_t count) final;
	void OnObjectBegin() final;
	void OnObjectEnd() final;
	void OnPairBegin() final;
	void OnPairKeyBegin() final;
	void OnPairValueBegin() final;
	void OnPairEnd() final;

	//===--------------------------------------------------------------------===//
	// Primitive Types
	//===--------------------------------------------------------------------===//
	void WriteNull() final;
	void WriteValue(uint8_t value) final;
	void WriteValue(int8_t value) final;
	void WriteValue(uint16_t value) final;
	void WriteValue(int16_t value) final;
	void WriteValue(uint32_t value) final;
	void WriteValue(int32_t value) final;
	void WriteValue(uint64_t value) final;
	void WriteValue(int64_t value) final;
	void WriteValue(hugeint_t value) final;
	void WriteValue(float value) final;
	void WriteValue(double value) final;
	void WriteValue(interval_t value) final;
	void WriteValue(const string_t value) final;
	void WriteValue(const string &value) final;
	void WriteValue(const char *value) final;
	void WriteValue(bool value) final;
	void WriteDataPtr(const_data_ptr_t ptr, idx_t count) final;
};

} // namespace duckdb
