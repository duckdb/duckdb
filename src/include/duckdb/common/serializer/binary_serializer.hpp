//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/binary_serializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/format_serializer.hpp"

namespace duckdb {

struct BinarySerializer : public FormatSerializer {
private:
	struct State {
		// how many fields are present in the object
		uint32_t field_count;
		// the size of the object
		uint64_t size;
		// the offset of the object start in the buffer
		uint64_t offset;
	};

	const char *current_tag;
	field_id_t current_field_id = 0;

	vector<data_t> data;
	vector<State> stack;

	template <class T>
	void Write(T element) {
		static_assert(std::is_trivially_destructible<T>(), "Write element must be trivially destructible");
		WriteDataInternal(const_data_ptr_cast(&element), sizeof(T));
	}
	void WriteDataInternal(const_data_ptr_t buffer, idx_t write_size) {
		data.insert(data.end(), buffer, buffer + write_size);
		stack.back().size += write_size;
	}
	void WriteDataInternal(const char *ptr, idx_t write_size) {
		WriteDataInternal(const_data_ptr_cast(ptr), write_size);
	}

	explicit BinarySerializer() {
		serialize_enum_as_string = false;
	}

public:
	template <class T>
	static vector<data_t> Serialize(T &obj) {
		BinarySerializer serializer;
		serializer.OnObjectBegin();
		obj.FormatSerialize(serializer);
		serializer.OnObjectEnd();
		return std::move(serializer.data);
	}

	void SetTag(const field_id_t field_id, const char *tag) final;

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
