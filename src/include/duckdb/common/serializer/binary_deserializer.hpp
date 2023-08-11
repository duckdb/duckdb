//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/binary_deserializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {
class ClientContext;

class BinaryDeserializer : public FormatDeserializer {
public:
	template <class T>
	unique_ptr<T> Deserialize() {
		OnObjectBegin();
		auto result = T::FormatDeserialize(*this);
		OnObjectEnd();
		return result;
	}

	template <class T>
	static unique_ptr<T> Deserialize(data_ptr_t ptr, idx_t length) {
		BinaryDeserializer deserializer(ptr, length);
		return deserializer.template Deserialize<T>();
	}

	template <class T>
	static unique_ptr<T> Deserialize(ClientContext &context, bound_parameter_map_t &parameters, data_ptr_t ptr,
	                                 idx_t length) {
		BinaryDeserializer deserializer(ptr, length);
		deserializer.Set<ClientContext &>(context);
		deserializer.Set<bound_parameter_map_t &>(parameters);
		return deserializer.template Deserialize<T>();
	}

private:
	explicit BinaryDeserializer(data_ptr_t ptr, idx_t length) : ptr(ptr), end_ptr(ptr + length) {
		deserialize_enum_from_string = false;
	}
	struct State {
		uint32_t expected_field_count;
		idx_t expected_size;
		field_id_t expected_field_id;
		uint32_t read_field_count;

		State(uint32_t expected_field_count, idx_t expected_size, field_id_t expected_field_id)
		    : expected_field_count(expected_field_count), expected_size(expected_size),
		      expected_field_id(expected_field_id), read_field_count(0) {
		}
	};

	const char *current_tag = nullptr;
	field_id_t current_field_id = 0;
	data_ptr_t ptr;
	data_ptr_t end_ptr;
	vector<State> stack;

	template <class T>
	T ReadPrimitive() {
		T value;
		ReadData(data_ptr_cast(&value), sizeof(T));
		return value;
	}

	void ReadData(data_ptr_t buffer, idx_t read_size) {
		if (ptr + read_size > end_ptr) {
			throw SerializationException("Failed to deserialize: not enough data in buffer to fulfill read request");
		}
		memcpy(buffer, ptr, read_size);
		ptr += read_size;
	}

	// Set the 'tag' of the property to read
	void SetTag(const field_id_t field_id, const char *tag) final;

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
