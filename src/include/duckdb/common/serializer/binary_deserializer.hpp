//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/binary_deserializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/format_deserializer.hpp"
#include "duckdb/common/serializer/binary_common.hpp"

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
		data_ptr_t start_offset;
		idx_t expected_size;
		field_id_t expected_field_id;

		State(data_ptr_t start_offset, idx_t expected_size, field_id_t expected_field_id)
		    : start_offset(start_offset), expected_size(expected_size), expected_field_id(expected_field_id) {
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
			throw InternalException("Failed to deserialize: not enough data in buffer to fulfill read request");
		}
		memcpy(buffer, ptr, read_size);
		ptr += read_size;
	}

	void ReadField(field_id_t field_id, BinaryMessageKind kind) {
		auto read_field_id = ReadPrimitive<uint32_t>();
		if (read_field_id != field_id) {
			throw InternalException("Failed to deserialize: field id mismatch, expected: %d, got: %d", field_id,
			                             read_field_id);
		}
		auto read_kind = static_cast<BinaryMessageKind>(ReadPrimitive<uint8_t>());
		if (read_kind != kind) {
			throw InternalException("Failed to deserialize: message kind mismatch, expected: %d, got: %d", kind,
			                             read_kind);
		}
	}

	// Set the 'tag' of the property to read
	void SetTag(const field_id_t field_id, const char *tag) final;
	bool HasTag(const field_id_t field_id, const char *tag) final;
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
	void OnOptionalEnd() final;

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
	hugeint_t ReadHugeInt() final;
	void ReadDataPtr(data_ptr_t &ptr, idx_t count) final;
};

} // namespace duckdb
