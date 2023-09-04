//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/binary_deserializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/format_deserializer.hpp"
#include "duckdb/common/serializer/encoding_util.hpp"

namespace duckdb {
class ClientContext;

class BinaryDeserializer : public FormatDeserializer {
public:
	template <class T>
	unique_ptr<T> Deserialize() {
		OnObjectBegin();
		auto result = T::FormatDeserialize(*this);
		OnObjectEnd();
		D_ASSERT(nesting_level == 0); // make sure we are at the root level
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

	data_ptr_t ptr;
	data_ptr_t end_ptr;
	idx_t nesting_level = 0;

	// Allow peeking 1 field ahead
	bool has_buffered_field = false;
	field_id_t buffered_field = 0;
	field_id_t PeekField() {
		if (!has_buffered_field) {
			buffered_field = ReadPrimitive<field_id_t>();
			has_buffered_field = true;
		}
		return buffered_field;
	}
	void ConsumeField() {
		if (!has_buffered_field) {
			buffered_field = ReadPrimitive<field_id_t>();
		} else {
			has_buffered_field = false;
		}
	}
	field_id_t NextField() {
		if (has_buffered_field) {
			has_buffered_field = false;
			return buffered_field;
		}
		return ReadPrimitive<field_id_t>();
	}

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

	template <class T>
	T VarIntDecode() {
		T value;
		auto read_size = EncodingUtil::DecodeLEB128<T>(ptr, value);
		ptr += read_size;
		return value;
	}

	//===--------------------------------------------------------------------===//
	// Nested Types Hooks
	//===--------------------------------------------------------------------===//
	void OnPropertyBegin(const field_id_t field_id, const char *tag) final;
	void OnPropertyEnd() final;
	bool OnOptionalPropertyBegin(const field_id_t field_id, const char *tag) final;
	void OnOptionalPropertyEnd(bool present) final;
	void OnObjectBegin() final;
	void OnObjectEnd() final;
	idx_t OnListBegin() final;
	void OnListEnd() final;
	bool OnNullableBegin() final;
	void OnNullableEnd() final;

	//===--------------------------------------------------------------------===//
	// Primitive Types
	//===--------------------------------------------------------------------===//
	bool ReadBool() final;
	char ReadChar() final;
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
