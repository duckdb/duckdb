//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/binary_deserializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/encoding_util.hpp"
#include "duckdb/common/serializer/read_stream.hpp"

namespace duckdb {
class ClientContext;

class BinaryDeserializer : public Deserializer {
public:
	explicit BinaryDeserializer(ReadStream &stream) : stream(stream) {
		deserialize_enum_from_string = false;
	}

	template <class T>
	unique_ptr<T> Deserialize() {
		OnObjectBegin();
		auto result = T::Deserialize(*this);
		OnObjectEnd();
		D_ASSERT(nesting_level == 0); // make sure we are at the root level
		return result;
	}

	template <class T>
	static unique_ptr<T> Deserialize(ReadStream &stream) {
		BinaryDeserializer deserializer(stream);
		return deserializer.template Deserialize<T>();
	}

	template <class T>
	static unique_ptr<T> Deserialize(ReadStream &stream, ClientContext &context, bound_parameter_map_t &parameters) {
		BinaryDeserializer deserializer(stream);
		deserializer.Set<ClientContext &>(context);
		deserializer.Set<bound_parameter_map_t &>(parameters);
		return deserializer.template Deserialize<T>();
	}

	void Begin() {
		OnObjectBegin();
	}

	void End() {
		OnObjectEnd();
		D_ASSERT(nesting_level == 0); // make sure we are at the root level
	}

	ReadStream &GetStream() {
		return stream;
	}

private:
	ReadStream &stream;
	idx_t nesting_level = 0;

	// Allow peeking 1 field ahead
	bool has_buffered_field = false;
	field_id_t buffered_field = 0;

private:
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

	void ReadData(data_ptr_t buffer, idx_t read_size) {
		stream.ReadData(buffer, read_size);
	}

	template <class T>
	T ReadPrimitive() {
		T value;
		ReadData(data_ptr_cast(&value), sizeof(T));
		return value;
	}

	template <class T>
	T VarIntDecode() {
		// FIXME: maybe we should pass a source to EncodingUtil instead
		uint8_t buffer[16];
		idx_t varint_size;
		for (varint_size = 0; varint_size < 16; varint_size++) {
			ReadData(buffer + varint_size, 1);
			if (!(buffer[varint_size] & 0x80)) {
				varint_size++;
				break;
			}
		}
		T value;
		auto read_size = EncodingUtil::DecodeLEB128<T>(buffer, value);
		D_ASSERT(read_size == varint_size);
		(void)read_size;
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
