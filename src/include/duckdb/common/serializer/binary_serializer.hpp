//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/binary_serializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/pair.hpp"
#include "duckdb/common/serializer/encoding_util.hpp"
#include "duckdb/common/serializer/serialization_data.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/write_stream.hpp"

namespace duckdb {

class BinarySerializer : public Serializer {
public:
	explicit BinarySerializer(WriteStream &stream, SerializationOptions options_p = SerializationOptions())
	    : stream(stream) {
		options = std::move(options_p);
		// Override the value set by the passed in SerializationOptions
		options.serialize_enum_as_string = false;
	}

private:
	struct DebugState {
		unordered_set<const char *> seen_field_tags;
		unordered_set<field_id_t> seen_field_ids;
		vector<pair<const char *, field_id_t>> seen_fields;
	};

	void WriteData(const_data_ptr_t buffer, idx_t write_size) {
		stream.WriteData(buffer, write_size);
	}

	template <class T>
	void Write(T element) {
		static_assert(std::is_trivially_destructible<T>(), "Write element must be trivially destructible");
		WriteData(const_data_ptr_cast(&element), sizeof(T));
	}
	void WriteData(const char *ptr, idx_t write_size) {
		WriteData(const_data_ptr_cast(ptr), write_size);
	}

	template <class T>
	void VarIntEncode(T value) {
		uint8_t buffer[16] = {};
		auto write_size = EncodingUtil::EncodeLEB128<T>(buffer, value);
		D_ASSERT(write_size <= sizeof(buffer));
		WriteData(buffer, write_size);
	}

public:
	template <class T>
	static void Serialize(const T &value, WriteStream &stream, SerializationOptions options = SerializationOptions()) {
		BinarySerializer serializer(stream, std::move(options));
		serializer.OnObjectBegin();
		value.Serialize(serializer);
		serializer.OnObjectEnd();
	}

	void Begin() {
		OnObjectBegin();
	}
	void End() {
		OnObjectEnd();
	}

protected:
	//-------------------------------------------------------------------------
	// Nested Type Hooks
	//-------------------------------------------------------------------------
	// We serialize optional values as a message with a "present" flag, followed by the value.
	void OnPropertyBegin(const field_id_t field_id, const char *tag) final;
	void OnPropertyEnd() final;
	void OnOptionalPropertyBegin(const field_id_t field_id, const char *tag, bool present) final;
	void OnOptionalPropertyEnd(bool present) final;
	void OnListBegin(idx_t count) final;
	void OnListEnd() final;
	void OnObjectBegin() final;
	void OnObjectEnd() final;
	void OnNullableBegin(bool present) final;
	void OnNullableEnd() final;

	//-------------------------------------------------------------------------
	// Primitive Types
	//-------------------------------------------------------------------------
	void WriteNull() final;
	void WriteValue(char value) final;
	void WriteValue(uint8_t value) final;
	void WriteValue(int8_t value) final;
	void WriteValue(uint16_t value) final;
	void WriteValue(int16_t value) final;
	void WriteValue(uint32_t value) final;
	void WriteValue(int32_t value) final;
	void WriteValue(uint64_t value) final;
	void WriteValue(int64_t value) final;
	void WriteValue(hugeint_t value) final;
	void WriteValue(uhugeint_t value) final;
	void WriteValue(float value) final;
	void WriteValue(double value) final;
	void WriteValue(const string_t value) final;
	void WriteValue(const string &value) final;
	void WriteValue(const char *value) final;
	void WriteValue(bool value) final;
	void WriteDataPtr(const_data_ptr_t ptr, idx_t count) final;

private:
	vector<DebugState> debug_stack;
	WriteStream &stream;

protected:
	duckdb::SerializationData data;
};

} // namespace duckdb
