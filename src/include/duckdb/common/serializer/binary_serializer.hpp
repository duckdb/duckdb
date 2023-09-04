//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/binary_serializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/serializer/encoding_util.hpp"

namespace duckdb {

struct BinarySerializer : public FormatSerializer {
private:
	struct DebugState {
		unordered_set<const char *> seen_field_tags;
		unordered_set<field_id_t> seen_field_ids;
		vector<pair<const char *, field_id_t>> seen_fields;
	};

	vector<DebugState> debug_stack;
	vector<data_t> data;

	template <class T>
	void Write(T element) {
		static_assert(std::is_trivially_destructible<T>(), "Write element must be trivially destructible");
		WriteDataInternal(const_data_ptr_cast(&element), sizeof(T));
	}
	void WriteDataInternal(const_data_ptr_t buffer, idx_t write_size) {
		data.insert(data.end(), buffer, buffer + write_size);
	}
	void WriteDataInternal(const char *ptr, idx_t write_size) {
		WriteDataInternal(const_data_ptr_cast(ptr), write_size);
	}

	template <class T>
	void VarIntEncode(T value) {
		uint8_t buffer[16];
		auto write_size = EncodingUtil::EncodeLEB128<T>(buffer, value);
		D_ASSERT(write_size <= sizeof(buffer));
		WriteDataInternal(buffer, write_size);
	}

	explicit BinarySerializer(bool serialize_default_values_p) {
		serialize_default_values = serialize_default_values_p;
		serialize_enum_as_string = false;
	}

public:
	//! Serializes the given object into a binary blob, optionally serializing default values if
	//! serialize_default_values is set to true, otherwise properties set to their provided default value
	//! will not be serialized
	template <class T>
	static vector<data_t> Serialize(T &obj, bool serialize_default_values) {
		BinarySerializer serializer(serialize_default_values);
		serializer.OnObjectBegin();
		obj.FormatSerialize(serializer);
		serializer.OnObjectEnd();
		return std::move(serializer.data);
	}

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
	void WriteValue(float value) final;
	void WriteValue(double value) final;
	void WriteValue(const string_t value) final;
	void WriteValue(const string &value) final;
	void WriteValue(const char *value) final;
	void WriteValue(bool value) final;
	void WriteDataPtr(const_data_ptr_t ptr, idx_t count) final;
};

} // namespace duckdb
