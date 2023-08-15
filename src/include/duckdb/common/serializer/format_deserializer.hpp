//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/format_serializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/serializer/serialization_traits.hpp"
#include "duckdb/common/serializer/deserialization_data.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

class FormatDeserializer {
	friend Vector;

protected:
	bool deserialize_enum_from_string = false;
	DeserializationData data;

public:
	// Read into an existing value
	template <typename T>
	inline void ReadProperty(const field_id_t field_id, const char *tag, T &ret) {
		SetTag(field_id, tag);
		ret = Read<T>();
	}

	// Read and return a value
	template <typename T>
	inline T ReadProperty(const field_id_t field_id, const char *tag) {
		SetTag(field_id, tag);
		return Read<T>();
	}

	// Read optional property and return a value, or forward a default value
	template <typename T>
	inline T ReadOptionalPropertyOrDefault(const field_id_t field_id, const char *tag, T &&default_value) {
		SetTag(field_id, tag);
		auto present = OnOptionalBegin();
		if (present) {
			auto item = Read<T>();
			OnOptionalEnd();
			return item;
		} else {
			OnOptionalEnd();
			return std::forward<T>(default_value);
		}
	}

	// Read optional property into an existing value, or use a default value
	template <typename T>
	inline void ReadOptionalPropertyOrDefault(const field_id_t field_id, const char *tag, T &ret, T &&default_value) {
		SetTag(field_id, tag);
		auto present = OnOptionalBegin();
		if (present) {
			ret = Read<T>();
			OnOptionalEnd();
		} else {
			ret = std::forward<T>(default_value);
			OnOptionalEnd();
		}
	}

	// Read optional property and return a value, or default construct it
	template <typename T>
	inline typename std::enable_if<std::is_default_constructible<T>::value, T>::type
	ReadOptionalProperty(const field_id_t field_id, const char *tag) {
		SetTag(field_id, tag);
		auto present = OnOptionalBegin();
		if (present) {
			auto item = Read<T>();
			OnOptionalEnd();
			return item;
		} else {
			OnOptionalEnd();
			return T();
		}
	}

	// Read optional property into an existing value, or default construct it
	template <typename T>
	inline typename std::enable_if<std::is_default_constructible<T>::value, void>::type
	ReadOptionalProperty(const field_id_t field_id, const char *tag, T &ret) {
		SetTag(field_id, tag);
		auto present = OnOptionalBegin();
		if (present) {
			ret = Read<T>();
			OnOptionalEnd();
		} else {
			ret = T();
			OnOptionalEnd();
		}
	}

	// Special case:
	// Read into an existing data_ptr_t
	inline void ReadProperty(const field_id_t field_id, const char *tag, data_ptr_t ret, idx_t count) {
		SetTag(field_id, tag);
		ReadDataPtr(ret, count);
	}

	//! Set a serialization property
	template <class T>
	void Set(T entry) {
		return data.Set<T>(entry);
	}

	//! Retrieve the last set serialization property of this type
	template <class T>
	T Get() {
		return data.Get<T>();
	}

	//! Unset a serialization property
	template <class T>
	void Unset() {
		return data.Unset<T>();
	}

	// Manually begin an object - should be followed by EndObject
	void BeginObject(const field_id_t field_id, const char *tag) {
		SetTag(field_id, tag);
		OnObjectBegin();
	}

	void EndObject() {
		OnObjectEnd();
	}

private:
	// Deserialize anything implementing a FormatDeserialize method
	template <typename T = void>
	inline typename std::enable_if<has_deserialize<T>::value, T>::type Read() {
		OnObjectBegin();
		auto val = T::FormatDeserialize(*this);
		OnObjectEnd();
		return val;
	}

	// Structural Types
	// Deserialize a unique_ptr
	template <class T = void>
	inline typename std::enable_if<is_unique_ptr<T>::value, T>::type Read() {
		using ELEMENT_TYPE = typename is_unique_ptr<T>::ELEMENT_TYPE;
		OnObjectBegin();
		auto val = ELEMENT_TYPE::FormatDeserialize(*this);
		OnObjectEnd();
		return val;
	}

	// Deserialize shared_ptr
	template <typename T = void>
	inline typename std::enable_if<is_shared_ptr<T>::value, T>::type Read() {
		using ELEMENT_TYPE = typename is_shared_ptr<T>::ELEMENT_TYPE;
		OnObjectBegin();
		auto val = ELEMENT_TYPE::FormatDeserialize(*this);
		OnObjectEnd();
		return val;
	}

	// Deserialize a vector
	template <typename T = void>
	inline typename std::enable_if<is_vector<T>::value, T>::type Read() {
		using ELEMENT_TYPE = typename is_vector<T>::ELEMENT_TYPE;
		T vec;
		auto size = OnListBegin();
		for (idx_t i = 0; i < size; i++) {
			vec.push_back(Read<ELEMENT_TYPE>());
		}
		OnListEnd();

		return vec;
	}

	template <typename T = void>
	inline typename std::enable_if<is_unsafe_vector<T>::value, T>::type Read() {
		using ELEMENT_TYPE = typename is_unsafe_vector<T>::ELEMENT_TYPE;
		T vec;
		auto size = OnListBegin();
		for (idx_t i = 0; i < size; i++) {
			vec.push_back(Read<ELEMENT_TYPE>());
		}
		OnListEnd();

		return vec;
	}

	// Deserialize a map
	template <typename T = void>
	inline typename std::enable_if<is_unordered_map<T>::value, T>::type Read() {
		using KEY_TYPE = typename is_unordered_map<T>::KEY_TYPE;
		using VALUE_TYPE = typename is_unordered_map<T>::VALUE_TYPE;

		T map;
		auto size = OnMapBegin();
		for (idx_t i = 0; i < size; i++) {
			OnMapEntryBegin();
			OnMapKeyBegin();
			auto key = Read<KEY_TYPE>();
			OnMapKeyEnd();
			OnMapValueBegin();
			auto value = Read<VALUE_TYPE>();
			OnMapValueEnd();
			OnMapEntryEnd();
			map[std::move(key)] = std::move(value);
		}
		OnMapEnd();
		return map;
	}

	template <typename T = void>
	inline typename std::enable_if<is_map<T>::value, T>::type Read() {
		using KEY_TYPE = typename is_map<T>::KEY_TYPE;
		using VALUE_TYPE = typename is_map<T>::VALUE_TYPE;

		T map;
		auto size = OnMapBegin();
		for (idx_t i = 0; i < size; i++) {
			OnMapEntryBegin();
			OnMapKeyBegin();
			auto key = Read<KEY_TYPE>();
			OnMapKeyEnd();
			OnMapValueBegin();
			auto value = Read<VALUE_TYPE>();
			OnMapValueEnd();
			OnMapEntryEnd();
			map[std::move(key)] = std::move(value);
		}
		OnMapEnd();
		return map;
	}

	// Deserialize an unordered set
	template <typename T = void>
	inline typename std::enable_if<is_unordered_set<T>::value, T>::type Read() {
		using ELEMENT_TYPE = typename is_unordered_set<T>::ELEMENT_TYPE;
		auto size = OnListBegin();
		T set;
		for (idx_t i = 0; i < size; i++) {
			set.insert(Read<ELEMENT_TYPE>());
		}
		OnListEnd();
		return set;
	}

	// Deserialize a set
	template <typename T = void>
	inline typename std::enable_if<is_set<T>::value, T>::type Read() {
		using ELEMENT_TYPE = typename is_set<T>::ELEMENT_TYPE;
		auto size = OnListBegin();
		T set;
		for (idx_t i = 0; i < size; i++) {
			set.insert(Read<ELEMENT_TYPE>());
		}
		OnListEnd();
		return set;
	}

	// Deserialize a pair
	template <typename T = void>
	inline typename std::enable_if<is_pair<T>::value, T>::type Read() {
		using FIRST_TYPE = typename is_pair<T>::FIRST_TYPE;
		using SECOND_TYPE = typename is_pair<T>::SECOND_TYPE;

		OnPairBegin();
		OnPairKeyBegin();
		FIRST_TYPE first = Read<FIRST_TYPE>();
		OnPairKeyEnd();
		OnPairValueBegin();
		SECOND_TYPE second = Read<SECOND_TYPE>();
		OnPairValueEnd();
		OnPairEnd();
		return std::make_pair(first, second);
	}

	// Primitive types
	// Deserialize a bool
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, bool>::value, T>::type Read() {
		return ReadBool();
	}

	// Deserialize a int8_t
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, int8_t>::value, T>::type Read() {
		return ReadSignedInt8();
	}

	// Deserialize a uint8_t
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, uint8_t>::value, T>::type Read() {
		return ReadUnsignedInt8();
	}

	// Deserialize a int16_t
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, int16_t>::value, T>::type Read() {
		return ReadSignedInt16();
	}

	// Deserialize a uint16_t
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, uint16_t>::value, T>::type Read() {
		return ReadUnsignedInt16();
	}

	// Deserialize a int32_t
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, int32_t>::value, T>::type Read() {
		return ReadSignedInt32();
	}

	// Deserialize a uint32_t
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, uint32_t>::value, T>::type Read() {
		return ReadUnsignedInt32();
	}

	// Deserialize a int64_t
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, int64_t>::value, T>::type Read() {
		return ReadSignedInt64();
	}

	// Deserialize a uint64_t
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, uint64_t>::value, T>::type Read() {
		return ReadUnsignedInt64();
	}

	// Deserialize a float
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, float>::value, T>::type Read() {
		return ReadFloat();
	}

	// Deserialize a double
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, double>::value, T>::type Read() {
		return ReadDouble();
	}

	// Deserialize a string
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, string>::value, T>::type Read() {
		return ReadString();
	}

	// Deserialize a Enum
	template <typename T = void>
	inline typename std::enable_if<std::is_enum<T>::value, T>::type Read() {
		if (deserialize_enum_from_string) {
			auto str = ReadString();
			return EnumUtil::FromString<T>(str.c_str());
		} else {
			return (T)Read<typename std::underlying_type<T>::type>();
		}
	}

	// Deserialize a interval_t
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, interval_t>::value, T>::type Read() {
		return ReadInterval();
	}

	// Deserialize a hugeint_t
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, hugeint_t>::value, T>::type Read() {
		return ReadHugeInt();
	}

	// Deserialize a LogicalIndex
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, LogicalIndex>::value, T>::type Read() {
		return LogicalIndex(ReadUnsignedInt64());
	}

	// Deserialize a PhysicalIndex
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, PhysicalIndex>::value, T>::type Read() {
		return PhysicalIndex(ReadUnsignedInt64());
	}

protected:
	virtual void SetTag(const field_id_t field_id, const char *tag) {
		(void)field_id;
		(void)tag;
	}

	virtual idx_t OnListBegin() = 0;
	virtual void OnListEnd() {
	}
	virtual idx_t OnMapBegin() = 0;
	virtual void OnMapEnd() {
	}
	virtual void OnMapEntryBegin() {
	}
	virtual void OnMapEntryEnd() {
	}
	virtual void OnMapKeyBegin() {
	}
	virtual void OnMapKeyEnd() {
	}
	virtual void OnMapValueBegin() {
	}
	virtual void OnMapValueEnd() {
	}
	virtual bool OnOptionalBegin() = 0;
	virtual void OnOptionalEnd() {
	}
	virtual void OnObjectBegin() {
	}
	virtual void OnObjectEnd() {
	}
	virtual void OnPairBegin() {
	}
	virtual void OnPairKeyBegin() {
	}
	virtual void OnPairKeyEnd() {
	}
	virtual void OnPairValueBegin() {
	}
	virtual void OnPairValueEnd() {
	}
	virtual void OnPairEnd() {
	}

	virtual bool ReadBool() = 0;
	virtual int8_t ReadSignedInt8() = 0;
	virtual uint8_t ReadUnsignedInt8() = 0;
	virtual int16_t ReadSignedInt16() = 0;
	virtual uint16_t ReadUnsignedInt16() = 0;
	virtual int32_t ReadSignedInt32() = 0;
	virtual uint32_t ReadUnsignedInt32() = 0;
	virtual int64_t ReadSignedInt64() = 0;
	virtual uint64_t ReadUnsignedInt64() = 0;
	virtual hugeint_t ReadHugeInt() = 0;
	virtual float ReadFloat() = 0;
	virtual double ReadDouble() = 0;
	virtual string ReadString() = 0;
	virtual interval_t ReadInterval() = 0;
	virtual void ReadDataPtr(data_ptr_t &ptr, idx_t count) = 0;
};

} // namespace duckdb
