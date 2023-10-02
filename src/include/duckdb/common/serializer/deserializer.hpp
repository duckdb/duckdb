//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/format_serializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/serializer/serialization_traits.hpp"
#include "duckdb/common/serializer/deserialization_data.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

class Deserializer {
protected:
	bool deserialize_enum_from_string = false;
	DeserializationData data;

public:
	virtual ~Deserializer() {
	}

	class List {
		friend Deserializer;

	private:
		Deserializer &deserializer;
		explicit List(Deserializer &deserializer) : deserializer(deserializer) {
		}

	public:
		// Deserialize an element
		template <class T>
		T ReadElement();

		// Deserialize an object
		template <class FUNC>
		void ReadObject(FUNC f);
	};

public:
	// Read into an existing value
	template <typename T>
	inline void ReadProperty(const field_id_t field_id, const char *tag, T &ret) {
		OnPropertyBegin(field_id, tag);
		ret = Read<T>();
		OnPropertyEnd();
	}

	// Read and return a value
	template <typename T>
	inline T ReadProperty(const field_id_t field_id, const char *tag) {
		OnPropertyBegin(field_id, tag);
		auto ret = Read<T>();
		OnPropertyEnd();
		return ret;
	}

	// Default Value return
	template <typename T>
	inline T ReadPropertyWithDefault(const field_id_t field_id, const char *tag) {
		if (!OnOptionalPropertyBegin(field_id, tag)) {
			OnOptionalPropertyEnd(false);
			return std::forward<T>(SerializationDefaultValue::GetDefault<T>());
		}
		auto ret = Read<T>();
		OnOptionalPropertyEnd(true);
		return ret;
	}

	template <typename T>
	inline T ReadPropertyWithDefault(const field_id_t field_id, const char *tag, T &&default_value) {
		if (!OnOptionalPropertyBegin(field_id, tag)) {
			OnOptionalPropertyEnd(false);
			return std::forward<T>(default_value);
		}
		auto ret = Read<T>();
		OnOptionalPropertyEnd(true);
		return ret;
	}

	// Default value in place
	template <typename T>
	inline void ReadPropertyWithDefault(const field_id_t field_id, const char *tag, T &ret) {
		if (!OnOptionalPropertyBegin(field_id, tag)) {
			ret = std::forward<T>(SerializationDefaultValue::GetDefault<T>());
			OnOptionalPropertyEnd(false);
			return;
		}
		ret = Read<T>();
		OnOptionalPropertyEnd(true);
	}

	template <typename T>
	inline void ReadPropertyWithDefault(const field_id_t field_id, const char *tag, T &ret, T &&default_value) {
		if (!OnOptionalPropertyBegin(field_id, tag)) {
			ret = std::forward<T>(default_value);
			OnOptionalPropertyEnd(false);
			return;
		}
		ret = Read<T>();
		OnOptionalPropertyEnd(true);
	}

	// Special case:
	// Read into an existing data_ptr_t
	inline void ReadProperty(const field_id_t field_id, const char *tag, data_ptr_t ret, idx_t count) {
		OnPropertyBegin(field_id, tag);
		ReadDataPtr(ret, count);
		OnPropertyEnd();
	}

	// Try to read a property, if it is not present, continue, otherwise read and discard the value
	template <typename T>
	inline void ReadDeletedProperty(const field_id_t field_id, const char *tag) {
		// Try to read the property. If not present, great!
		if (!OnOptionalPropertyBegin(field_id, tag)) {
			OnOptionalPropertyEnd(false);
			return;
		}
		// Otherwise read and discard the value
		(void)Read<T>();
		OnOptionalPropertyEnd(true);
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

	template <class FUNC>
	void ReadList(const field_id_t field_id, const char *tag, FUNC func) {
		OnPropertyBegin(field_id, tag);
		auto size = OnListBegin();
		List list {*this};
		for (idx_t i = 0; i < size; i++) {
			func(list, i);
		}
		OnListEnd();
		OnPropertyEnd();
	}

	template <class FUNC>
	void ReadObject(const field_id_t field_id, const char *tag, FUNC func) {
		OnPropertyBegin(field_id, tag);
		OnObjectBegin();
		func(*this);
		OnObjectEnd();
		OnPropertyEnd();
	}

private:
	// Deserialize anything implementing a Deserialize method
	template <typename T = void>
	inline typename std::enable_if<has_deserialize<T>::value, T>::type Read() {
		OnObjectBegin();
		auto val = T::Deserialize(*this);
		OnObjectEnd();
		return val;
	}

	template <class T = void>
	inline typename std::enable_if<is_unique_ptr<T>::value, T>::type Read() {
		using ELEMENT_TYPE = typename is_unique_ptr<T>::ELEMENT_TYPE;
		unique_ptr<ELEMENT_TYPE> ptr = nullptr;
		auto is_present = OnNullableBegin();
		if (is_present) {
			OnObjectBegin();
			ptr = ELEMENT_TYPE::Deserialize(*this);
			OnObjectEnd();
		}
		OnNullableEnd();
		return ptr;
	}

	// Deserialize shared_ptr
	template <typename T = void>
	inline typename std::enable_if<is_shared_ptr<T>::value, T>::type Read() {
		using ELEMENT_TYPE = typename is_shared_ptr<T>::ELEMENT_TYPE;
		shared_ptr<ELEMENT_TYPE> ptr = nullptr;
		auto is_present = OnNullableBegin();
		if (is_present) {
			OnObjectBegin();
			ptr = ELEMENT_TYPE::Deserialize(*this);
			OnObjectEnd();
		}
		OnNullableEnd();
		return ptr;
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
		auto size = OnListBegin();
		for (idx_t i = 0; i < size; i++) {
			OnObjectBegin();
			auto key = ReadProperty<KEY_TYPE>(0, "key");
			auto value = ReadProperty<VALUE_TYPE>(1, "value");
			OnObjectEnd();
			map[std::move(key)] = std::move(value);
		}
		OnListEnd();
		return map;
	}

	template <typename T = void>
	inline typename std::enable_if<is_map<T>::value, T>::type Read() {
		using KEY_TYPE = typename is_map<T>::KEY_TYPE;
		using VALUE_TYPE = typename is_map<T>::VALUE_TYPE;

		T map;
		auto size = OnListBegin();
		for (idx_t i = 0; i < size; i++) {
			OnObjectBegin();
			auto key = ReadProperty<KEY_TYPE>(0, "key");
			auto value = ReadProperty<VALUE_TYPE>(1, "value");
			OnObjectEnd();
			map[std::move(key)] = std::move(value);
		}
		OnListEnd();
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
		OnObjectBegin();
		auto first = ReadProperty<FIRST_TYPE>(0, "first");
		auto second = ReadProperty<SECOND_TYPE>(1, "second");
		OnObjectEnd();
		return std::make_pair(first, second);
	}

	// Primitive types
	// Deserialize a bool
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, bool>::value, T>::type Read() {
		return ReadBool();
	}

	// Deserialize a char
	template <typename T = void>
	inline typename std::enable_if<std::is_same<T, char>::value, T>::type Read() {
		return ReadChar();
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
	// Hooks for subclasses to override to implement custom behavior
	virtual void OnPropertyBegin(const field_id_t field_id, const char *tag) = 0;
	virtual void OnPropertyEnd() = 0;
	virtual bool OnOptionalPropertyBegin(const field_id_t field_id, const char *tag) = 0;
	virtual void OnOptionalPropertyEnd(bool present) = 0;

	virtual void OnObjectBegin() = 0;
	virtual void OnObjectEnd() = 0;
	virtual idx_t OnListBegin() = 0;
	virtual void OnListEnd() = 0;
	virtual bool OnNullableBegin() = 0;
	virtual void OnNullableEnd() = 0;

	// Handle primitive types, a serializer needs to implement these.
	virtual bool ReadBool() = 0;
	virtual char ReadChar() {
		throw NotImplementedException("ReadChar not implemented");
	}
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
	virtual void ReadDataPtr(data_ptr_t &ptr, idx_t count) = 0;
};

template <class FUNC>
void Deserializer::List::ReadObject(FUNC f) {
	deserializer.OnObjectBegin();
	f(deserializer);
	deserializer.OnObjectEnd();
}

template <class T>
T Deserializer::List::ReadElement() {
	return deserializer.Read<T>();
}

} // namespace duckdb
