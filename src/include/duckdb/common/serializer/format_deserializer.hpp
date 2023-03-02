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
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

#include "duckdb/common/serializer/serialization_traits.hpp"

namespace duckdb {

class FormatDeserializer {
protected:
	bool deserialize_enum_from_string = false;

public:
	// We fake return-type overloading using templates and enable_if
	template<typename T>
	T ReadProperty(const char* tag) {
		SetTag(tag);
		return std::move(Read<T>());
	}

	template<typename T>
	void ReadProperty(const char* tag, T& ret) {
		SetTag(tag);
		ret = std::move(Read<T>());
	}

	template<typename T>
	T ReadOptionalProperty(const char* tag, T&& default_value){
		SetTag(tag);
		auto present = BeginReadOptional();
		if (!present) {
			return std::forward<T>(default_value);
		}
		return Read<T>();
	}

	// Read an optional property into a reference, if not present the default value argument is forwarded
	template<typename T>
	void ReadOptionalProperty(const char* tag, T& ret, T&& default_value){
		SetTag(tag);
		auto present = BeginReadOptional();
		if (!present) {
			ret = std::forward<T>(default_value);
			return;
		}
		ret = Read<T>();
	}

	// Read an optional property into a reference, if not present the default constructor is used
	template<typename T>
	typename std::enable_if<std::is_default_constructible<T>::value, void>::type
	ReadOptionalProperty(const char* tag, T& ret){
		SetTag(tag);
		auto present = BeginReadOptional();
		if (!present) {
			ret = T();
			return;
		}
		ret = Read<T>();
	}


	// Primitive types
	// Deserialize a bool
	template<typename T>
	typename std::enable_if<std::is_same<T, bool>::value, bool>::type
	Read() {
		return ReadBool();
	}

	// Deserialize a int8_t
	template<typename T>
	typename std::enable_if<std::is_same<T, int8_t>::value, int8_t>::type
	Read() {
		return ReadSignedInt8();
	}

	// Deserialize a uint8_t
	template<typename T>
	typename std::enable_if<std::is_same<T, uint8_t>::value, uint8_t>::type
	Read() {
		return ReadUnsignedInt8();
	}

	// Deserialize a int16_t
	template<typename T>
	typename std::enable_if<std::is_same<T, int16_t>::value, int16_t>::type
	Read() {
		return ReadSignedInt16();
	}

	// Deserialize a uint16_t
	template<typename T>
	typename std::enable_if<std::is_same<T, uint16_t>::value, uint16_t>::type
	Read() {
		return ReadUnsignedInt16();
	}

	// Deserialize a int32_t
	template<typename T>
	typename std::enable_if<std::is_same<T, int32_t>::value, int32_t>::type
	Read() {
		return ReadSignedInt32();
	}

	// Deserialize a uint32_t
	template<typename T>
	typename std::enable_if<std::is_same<T, uint32_t>::value, uint32_t>::type
	Read() {
		return ReadUnsignedInt32();
	}

	// Deserialize a int64_t
	template<typename T>
	typename std::enable_if<std::is_same<T, int64_t>::value, int64_t>::type
	Read() {
		return ReadSignedInt64();
	}

	// Deserialize a uint64_t
	template<typename T>
	typename std::enable_if<std::is_same<T, uint64_t>::value, uint64_t>::type
	Read() {
		return ReadUnsignedInt64();
	}

	// Deserialize a float
	template<typename T>
	typename std::enable_if<std::is_same<T, float>::value, bool>::type
	Read() {
		return ReadFloat();
	}

	// Deserialize a double
	template<typename T>
	typename std::enable_if<std::is_same<T, double>::value, bool>::type
	Read() {
		return ReadDouble();
	}

	// Deserialize a string
	template<typename T>
	typename std::enable_if<std::is_same<T, string>::value, string>::type
	Read() {
		return ReadString();
	}

	// Deserialize a Enum
	template<typename T>
	typename std::enable_if<std::is_enum<T>::value, T>::type
	Read() {
		auto str = ReadString();
		return EnumSerializer::StringToEnum<T>(str.c_str());
	}

	// Deserialize a interval_t
	template<typename T>
	typename std::enable_if<std::is_same<T, interval_t>::value, interval_t>::type
	Read() {
		return ReadInterval();
	}

	// Structural types
	// Deserialize anything implementing a "FormatDeserialize -> unique_ptr<T>" static method
	template<typename T>
	typename std::enable_if<is_unique_ptr<T>::value && has_deserialize<typename is_unique_ptr<T>::inner_type>::value, T>::type
	Read() {
		using inner = typename is_unique_ptr<T>::inner_type;
		BeginReadObject();
		auto ret = std::move(inner::FormatDeserialize(*this));
		EndReadObject();
		return std::move(ret);
	}

	// Deserialize anything implementing a "FormatDeserialize -> shared_ptr<T>" static method
	template<typename T>
	typename std::enable_if<is_shared_ptr<T>::value && has_deserialize<typename is_shared_ptr<T>::inner_type>::value, T>::type
	Read() {
		using inner = typename is_shared_ptr<T>::inner_type;
		BeginReadObject();
		auto ret = std::move(inner::FormatDeserialize(*this));
		EndReadObject();
		return std::move(ret);
	}

	// Deserialize anything implementing a "FormatDeserialize -> T&&" static method
	template <typename T>
	typename std::enable_if<has_deserialize<T>::value, T&&>::type
	Read() {
		BeginReadObject();
		auto ret = std::move(T::FormatDeserialize(*this));
		EndReadObject();
		return std::move(ret);
	};

	// Deserialize a vector
	template<typename T>
	typename std::enable_if<is_vector<T>::value, T&&>::type
	Read() {
		using inner = typename is_vector<T>::inner_type;
		auto size = BeginReadList();
		T items;
		for(idx_t i = 0; i < size; i++) {
			auto item = Read<inner>();
			items.push_back(std::move(item));
		}
		EndReadList();
		return std::move(items);
	}

	// Deserialize a map
	template<typename T>
	typename std::enable_if<is_unordered_map<T>::value, T&&>::type
	Read() {
		using key = typename is_unordered_map<T>::key_type;
		using value = typename is_unordered_map<T>::value_type;
		auto size = BeginReadMap();
		T items;
		for (idx_t i = 0; i < size; i++) {
			key k = Read<key>();
			value v = Read<value>();
			items.emplace(std::move(k), std::move(v));
		}
		EndReadMap();
		return std::move(items);
	}

	// Deserialize a pair
	template<typename T>
	typename std::enable_if<is_pair<T>::value, T&&>::type
	Read() {
		using first = typename is_pair<T>::first_type;
		using second = typename is_pair<T>::second_type;
		BeginReadObject();
		SetTag("key");
		auto first_item = Read<first>();
		SetTag("value");
		auto second_item = Read<second>();
		EndReadObject();
		return std::move(std::make_pair(std::move(first_item), std::move(second_item)));
	}

	// Deserialize a set
	template<typename T>
	typename std::enable_if<is_set<T>::value, T&&>::type
	Read() {
		using inner = typename is_set<T>::inner_type;

		auto size = BeginReadList();
		T items;
		for (idx_t i = 0; i < size; i++) {
			auto item = Read<inner>();
			items.insert(std::move(item));
		}
		EndReadList();
		return std::move(items);
	}

	template<typename T>
	typename std::enable_if<is_unordered_set<T>::value, T&&>::type
	Read() {
		using inner = typename is_unordered_set<T>::inner_type;

		auto size = BeginReadList();
		T items;
		for (idx_t i = 0; i < size; i++) {
			auto item = Read<inner>();
			items.insert(std::move(item));
		}
		EndReadList();
		return std::move(items);
	}

	// Deserialize a list

	// Alternative API
	/*
	template<typename T>
	void ReadValueProperty(const char* tag, T &value) {
		ReadValue(value);
	}

	template <typename T>
	void ReadValue(vector<T> &ret) {
		auto count = BeginReadList();
		for (idx_t i = 0; i < count; i++) {
			T item;
			ReadValue(item);
			ret.push_back(item);
		}
	}

	template <typename T>
	void ReadValue(unique_ptr<T> &ret) {
		ReadValue(*ret);
	}

	template <typename T>
	typename std::enable_if<is_unordered_map<T>::value, void>::type
	ReadValue(T &ret) {
		auto count = BeginReadMap();
		for (idx_t i = 0; i < count; i++) {
			typename is_unordered_map<T>::key_type key;
			typename is_unordered_map<T>::value_type value;
			ReadValue(key);
			ReadValue(value);
			ret.emplace(std::move(key), std::move(value));
		}
	}

	template <typename T>
	typename std::enable_if<has_deserialize<T>::value, void>::type
	ReadValue(T &ret) {
		unique_ptr<T> ptr = T::FormatDeserialize(*this);
		ret = std::move(*ptr);
	}

	void ReadValue(bool &ret) {
		ret = ReadBool();
	}

	void ReadValue(string &ret) {
		ret = ReadString();
	}
	*/

protected:
	virtual void SetTag(const char* tag) = 0;
	virtual void BeginReadObject() = 0;
	virtual void EndReadObject() = 0;
	virtual idx_t BeginReadList() = 0;
	virtual void EndReadList() = 0;
	virtual idx_t BeginReadMap() = 0;
	virtual void EndReadMap() = 0;
	virtual bool BeginReadOptional() = 0;

	virtual bool ReadBool() = 0;
	virtual int8_t ReadSignedInt8() = 0;
	virtual uint8_t ReadUnsignedInt8() = 0;
	virtual int16_t ReadSignedInt16() = 0;
	virtual uint16_t ReadUnsignedInt16() = 0;
	virtual int32_t ReadSignedInt32() = 0;
	virtual uint32_t ReadUnsignedInt32() = 0;
	virtual int64_t ReadSignedInt64() = 0;
	virtual uint64_t ReadUnsignedInt64() = 0;
	virtual float ReadFloat() = 0;
	virtual double ReadDouble() = 0;
	virtual string ReadString() = 0;
	virtual interval_t ReadInterval() = 0;
};

} // namespace duckdb
