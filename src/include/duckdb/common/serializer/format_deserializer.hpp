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
		return Read<T>();
	}

	// Primitive types
	// Deserialize a bool
	template<typename T>
	typename std::enable_if<std::is_same<T, bool>::value, bool>::type
	Read() {
		return ReadBool();
	}

	// Deserialize a float
	template<typename T>
	typename std::enable_if<std::is_same<T, float>::value, bool>::type
	Read() {
		return ReadFloat();
	}

	// Deserialize a uint32_t
	template<typename T>
	typename std::enable_if<std::is_same<T, uint32_t>::value, uint32_t>::type
	Read() {
		return ReadUnsignedInt32();
	}

	// Deserialize a Enum
	template<typename T>
	typename std::enable_if<std::is_enum<T>::value, T>::type
	Read() {
		auto str = ReadString();
		return EnumSerializer::StringToEnum<T>(str.c_str());
	}

	// Structural types
	// Deserialize anything implementing a "FormatDeserialize -> unique_ptr<T>" static method
	template<typename T>
	typename std::enable_if<is_unique_ptr<T>::value && has_deserialize<typename is_unique_ptr<T>::inner_type>::value, T>::type
	Read() {
		using inner = typename is_unique_ptr<T>::inner_type;
		return inner::FormatDeserialize(*this);
	}

	// Deserialize a vector
	template<typename T>
	typename std::enable_if<is_vector<T>::value, T>::type
	Read() {
		using inner = typename is_vector<T>::inner_type;
		auto size = BeginReadList();
		vector<inner> items(size);
		for(idx_t i = 0; i < size; i++) {
			auto item = Read<inner>();
			items.push_back(std::move(item));
		}
		EndReadList();
		return items;
	}

	// Deserialize a map
	/*
	template<typename T>
	typename std::enable_if<is_unordered_map<T>::value, T>::type
	Read() {
		using inner = typename is_unordered_map<T>::inner_type;
		throw NotImplementedException("TODO");
	}
 	*/

protected:
	virtual void SetTag(const char* tag) = 0;
	virtual idx_t BeginReadList() = 0;
	virtual void EndReadList() = 0;

	virtual bool ReadBool() = 0;
	virtual uint32_t ReadUnsignedInt32() = 0;
	virtual int32_t ReadSignedInt32() = 0;
	virtual float ReadFloat() = 0;
	virtual double ReadDouble() = 0;
	virtual string ReadString() = 0;
};

} // namespace duckdb
