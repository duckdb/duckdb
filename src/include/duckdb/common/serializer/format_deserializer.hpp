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

	// Structural types
	// Deserialize anything implementing a Deserialize static method
	template<typename T>
	typename std::enable_if<has_deserialize_v<T>(), unique_ptr<T>>::type
	Read() {
		return T::FormatDeserialize(*this);
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
			items.push_back(item);
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
