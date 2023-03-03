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
	/*
	template<typename T>
	T ReadProperty(const char* tag) {
		SetTag(tag);
		return std::move<T>(Read<T>());
	}
	 */

	template<typename T>
	void ReadProperty(const char* tag, T& ret) {
		SetTag(tag);
		Read(ret);
	}

	template<typename T>
	void ReadOptionalProperty(const char* tag, T& ret, T&& default_value) {
		SetTag(tag);
		auto present = OnOptionalBegin();
		if(present) {
			Read(ret);
		} else {
			ret = std::forward<T>(default_value);
		}
	}

	template<typename T, typename = typename std::enable_if<std::is_default_constructible<T>::value>::type>
	void ReadOptionalProperty(const char* tag, T& ret) {
		SetTag(tag);
		auto present = OnOptionalBegin();
		if(present) {
			Read(ret);
		} // Otherwise leave ret as is
	}

	// Helper: if its default constructible, we can create it ourselves
	template<typename T, typename = typename
	    std::enable_if<std::is_default_constructible<T>::value>::type>
	T ReadProperty(const char* tag) {
		SetTag(tag);
		T ret;
		Read(ret);
		return std::move(ret);
	}

	// Deserialize anything implementing a FormatDeserialize method
	template<typename T, typename = typename std::enable_if<has_deserialize<T>::value>::type>
	void Read(T &ret) {
		ret = T::FormatDeserialize(*this);
	}

	// Structural Types
	// Deserialize a unique_ptr
	template<typename T, typename... ARGS, typename = typename std::enable_if<has_deserialize<T>::value>::type>
	void Read(unique_ptr<T, ARGS...> &ret) {
		ret = std::move(T::FormatDeserialize(*this));
	}

	// Deserialize shared_ptr
	template<typename T, typename... ARGS, typename = typename std::enable_if<has_deserialize<T>::value>::type>
	void Read(shared_ptr<T> &ret) {
		ret = std::move(T::FormatDeserialize(*this));
	}

	// Deserialize a vector
	template<typename T>
	void Read(vector<T> &ret) {
		auto size = ReadUnsignedInt32();
		ret.resize(size);
		for (idx_t i = 0; i < size; i++) {
			Read(ret[i]);
		}
	}

	// Deserialize a map
	template<typename K, typename V, typename ...ARGS>
	void Read(unordered_map<K, V, ARGS...> &ret) {
		auto size = ReadUnsignedInt32();
		for (idx_t i = 0; i < size; i++) {
			K key;
			Read(key);
			Read(ret[key]);
		}
	}

	// Deserialize an unordered set
	template<typename T, typename ...ARGS>
	void Read(unordered_set<T, ARGS...> &ret) {
		auto size = ReadUnsignedInt32();
		for (idx_t i = 0; i < size; i++) {
			T key;
			Read(key);
			ret.insert(key);
		}
	}

	// Deserialize a set
	template<typename T, typename ...ARGS>
	void Read(set<T, ARGS...> &ret) {
		auto size = ReadUnsignedInt32();
		for (idx_t i = 0; i < size; i++) {
			T key;
			Read(key);
			ret.insert(key);
		}
	}

	// Deserialize a pair
	template<typename K, typename V>
	void Read(std::pair<K, V> &ret) {
		OnPairBegin();
		OnPairKeyBegin();
		Read(ret.first);
		OnPairKeyEnd();
		OnPairValueBegin();
		Read(ret.second);
		OnPairValueEnd();
		OnPairEnd();
	}

	// Primitive types
	// Deserialize a bool
	void Read(bool &ret) {
		ret = ReadBool();
	}

	// Deserialize a int8_t
	void Read(int8_t &ret) {
		ret = ReadSignedInt8();
	}

	// Deserialize a uint8_t
	void Read(uint8_t &ret) {
		ret = ReadUnsignedInt8();
	}

	// Deserialize a int16_t
	void Read(int16_t &ret) {
		ret = ReadSignedInt16();
	}

	// Deserialize a uint16_t
	void Read(uint16_t &ret) {
		ret = ReadUnsignedInt16();
	}

	// Deserialize a int32_t
	void Read(int32_t &ret) {
		ret = ReadSignedInt32();
	}

	// Deserialize a uint32_t
	void Read(uint32_t &ret) {
		ret = ReadUnsignedInt32();
	}

	// Deserialize a int64_t
	void Read(int64_t &ret) {
		ret = ReadSignedInt64();
	}

	// Deserialize a uint64_t
	void Read(uint64_t &ret) {
		ret = ReadUnsignedInt64();
	}

	// Deserialize a float
	void Read(float &ret) {
		ret = ReadFloat();
	}

	// Deserialize a double
	void Read(double &ret) {
		ret = ReadDouble();
	}

	// Deserialize a string
	void Read(string &ret) {
		ret = ReadString();
	}

	// Deserialize a Enum
	template<typename T>
	typename std::enable_if<std::is_enum<T>::value, void>::type
	Read(T &ret) {
		auto str = ReadString();
		ret = EnumSerializer::StringToEnum<T>(str.c_str());
	}

	// Deserialize a interval_t
	void Read(interval_t &ret) {
		ret = ReadInterval();
	}



protected:
	virtual void SetTag(const char* tag) { (void)tag; }

	virtual idx_t OnListBegin() = 0;
	virtual void OnListEnd() { }
	virtual idx_t OnMapBegin() = 0;
	virtual void OnMapEnd() { }
	virtual void OnMapEntryBegin() { }
	virtual void OnMapEntryEnd() { }
	virtual void OnMapKeyBegin() { }
	virtual void OnMapKeyEnd() { }
	virtual void OnMapValueBegin() {}
	virtual void OnMapValueEnd() { }
	virtual bool OnOptionalBegin() = 0;
	virtual void OnOptionalEnd() { }
	virtual void OnObjectBegin() { }
	virtual void OnObjectEnd() { }
	virtual void OnPairBegin() { }
	virtual void OnPairKeyBegin() { }
	virtual void OnPairKeyEnd() { }
	virtual void OnPairValueBegin() { }
	virtual void OnPairValueEnd() { }
	virtual void OnPairEnd() { }

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
