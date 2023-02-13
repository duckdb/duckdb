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

#include <type_traits>

namespace duckdb {

class FormatSerializer; // Forward declare

// Type trait helpers for anything implementing a `void FormatSerialize(FormatSerializer &FormatSerializer)`
template <class...>
using void_t = void; // Backport to c++11

template <typename SELF, typename = void_t<>>
struct has_serialize : std::false_type {};

template <typename SELF>
struct has_serialize<SELF, void_t<decltype(std::declval<SELF>().FormatSerialize(std::declval<FormatSerializer &>()))>>
    : std::true_type {};

template <typename SELF>
constexpr bool has_serialize_v() {
	return has_serialize<SELF>::value;
}

class FormatSerializer {
protected:
	bool serialize_enum_as_string = false;
public:
	// Pass by value
	template <class T>
	typename std::enable_if<std::is_trivially_copyable<T>::value && !std::is_enum<T>::value, void>::type
	WriteProperty(const char *tag, T value) {
		WriteTag(tag);
		WriteValue(value);
	}

	// Pass by reference
	template <class T>
	typename std::enable_if<!std::is_trivially_copyable<T>::value, void>::type
	WriteProperty(const char *tag, T &value) {
		WriteTag(tag);
		WriteValue(value);
	}

	// Serialize a range
	template <class T>
	typename std::enable_if<std::is_pointer<T>::value, void>::type WriteProperty(const char *tag, const T start,
	                                                                             idx_t count) {
		WriteTag(tag);
		WriteValue(start, count);
	}

	// Serialize an enum
	template <class T>
	typename std::enable_if<std::is_enum<T>::value, void>::type WriteProperty(const char *tag, T value,
	                                                                          const char *(*to_string)(T)) {
		WriteTag(tag);
		if (serialize_enum_as_string) {
			// Use the provided tostring function
			WriteValue(to_string(value));
		} else {
			// Use the underlying type
			WriteValue(static_cast<typename std::underlying_type<T>::type>(value));
		}
	}

	template <class T>
	typename std::enable_if<std::is_enum<T>::value, void>::type WriteProperty(const char *tag, T value,
	                                                                          string (*to_string)(T)) {
		WriteTag(tag);
		if (serialize_enum_as_string) {
			// Use the provided tostring function
			WriteValue(to_string(value));
		} else {
			// Use the underlying type
			WriteValue(static_cast<typename std::underlying_type<T>::type>(value));
		}
	}

	// Optional, by value
	template <class T>
	typename std::enable_if<std::is_trivially_copyable<T>::value && !std::is_enum<T>::value, void>::type
	WriteOptionalProperty(const char *tag, T ptr) {
		WriteTag(tag);
		BeginWriteOptional(false);
		if(ptr == nullptr) {
			BeginWriteOptional(false);
			EndWriteOptional(false);
		} else {
			BeginWriteOptional(true);
			WriteValue(*ptr);
			EndWriteOptional(true);
		}
	}

	// Optional, by ref (required for unique_ptr)
	template <class T>
	typename std::enable_if<!std::is_trivially_copyable<T>::value, void>::type
	WriteOptionalProperty(const char *tag, T& ptr) {
		WriteTag(tag);
		BeginWriteOptional(false);
		if(ptr == nullptr) {
			BeginWriteOptional(false);
			EndWriteOptional(false);
		} else {
			BeginWriteOptional(true);
			WriteValue(*ptr);
			EndWriteOptional(true);
		}
	}

protected:

	// Unique Pointer Ref
	template <typename T>
	void WriteValue(const unique_ptr<T> &ptr) {
		WriteValue(ptr.get());
	}

	// Pointer
	template <typename T>
	typename std::enable_if<std::is_pointer<T>::value, void>::type WriteValue(const T ptr) {
		if (ptr == nullptr) {
			WriteNull();
		} else {
			WriteValue(*ptr);
		}
	}

	// data_ptr_t
	void WriteValue(data_ptr_t ptr, idx_t count) {
		BeginWriteList(count);
		auto end = ptr + count;
		while (ptr != end) {
			WriteValue(*ptr);
			ptr++;
		}
		EndWriteList(count);
	}

	void WriteValue(const_data_ptr_t ptr, idx_t count) {
		BeginWriteList(count);
		auto end = ptr + count;
		while (ptr != end) {
			WriteValue(*ptr);
			ptr++;
		}
		EndWriteList(count);
	}

	// Pair
	template <class K, class V>
	void WriteValue(const std::pair<K, V> &pair) {
		BeginWriteObject();
		WriteProperty("key", pair.first);
		WriteProperty("value", pair.second);
		EndWriteObject();
	}

	// Vector
	template <class T>
	void WriteValue(const vector<T> &vec) {
		auto count = vec.size();
		BeginWriteList(count);
		for (auto &item : vec) {
			WriteValue(item);
		}
		EndWriteList(count);
	}

	// UnorderedSet
	// Serialized the same way as a list/vector
	template <class T, class HASH, class CMP>
	void WriteValue(const unordered_set<T, HASH, CMP> &set) {
		auto count = set.size();
		BeginWriteList(count);
		for (auto &item : set) {
			WriteValue(item);
		}
		EndWriteList(count);
	}

	// Set
	// Serialized the same way as a list/vector
	template <class T, class HASH, class CMP>
	void WriteValue(const set<T, HASH, CMP> &set) {
		auto count = set.size();
		BeginWriteList(count);
		for (auto &item : set) {
			WriteValue(item);
		}
		EndWriteList(count);
	}

	// Map
	template <class K, class V, class HASH, class CMP>
	void WriteValue(const std::unordered_map<K, V, HASH, CMP> &map) {
		auto count = map.size();
		BeginWriteMap(count);
		for (auto &item : map) {
			WriteValue(item.first);
			WriteValue(item.second);
		}
		EndWriteMap(count);
	}

	// class or struct implementing `Serialize(FormatSerializer& FormatSerializer)`;
	template <typename T>
	typename std::enable_if<has_serialize_v<T>(), void>::type WriteValue(T &value) {
		// Else, we defer to the .Serialize method
		BeginWriteObject();
		value.FormatSerialize(*this);
		EndWriteObject();
	}

	// Hooks for subclasses to override (if they want to)
	virtual void BeginWriteList(idx_t count) {
		(void)count;
	}

	virtual void EndWriteList(idx_t count) {
		(void)count;
	}

	virtual void BeginWriteMap(idx_t count) {
		(void)count;
	}

	virtual void EndWriteMap(idx_t count) {
		(void)count;
	}

	virtual void BeginWriteOptional(bool present) {
		(void)present;
	}

	virtual void EndWriteOptional(bool present) {
		(void)present;
	}

	virtual void BeginWriteObject() {
	}

	virtual void EndWriteObject() {
	}

	// Handle writing a "tag" (optional)
	virtual void WriteTag(const char *tag) {
		(void)tag;
	}

	// Handle primitive types, a serializer needs to implement these.
	virtual void WriteNull() = 0;
	virtual void WriteValue(uint8_t value) = 0;
	virtual void WriteValue(int8_t value) = 0;
	virtual void WriteValue(uint16_t value) = 0;
	virtual void WriteValue(int16_t value) = 0;
	virtual void WriteValue(uint32_t value) = 0;
	virtual void WriteValue(int32_t value) = 0;
	virtual void WriteValue(uint64_t value) = 0;
	virtual void WriteValue(int64_t value) = 0;
	virtual void WriteValue(hugeint_t value) = 0;
	virtual void WriteValue(float value) = 0;
	virtual void WriteValue(double value) = 0;
	virtual void WriteValue(interval_t value) = 0;
	virtual void WriteValue(const string &value) = 0;
	virtual void WriteValue(const string_t value) = 0;
	virtual void WriteValue(const char *value) = 0;
	virtual void WriteValue(bool value) = 0;
};

} // namespace duckdb
