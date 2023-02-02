//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/FormatSerializer/FormatSerializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <type_traits>
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

// A FormatSerializer is a higher-level class that wraps a BinaryWriter to assist in serializing
// more advanced and nested structures
class FormatSerializer; // Forward declare

// Type trait helpers for anything implementing a `void FormatSerialize(FormatSerializer &FormatSerializer)`
template <class...>
using void_t = void; // Backport to c++11

template <typename T> // Backport to c++11
struct dependent_false : std::false_type {};

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
	typename std::enable_if<!std::is_trivially_copyable<T>::value, void>::type WriteProperty(const char *tag,
	                                                                                         T &value) {
		WriteTag(tag);
		WriteValue(value);
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

protected:
	// Unique Pointer Ref
	template <typename T>
	void WriteValue(const unique_ptr<T> &ptr) {
		WriteValue(ptr.get());
	};

	// Pointer
	template <typename T>
	typename std::enable_if<std::is_pointer<T>::value, void>::type WriteValue(const T ptr) {
		if (ptr == nullptr) {
			BeginWriteOptional(false);
			EndWriteOptional(false);
		} else {
			BeginWriteOptional(true);
			WriteValue(*ptr);
			EndWriteOptional(true);
		}
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
	};
	virtual void EndWriteList(idx_t count) {
		(void)count;
	};
	virtual void BeginWriteMap(idx_t count) {
		(void)count;
	};
	virtual void EndWriteMap(idx_t count) {
		(void)count;
	}
	virtual void BeginWriteOptional(bool present) {
		(void)present;
	}
	virtual void EndWriteOptional(bool present) {
		(void)present;
	}
	virtual void BeginWriteObject() {};
	virtual void EndWriteObject() {};

	// Handle writing a "tag" (optional)
	virtual void WriteTag(const char *tag) {
		(void)tag;
	};

	// Handle primitive types
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
	virtual void WriteValue(const char *value) = 0;
	virtual void WriteValue(bool value) = 0;
};

struct BinarySerializer : FormatSerializer {
protected:
	std::unique_ptr<FieldWriter> writer;

public:
	explicit BinarySerializer(std::unique_ptr<FieldWriter> writer_p) : writer(std::move(writer_p)) {
	}

	void BeginWriteList(idx_t count) override {
		writer->WriteField((uint32_t)count);
	}

	void BeginWriteMap(idx_t count) override {
		writer->WriteField((uint32_t)count);
	}

	void WriteValue(uint8_t value) override {
		writer->WriteField(value);
	};

	void WriteValue(int8_t value) override {
		writer->WriteField(value);
	}

	void WriteValue(const string &value) override {
		writer->WriteString(value);
	}
	void WriteValue(const char *value) override {
		writer->WriteString(value);
	}

	void WriteValue(uint64_t value) override {
		writer->WriteField(value);
	}

	void WriteValue(uint16_t value) override {
		writer->WriteField(value);
	}

	void WriteValue(int16_t value) override {
		writer->WriteField(value);
	}
	void WriteValue(uint32_t value) override {
		writer->WriteField(value);
	}
	void WriteValue(int32_t value) override {
		writer->WriteField(value);
	}
	void WriteValue(int64_t value) override {
		writer->WriteField(value);
	}
	void WriteValue(hugeint_t value) override {
		writer->WriteField(value);
	}
	void WriteValue(float value) override {
		writer->WriteField(value);
	}
	void WriteValue(double value) override {
		writer->WriteField(value);
	}
	void WriteValue(interval_t value) override {
		writer->WriteField(value);
	}
	void WriteValue(bool value) override {
		writer->WriteField(value);
	}
};

/*
// A FormatSerializer is a higher-level class that wraps a BinaryWriter to assist in serializing
// more advanced and nested structures
template <class SERIALIZER_IMPL>
struct FormatSerializer; // Forward declare

// Type trait helpers for anything implementing a `void Serialize(FormatSerializer<IMPL> &FormatSerializer)`
template<class...>
using void_t = void; // Backport to c++11

template<typename T> // Backport to c++11
struct dependent_false: std::false_type {};

template <typename SELF, typename SERIALIZER_IMPL, typename = void_t<>>
struct has_serialize : std::false_type{};

template <typename SELF, typename SERIALIZER_IMPL>
struct has_serialize<SELF, SERIALIZER_IMPL,
void_t<decltype(std::declval<SELF>().Serialize(std::declval<FormatSerializer<SERIALIZER_IMPL>&>()))>>: std::true_type{};

template <typename SELF, typename SERIALIZER_IMPL>
constexpr bool has_serialize_v() { return has_serialize<SELF, SERIALIZER_IMPL>::value; }


// We use a "CRTP" class to enable polymorphic serialization.
// I.E. Every subclass X should inherit from FormatSerializer<X>
template <class SERIALIZER_IMPL>
struct FormatSerializer {

    template<typename T>
    typename std::enable_if<!has_serialize_v<T, SERIALIZER_IMPL>(), void>::type
    WriteField(const char* name, T value) {
        // If the target value doesn't have a .Serialize(FormatSerializer<IMPL> &serializer_ref) method,
        // we use the field write implementation
        static_cast<SERIALIZER_IMPL*>(this)->WriteFieldImpl(name, value);
    }

    template<typename T>
    typename std::enable_if<has_serialize_v<T, SERIALIZER_IMPL>(), void>::type
    WriteField(const char* name, T value) {
        // Else, we defer to the .Serialize method
        value.Serialize(*this);
    }
};

struct NoopSerializer : FormatSerializer<NoopSerializer> {

    template<class T>
    void WriteFieldImpl(const char* name, T value) {

    };
};

struct SerializerBase {};

struct BinarySerializer : FormatSerializer<BinarySerializer> {
protected:
    std::unique_ptr<BinaryWriter> writer;
public:
    explicit BinarySerializer(std::unique_ptr<BinaryWriter> writer_p) : writer(std::move(writer_p)) {
    }

    template<class T>
    void WriteFieldImpl(const char* name, T value) {
        if (std::is_trivially_destructible<T>()) {
            (void)name;
            writer->Write(value);
        }
        else {
            static_assert(dependent_false<T>::value, "Only specializations or calls with trivially destructible template
arguments of WriteField may be used" );
        }
    };

    // Overload for Unordered Map
    template<class K, class V>
    void WriteFieldImpl(const char* name, unordered_map<K, V> &map) {
        (void)name;
        writer->Write(map.size());
        for (auto &elem : map) {
            WriteField(elem.first);
            WriteField(elem.second);
        }
    };

    // Overload for Vec
    template<class T>
    void WriteFieldImpl(const char* name, vector<T> &vec) {
        (void)name;
        writer->Write<uint32_t>((uint32_t)vec.size());
        for (auto &child : vec) {
            WriteField(child);
        }
    }
};
 */

} // namespace duckdb
