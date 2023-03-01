#pragma once
#include <type_traits>
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {


class FormatSerializer; // Forward declare
class FormatDeserializer; // Forward declare

// Backport to c++11
template <class...> using void_t = void;

// Check for anything implementing a `void FormatSerialize(FormatSerializer &FormatSerializer)` method
template <typename T, typename = void_t<>>
struct has_serialize : std::false_type {};

template <typename T>
struct has_serialize<T, void_t<decltype(std::declval<T>().FormatSerialize(std::declval<FormatSerializer &>()))>>
    : std::true_type {};

template <typename T>
constexpr bool has_serialize_v() {
	return has_serialize<T>::value;
}

// Check for anything implementing a static `T FormatDeserialize(FormatDeserializer&)` method
template <typename T, typename = void>
struct has_deserialize : std::false_type {};

template <typename T>
struct has_deserialize<T, void_t<decltype(T::FormatDeserialize)>>
    : std::true_type {};

template <typename T>
constexpr bool has_deserialize_v() {
	return has_deserialize<T>::value;
}

// Check if T is a vector, and provide access to the inner type
template <typename T>
struct is_vector : std::false_type {};
template <typename T>
struct is_vector<typename std::vector<T>> : std::true_type {
	typedef T inner_type;
};

// Check if T is a unordered map, and provide access to the inner type
template <typename T, typename ...Args>
struct is_unordered_map : std::false_type {};
template <typename T, typename ...Args>
struct is_unordered_map<typename std::unordered_map<T, Args...>> : std::true_type {
	typedef T inner_type;
};

template <typename T>
struct is_unique_ptr : std::false_type
{};

template <typename T, typename D>
struct is_unique_ptr<typename std::unique_ptr<T, D>> : std::true_type
{
	typedef T inner_type;
	typedef D deleter_type;
};

}