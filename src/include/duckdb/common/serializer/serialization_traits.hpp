#pragma once
#include <type_traits>
#include <cstdint>

#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/shared_ptr.hpp"
namespace duckdb {

class FormatSerializer;   // Forward declare
class FormatDeserializer; // Forward declare

typedef uint32_t field_id_t;

// Backport to c++11
template <class...>
using void_t = void;

// Check for anything implementing a `void FormatSerialize(FormatSerializer &FormatSerializer)` method
template <typename T, typename = T>
struct has_serialize : std::false_type {};
template <typename T>
struct has_serialize<
    T, typename std::enable_if<
           std::is_same<decltype(std::declval<T>().FormatSerialize(std::declval<FormatSerializer &>())), void>::value,
           T>::type> : std::true_type {};

template <typename T, typename = T>
struct has_deserialize : std::false_type {};

// Accept `static unique_ptr<T> FormatDeserialize(FormatDeserializer& deserializer)`
template <typename T>
struct has_deserialize<
    T, typename std::enable_if<std::is_same<decltype(T::FormatDeserialize), unique_ptr<T>(FormatDeserializer &)>::value,
                               T>::type> : std::true_type {};

// Accept `static shared_ptr<T> FormatDeserialize(FormatDeserializer& deserializer)`
template <typename T>
struct has_deserialize<
    T, typename std::enable_if<std::is_same<decltype(T::FormatDeserialize), shared_ptr<T>(FormatDeserializer &)>::value,
                               T>::type> : std::true_type {};

// Accept `static T FormatDeserialize(FormatDeserializer& deserializer)`
template <typename T>
struct has_deserialize<
    T, typename std::enable_if<std::is_same<decltype(T::FormatDeserialize), T(FormatDeserializer &)>::value, T>::type>
    : std::true_type {};

// Check if T is a vector, and provide access to the inner type
template <typename T>
struct is_vector : std::false_type {};
template <typename T>
struct is_vector<typename duckdb::vector<T>> : std::true_type {
	typedef T ELEMENT_TYPE;
};

template <typename T>
struct is_unsafe_vector : std::false_type {};
template <typename T>
struct is_unsafe_vector<typename duckdb::unsafe_vector<T>> : std::true_type {
	typedef T ELEMENT_TYPE;
};

// Check if T is a unordered map, and provide access to the inner type
template <typename T>
struct is_unordered_map : std::false_type {};
template <typename... Args>
struct is_unordered_map<typename duckdb::unordered_map<Args...>> : std::true_type {
	typedef typename std::tuple_element<0, std::tuple<Args...>>::type KEY_TYPE;
	typedef typename std::tuple_element<1, std::tuple<Args...>>::type VALUE_TYPE;
	typedef typename std::tuple_element<2, std::tuple<Args...>>::type HASH_TYPE;
	typedef typename std::tuple_element<3, std::tuple<Args...>>::type EQUAL_TYPE;
};

template <typename T>
struct is_map : std::false_type {};
template <typename... Args>
struct is_map<typename duckdb::map<Args...>> : std::true_type {
	typedef typename std::tuple_element<0, std::tuple<Args...>>::type KEY_TYPE;
	typedef typename std::tuple_element<1, std::tuple<Args...>>::type VALUE_TYPE;
	typedef typename std::tuple_element<2, std::tuple<Args...>>::type HASH_TYPE;
	typedef typename std::tuple_element<3, std::tuple<Args...>>::type EQUAL_TYPE;
};

template <typename T>
struct is_unique_ptr : std::false_type {};

template <typename T>
struct is_unique_ptr<unique_ptr<T>> : std::true_type {
	typedef T ELEMENT_TYPE;
};

template <typename T>
struct is_shared_ptr : std::false_type {};

template <typename T>
struct is_shared_ptr<shared_ptr<T>> : std::true_type {
	typedef T ELEMENT_TYPE;
};

template <typename T>
struct is_pair : std::false_type {};

template <typename T, typename U>
struct is_pair<std::pair<T, U>> : std::true_type {
	typedef T FIRST_TYPE;
	typedef U SECOND_TYPE;
};

template <typename T>
struct is_unordered_set : std::false_type {};
template <typename... Args>
struct is_unordered_set<duckdb::unordered_set<Args...>> : std::true_type {
	typedef typename std::tuple_element<0, std::tuple<Args...>>::type ELEMENT_TYPE;
	typedef typename std::tuple_element<1, std::tuple<Args...>>::type HASH_TYPE;
	typedef typename std::tuple_element<2, std::tuple<Args...>>::type EQUAL_TYPE;
};

template <typename T>
struct is_set : std::false_type {};
template <typename... Args>
struct is_set<duckdb::set<Args...>> : std::true_type {
	typedef typename std::tuple_element<0, std::tuple<Args...>>::type ELEMENT_TYPE;
	typedef typename std::tuple_element<1, std::tuple<Args...>>::type HASH_TYPE;
	typedef typename std::tuple_element<2, std::tuple<Args...>>::type EQUAL_TYPE;
};

} // namespace duckdb
