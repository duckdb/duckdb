//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include <string.h>
#include <type_traits>

#ifdef _MSC_VER
#define suint64_t int64_t
#endif

#if defined(_WIN32) || defined(_WIN64)
#define DUCKDB_WINDOWS
#elif defined(__unix__) || defined(__unix) || (defined(__APPLE__) && defined(__MACH__))
#define DUCKDB_POSIX
#endif

namespace duckdb {

// explicit fallthrough for switch_statementss
#ifndef __has_cpp_attribute // For backwards compatibility
#define __has_cpp_attribute(x) 0
#endif
#if __has_cpp_attribute(clang::fallthrough)
#define DUCKDB_EXPLICIT_FALLTHROUGH [[clang::fallthrough]]
#elif __has_cpp_attribute(gnu::fallthrough)
#define DUCKDB_EXPLICIT_FALLTHROUGH [[gnu::fallthrough]]
#else
#define DUCKDB_EXPLICIT_FALLTHROUGH
#endif

template <class... T>
struct AlwaysFalse {
	static constexpr bool VALUE = false;
};

template<typename T>
using reference = std::reference_wrapper<T>;

template<class DATA_TYPE, bool SAFE = true>
struct TemplatedUniqueIf
{
    typedef unique_ptr<DATA_TYPE, std::default_delete<DATA_TYPE>, SAFE> templated_unique_single_t;
};

template<class DATA_TYPE, size_t N>
struct TemplatedUniqueIf<DATA_TYPE[N]>
{
    typedef void TemplatedUniqueArrayKnownBound; // NOLINT: mimic std style
};

template<class DATA_TYPE, class... ARGS>
inline 
typename TemplatedUniqueIf<DATA_TYPE, true>::templated_unique_single_t
make_uniq(ARGS&&... args) // NOLINT: mimic std style
{
    return unique_ptr<DATA_TYPE, std::default_delete<DATA_TYPE>, true>(new DATA_TYPE(std::forward<ARGS>(args)...));
}

template<class DATA_TYPE, class... ARGS>
inline 
shared_ptr<DATA_TYPE>
make_shared_ptr(ARGS&&... args) // NOLINT: mimic std style
{
	return shared_ptr<DATA_TYPE>(std::make_shared<DATA_TYPE>(std::forward<ARGS>(args)...));
}

template<class DATA_TYPE, class... ARGS>
inline 
typename TemplatedUniqueIf<DATA_TYPE, false>::templated_unique_single_t
make_unsafe_uniq(ARGS&&... args) // NOLINT: mimic std style
{
    return unique_ptr<DATA_TYPE, std::default_delete<DATA_TYPE>, false>(new DATA_TYPE(std::forward<ARGS>(args)...));
}

template<class DATA_TYPE>
inline unique_ptr<DATA_TYPE[], std::default_delete<DATA_TYPE>, true>
make_uniq_array(size_t n) // NOLINT: mimic std style
{
	return unique_ptr<DATA_TYPE[], std::default_delete<DATA_TYPE>, true>(new DATA_TYPE[n]());
}

template<class DATA_TYPE>
inline unique_ptr<DATA_TYPE[], std::default_delete<DATA_TYPE>, true>
make_uniq_array_uninitialized(size_t n) // NOLINT: mimic std style
{
	return unique_ptr<DATA_TYPE[], std::default_delete<DATA_TYPE>, true>(new DATA_TYPE[n]);
}

template<class DATA_TYPE>
inline unique_ptr<DATA_TYPE[], std::default_delete<DATA_TYPE>, false>
make_unsafe_uniq_array(size_t n) // NOLINT: mimic std style
{
	return unique_ptr<DATA_TYPE[], std::default_delete<DATA_TYPE>, false>(new DATA_TYPE[n]());
}

template<class DATA_TYPE>
inline unique_ptr<DATA_TYPE[], std::default_delete<DATA_TYPE>, false>
make_unsafe_uniq_array_uninitialized(size_t n) // NOLINT: mimic std style
{
	return unique_ptr<DATA_TYPE[], std::default_delete<DATA_TYPE>, false>(new DATA_TYPE[n]);
}

template<class DATA_TYPE, class... ARGS>
    typename TemplatedUniqueIf<DATA_TYPE>::TemplatedUniqueArrayKnownBound
    make_uniq(ARGS&&...) = delete; // NOLINT: mimic std style


template <typename S, typename T, typename... ARGS>
unique_ptr<S> make_uniq_base(ARGS &&... args) { // NOLINT: mimic std style
	return unique_ptr<S>(new T(std::forward<ARGS>(args)...));
}

#ifdef DUCKDB_ENABLE_DEPRECATED_API
template <typename S, typename T, typename... Args>
unique_ptr<S> make_unique_base(Args &&... args) {
	return unique_ptr<S>(new T(std::forward<Args>(args)...));
}
#endif // DUCKDB_ENABLE_DEPRECATED_API

template <typename SRC, typename TGT>
unique_ptr<TGT> unique_ptr_cast(unique_ptr<SRC> src) { // NOLINT: mimic std style
	return unique_ptr<TGT>(static_cast<TGT *>(src.release()));
}

template <typename SRC, typename TGT>
shared_ptr<TGT> shared_ptr_cast(shared_ptr<SRC> src) { // NOLINT: mimic std style
	return shared_ptr<TGT>(std::static_pointer_cast<TGT, SRC>(src.internal));
}

struct SharedConstructor {
	template <class T, typename... ARGS>
	static shared_ptr<T> Create(ARGS &&...args) {
		return make_shared_ptr<T>(std::forward<ARGS>(args)...);
	}
};

struct UniqueConstructor {
	template <class T, typename... ARGS>
	static unique_ptr<T> Create(ARGS &&...args) {
		return make_uniq<T>(std::forward<ARGS>(args)...);
	}
};

#ifdef DUCKDB_DEBUG_MOVE
template<class T>
typename std::remove_reference<T>::type&& move(T&& t) noexcept {
	// the nonsensical sizeof check ensures this is never instantiated
	static_assert(sizeof(T) == 0, "Use std::move instead of unqualified move or duckdb::move");
}
#endif

template <class T, class... ARGS>
static duckdb::unique_ptr<T> make_unique(ARGS&&... __args) { // NOLINT: mimic std style
#ifndef DUCKDB_ENABLE_DEPRECATED_API
	static_assert(sizeof(T) == 0, "Use make_uniq instead of make_unique!");
#endif // DUCKDB_ENABLE_DEPRECATED_API
	return unique_ptr<T>(new T(std::forward<ARGS>(__args)...));
}

template <class T, class... ARGS>
static duckdb::shared_ptr<T> make_shared(ARGS&&... __args) { // NOLINT: mimic std style
#ifndef DUCKDB_ENABLE_DEPRECATED_API
	static_assert(sizeof(T) == 0, "Use make_shared_ptr instead of make_shared!");
#endif // DUCKDB_ENABLE_DEPRECATED_API
	return shared_ptr<T>(new T(std::forward<ARGS>(__args)...));
}

template <typename T>
constexpr T MaxValue(T a, T b) {
	return a > b ? a : b;
}

template <typename T>
constexpr T MinValue(T a, T b) {
	return a < b ? a : b;
}

template <typename T>
T AbsValue(T a) {
	return a < 0 ? -a : a;
}

//! Align value (ceiling)
template<class T, T val=8>
static inline T AlignValue(T n) {
	return ((n + (val - 1)) / val) * val;
}

template<class T, T val=8>
constexpr inline T AlignValueFloor(T n) {
	return (n / val) * val;
}

template<class T, T val=8>
static inline bool ValueIsAligned(T n) {
	return (n % val) == 0;
}

template <typename T>
T SignValue(T a) {
	return a < 0 ? -1 : 1;
}

template <typename T>
const T Load(const_data_ptr_t ptr) {
	T ret;
	memcpy(&ret, ptr, sizeof(ret)); // NOLINT
	return ret;
}

template <typename T>
void Store(const T &val, data_ptr_t ptr) {
	memcpy(ptr, (void *)&val, sizeof(val)); // NOLINT
}

//! This assigns a shared pointer, but ONLY assigns if "target" is not equal to "source"
//! If this is often the case, this manner of assignment is significantly faster (~20X faster)
//! Since it avoids the need of an atomic incref/decref at the cost of a single pointer comparison
//! Benchmark: https://gist.github.com/Mytherin/4db3faa8e233c4a9b874b21f62bb4b96
//! If the shared pointers are not the same, the penalty is very low (on the order of 1%~ slower)
//! This method should always be preferred if there is a (reasonable) chance that the pointers are the same
template<class T>
void AssignSharedPointer(shared_ptr<T> &target, const shared_ptr<T> &source) {
	if (target.get() != source.get()) {
		target = source;
	}
}

template<typename T>
using const_reference = std::reference_wrapper<const T>;

//! Returns whether or not two reference wrappers refer to the same object
template<class T>
bool RefersToSameObject(const reference<T> &a, const reference<T> &b) {
	return &a.get() == &b.get();
}

template<class T>
bool RefersToSameObject(const T &a, const T &b) {
	return &a == &b;
}

template<class T, class SRC>
void DynamicCastCheck(const SRC *source) {
#ifndef __APPLE__
	// Actual check is on the fact that dynamic_cast and reinterpret_cast are equivalent
	D_ASSERT(reinterpret_cast<const T *>(source) == dynamic_cast<const T *>(source));
#endif
}

} // namespace duckdb
