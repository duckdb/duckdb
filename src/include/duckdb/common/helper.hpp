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
	static constexpr bool value = false;
};

template<typename T>
using reference = std::reference_wrapper<T>;

template<class _Tp, bool SAFE = true>
struct __unique_if
{
    typedef unique_ptr<_Tp, std::default_delete<_Tp>, SAFE> __unique_single;
};

template<class _Tp>
struct __unique_if<_Tp[]>
{
    typedef unique_ptr<_Tp[]> __unique_array_unknown_bound;
};

template<class _Tp, size_t _Np>
struct __unique_if<_Tp[_Np]>
{
    typedef void __unique_array_known_bound;
};

template<class _Tp, class... _Args>
inline 
typename __unique_if<_Tp, true>::__unique_single
make_uniq(_Args&&... __args)
{
    return unique_ptr<_Tp, std::default_delete<_Tp>, true>(new _Tp(std::forward<_Args>(__args)...));
}

template<class _Tp, class... _Args>
inline 
typename __unique_if<_Tp, false>::__unique_single
make_unsafe_uniq(_Args&&... __args)
{
    return unique_ptr<_Tp, std::default_delete<_Tp>, false>(new _Tp(std::forward<_Args>(__args)...));
}

template<class _Tp>
inline unique_ptr<_Tp[], std::default_delete<_Tp>, true>
make_uniq_array(size_t __n)
{
    return unique_ptr<_Tp[], std::default_delete<_Tp>, true>(new _Tp[__n]());
}

template<class _Tp>
inline unique_ptr<_Tp[], std::default_delete<_Tp>, false>
make_unsafe_uniq_array(size_t __n)
{
    return unique_ptr<_Tp[], std::default_delete<_Tp>, false>(new _Tp[__n]());
}

template<class _Tp, class... _Args>
    typename __unique_if<_Tp>::__unique_array_known_bound
    make_uniq(_Args&&...) = delete;


template <typename S, typename T, typename... Args>
unique_ptr<S> make_uniq_base(Args &&... args) {
	return unique_ptr<S>(new T(std::forward<Args>(args)...));
}

#ifdef DUCKDB_ENABLE_DEPRECATED_API
template <typename S, typename T, typename... Args>
unique_ptr<S> make_unique_base(Args &&... args) {
	return unique_ptr<S>(new T(std::forward<Args>(args)...));
}
#endif // DUCKDB_ENABLE_DEPRECATED_API

template <typename T, typename S>
unique_ptr<S> unique_ptr_cast(unique_ptr<T> src) {
	return unique_ptr<S>(static_cast<S *>(src.release()));
}

struct SharedConstructor {
	template <class T, typename... ARGS>
	static shared_ptr<T> Create(ARGS &&...args) {
		return make_shared<T>(std::forward<ARGS>(args)...);
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

template <class T, class... _Args>
static duckdb::unique_ptr<T> make_unique(_Args&&... __args) {
#ifndef DUCKDB_ENABLE_DEPRECATED_API
	static_assert(sizeof(T) == 0, "Use make_uniq instead of make_unique!");
#endif // DUCKDB_ENABLE_DEPRECATED_API
	return unique_ptr<T>(new T(std::forward<_Args>(__args)...));
}

template <typename T>
T MaxValue(T a, T b) {
	return a > b ? a : b;
}

template <typename T>
T MinValue(T a, T b) {
	return a < b ? a : b;
}

template <typename T>
T AbsValue(T a) {
	return a < 0 ? -a : a;
}

//Align value (ceiling)
template<class T, T val=8>
static inline T AlignValue(T n) {
	return ((n + (val - 1)) / val) * val;
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
	memcpy(&ret, ptr, sizeof(ret));
	return ret;
}

template <typename T>
void Store(const T &val, data_ptr_t ptr) {
	memcpy(ptr, (void *)&val, sizeof(val));
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
bool RefersToSameObject(const reference<T> &A, const reference<T> &B) {
	return &A.get() == &B.get();
}

} // namespace duckdb
