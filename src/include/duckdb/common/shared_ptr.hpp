//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/shared_ptr.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/memory_safety.hpp"

#include <memory>
#include <type_traits>

namespace duckdb {

// This implementation is taken from the llvm-project, at this commit hash:
// https://github.com/llvm/llvm-project/blob/08bb121835be432ac52372f92845950628ce9a4a/libcxx/include/__memory/shared_ptr.h#353
// originally named '__compatible_with'

#if _LIBCPP_STD_VER >= 17
template <class _Yp, class _Tp>
struct __bounded_convertible_to_unbounded : false_type {};

template <class _Up, std::size_t _Np, class _Tp>
struct __bounded_convertible_to_unbounded<_Up[_Np], _Tp> : is_same<std::remove_cv<_Tp>, _Up[]> {};

template <class _Yp, class _Tp>
struct compatible_with_t : _Or<std::is_convertible<_Yp *, _Tp *>, __bounded_convertible_to_unbounded<_Yp, _Tp>> {};
#else
template <class _Yp, class _Tp>
struct compatible_with_t : std::is_convertible<_Yp *, _Tp *> {};
#endif // _LIBCPP_STD_VER >= 17

} // namespace duckdb

#include "duckdb/common/shared_ptr.ipp"
#include "duckdb/common/weak_ptr.ipp"
#include "duckdb/common/enable_shared_from_this.ipp"

namespace duckdb {

template <typename T>
using unsafe_shared_ptr = shared_ptr<T, false>;

template <typename T>
using unsafe_weak_ptr = weak_ptr<T, false>;

} // namespace duckdb
