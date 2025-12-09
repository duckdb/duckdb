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
template <class U, class T>
struct __bounded_convertible_to_unbounded : std::false_type {};

template <class _Up, std::size_t _Np, class T>
struct __bounded_convertible_to_unbounded<_Up[_Np], T> : std::is_same<std::remove_cv<T>, _Up[]> {};

template <class U, class T>
struct compatible_with_t : std::_Or<std::is_convertible<U *, T *>, __bounded_convertible_to_unbounded<U, T>> {};
#else
template <class U, class T>
struct compatible_with_t : std::is_convertible<U *, T *> {}; // NOLINT: invalid case style
#endif // _LIBCPP_STD_VER >= 17

} // namespace duckdb
