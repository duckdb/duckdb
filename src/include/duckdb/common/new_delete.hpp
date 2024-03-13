//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/new_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/winapi.hpp"

#include <memory>

namespace duckdb {

struct AllocatorWrapper {
	DUCKDB_API static data_ptr_t Allocate(idx_t size);
	DUCKDB_API static void Free(data_ptr_t pointer);
};

template <class _Tp, class... _Args>
DUCKDB_API _Tp *duck_new_object(_Args &&...__args) {
	static_assert(!std::is_function<_Tp>::value, "duck_new_object cannot be instantiated for function types");
	static_assert(sizeof(_Tp) >= 0, "cannot create an incomplete type");
	static_assert(!std::is_void<_Tp>::value, "cannot create an incomplete type");
	return new (AllocatorWrapper::Allocate(sizeof(_Tp))) _Tp(std::forward<_Args>(__args)...);
}

template <class _Tp>
struct duck_delete_object {
	static_assert(!std::is_function<_Tp>::value, "duck_delete_object cannot be instantiated for function types");
	DUCKDB_API constexpr duck_delete_object() noexcept = default;

	template <class _Up>
	DUCKDB_API
	duck_delete_object(const duck_delete_object<_Up> &,
	                   typename std::enable_if<std::is_convertible<_Up *, _Tp *>::value>::type * = 0) noexcept {
	}

	DUCKDB_API void operator()(_Tp *__ptr) const noexcept {
		static_assert(sizeof(_Tp) >= 0, "cannot delete an incomplete type");
		static_assert(!std::is_void<_Tp>::value, "cannot delete an incomplete type");
		__ptr->~_Tp();
		AllocatorWrapper::Free((data_ptr_t)__ptr);
	}
};

template <class _Tp>
DUCKDB_API _Tp *duck_new_array(size_t __n) {
	static_assert(!std::is_function<_Tp>::value, "duck_new_object cannot be instantiated for function types");
	static_assert(sizeof(_Tp) >= 0, "cannot create array of an incomplete type");
	static_assert(!std::is_void<_Tp>::value, "cannot create array of an incomplete type");
	return reinterpret_cast<_Tp *>(AllocatorWrapper::Allocate(__n * sizeof(_Tp)));
}

template <class _Tp>
struct duck_delete_array {
	static_assert(!std::is_function<_Tp>::value, "duck_delete_array cannot be instantiated for function types");
	DUCKDB_API constexpr duck_delete_array() noexcept = default;

	template <class _Up>
	DUCKDB_API
	duck_delete_array(const duck_delete_array<_Up> &,
	                  typename std::enable_if<std::is_convertible<_Up *, _Tp *>::value>::type * = 0) noexcept {
	}

	DUCKDB_API void operator()(_Tp *__ptr) const noexcept {
		static_assert(sizeof(_Tp) >= 0, "cannot delete array of an incomplete type");
		static_assert(!std::is_void<_Tp>::value, "cannot delete array of an incomplete type");
		AllocatorWrapper::Free((data_ptr_t)__ptr);
	}
};

} // namespace duckdb
