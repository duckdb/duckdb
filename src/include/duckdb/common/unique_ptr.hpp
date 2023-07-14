#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/memory_safety.hpp"

#include <memory>
#include <type_traits>

namespace duckdb {

namespace unique_ptr_helper {
template <class _Tp>
struct get_inner {
	using inner = _Tp;
};

template <class _Tp>
struct get_inner<_Tp[]> {
	using inner = _Tp;
};
} // namespace unique_ptr_helper

template <class _Tp, class _Dp = std::default_delete<_Tp>, bool SAFE = true>
class unique_ptr : public std::unique_ptr<_Tp, _Dp> {
public:
	using original = std::unique_ptr<_Tp, _Dp>;
	using original::original;

private:
	static inline void AssertNotNull(const bool null) {
#if defined(DUCKDB_DEBUG_NO_SAFETY) || defined(DUCKDB_CLANG_TIDY)
		return;
#else
		if (DUCKDB_UNLIKELY(null)) {
			throw duckdb::InternalException("Attempted to dereference unique_ptr that is NULL!");
		}
#endif
	}

public:
	typename std::add_lvalue_reference<typename unique_ptr_helper::get_inner<_Tp>::inner>::type operator*() const {
		const auto ptr = original::get();
		if (MemorySafety<SAFE>::enabled) {
			AssertNotNull(!ptr);
		}
		return *ptr;
	}

	typename original::pointer operator->() const {
		const auto ptr = original::get();
		if (MemorySafety<SAFE>::enabled) {
			AssertNotNull(!ptr);
		}
		return ptr;
	}
	typename std::add_lvalue_reference<typename unique_ptr_helper::get_inner<_Tp>::inner>::type
	operator[](size_t __i) const {
		const auto ptr = original::get();
		if (MemorySafety<SAFE>::enabled) {
			AssertNotNull(!ptr);
		}
		return ptr[__i];
	}

#ifdef DUCKDB_CLANG_TIDY
	// This is necessary to tell clang-tidy that it reinitializes the variable after a move
	[[clang::reinitializes]]
#endif
	inline void
	reset(typename original::pointer ptr = typename original::pointer()) noexcept {
		original::reset(ptr);
	}
};

template <typename T>
using unique_array = unique_ptr<T[], std::default_delete<T>, true>;

template <typename T>
using unsafe_unique_array = unique_ptr<T[], std::default_delete<T>, false>;

template <typename T>
using unsafe_unique_ptr = unique_ptr<T, std::default_delete<T>, false>;

} // namespace duckdb
