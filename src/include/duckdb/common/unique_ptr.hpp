#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/memory_safety.hpp"

#include <memory>
#include <type_traits>

namespace duckdb {

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
	typename std::add_lvalue_reference<_Tp>::type operator*() const {
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

#ifdef DUCKDB_CLANG_TIDY
	// This is necessary to tell clang-tidy that it reinitializes the variable after a move
	[[clang::reinitializes]]
#endif
	inline void
	reset(typename original::pointer ptr = typename original::pointer()) noexcept {
		original::reset(ptr);
	}
};

template <class _Tp, class _Dp, bool SAFE>
class unique_ptr<_Tp[], _Dp, SAFE> : public std::unique_ptr<_Tp[], std::default_delete<_Tp[]>> {
public:
	using original = std::unique_ptr<_Tp[], std::default_delete<_Tp[]>>;
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
	typename std::add_lvalue_reference<_Tp>::type operator[](size_t __i) const {
		const auto ptr = original::get();
		if (MemorySafety<SAFE>::enabled) {
			AssertNotNull(!ptr);
		}
		return ptr[__i];
	}
};

template <typename T>
using unique_array = unique_ptr<T[], std::default_delete<T>, true>;

template <typename T>
using unsafe_unique_array = unique_ptr<T[], std::default_delete<T>, false>;

template <typename T>
using unsafe_unique_ptr = unique_ptr<T, std::default_delete<T>, false>;

} // namespace duckdb
