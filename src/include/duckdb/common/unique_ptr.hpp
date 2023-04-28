#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/likely.hpp"

#include <memory>
#include <type_traits>

static void AssertNotNull(const bool null) {
#ifdef DEBUG
	if (DUCKDB_UNLIKELY(null)) {
		throw duckdb::InternalException("Attempted to dereference unique_ptr that is NULL!");
	}
#endif
}

namespace duckdb {

template <class _Tp, bool UNSAFE = false>
class unique_ptr : public std::unique_ptr<_Tp, std::default_delete<_Tp>> {
public:
	using original = std::unique_ptr<_Tp, std::default_delete<_Tp>>;
	using original::original;

	typename std::add_lvalue_reference<_Tp>::type operator*() const {
		const auto ptr = original::get();
		if (!UNSAFE) {
			AssertNotNull(!ptr);
		}
		return *ptr;
	}

	typename original::pointer operator->() const {
		const auto ptr = original::get();
		if (!UNSAFE) {
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

template <class _Tp, bool UNSAFE>
class unique_ptr<_Tp[], UNSAFE> : public std::unique_ptr<_Tp[], std::default_delete<_Tp>> {
public:
	using original = std::unique_ptr<_Tp[], std::default_delete<_Tp>>;
	using original::original;

	typename std::add_lvalue_reference<_Tp>::type operator[](size_t __i) const {
		const auto ptr = original::get();
		if (!UNSAFE) {
			AssertNotNull(!ptr);
		}
		return ptr[__i];
	}
};

} // namespace duckdb
