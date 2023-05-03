#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/likely.hpp"

#include <memory>
#include <type_traits>

namespace duckdb {

namespace {
struct __unique_ptr_utils {
	static inline void AssertNotNull(void *ptr) {
#ifdef DEBUG
		if (DUCKDB_UNLIKELY(!ptr)) {
			throw InternalException("Attempted to dereference unique_ptr that is NULL!");
		}
#endif
	}
};
} // namespace

template <class _Tp, class _Dp = std::default_delete<_Tp>>
class unique_ptr : public std::unique_ptr<_Tp, _Dp> {
public:
	using original = std::unique_ptr<_Tp, _Dp>;
	using original::original;

	typename std::add_lvalue_reference<_Tp>::type operator*() const {
		__unique_ptr_utils::AssertNotNull((void *)original::get());
		return *(original::get());
	}

	typename original::pointer operator->() const {
		__unique_ptr_utils::AssertNotNull((void *)original::get());
		return original::get();
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

template <class _Tp, class _Dp>
class unique_ptr<_Tp[], _Dp> : public std::unique_ptr<_Tp[], _Dp> {
public:
	using original = std::unique_ptr<_Tp[], _Dp>;
	using original::original;

	typename std::add_lvalue_reference<_Tp>::type operator[](size_t __i) const {
		__unique_ptr_utils::AssertNotNull((void *)original::get());
		return (original::get())[__i];
	}
};

} // namespace duckdb
