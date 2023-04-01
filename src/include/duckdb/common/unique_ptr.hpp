#pragma once

#include "duckdb/common/exception.hpp"

#include <memory>
#include <type_traits>

namespace duckdb {

namespace {
struct __unique_ptr_utils {
	static inline void AssertNotNull(void *ptr) {
		if (!ptr) {
#ifdef DEBUG
			throw InternalException("Attempted to dereference unique_ptr that is NULL!");
#endif
		}
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
#ifdef DEBUG
		__unique_ptr_utils::AssertNotNull((void *)original::get());
#endif
		return original::get();
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
