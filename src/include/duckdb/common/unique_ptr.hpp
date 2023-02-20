#pragma once

#include "duckdb/common/unique_ptr_utils.hpp"

#include <memory>
#include <type_traits>

using std::add_lvalue_reference;
using std::default_delete;

namespace duckdb {

template <class _Tp, class _Dp = default_delete<_Tp>>
class unique_ptr : public std::unique_ptr<_Tp, _Dp> {
public:
	using original = std::unique_ptr<_Tp, _Dp>;
	using original::original;

	typename add_lvalue_reference<_Tp>::type operator*() const {
		__unique_ptr_utils::AssertNotNull((void *)original::get());
		return *(original::get());
	}

	typename original::pointer operator->() const {
		__unique_ptr_utils::AssertNotNull((void *)original::get());
		return original::get();
	}
};

template <class _Tp, class _Dp>
class unique_ptr<_Tp[], _Dp> : public std::unique_ptr<_Tp[], _Dp> {
public:
	using original = std::unique_ptr<_Tp[], _Dp>;
	using original::original;

	typename add_lvalue_reference<_Tp>::type operator[](size_t __i) const {
		__unique_ptr_utils::AssertNotNull((void *)original::get());
		return (original::get())[__i];
	}
};

} // namespace duckdb
