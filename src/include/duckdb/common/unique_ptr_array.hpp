#pragma once

#include <type_traits>
#include <memory>

#include "duckdb/common/unique_ptr_single.hpp"

using std::__dependent_type;
using std::__pointer;
using std::add_lvalue_reference;
using std::enable_if;
using std::integral_constant;
using std::is_array;
using std::is_assignable;
using std::is_constructible;
using std::is_convertible;
using std::is_default_constructible;
using std::is_pointer;
using std::is_reference;
using std::is_same;

namespace duckdb {

template <class _Tp, class _Dp>
class unique_ptr<_Tp[], _Dp> {
public:
	typedef _Tp element_type;
	typedef _Dp deleter_type;
	typedef typename __pointer<_Tp, deleter_type>::type pointer;

private:
	__compressed_pair<pointer, deleter_type> __ptr_;

	template <class _From>
	struct _CheckArrayPointerConversion : is_same<_From, pointer> {};

	template <class _FromElem>
	struct _CheckArrayPointerConversion<_FromElem *>
	    : integral_constant<bool, is_same<_FromElem *, pointer>::value ||
	                                  (is_same<pointer, element_type *>::value &&
	                                   is_convertible<_FromElem (*)[], element_type (*)[]>::value)> {};

	typedef __unique_ptr_deleter_sfinae<_Dp> _DeleterSFINAE;

	template <bool _Dummy>
	using _LValRefType _LIBCPP_NODEBUG = typename __dependent_type<_DeleterSFINAE, _Dummy>::__lval_ref_type;

	template <bool _Dummy>
	using _GoodRValRefType _LIBCPP_NODEBUG = typename __dependent_type<_DeleterSFINAE, _Dummy>::__good_rval_ref_type;

	template <bool _Dummy>
	using _BadRValRefType _LIBCPP_NODEBUG = typename __dependent_type<_DeleterSFINAE, _Dummy>::__bad_rval_ref_type;

	template <bool _Dummy, class _Deleter = typename __dependent_type<__identity<deleter_type>, _Dummy>::type>
	using _EnableIfDeleterDefaultConstructible _LIBCPP_NODEBUG =
	    typename enable_if<is_default_constructible<_Deleter>::value && !is_pointer<_Deleter>::value>::type;

	template <class _ArgType>
	using _EnableIfDeleterConstructible _LIBCPP_NODEBUG =
	    typename enable_if<is_constructible<deleter_type, _ArgType>::value>::type;

	template <class _Pp>
	using _EnableIfPointerConvertible _LIBCPP_NODEBUG =
	    typename enable_if<_CheckArrayPointerConversion<_Pp>::value>::type;

	template <class _UPtr, class _Up, class _ElemT = typename _UPtr::element_type>
	using _EnableIfMoveConvertible _LIBCPP_NODEBUG =
	    typename enable_if<is_array<_Up>::value && is_same<pointer, element_type *>::value &&
	                       is_same<typename _UPtr::pointer, _ElemT *>::value &&
	                       is_convertible<_ElemT (*)[], element_type (*)[]>::value>::type;

	template <class _UDel>
	using _EnableIfDeleterConvertible _LIBCPP_NODEBUG =
	    typename enable_if<(is_reference<_Dp>::value && is_same<_Dp, _UDel>::value) ||
	                       (!is_reference<_Dp>::value && is_convertible<_UDel, _Dp>::value)>::type;

	template <class _UDel>
	using _EnableIfDeleterAssignable _LIBCPP_NODEBUG = typename enable_if<is_assignable<_Dp &, _UDel &&>::value>::type;

public:
	template <bool _Dummy = true, class = _EnableIfDeleterDefaultConstructible<_Dummy>>
	_LIBCPP_INLINE_VISIBILITY _LIBCPP_CONSTEXPR unique_ptr() _NOEXCEPT : __ptr_(pointer(), __default_init_tag()) {
	}

	template <bool _Dummy = true, class = _EnableIfDeleterDefaultConstructible<_Dummy>>
	_LIBCPP_INLINE_VISIBILITY _LIBCPP_CONSTEXPR unique_ptr(nullptr_t) _NOEXCEPT
	    : __ptr_(pointer(), __default_init_tag()) {
	}

	template <class _Pp, bool _Dummy = true, class = _EnableIfDeleterDefaultConstructible<_Dummy>,
	          class = _EnableIfPointerConvertible<_Pp>>
	_LIBCPP_INLINE_VISIBILITY explicit unique_ptr(_Pp __p) _NOEXCEPT : __ptr_(__p, __default_init_tag()) {
	}

	template <class _Pp, bool _Dummy = true, class = _EnableIfDeleterConstructible<_LValRefType<_Dummy>>,
	          class = _EnableIfPointerConvertible<_Pp>>
	_LIBCPP_INLINE_VISIBILITY unique_ptr(_Pp __p, _LValRefType<_Dummy> __d) _NOEXCEPT : __ptr_(__p, __d) {
	}

	template <bool _Dummy = true, class = _EnableIfDeleterConstructible<_LValRefType<_Dummy>>>
	_LIBCPP_INLINE_VISIBILITY unique_ptr(nullptr_t, _LValRefType<_Dummy> __d) _NOEXCEPT : __ptr_(nullptr, __d) {
	}

	template <class _Pp, bool _Dummy = true, class = _EnableIfDeleterConstructible<_GoodRValRefType<_Dummy>>,
	          class = _EnableIfPointerConvertible<_Pp>>
	_LIBCPP_INLINE_VISIBILITY unique_ptr(_Pp __p, _GoodRValRefType<_Dummy> __d) _NOEXCEPT
	    : __ptr_(__p, _VSTD::move(__d)) {
		static_assert(!is_reference<deleter_type>::value, "rvalue deleter bound to reference");
	}

	template <bool _Dummy = true, class = _EnableIfDeleterConstructible<_GoodRValRefType<_Dummy>>>
	_LIBCPP_INLINE_VISIBILITY unique_ptr(nullptr_t, _GoodRValRefType<_Dummy> __d) _NOEXCEPT
	    : __ptr_(nullptr, _VSTD::move(__d)) {
		static_assert(!is_reference<deleter_type>::value, "rvalue deleter bound to reference");
	}

	template <class _Pp, bool _Dummy = true, class = _EnableIfDeleterConstructible<_BadRValRefType<_Dummy>>,
	          class = _EnableIfPointerConvertible<_Pp>>
	_LIBCPP_INLINE_VISIBILITY unique_ptr(_Pp __p, _BadRValRefType<_Dummy> __d) = delete;

	_LIBCPP_INLINE_VISIBILITY
	unique_ptr(unique_ptr &&__u) _NOEXCEPT : __ptr_(__u.release(), _VSTD::forward<deleter_type>(__u.get_deleter())) {
	}

	_LIBCPP_INLINE_VISIBILITY
	unique_ptr &operator=(unique_ptr &&__u) _NOEXCEPT {
		reset(__u.release());
		__ptr_.second() = _VSTD::forward<deleter_type>(__u.get_deleter());
		return *this;
	}

	template <class _Up, class _Ep, class = _EnableIfMoveConvertible<unique_ptr<_Up, _Ep>, _Up>,
	          class = _EnableIfDeleterConvertible<_Ep>>
	_LIBCPP_INLINE_VISIBILITY unique_ptr(unique_ptr<_Up, _Ep> &&__u) _NOEXCEPT
	    : __ptr_(__u.release(), _VSTD::forward<_Ep>(__u.get_deleter())) {
	}

	template <class _Up, class _Ep, class = _EnableIfMoveConvertible<unique_ptr<_Up, _Ep>, _Up>,
	          class = _EnableIfDeleterAssignable<_Ep>>
	_LIBCPP_INLINE_VISIBILITY unique_ptr &operator=(unique_ptr<_Up, _Ep> &&__u) _NOEXCEPT {
		reset(__u.release());
		__ptr_.second() = _VSTD::forward<_Ep>(__u.get_deleter());
		return *this;
	}

#ifdef _LIBCPP_CXX03_LANG
	unique_ptr(unique_ptr const &) = delete;
	unique_ptr &operator=(unique_ptr const &) = delete;
#endif

public:
	_LIBCPP_INLINE_VISIBILITY
	~unique_ptr() {
		reset();
	}

	_LIBCPP_INLINE_VISIBILITY
	unique_ptr &operator=(nullptr_t) _NOEXCEPT {
		reset();
		return *this;
	}

	_LIBCPP_INLINE_VISIBILITY
	typename add_lvalue_reference<_Tp>::type operator[](size_t __i) const {
		return __ptr_.first()[__i];
	}
	_LIBCPP_INLINE_VISIBILITY
	pointer get() const _NOEXCEPT {
		return __ptr_.first();
	}

	_LIBCPP_INLINE_VISIBILITY
	deleter_type &get_deleter() _NOEXCEPT {
		return __ptr_.second();
	}

	_LIBCPP_INLINE_VISIBILITY
	const deleter_type &get_deleter() const _NOEXCEPT {
		return __ptr_.second();
	}
	_LIBCPP_INLINE_VISIBILITY
	explicit operator bool() const _NOEXCEPT {
		return __ptr_.first() != nullptr;
	}

	_LIBCPP_INLINE_VISIBILITY
	pointer release() _NOEXCEPT {
		pointer __t = __ptr_.first();
		__ptr_.first() = pointer();
		return __t;
	}

	template <class _Pp>
	_LIBCPP_INLINE_VISIBILITY typename enable_if<_CheckArrayPointerConversion<_Pp>::value>::type
	reset(_Pp __p) _NOEXCEPT {
		pointer __tmp = __ptr_.first();
		__ptr_.first() = __p;
		if (__tmp)
			__ptr_.second()(__tmp);
	}

	_LIBCPP_INLINE_VISIBILITY
	void reset(nullptr_t = nullptr) _NOEXCEPT {
		pointer __tmp = __ptr_.first();
		__ptr_.first() = nullptr;
		if (__tmp)
			__ptr_.second()(__tmp);
	}

	_LIBCPP_INLINE_VISIBILITY
	void swap(unique_ptr &__u) _NOEXCEPT {
		__ptr_.swap(__u.__ptr_);
	}
};

} // namespace duckdb
