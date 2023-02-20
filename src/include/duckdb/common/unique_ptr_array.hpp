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

template <class _Tp, class D>
class unique_ptr<_Tp[], D> {
public:
	typedef _Tp element_type;
	typedef D deleter_type;
	typedef typename __pointer<element_type, deleter_type>::type pointer;

private:
	template <class _From>
	struct _CheckArrayPointerConversion : is_same<_From, pointer> {};

	template <class _FromElem>
	struct _CheckArrayPointerConversion<_FromElem *>
	    : integral_constant<bool, is_same<_FromElem *, pointer>::value ||
	                                  (is_same<pointer, element_type *>::value &&
	                                   is_convertible<_FromElem (*)[], element_type (*)[]>::value)> {};

	typedef __unique_ptr_deleter_sfinae<deleter_type> _DeleterSFINAE;

	template <bool _Dummy>
	using _LValRefType = typename __dependent_type<_DeleterSFINAE, _Dummy>::__lval_ref_type;

	template <bool _Dummy>
	using _GoodRValRefType = typename __dependent_type<_DeleterSFINAE, _Dummy>::__good_rval_ref_type;

	template <bool _Dummy>
	using _BadRValRefType = typename __dependent_type<_DeleterSFINAE, _Dummy>::__bad_rval_ref_type;

	template <bool _Dummy, class _Deleter = typename __dependent_type<__identity<deleter_type>, _Dummy>::type>
	using _EnableIfDeleterDefaultConstructible =
	    typename enable_if<is_default_constructible<_Deleter>::value && !is_pointer<_Deleter>::value>::type;

	template <class _ArgType>
	using _EnableIfDeleterConstructible = typename enable_if<is_constructible<deleter_type, _ArgType>::value>::type;

	template <class _Pp>
	using _EnableIfPointerConvertible = typename enable_if<_CheckArrayPointerConversion<_Pp>::value>::type;

	template <class _UPtr, class _Up, class _ElemT = typename _UPtr::element_type>
	using _EnableIfMoveConvertible =
	    typename enable_if<is_array<_Up>::value && is_same<pointer, element_type *>::value &&
	                       is_same<typename _UPtr::pointer, _ElemT *>::value &&
	                       is_convertible<_ElemT (*)[], element_type (*)[]>::value>::type;

	template <class _UDel>
	using _EnableIfDeleterConvertible =
	    typename enable_if<(is_reference<deleter_type>::value && is_same<deleter_type, _UDel>::value) ||
	                       (!is_reference<deleter_type>::value && is_convertible<_UDel, deleter_type>::value)>::type;

	template <class _UDel>
	using _EnableIfDeleterAssignable = typename enable_if<is_assignable<deleter_type &, _UDel &&>::value>::type;

public:
	template <bool _Dummy = true, class = _EnableIfDeleterDefaultConstructible<_Dummy>>
	inline constexpr unique_ptr() throw() : ptr(pointer()), deleter(deleter_type()) {
	}

	template <bool _Dummy = true, class = _EnableIfDeleterDefaultConstructible<_Dummy>>
	inline constexpr unique_ptr(nullptr_t) throw() : ptr(pointer()), deleter(deleter_type()) {
	}

	template <class _Pp, bool _Dummy = true, class = _EnableIfDeleterDefaultConstructible<_Dummy>,
	          class = _EnableIfPointerConvertible<_Pp>>
	inline explicit unique_ptr(_Pp __p) throw() : ptr(pointer()), deleter(deleter_type()) {
	}

	template <class _Pp, bool _Dummy = true, class = _EnableIfDeleterConstructible<_LValRefType<_Dummy>>,
	          class = _EnableIfPointerConvertible<_Pp>>
	inline unique_ptr(_Pp __p, _LValRefType<_Dummy> __d) throw() : ptr(pointer()), deleter(deleter_type()) {
	}

	template <bool _Dummy = true, class = _EnableIfDeleterConstructible<_LValRefType<_Dummy>>>
	inline unique_ptr(nullptr_t, _LValRefType<_Dummy> __d) throw() : ptr(pointer()), deleter(deleter_type()) {
	}

	template <class _Pp, bool _Dummy = true, class = _EnableIfDeleterConstructible<_GoodRValRefType<_Dummy>>,
	          class = _EnableIfPointerConvertible<_Pp>>
	inline unique_ptr(_Pp __p, _GoodRValRefType<_Dummy> __d) throw() : ptr(pointer()), deleter(std::move(__d)) {
		static_assert(!is_reference<deleter_type>::value, "rvalue deleter bound to reference");
	}

	template <bool _Dummy = true, class = _EnableIfDeleterConstructible<_GoodRValRefType<_Dummy>>>
	inline unique_ptr(nullptr_t, _GoodRValRefType<_Dummy> __d) throw() : ptr(pointer()), deleter(std::move(__d)) {
		static_assert(!is_reference<deleter_type>::value, "rvalue deleter bound to reference");
	}

	template <class _Pp, bool _Dummy = true, class = _EnableIfDeleterConstructible<_BadRValRefType<_Dummy>>,
	          class = _EnableIfPointerConvertible<_Pp>>
	inline unique_ptr(_Pp __p, _BadRValRefType<_Dummy> __d) = delete;

	inline unique_ptr(unique_ptr &&__u) throw()
	    : ptr(pointer()), deleter(std::forward<deleter_type>(__u.get_deleter())) {
	}

	inline unique_ptr &operator=(unique_ptr &&__u) throw() {
		reset(__u.release());
		deleter = std::forward<deleter_type>(__u.get_deleter());
		return *this;
	}

	template <class _Up, class _Ep, class = _EnableIfMoveConvertible<unique_ptr<_Up, _Ep>, _Up>,
	          class = _EnableIfDeleterConvertible<_Ep>>
	inline unique_ptr(unique_ptr<_Up, _Ep> &&__u) throw()
	    : ptr(pointer()), deleter(std::forward<_Ep>(__u.get_deleter())) {
	}

	template <class _Up, class _Ep, class = _EnableIfMoveConvertible<unique_ptr<_Up, _Ep>, _Up>,
	          class = _EnableIfDeleterAssignable<_Ep>>
	inline unique_ptr &operator=(unique_ptr<_Up, _Ep> &&__u) throw() {
		reset(__u.release());
		deleter = std::forward<_Ep>(__u.get_deleter());
		return *this;
	}

#ifdef _LIBCPP_CXX03_LANG
	unique_ptr(unique_ptr const &) = delete;
	unique_ptr &operator=(unique_ptr const &) = delete;
#endif

public:
	inline ~unique_ptr() {
		reset();
	}

	inline unique_ptr &operator=(nullptr_t) throw() {
		reset();
		return *this;
	}

	inline typename add_lvalue_reference<_Tp>::type operator[](size_t __i) const {
		return ptr[__i];
	}
	inline pointer get() const throw() {
		return ptr;
	}

	inline deleter_type &get_deleter() throw() {
		return deleter;
	}

	inline const deleter_type &get_deleter() const throw() {
		return deleter;
	}
	inline explicit operator bool() const throw() {
		return ptr != nullptr;
	}

	inline pointer release() throw() {
		pointer __t = ptr;
		ptr = pointer();
		return __t;
	}

	template <class _Pp>
	inline typename enable_if<_CheckArrayPointerConversion<_Pp>::value>::type reset(_Pp __p) throw() {
		pointer __tmp = ptr;
		ptr = __p;
		if (__tmp)
			deleter(__tmp);
	}

	inline void reset(nullptr_t = nullptr) throw() {
		pointer __tmp = ptr;
		ptr = nullptr;
		if (__tmp)
			deleter(__tmp);
	}

	inline void swap(unique_ptr &__u) throw() {
		std::swap(ptr, __u.ptr);
		std::swap(deleter, __u.deleter);
	}

private:
	pointer ptr;
	deleter_type deleter;
};

} // namespace duckdb
