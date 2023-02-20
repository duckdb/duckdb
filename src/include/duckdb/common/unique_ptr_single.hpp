#pragma once

#include <type_traits>
#include <memory>

using std::common_type;
using std::false_type;
using std::true_type;

using std::enable_if;

using std::is_array;
using std::is_assignable;
using std::is_constructible;
using std::is_convertible;
using std::is_default_constructible;
using std::is_function;
using std::is_pointer;
using std::is_reference;
using std::is_same;
using std::is_void;

using std::add_lvalue_reference;
using std::forward;
using std::integral_constant;
using std::less;
using std::runtime_error;
using std::swap;

using std::__dependent_type;
using std::__enable_hash_helper;
using std::__is_swappable;
using std::__pointer;

namespace duckdb {

template <class _Tp>
struct default_delete {
	static_assert(!is_function<_Tp>::value, "default_delete cannot be instantiated for function types");
#ifndef _LIBCPP_CXX03_LANG
	inline constexpr default_delete() throw() = default;
#else
	inline default_delete() {
	}
#endif
	template <class _Up>
	inline default_delete(const default_delete<_Up> &,
	                      typename enable_if<is_convertible<_Up *, _Tp *>::value>::type * = 0) throw() {
	}

	inline void operator()(_Tp *__ptr) const throw() {
		static_assert(sizeof(_Tp) > 0, "default_delete can not delete incomplete type");
		static_assert(!is_void<_Tp>::value, "default_delete can not delete incomplete type");
		delete __ptr;
	}
};

template <class _Tp>
struct default_delete<_Tp[]> {
private:
	template <class _Up>
	struct _EnableIfConvertible : enable_if<is_convertible<_Up (*)[], _Tp (*)[]>::value> {};

public:
#ifndef _LIBCPP_CXX03_LANG
	inline constexpr default_delete() throw() = default;
#else
	inline default_delete() {
	}
#endif

	template <class _Up>
	inline default_delete(const default_delete<_Up[]> &, typename _EnableIfConvertible<_Up>::type * = 0) throw() {
	}

	template <class _Up>
	inline typename _EnableIfConvertible<_Up>::type operator()(_Up *__ptr) const throw() {
		static_assert(sizeof(_Tp) > 0, "default_delete can not delete incomplete type");
		static_assert(!is_void<_Tp>::value, "default_delete can not delete void type");
		delete[] __ptr;
	}
};

template <class _Deleter>
struct __unique_ptr_deleter_sfinae {
	static_assert(!is_reference<_Deleter>::value, "incorrect specialization");
	typedef const _Deleter &__lval_ref_type;
	typedef _Deleter &&__good_rval_ref_type;
	typedef true_type __enable_rval_overload;
};

template <class _Deleter>
struct __unique_ptr_deleter_sfinae<_Deleter const &> {
	typedef const _Deleter &__lval_ref_type;
	typedef const _Deleter &&__bad_rval_ref_type;
	typedef false_type __enable_rval_overload;
};

template <class _Deleter>
struct __unique_ptr_deleter_sfinae<_Deleter &> {
	typedef _Deleter &__lval_ref_type;
	typedef _Deleter &&__bad_rval_ref_type;
	typedef false_type __enable_rval_overload;
};

template <class _Tp>
struct __identity {
	typedef _Tp type;
};

template <class _Tp>
using __identity_t = typename __identity<_Tp>::type;

template <class T, class D = default_delete<T>>
class unique_ptr {
public:
	typedef T element_type;
	typedef D deleter_type;
	//! We use __pointer for the case where T is an array type
	typedef typename __pointer<element_type, deleter_type>::type pointer;

private:
	struct __nat {
		int __for_bool_;
	};

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
	using _EnableIfDeleterConstructible = typename enable_if<is_constructible<D, _ArgType>::value>::type;

	template <class _UPtr, class _Up>
	using _EnableIfMoveConvertible =
	    typename enable_if<is_convertible<typename _UPtr::pointer, pointer>::value && !is_array<_Up>::value>::type;

	template <class _UDel>
	using _EnableIfDeleterConvertible =
	    typename enable_if<(is_reference<D>::value && is_same<D, _UDel>::value) ||
	                       (!is_reference<D>::value && is_convertible<_UDel, D>::value)>::type;

	template <class _UDel>
	using _EnableIfDeleterAssignable = typename enable_if<is_assignable<D &, _UDel &&>::value>::type;

public:
	//===--------------------------------------------------------------------===//
	// Constructors
	//===--------------------------------------------------------------------===//

	template <bool _Dummy = true, class = _EnableIfDeleterDefaultConstructible<_Dummy>>
	inline constexpr unique_ptr() throw() : ptr(pointer()), deleter(deleter_type()) {
	}

	template <bool _Dummy = true, class = _EnableIfDeleterDefaultConstructible<_Dummy>>
	inline constexpr unique_ptr(nullptr_t) throw() : ptr(pointer()), deleter(deleter_type()) {
	}

	template <bool _Dummy = true, class = _EnableIfDeleterDefaultConstructible<_Dummy>>
	inline explicit unique_ptr(pointer __p) throw() : ptr(__p), deleter(deleter_type()) {
	}

	template <bool _Dummy = true, class = _EnableIfDeleterConstructible<_LValRefType<_Dummy>>>
	inline unique_ptr(pointer __p, _LValRefType<_Dummy> __d) throw() : ptr(__p), deleter(__d) {
	}

	template <bool _Dummy = true, class = _EnableIfDeleterConstructible<_GoodRValRefType<_Dummy>>>
	inline unique_ptr(pointer __p, _GoodRValRefType<_Dummy> __d) throw() : ptr(__p), deleter(std::move(__d)) {
		static_assert(!is_reference<deleter_type>::value, "rvalue deleter bound to reference");
	}

	template <bool _Dummy = true, class = _EnableIfDeleterConstructible<_BadRValRefType<_Dummy>>>
	inline unique_ptr(pointer __p, _BadRValRefType<_Dummy> __d) = delete;

	inline unique_ptr(unique_ptr &&__u) throw()
	    : ptr(__u.release()), deleter(std::forward<deleter_type>(__u.get_deleter())) {
	}

	template <class _Up, class _Ep, class = _EnableIfMoveConvertible<unique_ptr<_Up, _Ep>, _Up>,
	          class = _EnableIfDeleterConvertible<_Ep>>
	inline unique_ptr(unique_ptr<_Up, _Ep> &&__u) throw()
	    : ptr(__u.release()), deleter(std::forward<_Ep>(__u.get_deleter())) {
	}

	template <class _Up, class _Ep, class = _EnableIfMoveConvertible<unique_ptr<_Up, _Ep>, _Up>,
	          class = _EnableIfDeleterAssignable<_Ep>>
	inline unique_ptr &operator=(unique_ptr<_Up, _Ep> &&__u) throw() {
		reset(__u.release());
		deleter = forward<_Ep>(__u.get_deleter());
		return *this;
	}

	inline unique_ptr &operator=(nullptr_t) throw() {
		reset();
		return *this;
	}

	operator bool() const {
		return ptr;
	}

	inline deleter_type &get_deleter() throw() {
		return deleter;
	}
	inline const deleter_type &get_deleter() const throw() {
		return deleter;
	}

	inline typename add_lvalue_reference<element_type>::type operator*() const {
		if (!ptr) {
			throw runtime_error("Attempting to dereference a unique pointer that is not set");
		}
		return *ptr;
	}
	pointer operator->() const {
		return ptr;
	}

	inline pointer get() const throw() {
		return ptr;
	}
	pointer release() noexcept {
		auto res = ptr;
		ptr = nullptr;
		return res;
	}
	void reset(pointer new_ptr = nullptr) noexcept {
		if (ptr) {
			delete ptr;
		}
		ptr = new_ptr;
	}
	void swap(unique_ptr &other) noexcept {
		std::swap(ptr, other.ptr);
		std::swap(deleter, other.deleter);
	}

	inline typename add_lvalue_reference<element_type>::type operator[](size_t __i) const {
		AssertNotNull();
		return ptr[__i];
	}

private:
	inline void AssertNotNull() const {
		if (!ptr) {
			throw runtime_error("Attempting to reference a unique pointer that is not set");
		}
	}

private:
	pointer ptr;
	deleter_type deleter;
};

//===--------------------------------------------------------------------===//
// Comparison operators
//===--------------------------------------------------------------------===//

template <class _Tp, class _Dp>
inline _LIBCPP_INLINE_VISIBILITY typename enable_if<__is_swappable<_Dp>::value, void>::type
swap(unique_ptr<_Tp, _Dp> &__x, unique_ptr<_Tp, _Dp> &__y) _NOEXCEPT {
	__x.swap(__y);
}

template <class _T1, class _D1, class _T2, class _D2>
inline _LIBCPP_INLINE_VISIBILITY bool operator==(const unique_ptr<_T1, _D1> &__x, const unique_ptr<_T2, _D2> &__y) {
	return __x.get() == __y.get();
}

template <class _T1, class _D1, class _T2, class _D2>
inline _LIBCPP_INLINE_VISIBILITY bool operator!=(const unique_ptr<_T1, _D1> &__x, const unique_ptr<_T2, _D2> &__y) {
	return !(__x == __y);
}

template <class _T1, class _D1, class _T2, class _D2>
inline _LIBCPP_INLINE_VISIBILITY bool operator<(const unique_ptr<_T1, _D1> &__x, const unique_ptr<_T2, _D2> &__y) {
	typedef typename unique_ptr<_T1, _D1>::pointer _P1;
	typedef typename unique_ptr<_T2, _D2>::pointer _P2;
	typedef typename common_type<_P1, _P2>::type _Vp;
	return less<_Vp>()(__x.get(), __y.get());
}

template <class _T1, class _D1, class _T2, class _D2>
inline _LIBCPP_INLINE_VISIBILITY bool operator>(const unique_ptr<_T1, _D1> &__x, const unique_ptr<_T2, _D2> &__y) {
	return __y < __x;
}

template <class _T1, class _D1, class _T2, class _D2>
inline _LIBCPP_INLINE_VISIBILITY bool operator<=(const unique_ptr<_T1, _D1> &__x, const unique_ptr<_T2, _D2> &__y) {
	return !(__y < __x);
}

template <class _T1, class _D1, class _T2, class _D2>
inline _LIBCPP_INLINE_VISIBILITY bool operator>=(const unique_ptr<_T1, _D1> &__x, const unique_ptr<_T2, _D2> &__y) {
	return !(__x < __y);
}

template <class _T1, class _D1>
inline _LIBCPP_INLINE_VISIBILITY bool operator==(const unique_ptr<_T1, _D1> &__x, nullptr_t) _NOEXCEPT {
	return !__x;
}

template <class _T1, class _D1>
inline _LIBCPP_INLINE_VISIBILITY bool operator==(nullptr_t, const unique_ptr<_T1, _D1> &__x) _NOEXCEPT {
	return !__x;
}

template <class _T1, class _D1>
inline _LIBCPP_INLINE_VISIBILITY bool operator!=(const unique_ptr<_T1, _D1> &__x, nullptr_t) _NOEXCEPT {
	return static_cast<bool>(__x);
}

template <class _T1, class _D1>
inline _LIBCPP_INLINE_VISIBILITY bool operator!=(nullptr_t, const unique_ptr<_T1, _D1> &__x) _NOEXCEPT {
	return static_cast<bool>(__x);
}

template <class _T1, class _D1>
inline _LIBCPP_INLINE_VISIBILITY bool operator<(const unique_ptr<_T1, _D1> &__x, nullptr_t) {
	typedef typename unique_ptr<_T1, _D1>::pointer _P1;
	return less<_P1>()(__x.get(), nullptr);
}

template <class _T1, class _D1>
inline _LIBCPP_INLINE_VISIBILITY bool operator<(nullptr_t, const unique_ptr<_T1, _D1> &__x) {
	typedef typename unique_ptr<_T1, _D1>::pointer _P1;
	return less<_P1>()(nullptr, __x.get());
}

template <class _T1, class _D1>
inline _LIBCPP_INLINE_VISIBILITY bool operator>(const unique_ptr<_T1, _D1> &__x, nullptr_t) {
	return nullptr < __x;
}

template <class _T1, class _D1>
inline _LIBCPP_INLINE_VISIBILITY bool operator>(nullptr_t, const unique_ptr<_T1, _D1> &__x) {
	return __x < nullptr;
}

template <class _T1, class _D1>
inline _LIBCPP_INLINE_VISIBILITY bool operator<=(const unique_ptr<_T1, _D1> &__x, nullptr_t) {
	return !(nullptr < __x);
}

template <class _T1, class _D1>
inline _LIBCPP_INLINE_VISIBILITY bool operator<=(nullptr_t, const unique_ptr<_T1, _D1> &__x) {
	return !(__x < nullptr);
}

template <class _T1, class _D1>
inline _LIBCPP_INLINE_VISIBILITY bool operator>=(const unique_ptr<_T1, _D1> &__x, nullptr_t) {
	return !(__x < nullptr);
}

template <class _T1, class _D1>
inline _LIBCPP_INLINE_VISIBILITY bool operator>=(nullptr_t, const unique_ptr<_T1, _D1> &__x) {
	return !(nullptr < __x);
}

template <class _Tp>
struct _LIBCPP_TEMPLATE_VIS hash;

template <class _Tp, class _Dp>
#ifdef _LIBCPP_CXX03_LANG
struct _LIBCPP_TEMPLATE_VIS hash<unique_ptr<_Tp, _Dp>>
#else
struct _LIBCPP_TEMPLATE_VIS hash<__enable_hash_helper<unique_ptr<_Tp, _Dp>, typename unique_ptr<_Tp, _Dp>::pointer>>
#endif
{
#if _LIBCPP_STD_VER <= 17 || defined(_LIBCPP_ENABLE_CXX20_REMOVED_BINDER_TYPEDEFS)
	_LIBCPP_DEPRECATED_IN_CXX17 typedef unique_ptr<_Tp, _Dp> argument_type;
	_LIBCPP_DEPRECATED_IN_CXX17 typedef size_t result_type;
#endif

	_LIBCPP_INLINE_VISIBILITY
	size_t operator()(const unique_ptr<_Tp, _Dp> &__ptr) const {
		typedef typename unique_ptr<_Tp, _Dp>::pointer pointer;
		return hash<pointer>()(__ptr.get());
	}
};

} // namespace duckdb
