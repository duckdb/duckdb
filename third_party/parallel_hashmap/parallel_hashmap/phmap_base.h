#if !defined(phmap_base_h_guard_)
#define phmap_base_h_guard_

// ---------------------------------------------------------------------------
// Copyright (c) 2019, Gregory Popovitch - greg7mdp@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Includes work from abseil-cpp (https://github.com/abseil/abseil-cpp)
// with modifications.
// 
// Copyright 2018 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ---------------------------------------------------------------------------

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <initializer_list>
#include <iterator>
#include <string>
#include <type_traits>
#include <utility>
#include <functional>
#include <tuple>
#include <utility>
#include <memory>
#include <mutex> // for std::lock

#include "phmap_config.h"

#ifdef PHMAP_HAVE_SHARED_MUTEX
    #include <shared_mutex>  // after "phmap_config.h"
#endif

#ifdef _MSC_VER
    #pragma warning(push)
    #pragma warning(disable : 4514) // unreferenced inline function has been removed
    #pragma warning(disable : 4582) // constructor is not implicitly called
    #pragma warning(disable : 4625) // copy constructor was implicitly defined as deleted
    #pragma warning(disable : 4626) // assignment operator was implicitly defined as deleted
    #pragma warning(disable : 4710) // function not inlined
    #pragma warning(disable : 4711) //  selected for automatic inline expansion
    #pragma warning(disable : 4820) // '6' bytes padding added after data member
#endif  // _MSC_VER

namespace phmap {

template <class T> using Allocator = typename std::allocator<T>;

template<class T1, class T2> using Pair = typename std::pair<T1, T2>;

template <class T>
struct EqualTo
{
    inline bool operator()(const T& a, const T& b) const
    {
        return std::equal_to<T>()(a, b);
    }
};

template <class T>
struct Less
{
    inline bool operator()(const T& a, const T& b) const
    {
        return std::less<T>()(a, b);
    }
};

namespace type_traits_internal {

template <typename... Ts>
struct VoidTImpl {
  using type = void;
};

// NOTE: The `is_detected` family of templates here differ from the library
// fundamentals specification in that for library fundamentals, `Op<Args...>` is
// evaluated as soon as the type `is_detected<Op, Args...>` undergoes
// substitution, regardless of whether or not the `::value` is accessed. That
// is inconsistent with all other standard traits and prevents lazy evaluation
// in larger contexts (such as if the `is_detected` check is a trailing argument
// of a `conjunction`. This implementation opts to instead be lazy in the same
// way that the standard traits are (this "defect" of the detection idiom
// specifications has been reported).
// ---------------------------------------------------------------------------

template <class Enabler, template <class...> class Op, class... Args>
struct is_detected_impl {
  using type = std::false_type;
};

template <template <class...> class Op, class... Args>
struct is_detected_impl<typename VoidTImpl<Op<Args...>>::type, Op, Args...> {
  using type = std::true_type;
};

template <template <class...> class Op, class... Args>
struct is_detected : is_detected_impl<void, Op, Args...>::type {};

template <class Enabler, class To, template <class...> class Op, class... Args>
struct is_detected_convertible_impl {
  using type = std::false_type;
};

template <class To, template <class...> class Op, class... Args>
struct is_detected_convertible_impl<
    typename std::enable_if<std::is_convertible<Op<Args...>, To>::value>::type,
    To, Op, Args...> {
  using type = std::true_type;
};

template <class To, template <class...> class Op, class... Args>
struct is_detected_convertible
    : is_detected_convertible_impl<void, To, Op, Args...>::type {};

template <typename T>
using IsCopyAssignableImpl =
    decltype(std::declval<T&>() = std::declval<const T&>());

template <typename T>
using IsMoveAssignableImpl = decltype(std::declval<T&>() = std::declval<T&&>());

}  // namespace type_traits_internal

template <typename T>
struct is_copy_assignable : type_traits_internal::is_detected<
                                type_traits_internal::IsCopyAssignableImpl, T> {
};

template <typename T>
struct is_move_assignable : type_traits_internal::is_detected<
                                type_traits_internal::IsMoveAssignableImpl, T> {
};

// ---------------------------------------------------------------------------
// void_t()
//
// Ignores the type of any its arguments and returns `void`. In general, this
// metafunction allows you to create a general case that maps to `void` while
// allowing specializations that map to specific types.
//
// This metafunction is designed to be a drop-in replacement for the C++17
// `std::void_t` metafunction.
//
// NOTE: `phmap::void_t` does not use the standard-specified implementation so
// that it can remain compatible with gcc < 5.1. This can introduce slightly
// different behavior, such as when ordering partial specializations.
// ---------------------------------------------------------------------------
template <typename... Ts>
using void_t = typename type_traits_internal::VoidTImpl<Ts...>::type;

// ---------------------------------------------------------------------------
// conjunction
//
// Performs a compile-time logical AND operation on the passed types (which
// must have  `::value` members convertible to `bool`. Short-circuits if it
// encounters any `false` members (and does not compare the `::value` members
// of any remaining arguments).
//
// This metafunction is designed to be a drop-in replacement for the C++17
// `std::conjunction` metafunction.
// ---------------------------------------------------------------------------
template <typename... Ts>
struct conjunction;

template <typename T, typename... Ts>
struct conjunction<T, Ts...>
    : std::conditional<T::value, conjunction<Ts...>, T>::type {};

template <typename T>
struct conjunction<T> : T {};

template <>
struct conjunction<> : std::true_type {};

// ---------------------------------------------------------------------------
// disjunction
//
// Performs a compile-time logical OR operation on the passed types (which
// must have  `::value` members convertible to `bool`. Short-circuits if it
// encounters any `true` members (and does not compare the `::value` members
// of any remaining arguments).
//
// This metafunction is designed to be a drop-in replacement for the C++17
// `std::disjunction` metafunction.
// ---------------------------------------------------------------------------
template <typename... Ts>
struct disjunction;

template <typename T, typename... Ts>
struct disjunction<T, Ts...> :
      std::conditional<T::value, T, disjunction<Ts...>>::type {};

template <typename T>
struct disjunction<T> : T {};

template <>
struct disjunction<> : std::false_type {};

template <typename T>
struct negation : std::integral_constant<bool, !T::value> {};

#if defined(__GNUC__) && __GNUC__ < 5 && !defined(__clang__) && !defined(_MSC_VER) && !defined(__INTEL_COMPILER)
    #define PHMAP_OLD_GCC 1
#else
    #define PHMAP_OLD_GCC 0
#endif

#if PHMAP_OLD_GCC
  template <typename T>
  struct is_trivially_copy_constructible
     : std::integral_constant<bool,
                              __has_trivial_copy(typename std::remove_reference<T>::type) &&
                              std::is_copy_constructible<T>::value &&
                              std::is_trivially_destructible<T>::value> {};
 
  template <typename T>
  struct is_trivially_copy_assignable :
     std::integral_constant<bool,
                            __has_trivial_assign(typename std::remove_reference<T>::type) &&
                            phmap::is_copy_assignable<T>::value> {};

  template <typename T>
  struct is_trivially_copyable :
     std::integral_constant<bool, __has_trivial_copy(typename std::remove_reference<T>::type)> {};

#else
  template <typename T> using is_trivially_copy_constructible = std::is_trivially_copy_constructible<T>;
  template <typename T> using is_trivially_copy_assignable = std::is_trivially_copy_assignable<T>;
  template <typename T> using is_trivially_copyable = std::is_trivially_copyable<T>;
#endif

// -----------------------------------------------------------------------------
// C++14 "_t" trait aliases
// -----------------------------------------------------------------------------

template <typename T>
using remove_cv_t = typename std::remove_cv<T>::type;

template <typename T>
using remove_const_t = typename std::remove_const<T>::type;

template <typename T>
using remove_volatile_t = typename std::remove_volatile<T>::type;

template <typename T>
using add_cv_t = typename std::add_cv<T>::type;

template <typename T>
using add_const_t = typename std::add_const<T>::type;

template <typename T>
using add_volatile_t = typename std::add_volatile<T>::type;

template <typename T>
using remove_reference_t = typename std::remove_reference<T>::type;

template <typename T>
using add_lvalue_reference_t = typename std::add_lvalue_reference<T>::type;

template <typename T>
using add_rvalue_reference_t = typename std::add_rvalue_reference<T>::type;

template <typename T>
using remove_pointer_t = typename std::remove_pointer<T>::type;

template <typename T>
using add_pointer_t = typename std::add_pointer<T>::type;

template <typename T>
using make_signed_t = typename std::make_signed<T>::type;

template <typename T>
using make_unsigned_t = typename std::make_unsigned<T>::type;

template <typename T>
using remove_extent_t = typename std::remove_extent<T>::type;

template <typename T>
using remove_all_extents_t = typename std::remove_all_extents<T>::type;

template<std::size_t Len, std::size_t Align>
struct aligned_storage {
    struct type {
        alignas(Align) unsigned char data[Len];
    };
};

template< std::size_t Len, std::size_t Align>
using aligned_storage_t = typename aligned_storage<Len, Align>::type;

template <typename T>
using decay_t = typename std::decay<T>::type;

template <bool B, typename T = void>
using enable_if_t = typename std::enable_if<B, T>::type;

template <bool B, typename T, typename F>
using conditional_t = typename std::conditional<B, T, F>::type;


template <typename... T>
using common_type_t = typename std::common_type<T...>::type;

template <typename T>
using underlying_type_t = typename std::underlying_type<T>::type;

template< class F, class... ArgTypes>
#if PHMAP_HAVE_CC17 && defined(__cpp_lib_result_of_sfinae)
    using invoke_result_t = typename std::invoke_result_t<F, ArgTypes...>;
#else
    using invoke_result_t = typename std::result_of<F(ArgTypes...)>::type;
#endif

namespace type_traits_internal {

// ----------------------------------------------------------------------
// In MSVC we can't probe std::hash or stdext::hash because it triggers a
// static_assert instead of failing substitution. Libc++ prior to 4.0
// also used a static_assert.
// ----------------------------------------------------------------------
#if defined(_MSC_VER) || (defined(_LIBCPP_VERSION) && \
                          _LIBCPP_VERSION < 4000 && _LIBCPP_STD_VER > 11)
    #define PHMAP_META_INTERNAL_STD_HASH_SFINAE_FRIENDLY_ 0
#else
    #define PHMAP_META_INTERNAL_STD_HASH_SFINAE_FRIENDLY_ 1
#endif

#if !PHMAP_META_INTERNAL_STD_HASH_SFINAE_FRIENDLY_
    template <typename Key, typename = size_t>
    struct IsHashable : std::true_type {};
#else   // PHMAP_META_INTERNAL_STD_HASH_SFINAE_FRIENDLY_
    template <typename Key, typename = void>
    struct IsHashable : std::false_type {};

    template <typename Key>
    struct IsHashable<Key,
        phmap::enable_if_t<std::is_convertible<
            decltype(std::declval<std::hash<Key>&>()(std::declval<Key const&>())),
            std::size_t>::value>> : std::true_type {};
#endif

struct AssertHashEnabledHelper 
{
private:
    static void Sink(...) {}
    struct NAT {};

    template <class Key>
    static auto GetReturnType(int)
        -> decltype(std::declval<std::hash<Key>>()(std::declval<Key const&>()));
    template <class Key>
    static NAT GetReturnType(...);

    template <class Key>
    static std::nullptr_t DoIt() {
        static_assert(IsHashable<Key>::value,
                      "std::hash<Key> does not provide a call operator");
        static_assert(
            std::is_default_constructible<std::hash<Key>>::value,
            "std::hash<Key> must be default constructible when it is enabled");
        static_assert(
            std::is_copy_constructible<std::hash<Key>>::value,
            "std::hash<Key> must be copy constructible when it is enabled");
        static_assert(phmap::is_copy_assignable<std::hash<Key>>::value,
                      "std::hash<Key> must be copy assignable when it is enabled");
        // is_destructible is unchecked as it's implied by each of the
        // is_constructible checks.
        using ReturnType = decltype(GetReturnType<Key>(0));
        static_assert(std::is_same<ReturnType, NAT>::value ||
                      std::is_same<ReturnType, size_t>::value,
                      "std::hash<Key> must return size_t");
        return nullptr;
    }

    template <class... Ts>
    friend void AssertHashEnabled();
};

template <class... Ts>
inline void AssertHashEnabled
() 
{
    using Helper = AssertHashEnabledHelper;
    Helper::Sink(Helper::DoIt<Ts>()...);
}

}  // namespace type_traits_internal

}  // namespace phmap


// -----------------------------------------------------------------------------
//          hash_policy_traits
// -----------------------------------------------------------------------------
namespace phmap {
namespace priv {

// Defines how slots are initialized/destroyed/moved.
template <class Policy, class = void>
struct hash_policy_traits 
{
private:
    struct ReturnKey 
    {
        // We return `Key` here.
        // When Key=T&, we forward the lvalue reference.
        // When Key=T, we return by value to avoid a dangling reference.
        // eg, for string_hash_map.
        template <class Key, class... Args>
        Key operator()(Key&& k, const Args&...) const {
            return std::forward<Key>(k);
        }
    };

    template <class P = Policy, class = void>
    struct ConstantIteratorsImpl : std::false_type {};

    template <class P>
    struct ConstantIteratorsImpl<P, phmap::void_t<typename P::constant_iterators>>
        : P::constant_iterators {};

public:
    // The actual object stored in the hash table.
    using slot_type  = typename Policy::slot_type;

    // The type of the keys stored in the hashtable.
    using key_type   = typename Policy::key_type;

    // The argument type for insertions into the hashtable. This is different
    // from value_type for increased performance. See initializer_list constructor
    // and insert() member functions for more details.
    using init_type  = typename Policy::init_type;

    using reference  = decltype(Policy::element(std::declval<slot_type*>()));
    using pointer    = typename std::remove_reference<reference>::type*;
    using value_type = typename std::remove_reference<reference>::type;

    // Policies can set this variable to tell raw_hash_set that all iterators
    // should be constant, even `iterator`. This is useful for set-like
    // containers.
    // Defaults to false if not provided by the policy.
    using constant_iterators = ConstantIteratorsImpl<>;

    // PRECONDITION: `slot` is UNINITIALIZED
    // POSTCONDITION: `slot` is INITIALIZED
    template <class Alloc, class... Args>
    static void construct(Alloc* alloc, slot_type* slot, Args&&... args) {
        Policy::construct(alloc, slot, std::forward<Args>(args)...);
    }

    // PRECONDITION: `slot` is INITIALIZED
    // POSTCONDITION: `slot` is UNINITIALIZED
    template <class Alloc>
    static void destroy(Alloc* alloc, slot_type* slot) {
        Policy::destroy(alloc, slot);
    }

    // Transfers the `old_slot` to `new_slot`. Any memory allocated by the
    // allocator inside `old_slot` to `new_slot` can be transferred.
    //
    // OPTIONAL: defaults to:
    //
    //     clone(new_slot, std::move(*old_slot));
    //     destroy(old_slot);
    //
    // PRECONDITION: `new_slot` is UNINITIALIZED and `old_slot` is INITIALIZED
    // POSTCONDITION: `new_slot` is INITIALIZED and `old_slot` is
    //                UNINITIALIZED
    template <class Alloc>
    static void transfer(Alloc* alloc, slot_type* new_slot, slot_type* old_slot) {
        transfer_impl(alloc, new_slot, old_slot, 0);
    }

    // PRECONDITION: `slot` is INITIALIZED
    // POSTCONDITION: `slot` is INITIALIZED
    template <class P = Policy>
    static auto element(slot_type* slot) -> decltype(P::element(slot)) {
        return P::element(slot);
    }

    // Returns the amount of memory owned by `slot`, exclusive of `sizeof(*slot)`.
    //
    // If `slot` is nullptr, returns the constant amount of memory owned by any
    // full slot or -1 if slots own variable amounts of memory.
    //
    // PRECONDITION: `slot` is INITIALIZED or nullptr
    template <class P = Policy>
    static size_t space_used(const slot_type* slot) {
        return P::space_used(slot);
    }

    // Provides generalized access to the key for elements, both for elements in
    // the table and for elements that have not yet been inserted (or even
    // constructed).  We would like an API that allows us to say: `key(args...)`
    // but we cannot do that for all cases, so we use this more general API that
    // can be used for many things, including the following:
    //
    //   - Given an element in a table, get its key.
    //   - Given an element initializer, get its key.
    //   - Given `emplace()` arguments, get the element key.
    //
    // Implementations of this must adhere to a very strict technical
    // specification around aliasing and consuming arguments:
    //
    // Let `value_type` be the result type of `element()` without ref- and
    // cv-qualifiers. The first argument is a functor, the rest are constructor
    // arguments for `value_type`. Returns `std::forward<F>(f)(k, xs...)`, where
    // `k` is the element key, and `xs...` are the new constructor arguments for
    // `value_type`. It's allowed for `k` to alias `xs...`, and for both to alias
    // `ts...`. The key won't be touched once `xs...` are used to construct an
    // element; `ts...` won't be touched at all, which allows `apply()` to consume
    // any rvalues among them.
    //
    // If `value_type` is constructible from `Ts&&...`, `Policy::apply()` must not
    // trigger a hard compile error unless it originates from `f`. In other words,
    // `Policy::apply()` must be SFINAE-friendly. If `value_type` is not
    // constructible from `Ts&&...`, either SFINAE or a hard compile error is OK.
    //
    // If `Ts...` is `[cv] value_type[&]` or `[cv] init_type[&]`,
    // `Policy::apply()` must work. A compile error is not allowed, SFINAE or not.
    template <class F, class... Ts, class P = Policy>
    static auto apply(F&& f, Ts&&... ts)
        -> decltype(P::apply(std::forward<F>(f), std::forward<Ts>(ts)...)) {
        return P::apply(std::forward<F>(f), std::forward<Ts>(ts)...);
    }

    // Returns the "key" portion of the slot.
    // Used for node handle manipulation.
    template <class P = Policy>
    static auto key(slot_type* slot)
        -> decltype(P::apply(ReturnKey(), element(slot))) {
        return P::apply(ReturnKey(), element(slot));
    }

    // Returns the "value" (as opposed to the "key") portion of the element. Used
    // by maps to implement `operator[]`, `at()` and `insert_or_assign()`.
    template <class T, class P = Policy>
    static auto value(T* elem) -> decltype(P::value(elem)) {
        return P::value(elem);
    }

private:

    // Use auto -> decltype as an enabler.
    template <class Alloc, class P = Policy>
    static auto transfer_impl(Alloc* alloc, slot_type* new_slot,
                              slot_type* old_slot, int)
        -> decltype((void)P::transfer(alloc, new_slot, old_slot)) {
        P::transfer(alloc, new_slot, old_slot);
    }

    template <class Alloc>
    static void transfer_impl(Alloc* alloc, slot_type* new_slot,
                              slot_type* old_slot, char) {
        construct(alloc, new_slot, std::move(element(old_slot)));
        destroy(alloc, old_slot);
    }
};

}  // namespace priv
}  // namespace phmap

// -----------------------------------------------------------------------------
// file utility.h
// -----------------------------------------------------------------------------

// --------- identity.h
namespace phmap {
namespace internal {

template <typename T>
struct identity {
    typedef T type;
};

template <typename T>
using identity_t = typename identity<T>::type;

}  // namespace internal
}  // namespace phmap


// --------- inline_variable.h

#ifdef __cpp_inline_variables

#if defined(__clang__)
    #define PHMAP_INTERNAL_EXTERN_DECL(type, name) \
      extern const ::phmap::internal::identity_t<type> name;
#else  // Otherwise, just define the macro to do nothing.
    #define PHMAP_INTERNAL_EXTERN_DECL(type, name)
#endif  // defined(__clang__)

// See above comment at top of file for details.
#define PHMAP_INTERNAL_INLINE_CONSTEXPR(type, name, init) \
  PHMAP_INTERNAL_EXTERN_DECL(type, name)                  \
  inline constexpr ::phmap::internal::identity_t<type> name = init

#else

// See above comment at top of file for details.
//
// Note:
//   identity_t is used here so that the const and name are in the
//   appropriate place for pointer types, reference types, function pointer
//   types, etc..
#define PHMAP_INTERNAL_INLINE_CONSTEXPR(var_type, name, init)                  \
  template <class /*PhmapInternalDummy*/ = void>                               \
  struct PhmapInternalInlineVariableHolder##name {                             \
    static constexpr ::phmap::internal::identity_t<var_type> kInstance = init; \
  };                                                                          \
                                                                              \
  template <class PhmapInternalDummy>                                          \
  constexpr ::phmap::internal::identity_t<var_type>                            \
      PhmapInternalInlineVariableHolder##name<PhmapInternalDummy>::kInstance;   \
                                                                              \
  static constexpr const ::phmap::internal::identity_t<var_type>&              \
      name = /* NOLINT */                                                     \
      PhmapInternalInlineVariableHolder##name<>::kInstance;                    \
  static_assert(sizeof(void (*)(decltype(name))) != 0,                        \
                "Silence unused variable warnings.")

#endif  // __cpp_inline_variables

// ----------- throw_delegate

namespace phmap {
namespace base_internal {

namespace {

#ifdef PHMAP_HAVE_EXCEPTIONS
  #define PHMAP_THROW_IMPL_MSG(e, message) throw e(message)
  #define PHMAP_THROW_IMPL(e) throw e()
#else
  #define PHMAP_THROW_IMPL_MSG(e, message) do { (void)(message); std::abort(); } while(0)
  #define PHMAP_THROW_IMPL(e) std::abort()
#endif
}  // namespace

static inline void ThrowStdLogicError(const std::string& what_arg) {
  PHMAP_THROW_IMPL_MSG(std::logic_error, what_arg);
}
static inline void ThrowStdLogicError(const char* what_arg) {
  PHMAP_THROW_IMPL_MSG(std::logic_error, what_arg);
}
static inline void ThrowStdInvalidArgument(const std::string& what_arg) {
  PHMAP_THROW_IMPL_MSG(std::invalid_argument, what_arg);
}
static inline void ThrowStdInvalidArgument(const char* what_arg) {
  PHMAP_THROW_IMPL_MSG(std::invalid_argument, what_arg);
}

static inline void ThrowStdDomainError(const std::string& what_arg) {
  PHMAP_THROW_IMPL_MSG(std::domain_error, what_arg);
}
static inline void ThrowStdDomainError(const char* what_arg) {
  PHMAP_THROW_IMPL_MSG(std::domain_error, what_arg);
}

static inline void ThrowStdLengthError(const std::string& what_arg) {
  PHMAP_THROW_IMPL_MSG(std::length_error, what_arg);
}
static inline void ThrowStdLengthError(const char* what_arg) {
  PHMAP_THROW_IMPL_MSG(std::length_error, what_arg);
}

static inline void ThrowStdOutOfRange(const std::string& what_arg) {
  PHMAP_THROW_IMPL_MSG(std::out_of_range, what_arg);
}
static inline void ThrowStdOutOfRange(const char* what_arg) {
  PHMAP_THROW_IMPL_MSG(std::out_of_range, what_arg);
}

static inline void ThrowStdRuntimeError(const std::string& what_arg) {
  PHMAP_THROW_IMPL_MSG(std::runtime_error, what_arg);
}
static inline void ThrowStdRuntimeError(const char* what_arg) {
  PHMAP_THROW_IMPL_MSG(std::runtime_error, what_arg);
}

static inline void ThrowStdRangeError(const std::string& what_arg) {
  PHMAP_THROW_IMPL_MSG(std::range_error, what_arg);
}
static inline void ThrowStdRangeError(const char* what_arg) {
  PHMAP_THROW_IMPL_MSG(std::range_error, what_arg);
}

static inline void ThrowStdOverflowError(const std::string& what_arg) {
  PHMAP_THROW_IMPL_MSG(std::overflow_error, what_arg);
}
    
static inline void ThrowStdOverflowError(const char* what_arg) {
  PHMAP_THROW_IMPL_MSG(std::overflow_error, what_arg);
}

static inline void ThrowStdUnderflowError(const std::string& what_arg) {
  PHMAP_THROW_IMPL_MSG(std::underflow_error, what_arg);
}
    
static inline void ThrowStdUnderflowError(const char* what_arg) {
  PHMAP_THROW_IMPL_MSG(std::underflow_error, what_arg);
}
    
static inline void ThrowStdBadFunctionCall() {
  PHMAP_THROW_IMPL(std::bad_function_call);
}
    
static inline void ThrowStdBadAlloc() {
  PHMAP_THROW_IMPL(std::bad_alloc);
}

}  // namespace base_internal
}  // namespace phmap

// ----------- invoke.h

namespace phmap {
namespace base_internal {

template <typename Derived>
struct StrippedAccept 
{
    template <typename... Args>
    struct Accept : Derived::template AcceptImpl<typename std::remove_cv<
                                                     typename std::remove_reference<Args>::type>::type...> {};
};

// (t1.*f)(t2, ..., tN) when f is a pointer to a member function of a class T
// and t1 is an object of type T or a reference to an object of type T or a
// reference to an object of a type derived from T.
struct MemFunAndRef : StrippedAccept<MemFunAndRef> 
{
    template <typename... Args>
    struct AcceptImpl : std::false_type {};

    template <typename R, typename C, typename... Params, typename Obj,
              typename... Args>
    struct AcceptImpl<R (C::*)(Params...), Obj, Args...>
        : std::is_base_of<C, Obj> {};

    template <typename R, typename C, typename... Params, typename Obj,
              typename... Args>
    struct AcceptImpl<R (C::*)(Params...) const, Obj, Args...>
        : std::is_base_of<C, Obj> {};

    template <typename MemFun, typename Obj, typename... Args>
    static decltype((std::declval<Obj>().*
                     std::declval<MemFun>())(std::declval<Args>()...))
    Invoke(MemFun&& mem_fun, Obj&& obj, Args&&... args) {
        return (std::forward<Obj>(obj).*
                std::forward<MemFun>(mem_fun))(std::forward<Args>(args)...);
    }
};

// ((*t1).*f)(t2, ..., tN) when f is a pointer to a member function of a
// class T and t1 is not one of the types described in the previous item.
struct MemFunAndPtr : StrippedAccept<MemFunAndPtr> 
{
    template <typename... Args>
    struct AcceptImpl : std::false_type {};

    template <typename R, typename C, typename... Params, typename Ptr,
              typename... Args>
    struct AcceptImpl<R (C::*)(Params...), Ptr, Args...>
        : std::integral_constant<bool, !std::is_base_of<C, Ptr>::value> {};

    template <typename R, typename C, typename... Params, typename Ptr,
              typename... Args>
    struct AcceptImpl<R (C::*)(Params...) const, Ptr, Args...>
        : std::integral_constant<bool, !std::is_base_of<C, Ptr>::value> {};

    template <typename MemFun, typename Ptr, typename... Args>
    static decltype(((*std::declval<Ptr>()).*
                     std::declval<MemFun>())(std::declval<Args>()...))
    Invoke(MemFun&& mem_fun, Ptr&& ptr, Args&&... args) {
        return ((*std::forward<Ptr>(ptr)).*
                std::forward<MemFun>(mem_fun))(std::forward<Args>(args)...);
    }
};

// t1.*f when N == 1 and f is a pointer to member data of a class T and t1 is
// an object of type T or a reference to an object of type T or a reference
// to an object of a type derived from T.
struct DataMemAndRef : StrippedAccept<DataMemAndRef> 
{
    template <typename... Args>
    struct AcceptImpl : std::false_type {};

    template <typename R, typename C, typename Obj>
    struct AcceptImpl<R C::*, Obj> : std::is_base_of<C, Obj> {};

    template <typename DataMem, typename Ref>
    static decltype(std::declval<Ref>().*std::declval<DataMem>()) Invoke(
        DataMem&& data_mem, Ref&& ref) {
        return std::forward<Ref>(ref).*std::forward<DataMem>(data_mem);
    }
};

// (*t1).*f when N == 1 and f is a pointer to member data of a class T and t1
// is not one of the types described in the previous item.
struct DataMemAndPtr : StrippedAccept<DataMemAndPtr> 
{
    template <typename... Args>
    struct AcceptImpl : std::false_type {};

    template <typename R, typename C, typename Ptr>
    struct AcceptImpl<R C::*, Ptr>
        : std::integral_constant<bool, !std::is_base_of<C, Ptr>::value> {};

    template <typename DataMem, typename Ptr>
    static decltype((*std::declval<Ptr>()).*std::declval<DataMem>()) Invoke(
        DataMem&& data_mem, Ptr&& ptr) {
        return (*std::forward<Ptr>(ptr)).*std::forward<DataMem>(data_mem);
    }
};

// f(t1, t2, ..., tN) in all other cases.
struct Callable
{
    // Callable doesn't have Accept because it's the last clause that gets picked
    // when none of the previous clauses are applicable.
    template <typename F, typename... Args>
    static decltype(std::declval<F>()(std::declval<Args>()...)) Invoke(
        F&& f, Args&&... args) {
        return std::forward<F>(f)(std::forward<Args>(args)...);
    }
};

// Resolves to the first matching clause.
template <typename... Args>
struct Invoker 
{
    typedef typename std::conditional<
        MemFunAndRef::Accept<Args...>::value, MemFunAndRef,
        typename std::conditional<
            MemFunAndPtr::Accept<Args...>::value, MemFunAndPtr,
            typename std::conditional<
                DataMemAndRef::Accept<Args...>::value, DataMemAndRef,
                typename std::conditional<DataMemAndPtr::Accept<Args...>::value,
                                          DataMemAndPtr, Callable>::type>::type>::
        type>::type type;
};

// The result type of Invoke<F, Args...>.
template <typename F, typename... Args>
using InvokeT = decltype(Invoker<F, Args...>::type::Invoke(
    std::declval<F>(), std::declval<Args>()...));

// Invoke(f, args...) is an implementation of INVOKE(f, args...) from section
// [func.require] of the C++ standard.
template <typename F, typename... Args>
InvokeT<F, Args...> Invoke(F&& f, Args&&... args) {
  return Invoker<F, Args...>::type::Invoke(std::forward<F>(f),
                                           std::forward<Args>(args)...);
}
}  // namespace base_internal
}  // namespace phmap


// ----------- utility.h

namespace phmap {

// integer_sequence
//
// Class template representing a compile-time integer sequence. An instantiation
// of `integer_sequence<T, Ints...>` has a sequence of integers encoded in its
// type through its template arguments (which is a common need when
// working with C++11 variadic templates). `phmap::integer_sequence` is designed
// to be a drop-in replacement for C++14's `std::integer_sequence`.
//
// Example:
//
//   template< class T, T... Ints >
//   void user_function(integer_sequence<T, Ints...>);
//
//   int main()
//   {
//     // user_function's `T` will be deduced to `int` and `Ints...`
//     // will be deduced to `0, 1, 2, 3, 4`.
//     user_function(make_integer_sequence<int, 5>());
//   }
template <typename T, T... Ints>
struct integer_sequence 
{
    using value_type = T;
    static constexpr size_t size() noexcept { return sizeof...(Ints); }
};

// index_sequence
//
// A helper template for an `integer_sequence` of `size_t`,
// `phmap::index_sequence` is designed to be a drop-in replacement for C++14's
// `std::index_sequence`.
template <size_t... Ints>
using index_sequence = integer_sequence<size_t, Ints...>;

namespace utility_internal {

template <typename Seq, size_t SeqSize, size_t Rem>
struct Extend;

// Note that SeqSize == sizeof...(Ints). It's passed explicitly for efficiency.
template <typename T, T... Ints, size_t SeqSize>
struct Extend<integer_sequence<T, Ints...>, SeqSize, 0> {
  using type = integer_sequence<T, Ints..., (Ints + SeqSize)...>;
};

template <typename T, T... Ints, size_t SeqSize>
struct Extend<integer_sequence<T, Ints...>, SeqSize, 1> {
  using type = integer_sequence<T, Ints..., (Ints + SeqSize)..., 2 * SeqSize>;
};

// Recursion helper for 'make_integer_sequence<T, N>'.
// 'Gen<T, N>::type' is an alias for 'integer_sequence<T, 0, 1, ... N-1>'.
template <typename T, size_t N>
struct Gen {
  using type =
      typename Extend<typename Gen<T, N / 2>::type, N / 2, N % 2>::type;
};

template <typename T>
struct Gen<T, 0> {
  using type = integer_sequence<T>;
};

}  // namespace utility_internal

// Compile-time sequences of integers

// make_integer_sequence
//
// This template alias is equivalent to
// `integer_sequence<int, 0, 1, ..., N-1>`, and is designed to be a drop-in
// replacement for C++14's `std::make_integer_sequence`.
template <typename T, T N>
using make_integer_sequence = typename utility_internal::Gen<T, N>::type;

// make_index_sequence
//
// This template alias is equivalent to `index_sequence<0, 1, ..., N-1>`,
// and is designed to be a drop-in replacement for C++14's
// `std::make_index_sequence`.
template <size_t N>
using make_index_sequence = make_integer_sequence<size_t, N>;

// index_sequence_for
//
// Converts a typename pack into an index sequence of the same length, and
// is designed to be a drop-in replacement for C++14's
// `std::index_sequence_for()`
template <typename... Ts>
using index_sequence_for = make_index_sequence<sizeof...(Ts)>;

// Tag types

#ifdef PHMAP_HAVE_STD_OPTIONAL

using std::in_place_t;
using std::in_place;

#else  // PHMAP_HAVE_STD_OPTIONAL

// in_place_t
//
// Tag type used to specify in-place construction, such as with
// `phmap::optional`, designed to be a drop-in replacement for C++17's
// `std::in_place_t`.
struct in_place_t {};

PHMAP_INTERNAL_INLINE_CONSTEXPR(in_place_t, in_place, {});

#endif  // PHMAP_HAVE_STD_OPTIONAL

#if defined(PHMAP_HAVE_STD_ANY) || defined(PHMAP_HAVE_STD_VARIANT)
using std::in_place_type_t;
#else

// in_place_type_t
//
// Tag type used for in-place construction when the type to construct needs to
// be specified, such as with `phmap::any`, designed to be a drop-in replacement
// for C++17's `std::in_place_type_t`.
template <typename T>
struct in_place_type_t {};
#endif  // PHMAP_HAVE_STD_ANY || PHMAP_HAVE_STD_VARIANT

#ifdef PHMAP_HAVE_STD_VARIANT
using std::in_place_index_t;
#else

// in_place_index_t
//
// Tag type used for in-place construction when the type to construct needs to
// be specified, such as with `phmap::any`, designed to be a drop-in replacement
// for C++17's `std::in_place_index_t`.
template <size_t I>
struct in_place_index_t {};
#endif  // PHMAP_HAVE_STD_VARIANT

// Constexpr move and forward

// move()
//
// A constexpr version of `std::move()`, designed to be a drop-in replacement
// for C++14's `std::move()`.
template <typename T>
constexpr phmap::remove_reference_t<T>&& move(T&& t) noexcept {
  return static_cast<phmap::remove_reference_t<T>&&>(t);
}

// forward()
//
// A constexpr version of `std::forward()`, designed to be a drop-in replacement
// for C++14's `std::forward()`.
template <typename T>
constexpr T&& forward(
    phmap::remove_reference_t<T>& t) noexcept {  // NOLINT(runtime/references)
  return static_cast<T&&>(t);
}

namespace utility_internal {
// Helper method for expanding tuple into a called method.
template <typename Functor, typename Tuple, std::size_t... Indexes>
auto apply_helper(Functor&& functor, Tuple&& t, index_sequence<Indexes...>)
    -> decltype(phmap::base_internal::Invoke(
        phmap::forward<Functor>(functor),
        std::get<Indexes>(phmap::forward<Tuple>(t))...)) {
  return phmap::base_internal::Invoke(
      phmap::forward<Functor>(functor),
      std::get<Indexes>(phmap::forward<Tuple>(t))...);
}

}  // namespace utility_internal

// apply
//
// Invokes a Callable using elements of a tuple as its arguments.
// Each element of the tuple corresponds to an argument of the call (in order).
// Both the Callable argument and the tuple argument are perfect-forwarded.
// For member-function Callables, the first tuple element acts as the `this`
// pointer. `phmap::apply` is designed to be a drop-in replacement for C++17's
// `std::apply`. Unlike C++17's `std::apply`, this is not currently `constexpr`.
//
// Example:
//
//   class Foo {
//    public:
//     void Bar(int);
//   };
//   void user_function1(int, std::string);
//   void user_function2(std::unique_ptr<Foo>);
//   auto user_lambda = [](int, int) {};
//
//   int main()
//   {
//       std::tuple<int, std::string> tuple1(42, "bar");
//       // Invokes the first user function on int, std::string.
//       phmap::apply(&user_function1, tuple1);
//
//       std::tuple<std::unique_ptr<Foo>> tuple2(phmap::make_unique<Foo>());
//       // Invokes the user function that takes ownership of the unique
//       // pointer.
//       phmap::apply(&user_function2, std::move(tuple2));
//
//       auto foo = phmap::make_unique<Foo>();
//       std::tuple<Foo*, int> tuple3(foo.get(), 42);
//       // Invokes the method Bar on foo with one argument, 42.
//       phmap::apply(&Foo::Bar, tuple3);
//
//       std::tuple<int, int> tuple4(8, 9);
//       // Invokes a lambda.
//       phmap::apply(user_lambda, tuple4);
//   }
template <typename Functor, typename Tuple>
auto apply(Functor&& functor, Tuple&& t)
    -> decltype(utility_internal::apply_helper(
        phmap::forward<Functor>(functor), phmap::forward<Tuple>(t),
        phmap::make_index_sequence<std::tuple_size<
            typename std::remove_reference<Tuple>::type>::value>{})) {
  return utility_internal::apply_helper(
      phmap::forward<Functor>(functor), phmap::forward<Tuple>(t),
      phmap::make_index_sequence<std::tuple_size<
          typename std::remove_reference<Tuple>::type>::value>{});
}

#ifdef _MSC_VER
    #pragma warning(push)
    #pragma warning(disable : 4365) // '=': conversion from 'T' to 'T', signed/unsigned mismatch
#endif  // _MSC_VER

// exchange
//
// Replaces the value of `obj` with `new_value` and returns the old value of
// `obj`.  `phmap::exchange` is designed to be a drop-in replacement for C++14's
// `std::exchange`.
//
// Example:
//
//   Foo& operator=(Foo&& other) {
//     ptr1_ = phmap::exchange(other.ptr1_, nullptr);
//     int1_ = phmap::exchange(other.int1_, -1);
//     return *this;
//   }
template <typename T, typename U = T>
T exchange(T& obj, U&& new_value)
{
    T old_value = phmap::move(obj);
    obj = phmap::forward<U>(new_value);
    return old_value;
}

#ifdef _MSC_VER
    #pragma warning(pop)
#endif  // _MSC_VER


}  // namespace phmap

// -----------------------------------------------------------------------------
//          memory.h
// -----------------------------------------------------------------------------

namespace phmap {

template <typename T>
std::unique_ptr<T> WrapUnique(T* ptr) 
{
    static_assert(!std::is_array<T>::value, "array types are unsupported");
    static_assert(std::is_object<T>::value, "non-object types are unsupported");
    return std::unique_ptr<T>(ptr);
}

namespace memory_internal {

// Traits to select proper overload and return type for `phmap::make_unique<>`.
template <typename T>
struct MakeUniqueResult {
    using scalar = std::unique_ptr<T>;
};
template <typename T>
struct MakeUniqueResult<T[]> {
    using array = std::unique_ptr<T[]>;
};
template <typename T, size_t N>
struct MakeUniqueResult<T[N]> {
    using invalid = void;
};

}  // namespace memory_internal

#if (__cplusplus > 201103L || defined(_MSC_VER)) && \
    !(defined(__GNUC__) && __GNUC__ == 4 && __GNUC_MINOR__ == 8) 
    using std::make_unique;
#else

    template <typename T, typename... Args>
    typename memory_internal::MakeUniqueResult<T>::scalar make_unique(
        Args&&... args) {
        return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
    }
    
    template <typename T>
    typename memory_internal::MakeUniqueResult<T>::array make_unique(size_t n) {
        return std::unique_ptr<T>(new typename phmap::remove_extent_t<T>[n]());
    }
    
    template <typename T, typename... Args>
    typename memory_internal::MakeUniqueResult<T>::invalid make_unique(
        Args&&... /* args */) = delete;
#endif

template <typename T>
auto RawPtr(T&& ptr) -> decltype(std::addressof(*ptr))
{
    // ptr is a forwarding reference to support Ts with non-const operators.
    return (ptr != nullptr) ? std::addressof(*ptr) : nullptr;
}

inline std::nullptr_t RawPtr(std::nullptr_t) { return nullptr; }

template <typename T, typename D>
std::shared_ptr<T> ShareUniquePtr(std::unique_ptr<T, D>&& ptr) {
    return ptr ? std::shared_ptr<T>(std::move(ptr)) : std::shared_ptr<T>();
}

template <typename T>
std::weak_ptr<T> WeakenPtr(const std::shared_ptr<T>& ptr) {
    return std::weak_ptr<T>(ptr);
}

namespace memory_internal {

// ExtractOr<E, O, D>::type evaluates to E<O> if possible. Otherwise, D.
template <template <typename> class Extract, typename Obj, typename Default,
          typename>
struct ExtractOr {
    using type = Default;
};

template <template <typename> class Extract, typename Obj, typename Default>
struct ExtractOr<Extract, Obj, Default, void_t<Extract<Obj>>> {
    using type = Extract<Obj>;
};

template <template <typename> class Extract, typename Obj, typename Default>
using ExtractOrT = typename ExtractOr<Extract, Obj, Default, void>::type;

// Extractors for the features of allocators.
template <typename T>
using GetPointer = typename T::pointer;

template <typename T>
using GetConstPointer = typename T::const_pointer;

template <typename T>
using GetVoidPointer = typename T::void_pointer;

template <typename T>
using GetConstVoidPointer = typename T::const_void_pointer;

template <typename T>
using GetDifferenceType = typename T::difference_type;

template <typename T>
using GetSizeType = typename T::size_type;

template <typename T>
using GetPropagateOnContainerCopyAssignment =
    typename T::propagate_on_container_copy_assignment;

template <typename T>
using GetPropagateOnContainerMoveAssignment =
    typename T::propagate_on_container_move_assignment;

template <typename T>
using GetPropagateOnContainerSwap = typename T::propagate_on_container_swap;

template <typename T>
using GetIsAlwaysEqual = typename T::is_always_equal;

template <typename T>
struct GetFirstArg;

template <template <typename...> class Class, typename T, typename... Args>
struct GetFirstArg<Class<T, Args...>> {
  using type = T;
};

template <typename Ptr, typename = void>
struct ElementType {
  using type = typename GetFirstArg<Ptr>::type;
};

template <typename T>
struct ElementType<T, void_t<typename T::element_type>> {
  using type = typename T::element_type;
};

template <typename T, typename U>
struct RebindFirstArg;

template <template <typename...> class Class, typename T, typename... Args,
          typename U>
struct RebindFirstArg<Class<T, Args...>, U> {
  using type = Class<U, Args...>;
};

template <typename T, typename U, typename = void>
struct RebindPtr {
  using type = typename RebindFirstArg<T, U>::type;
};

template <typename T, typename U>
struct RebindPtr<T, U, void_t<typename T::template rebind<U>>> {
  using type = typename T::template rebind<U>;
};

template <typename T, typename U>
constexpr bool HasRebindAlloc(...) {
  return false;
}

template <typename T, typename U>
constexpr bool HasRebindAlloc(typename std::allocator_traits<T>::template rebind_alloc<U>*) {
  return true;
}

template <typename T, typename U, bool = HasRebindAlloc<T, U>(nullptr)>
struct RebindAlloc {
  using type = typename RebindFirstArg<T, U>::type;
};

template <typename A, typename U>
struct RebindAlloc<A, U, true> {
    using type = typename std::allocator_traits<A>::template rebind_alloc<U>;
};


}  // namespace memory_internal

template <typename Ptr>
struct pointer_traits 
{
    using pointer = Ptr;

    // element_type:
    // Ptr::element_type if present. Otherwise T if Ptr is a template
    // instantiation Template<T, Args...>
    using element_type = typename memory_internal::ElementType<Ptr>::type;

    // difference_type:
    // Ptr::difference_type if present, otherwise std::ptrdiff_t
    using difference_type =
        memory_internal::ExtractOrT<memory_internal::GetDifferenceType, Ptr,
                                    std::ptrdiff_t>;

    // rebind:
    // Ptr::rebind<U> if exists, otherwise Template<U, Args...> if Ptr is a
    // template instantiation Template<T, Args...>
    template <typename U>
    using rebind = typename memory_internal::RebindPtr<Ptr, U>::type;

    // pointer_to:
    // Calls Ptr::pointer_to(r)
    static pointer pointer_to(element_type& r) {  // NOLINT(runtime/references)
        return Ptr::pointer_to(r);
    }
};

// Specialization for T*.
template <typename T>
struct pointer_traits<T*> 
{
    using pointer = T*;
    using element_type = T;
    using difference_type = std::ptrdiff_t;

    template <typename U>
    using rebind = U*;

    // pointer_to:
    // Calls std::addressof(r)
    static pointer pointer_to(
        element_type& r) noexcept {  // NOLINT(runtime/references)
        return std::addressof(r);
    }
};

// -----------------------------------------------------------------------------
// Class Template: allocator_traits
// -----------------------------------------------------------------------------
//
// A C++11 compatible implementation of C++17's std::allocator_traits.
//
template <typename Alloc>
struct allocator_traits 
{
    using allocator_type = Alloc;

    // value_type:
    // Alloc::value_type
    using value_type = typename Alloc::value_type;

    // pointer:
    // Alloc::pointer if present, otherwise value_type*
    using pointer = memory_internal::ExtractOrT<memory_internal::GetPointer,
                                                Alloc, value_type*>;

    // const_pointer:
    // Alloc::const_pointer if present, otherwise
    // phmap::pointer_traits<pointer>::rebind<const value_type>
    using const_pointer =
        memory_internal::ExtractOrT<memory_internal::GetConstPointer, Alloc,
                                    typename phmap::pointer_traits<pointer>::
                                    template rebind<const value_type>>;

    // void_pointer:
    // Alloc::void_pointer if present, otherwise
    // phmap::pointer_traits<pointer>::rebind<void>
    using void_pointer = memory_internal::ExtractOrT<
        memory_internal::GetVoidPointer, Alloc,
        typename phmap::pointer_traits<pointer>::template rebind<void>>;

    // const_void_pointer:
    // Alloc::const_void_pointer if present, otherwise
    // phmap::pointer_traits<pointer>::rebind<const void>
    using const_void_pointer = memory_internal::ExtractOrT<
        memory_internal::GetConstVoidPointer, Alloc,
        typename phmap::pointer_traits<pointer>::template rebind<const void>>;

    // difference_type:
    // Alloc::difference_type if present, otherwise
    // phmap::pointer_traits<pointer>::difference_type
    using difference_type = memory_internal::ExtractOrT<
        memory_internal::GetDifferenceType, Alloc,
        typename phmap::pointer_traits<pointer>::difference_type>;

    // size_type:
    // Alloc::size_type if present, otherwise
    // std::make_unsigned<difference_type>::type
    using size_type = memory_internal::ExtractOrT<
        memory_internal::GetSizeType, Alloc,
        typename std::make_unsigned<difference_type>::type>;

    // propagate_on_container_copy_assignment:
    // Alloc::propagate_on_container_copy_assignment if present, otherwise
    // std::false_type
    using propagate_on_container_copy_assignment = memory_internal::ExtractOrT<
        memory_internal::GetPropagateOnContainerCopyAssignment, Alloc,
        std::false_type>;

    // propagate_on_container_move_assignment:
    // Alloc::propagate_on_container_move_assignment if present, otherwise
    // std::false_type
    using propagate_on_container_move_assignment = memory_internal::ExtractOrT<
        memory_internal::GetPropagateOnContainerMoveAssignment, Alloc,
        std::false_type>;

    // propagate_on_container_swap:
    // Alloc::propagate_on_container_swap if present, otherwise std::false_type
    using propagate_on_container_swap =
        memory_internal::ExtractOrT<memory_internal::GetPropagateOnContainerSwap,
                                    Alloc, std::false_type>;

    // is_always_equal:
    // Alloc::is_always_equal if present, otherwise std::is_empty<Alloc>::type
    using is_always_equal =
        memory_internal::ExtractOrT<memory_internal::GetIsAlwaysEqual, Alloc,
                                    typename std::is_empty<Alloc>::type>;

    // rebind_alloc:
    // Alloc::rebind<T>::other if present, otherwise Alloc<T, Args> if this Alloc
    // is Alloc<U, Args>
    template <typename T>
    using rebind_alloc = typename memory_internal::RebindAlloc<Alloc, T>::type;

    // rebind_traits:
    // phmap::allocator_traits<rebind_alloc<T>>
    template <typename T>
    using rebind_traits = phmap::allocator_traits<rebind_alloc<T>>;

    // allocate(Alloc& a, size_type n):
    // Calls a.allocate(n)
    static pointer allocate(Alloc& a,  // NOLINT(runtime/references)
                            size_type n) {
        return a.allocate(n);
    }

    // allocate(Alloc& a, size_type n, const_void_pointer hint):
    // Calls a.allocate(n, hint) if possible.
    // If not possible, calls a.allocate(n)
    static pointer allocate(Alloc& a, size_type n,  // NOLINT(runtime/references)
                            const_void_pointer hint) {
        return allocate_impl(0, a, n, hint);
    }

    // deallocate(Alloc& a, pointer p, size_type n):
    // Calls a.deallocate(p, n)
    static void deallocate(Alloc& a, pointer p,  // NOLINT(runtime/references)
                           size_type n) {
        a.deallocate(p, n);
    }

    // construct(Alloc& a, T* p, Args&&... args):
    // Calls a.construct(p, std::forward<Args>(args)...) if possible.
    // If not possible, calls
    //   ::new (static_cast<void*>(p)) T(std::forward<Args>(args)...)
    template <typename T, typename... Args>
    static void construct(Alloc& a, T* p,  // NOLINT(runtime/references)
                          Args&&... args) {
        construct_impl(0, a, p, std::forward<Args>(args)...);
    }

    // destroy(Alloc& a, T* p):
    // Calls a.destroy(p) if possible. If not possible, calls p->~T().
    template <typename T>
    static void destroy(Alloc& a, T* p) {  // NOLINT(runtime/references)
        destroy_impl(0, a, p);
    }

    // max_size(const Alloc& a):
    // Returns a.max_size() if possible. If not possible, returns
    //   std::numeric_limits<size_type>::max() / sizeof(value_type)
    static size_type max_size(const Alloc& a) { return max_size_impl(0, a); }

    // select_on_container_copy_construction(const Alloc& a):
    // Returns a.select_on_container_copy_construction() if possible.
    // If not possible, returns a.
    static Alloc select_on_container_copy_construction(const Alloc& a) {
        return select_on_container_copy_construction_impl(0, a);
    }

private:
    template <typename A>
    static auto allocate_impl(int, A& a,  // NOLINT(runtime/references)
                              size_type n, const_void_pointer hint)
        -> decltype(a.allocate(n, hint)) {
        return a.allocate(n, hint);
    }
    static pointer allocate_impl(char, Alloc& a,  // NOLINT(runtime/references)
                                 size_type n, const_void_pointer) {
        return a.allocate(n);
    }

    template <typename A, typename... Args>
    static auto construct_impl(int, A& a,  // NOLINT(runtime/references)
                               Args&&... args)
        -> decltype(std::allocator_traits<A>::construct(a, std::forward<Args>(args)...)) {
        std::allocator_traits<A>::construct(a, std::forward<Args>(args)...);
    }

    template <typename T, typename... Args>
    static void construct_impl(char, Alloc&, T* p, Args&&... args) {
        ::new (static_cast<void*>(p)) T(std::forward<Args>(args)...);
    }

    template <typename A, typename T>
    static auto destroy_impl(int, A& a,  // NOLINT(runtime/references)
                             T* p) -> decltype(std::allocator_traits<A>::destroy(a, p)) {
        std::allocator_traits<A>::destroy(a, p);
    }
    template <typename T>
    static void destroy_impl(char, Alloc&, T* p) {
        p->~T();
    }

    template <typename A>
    static auto max_size_impl(int, const A& a) -> decltype(a.max_size()) {
        return a.max_size();
    }
    static size_type max_size_impl(char, const Alloc&) {
        return (std::numeric_limits<size_type>::max)() / sizeof(value_type);
    }

    template <typename A>
    static auto select_on_container_copy_construction_impl(int, const A& a)
        -> decltype(a.select_on_container_copy_construction()) {
        return a.select_on_container_copy_construction();
    }
    static Alloc select_on_container_copy_construction_impl(char,
                                                            const Alloc& a) {
        return a;
    }
};

namespace memory_internal {

// This template alias transforms Alloc::is_nothrow into a metafunction with
// Alloc as a parameter so it can be used with ExtractOrT<>.
template <typename Alloc>
using GetIsNothrow = typename Alloc::is_nothrow;

}  // namespace memory_internal

// PHMAP_ALLOCATOR_NOTHROW is a build time configuration macro for user to
// specify whether the default allocation function can throw or never throws.
// If the allocation function never throws, user should define it to a non-zero
// value (e.g. via `-DPHMAP_ALLOCATOR_NOTHROW`).
// If the allocation function can throw, user should leave it undefined or
// define it to zero.
//
// allocator_is_nothrow<Alloc> is a traits class that derives from
// Alloc::is_nothrow if present, otherwise std::false_type. It's specialized
// for Alloc = std::allocator<T> for any type T according to the state of
// PHMAP_ALLOCATOR_NOTHROW.
//
// default_allocator_is_nothrow is a class that derives from std::true_type
// when the default allocator (global operator new) never throws, and
// std::false_type when it can throw. It is a convenience shorthand for writing
// allocator_is_nothrow<std::allocator<T>> (T can be any type).
// NOTE: allocator_is_nothrow<std::allocator<T>> is guaranteed to derive from
// the same type for all T, because users should specialize neither
// allocator_is_nothrow nor std::allocator.
template <typename Alloc>
struct allocator_is_nothrow
    : memory_internal::ExtractOrT<memory_internal::GetIsNothrow, Alloc,
                                  std::false_type> {};

#if defined(PHMAP_ALLOCATOR_NOTHROW) && PHMAP_ALLOCATOR_NOTHROW
    template <typename T>
    struct allocator_is_nothrow<std::allocator<T>> : std::true_type {};
    struct default_allocator_is_nothrow : std::true_type {};
#else
    struct default_allocator_is_nothrow : std::false_type {};
#endif

namespace memory_internal {
template <typename Allocator, typename Iterator, typename... Args>
void ConstructRange(Allocator& alloc, Iterator first, Iterator last,
                    const Args&... args) 
{
    for (Iterator cur = first; cur != last; ++cur) {
        PHMAP_INTERNAL_TRY {
            std::allocator_traits<Allocator>::construct(alloc, std::addressof(*cur),
                                                        args...);
        }
        PHMAP_INTERNAL_CATCH_ANY {
            while (cur != first) {
                --cur;
                std::allocator_traits<Allocator>::destroy(alloc, std::addressof(*cur));
            }
            PHMAP_INTERNAL_RETHROW;
        }
    }
}

template <typename Allocator, typename Iterator, typename InputIterator>
void CopyRange(Allocator& alloc, Iterator destination, InputIterator first,
               InputIterator last) 
{
    for (Iterator cur = destination; first != last;
         static_cast<void>(++cur), static_cast<void>(++first)) {
        PHMAP_INTERNAL_TRY {
            std::allocator_traits<Allocator>::construct(alloc, std::addressof(*cur),
                                                        *first);
        }
        PHMAP_INTERNAL_CATCH_ANY {
            while (cur != destination) {
                --cur;
                std::allocator_traits<Allocator>::destroy(alloc, std::addressof(*cur));
            }
            PHMAP_INTERNAL_RETHROW;
        }
    }
}
}  // namespace memory_internal
}  // namespace phmap


// -----------------------------------------------------------------------------
//          optional.h
// -----------------------------------------------------------------------------
#ifdef PHMAP_HAVE_STD_OPTIONAL

#include <optional>  // IWYU pragma: export

namespace phmap {
using std::bad_optional_access;
using std::optional;
using std::make_optional;
using std::nullopt_t;
using std::nullopt;
}  // namespace phmap

#else

#if defined(__clang__)
    #if __has_feature(cxx_inheriting_constructors)
        #define PHMAP_OPTIONAL_USE_INHERITING_CONSTRUCTORS 1
    #endif
#elif (defined(__GNUC__) &&                                       \
       (__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 8)) || \
    (__cpp_inheriting_constructors >= 200802) ||                  \
    (defined(_MSC_VER) && _MSC_VER >= 1910)

    #define PHMAP_OPTIONAL_USE_INHERITING_CONSTRUCTORS 1
#endif

namespace phmap {

class bad_optional_access : public std::exception 
{
public:
    bad_optional_access() = default;
    ~bad_optional_access() override;
    const char* what() const noexcept override;
};

template <typename T>
class optional;

// --------------------------------
struct nullopt_t 
{
    struct init_t {};
    static init_t init;

    explicit constexpr nullopt_t(init_t& /*unused*/) {}
};

constexpr nullopt_t nullopt(nullopt_t::init);

namespace optional_internal {

// throw delegator
[[noreturn]] void throw_bad_optional_access();


struct empty_struct {};

// This class stores the data in optional<T>.
// It is specialized based on whether T is trivially destructible.
// This is the specialization for non trivially destructible type.
template <typename T, bool unused = std::is_trivially_destructible<T>::value>
class optional_data_dtor_base 
{
    struct dummy_type {
        static_assert(sizeof(T) % sizeof(empty_struct) == 0, "");
        // Use an array to avoid GCC 6 placement-new warning.
        empty_struct data[sizeof(T) / sizeof(empty_struct)];
    };

protected:
    // Whether there is data or not.
    bool engaged_;
    // Data storage
    union {
        dummy_type dummy_;
        T data_;
    };

    void destruct() noexcept {
        if (engaged_) {
            data_.~T();
            engaged_ = false;
        }
    }

    // dummy_ must be initialized for constexpr constructor.
    constexpr optional_data_dtor_base() noexcept : engaged_(false), dummy_{{}} {}

    template <typename... Args>
    constexpr explicit optional_data_dtor_base(in_place_t, Args&&... args)
        : engaged_(true), data_(phmap::forward<Args>(args)...) {}

    ~optional_data_dtor_base() { destruct(); }
};

// Specialization for trivially destructible type.
template <typename T>
class optional_data_dtor_base<T, true> 
{
    struct dummy_type {
        static_assert(sizeof(T) % sizeof(empty_struct) == 0, "");
        // Use array to avoid GCC 6 placement-new warning.
        empty_struct data[sizeof(T) / sizeof(empty_struct)];
    };

protected:
    // Whether there is data or not.
    bool engaged_;
    // Data storage
    union {
        dummy_type dummy_;
        T data_;
    };
    void destruct() noexcept { engaged_ = false; }

    // dummy_ must be initialized for constexpr constructor.
    constexpr optional_data_dtor_base() noexcept : engaged_(false), dummy_{{}} {}

    template <typename... Args>
    constexpr explicit optional_data_dtor_base(in_place_t, Args&&... args)
        : engaged_(true), data_(phmap::forward<Args>(args)...) {}
};

template <typename T>
class optional_data_base : public optional_data_dtor_base<T> 
{
protected:
    using base = optional_data_dtor_base<T>;
#if PHMAP_OPTIONAL_USE_INHERITING_CONSTRUCTORS
    using base::base;
#else
    optional_data_base() = default;

    template <typename... Args>
    constexpr explicit optional_data_base(in_place_t t, Args&&... args)
        : base(t, phmap::forward<Args>(args)...) {}
#endif

    template <typename... Args>
    void construct(Args&&... args) {
        // Use dummy_'s address to work around casting cv-qualified T* to void*.
        ::new (static_cast<void*>(&this->dummy_)) T(std::forward<Args>(args)...);
        this->engaged_ = true;
    }

    template <typename U>
    void assign(U&& u) {
        if (this->engaged_) {
            this->data_ = std::forward<U>(u);
        } else {
            construct(std::forward<U>(u));
        }
    }
};

// TODO: Add another class using
// std::is_trivially_move_constructible trait when available to match
// http://cplusplus.github.io/LWG/lwg-defects.html#2900, for types that
// have trivial move but nontrivial copy.
// Also, we should be checking is_trivially_copyable here, which is not
// supported now, so we use is_trivially_* traits instead.
template <typename T,
          bool unused =
          phmap::is_trivially_copy_constructible<T>::value &&
          phmap::is_trivially_copy_assignable<typename std::remove_cv<T>::type>::value &&
          std::is_trivially_destructible<T>::value>
class optional_data;

// Trivially copyable types
template <typename T>
class optional_data<T, true> : public optional_data_base<T> 
{
protected:
#if PHMAP_OPTIONAL_USE_INHERITING_CONSTRUCTORS
    using optional_data_base<T>::optional_data_base;
#else
    optional_data() = default;

    template <typename... Args>
    constexpr explicit optional_data(in_place_t t, Args&&... args)
        : optional_data_base<T>(t, phmap::forward<Args>(args)...) {}
#endif
};

template <typename T>
class optional_data<T, false> : public optional_data_base<T> 
{
protected:
#if PHMAP_OPTIONAL_USE_INHERITING_CONSTRUCTORS
    using optional_data_base<T>::optional_data_base;
#else
    template <typename... Args>
    constexpr explicit optional_data(in_place_t t, Args&&... args)
        : optional_data_base<T>(t, phmap::forward<Args>(args)...) {}
#endif

    optional_data() = default;

    optional_data(const optional_data& rhs) : optional_data_base<T>() {
        if (rhs.engaged_) {
            this->construct(rhs.data_);
        }
    }

    optional_data(optional_data&& rhs) noexcept(
        phmap::default_allocator_is_nothrow::value ||
        std::is_nothrow_move_constructible<T>::value)
    : optional_data_base<T>() {
        if (rhs.engaged_) {
            this->construct(std::move(rhs.data_));
        }
    }

    optional_data& operator=(const optional_data& rhs) {
        if (rhs.engaged_) {
            this->assign(rhs.data_);
        } else {
            this->destruct();
        }
        return *this;
    }

    optional_data& operator=(optional_data&& rhs) noexcept(
        std::is_nothrow_move_assignable<T>::value&&
        std::is_nothrow_move_constructible<T>::value) {
        if (rhs.engaged_) {
            this->assign(std::move(rhs.data_));
        } else {
            this->destruct();
        }
        return *this;
    }
};

// Ordered by level of restriction, from low to high.
// Copyable implies movable.
enum class copy_traits { copyable = 0, movable = 1, non_movable = 2 };

// Base class for enabling/disabling copy/move constructor.
template <copy_traits>
class optional_ctor_base;

template <>
class optional_ctor_base<copy_traits::copyable> 
{
public:
    constexpr optional_ctor_base() = default;
    optional_ctor_base(const optional_ctor_base&) = default;
    optional_ctor_base(optional_ctor_base&&) = default;
    optional_ctor_base& operator=(const optional_ctor_base&) = default;
    optional_ctor_base& operator=(optional_ctor_base&&) = default;
};

template <>
class optional_ctor_base<copy_traits::movable> 
{
public:
    constexpr optional_ctor_base() = default;
    optional_ctor_base(const optional_ctor_base&) = delete;
    optional_ctor_base(optional_ctor_base&&) = default;
    optional_ctor_base& operator=(const optional_ctor_base&) = default;
    optional_ctor_base& operator=(optional_ctor_base&&) = default;
};

template <>
class optional_ctor_base<copy_traits::non_movable> 
{
public:
    constexpr optional_ctor_base() = default;
    optional_ctor_base(const optional_ctor_base&) = delete;
    optional_ctor_base(optional_ctor_base&&) = delete;
    optional_ctor_base& operator=(const optional_ctor_base&) = default;
    optional_ctor_base& operator=(optional_ctor_base&&) = default;
};

// Base class for enabling/disabling copy/move assignment.
template <copy_traits>
class optional_assign_base;

template <>
class optional_assign_base<copy_traits::copyable> 
{
public:
    constexpr optional_assign_base() = default;
    optional_assign_base(const optional_assign_base&) = default;
    optional_assign_base(optional_assign_base&&) = default;
    optional_assign_base& operator=(const optional_assign_base&) = default;
    optional_assign_base& operator=(optional_assign_base&&) = default;
};

template <>
class optional_assign_base<copy_traits::movable> 
{
public:
    constexpr optional_assign_base() = default;
    optional_assign_base(const optional_assign_base&) = default;
    optional_assign_base(optional_assign_base&&) = default;
    optional_assign_base& operator=(const optional_assign_base&) = delete;
    optional_assign_base& operator=(optional_assign_base&&) = default;
};

template <>
class optional_assign_base<copy_traits::non_movable> 
{
public:
    constexpr optional_assign_base() = default;
    optional_assign_base(const optional_assign_base&) = default;
    optional_assign_base(optional_assign_base&&) = default;
    optional_assign_base& operator=(const optional_assign_base&) = delete;
    optional_assign_base& operator=(optional_assign_base&&) = delete;
};

template <typename T>
constexpr copy_traits get_ctor_copy_traits() 
{
    return std::is_copy_constructible<T>::value
        ? copy_traits::copyable
        : std::is_move_constructible<T>::value ? copy_traits::movable
        : copy_traits::non_movable;
}

template <typename T>
constexpr copy_traits get_assign_copy_traits() 
{
    return phmap::is_copy_assignable<T>::value &&
                 std::is_copy_constructible<T>::value
             ? copy_traits::copyable
             : phmap::is_move_assignable<T>::value &&
                       std::is_move_constructible<T>::value
                   ? copy_traits::movable
                   : copy_traits::non_movable;
}

// Whether T is constructible or convertible from optional<U>.
template <typename T, typename U>
struct is_constructible_convertible_from_optional
    : std::integral_constant<
          bool, std::is_constructible<T, optional<U>&>::value ||
                    std::is_constructible<T, optional<U>&&>::value ||
                    std::is_constructible<T, const optional<U>&>::value ||
                    std::is_constructible<T, const optional<U>&&>::value ||
                    std::is_convertible<optional<U>&, T>::value ||
                    std::is_convertible<optional<U>&&, T>::value ||
                    std::is_convertible<const optional<U>&, T>::value ||
                    std::is_convertible<const optional<U>&&, T>::value> {};

// Whether T is constructible or convertible or assignable from optional<U>.
template <typename T, typename U>
struct is_constructible_convertible_assignable_from_optional
    : std::integral_constant<
          bool, is_constructible_convertible_from_optional<T, U>::value ||
                    std::is_assignable<T&, optional<U>&>::value ||
                    std::is_assignable<T&, optional<U>&&>::value ||
                    std::is_assignable<T&, const optional<U>&>::value ||
                    std::is_assignable<T&, const optional<U>&&>::value> {};

// Helper function used by [optional.relops], [optional.comp_with_t],
// for checking whether an expression is convertible to bool.
bool convertible_to_bool(bool);

// Base class for std::hash<phmap::optional<T>>:
// If std::hash<std::remove_const_t<T>> is enabled, it provides operator() to
// compute the hash; Otherwise, it is disabled.
// Reference N4659 23.14.15 [unord.hash].
template <typename T, typename = size_t>
struct optional_hash_base 
{
    optional_hash_base() = delete;
    optional_hash_base(const optional_hash_base&) = delete;
    optional_hash_base(optional_hash_base&&) = delete;
    optional_hash_base& operator=(const optional_hash_base&) = delete;
    optional_hash_base& operator=(optional_hash_base&&) = delete;
};

template <typename T>
struct optional_hash_base<T, decltype(std::hash<phmap::remove_const_t<T> >()(
                                 std::declval<phmap::remove_const_t<T> >()))> 
{
    using argument_type = phmap::optional<T>;
    using result_type = size_t;
    size_t operator()(const phmap::optional<T>& opt) const {
        phmap::type_traits_internal::AssertHashEnabled<phmap::remove_const_t<T>>();
        if (opt) {
            return std::hash<phmap::remove_const_t<T> >()(*opt);
        } else {
            return static_cast<size_t>(0x297814aaad196e6dULL);
        }
    }
};

}  // namespace optional_internal


// -----------------------------------------------------------------------------
// phmap::optional class definition
// -----------------------------------------------------------------------------
#if PHMAP_OLD_GCC
    #define PHMAP_OPTIONAL_NOEXCEPT
#else
    #define PHMAP_OPTIONAL_NOEXCEPT noexcept
#endif

template <typename T>
class optional : private optional_internal::optional_data<T>,
                 private optional_internal::optional_ctor_base<
                     optional_internal::get_ctor_copy_traits<T>()>,
                 private optional_internal::optional_assign_base<
                     optional_internal::get_assign_copy_traits<T>()> 
{
    using data_base = optional_internal::optional_data<T>;

public:
    typedef T value_type;

    // Constructors

    // Constructs an `optional` holding an empty value, NOT a default constructed
    // `T`.
    constexpr optional() noexcept {}

    // Constructs an `optional` initialized with `nullopt` to hold an empty value.
    constexpr optional(nullopt_t) noexcept {}  // NOLINT(runtime/explicit)

    // Copy constructor, standard semantics
    optional(const optional& src) = default;

    // Move constructor, standard semantics
    optional(optional&& src) PHMAP_OPTIONAL_NOEXCEPT = default;

    // Constructs a non-empty `optional` direct-initialized value of type `T` from
    // the arguments `std::forward<Args>(args)...`  within the `optional`.
    // (The `in_place_t` is a tag used to indicate that the contained object
    // should be constructed in-place.)
    template <typename InPlaceT, typename... Args,
              phmap::enable_if_t<phmap::conjunction<
                                    std::is_same<InPlaceT, in_place_t>,
                                    std::is_constructible<T, Args&&...> >::value>* = nullptr>
        constexpr explicit optional(InPlaceT, Args&&... args)
        : data_base(in_place_t(), phmap::forward<Args>(args)...) {}

    // Constructs a non-empty `optional` direct-initialized value of type `T` from
    // the arguments of an initializer_list and `std::forward<Args>(args)...`.
    // (The `in_place_t` is a tag used to indicate that the contained object
    // should be constructed in-place.)
    template <typename U, typename... Args,
              typename = typename std::enable_if<std::is_constructible<
                                                     T, std::initializer_list<U>&, Args&&...>::value>::type>
        constexpr explicit optional(in_place_t, std::initializer_list<U> il,
                                    Args&&... args)
        : data_base(in_place_t(), il, phmap::forward<Args>(args)...) {
    }

    // Value constructor (implicit)
    template <
        typename U = T,
        typename std::enable_if<
            phmap::conjunction<phmap::negation<std::is_same<
                                                 in_place_t, typename std::decay<U>::type> >,
                              phmap::negation<std::is_same<
                                                 optional<T>, typename std::decay<U>::type> >,
                              std::is_convertible<U&&, T>,
                              std::is_constructible<T, U&&> >::value,
            bool>::type = false>
        constexpr optional(U&& v) : data_base(in_place_t(), phmap::forward<U>(v)) {}

    // Value constructor (explicit)
    template <
        typename U = T,
        typename std::enable_if<
            phmap::conjunction<phmap::negation<std::is_same<
                                                 in_place_t, typename std::decay<U>::type>>,
                              phmap::negation<std::is_same<
                                                 optional<T>, typename std::decay<U>::type>>,
                              phmap::negation<std::is_convertible<U&&, T>>,
                              std::is_constructible<T, U&&>>::value,
            bool>::type = false>
        explicit constexpr optional(U&& v)
        : data_base(in_place_t(), phmap::forward<U>(v)) {}

    // Converting copy constructor (implicit)
    template <typename U,
              typename std::enable_if<
                  phmap::conjunction<
                      phmap::negation<std::is_same<T, U> >,
                      std::is_constructible<T, const U&>,
                      phmap::negation<
                          optional_internal::
                          is_constructible_convertible_from_optional<T, U> >,
                      std::is_convertible<const U&, T> >::value,
                  bool>::type = false>
    optional(const optional<U>& rhs) {
        if (rhs) {
            this->construct(*rhs);
        }
    }

    // Converting copy constructor (explicit)
    template <typename U,
              typename std::enable_if<
                  phmap::conjunction<
                      phmap::negation<std::is_same<T, U>>,
                      std::is_constructible<T, const U&>,
                      phmap::negation<
                          optional_internal::
                          is_constructible_convertible_from_optional<T, U>>,
                      phmap::negation<std::is_convertible<const U&, T>>>::value,
                  bool>::type = false>
        explicit optional(const optional<U>& rhs) {
        if (rhs) {
            this->construct(*rhs);
        }
    }

    // Converting move constructor (implicit)
    template <typename U,
              typename std::enable_if<
                  phmap::conjunction<
                      phmap::negation<std::is_same<T, U> >,
                      std::is_constructible<T, U&&>,
                      phmap::negation<
                          optional_internal::
                          is_constructible_convertible_from_optional<T, U> >,
                      std::is_convertible<U&&, T> >::value,
                  bool>::type = false>
        optional(optional<U>&& rhs) {
        if (rhs) {
            this->construct(std::move(*rhs));
        }
    }

    // Converting move constructor (explicit)
    template <
        typename U,
        typename std::enable_if<
            phmap::conjunction<
                phmap::negation<std::is_same<T, U>>, std::is_constructible<T, U&&>,
                phmap::negation<
                    optional_internal::is_constructible_convertible_from_optional<
                        T, U>>,
                phmap::negation<std::is_convertible<U&&, T>>>::value,
            bool>::type = false>
        explicit optional(optional<U>&& rhs) {
        if (rhs) {
            this->construct(std::move(*rhs));
        }
    }

    // Destructor. Trivial if `T` is trivially destructible.
    ~optional() = default;

    // Assignment Operators

    // Assignment from `nullopt`
    //
    // Example:
    //
    //   struct S { int value; };
    //   optional<S> opt = phmap::nullopt;  // Could also use opt = { };
    optional& operator=(nullopt_t) noexcept {
        this->destruct();
        return *this;
    }

    // Copy assignment operator, standard semantics
    optional& operator=(const optional& src) = default;

    // Move assignment operator, standard semantics
    optional& operator=(optional&& src) PHMAP_OPTIONAL_NOEXCEPT = default;

    // Value assignment operators
    template <
        typename U = T,
        typename = typename std::enable_if<phmap::conjunction<
                                               phmap::negation<
                                                   std::is_same<optional<T>, typename std::decay<U>::type>>,
                                               phmap::negation<
                                                   phmap::conjunction<std::is_scalar<T>,
                                                                     std::is_same<T, typename std::decay<U>::type>>>,
                                               std::is_constructible<T, U>, std::is_assignable<T&, U>>::value>::type>
        optional& operator=(U&& v) {
        this->assign(std::forward<U>(v));
        return *this;
    }

    template <
        typename U,
        typename = typename std::enable_if<phmap::conjunction<
                                               phmap::negation<std::is_same<T, U>>,
                                               std::is_constructible<T, const U&>, std::is_assignable<T&, const U&>,
                                               phmap::negation<
                                                   optional_internal::
                                                   is_constructible_convertible_assignable_from_optional<
                                                       T, U>>>::value>::type>
        optional& operator=(const optional<U>& rhs) {
        if (rhs) {
            this->assign(*rhs);
        } else {
            this->destruct();
        }
        return *this;
    }

    template <typename U,
              typename = typename std::enable_if<phmap::conjunction<
                                                     phmap::negation<std::is_same<T, U>>, std::is_constructible<T, U>,
                                                     std::is_assignable<T&, U>,
                                                     phmap::negation<
                                                         optional_internal::
                                                         is_constructible_convertible_assignable_from_optional<
                                                             T, U>>>::value>::type>
        optional& operator=(optional<U>&& rhs) {
        if (rhs) {
            this->assign(std::move(*rhs));
        } else {
            this->destruct();
        }
        return *this;
    }

    // Modifiers

    // optional::reset()
    //
    // Destroys the inner `T` value of an `phmap::optional` if one is present.
    PHMAP_ATTRIBUTE_REINITIALIZES void reset() noexcept { this->destruct(); }

    // optional::emplace()
    //
    // (Re)constructs the underlying `T` in-place with the given forwarded
    // arguments.
    //
    // Example:
    //
    //   optional<Foo> opt;
    //   opt.emplace(arg1,arg2,arg3);  // Constructs Foo(arg1,arg2,arg3)
    //
    // If the optional is non-empty, and the `args` refer to subobjects of the
    // current object, then behaviour is undefined, because the current object
    // will be destructed before the new object is constructed with `args`.
    template <typename... Args,
              typename = typename std::enable_if<
                  std::is_constructible<T, Args&&...>::value>::type>
        T& emplace(Args&&... args) {
        this->destruct();
        this->construct(std::forward<Args>(args)...);
        return reference();
    }

    // Emplace reconstruction overload for an initializer list and the given
    // forwarded arguments.
    //
    // Example:
    //
    //   struct Foo {
    //     Foo(std::initializer_list<int>);
    //   };
    //
    //   optional<Foo> opt;
    //   opt.emplace({1,2,3});  // Constructs Foo({1,2,3})
    template <typename U, typename... Args,
              typename = typename std::enable_if<std::is_constructible<
                                                     T, std::initializer_list<U>&, Args&&...>::value>::type>
        T& emplace(std::initializer_list<U> il, Args&&... args) {
        this->destruct();
        this->construct(il, std::forward<Args>(args)...);
        return reference();
    }

    // Swaps

    // Swap, standard semantics
    void swap(optional& rhs) noexcept(
        std::is_nothrow_move_constructible<T>::value&&
        std::is_trivial<T>::value) {
        if (*this) {
            if (rhs) {
                using std::swap;
                swap(**this, *rhs);
            } else {
                rhs.construct(std::move(**this));
                this->destruct();
            }
        } else {
            if (rhs) {
                this->construct(std::move(*rhs));
                rhs.destruct();
            } else {
                // No effect (swap(disengaged, disengaged)).
            }
        }
    }

    // Observers

    // optional::operator->()
    //
    // Accesses the underlying `T` value's member `m` of an `optional`. If the
    // `optional` is empty, behavior is undefined.
    //
    // If you need myOpt->foo in constexpr, use (*myOpt).foo instead.
    const T* operator->() const {
        assert(this->engaged_);
        return std::addressof(this->data_);
    }
    T* operator->() {
        assert(this->engaged_);
        return std::addressof(this->data_);
    }

    // optional::operator*()
    //
    // Accesses the underlying `T` value of an `optional`. If the `optional` is
    // empty, behavior is undefined.
    constexpr const T& operator*() const & { return reference(); }
    T& operator*() & {
        assert(this->engaged_);
        return reference();
    }
    constexpr const T&& operator*() const && {
        return phmap::move(reference());
    }
    T&& operator*() && {
        assert(this->engaged_);
        return std::move(reference());
    }

    // optional::operator bool()
    //
    // Returns false if and only if the `optional` is empty.
    //
    //   if (opt) {
    //     // do something with opt.value();
    //   } else {
    //     // opt is empty.
    //   }
    //
    constexpr explicit operator bool() const noexcept { return this->engaged_; }

    // optional::has_value()
    //
    // Determines whether the `optional` contains a value. Returns `false` if and
    // only if `*this` is empty.
    constexpr bool has_value() const noexcept { return this->engaged_; }

// Suppress bogus warning on MSVC: MSVC complains call to reference() after
// throw_bad_optional_access() is unreachable.
#ifdef _MSC_VER
    #pragma warning(push)
    #pragma warning(disable : 4702)
#endif  // _MSC_VER
    // optional::value()
    //
    // Returns a reference to an `optional`s underlying value. The constness
    // and lvalue/rvalue-ness of the `optional` is preserved to the view of
    // the `T` sub-object. Throws `phmap::bad_optional_access` when the `optional`
    // is empty.
    constexpr const T& value() const & {
        return static_cast<bool>(*this)
            ? reference()
            : (optional_internal::throw_bad_optional_access(), reference());
    }
    T& value() & {
        return static_cast<bool>(*this)
            ? reference()
            : (optional_internal::throw_bad_optional_access(), reference());
    }
    T&& value() && {  // NOLINT(build/c++11)
        return std::move(
            static_cast<bool>(*this)
            ? reference()
            : (optional_internal::throw_bad_optional_access(), reference()));
    }
    constexpr const T&& value() const && {  // NOLINT(build/c++11)
        return phmap::move(
            static_cast<bool>(*this)
            ? reference()
            : (optional_internal::throw_bad_optional_access(), reference()));
    }
#ifdef _MSC_VER
    #pragma warning(pop)
#endif  // _MSC_VER

    // optional::value_or()
    //
    // Returns either the value of `T` or a passed default `v` if the `optional`
    // is empty.
    template <typename U>
    constexpr T value_or(U&& v) const& {
        static_assert(std::is_copy_constructible<value_type>::value,
                      "optional<T>::value_or: T must by copy constructible");
        static_assert(std::is_convertible<U&&, value_type>::value,
                      "optional<T>::value_or: U must be convertible to T");
        return static_cast<bool>(*this)
            ? **this
            : static_cast<T>(phmap::forward<U>(v));
    }
    template <typename U>
    T value_or(U&& v) && {  // NOLINT(build/c++11)
        static_assert(std::is_move_constructible<value_type>::value,
                      "optional<T>::value_or: T must by move constructible");
        static_assert(std::is_convertible<U&&, value_type>::value,
                      "optional<T>::value_or: U must be convertible to T");
        return static_cast<bool>(*this) ? std::move(**this)
            : static_cast<T>(std::forward<U>(v));
    }

private:
    // Private accessors for internal storage viewed as reference to T.
    constexpr const T& reference() const { return this->data_; }
    T& reference() { return this->data_; }

    // T constraint checks.  You can't have an optional of nullopt_t, in_place_t
    // or a reference.
    static_assert(
        !std::is_same<nullopt_t, typename std::remove_cv<T>::type>::value,
        "optional<nullopt_t> is not allowed.");
    static_assert(
        !std::is_same<in_place_t, typename std::remove_cv<T>::type>::value,
        "optional<in_place_t> is not allowed.");
    static_assert(!std::is_reference<T>::value,
                  "optional<reference> is not allowed.");
};

// Non-member functions

// swap()
//
// Performs a swap between two `phmap::optional` objects, using standard
// semantics.
//
// NOTE: we assume `is_swappable()` is always `true`. A compile error will
// result if this is not the case.
template <typename T,
          typename std::enable_if<std::is_move_constructible<T>::value,
                                  bool>::type = false>
void swap(optional<T>& a, optional<T>& b) noexcept(noexcept(a.swap(b))) {
    a.swap(b);
}

// make_optional()
//
// Creates a non-empty `optional<T>` where the type of `T` is deduced. An
// `phmap::optional` can also be explicitly instantiated with
// `make_optional<T>(v)`.
//
// Note: `make_optional()` constructions may be declared `constexpr` for
// trivially copyable types `T`. Non-trivial types require copy elision
// support in C++17 for `make_optional` to support `constexpr` on such
// non-trivial types.
//
// Example:
//
//   constexpr phmap::optional<int> opt = phmap::make_optional(1);
//   static_assert(opt.value() == 1, "");
template <typename T>
constexpr optional<typename std::decay<T>::type> make_optional(T&& v) {
    return optional<typename std::decay<T>::type>(phmap::forward<T>(v));
}

template <typename T, typename... Args>
constexpr optional<T> make_optional(Args&&... args) {
    return optional<T>(in_place_t(), phmap::forward<Args>(args)...);
}

template <typename T, typename U, typename... Args>
constexpr optional<T> make_optional(std::initializer_list<U> il,
                                    Args&&... args) {
    return optional<T>(in_place_t(), il,
                       phmap::forward<Args>(args)...);
}

// Relational operators [optional.relops]

// Empty optionals are considered equal to each other and less than non-empty
// optionals. Supports relations between optional<T> and optional<U>, between
// optional<T> and U, and between optional<T> and nullopt.
//
// Note: We're careful to support T having non-bool relationals.

// Requires: The expression, e.g. "*x == *y" shall be well-formed and its result
// shall be convertible to bool.
// The C++17 (N4606) "Returns:" statements are translated into
// code in an obvious way here, and the original text retained as function docs.
// Returns: If bool(x) != bool(y), false; otherwise if bool(x) == false, true;
// otherwise *x == *y.
template <typename T, typename U>
constexpr auto operator==(const optional<T>& x, const optional<U>& y)
    -> decltype(optional_internal::convertible_to_bool(*x == *y)) {
    return static_cast<bool>(x) != static_cast<bool>(y)
             ? false
             : static_cast<bool>(x) == false ? true
                                             : static_cast<bool>(*x == *y);
}

// Returns: If bool(x) != bool(y), true; otherwise, if bool(x) == false, false;
// otherwise *x != *y.
template <typename T, typename U>
constexpr auto operator!=(const optional<T>& x, const optional<U>& y)
    -> decltype(optional_internal::convertible_to_bool(*x != *y)) {
    return static_cast<bool>(x) != static_cast<bool>(y)
             ? true
             : static_cast<bool>(x) == false ? false
                                             : static_cast<bool>(*x != *y);
}
// Returns: If !y, false; otherwise, if !x, true; otherwise *x < *y.
template <typename T, typename U>
constexpr auto operator<(const optional<T>& x, const optional<U>& y)
    -> decltype(optional_internal::convertible_to_bool(*x < *y)) {
    return !y ? false : !x ? true : static_cast<bool>(*x < *y);
}
// Returns: If !x, false; otherwise, if !y, true; otherwise *x > *y.
template <typename T, typename U>
constexpr auto operator>(const optional<T>& x, const optional<U>& y)
    -> decltype(optional_internal::convertible_to_bool(*x > *y)) {
    return !x ? false : !y ? true : static_cast<bool>(*x > *y);
}
// Returns: If !x, true; otherwise, if !y, false; otherwise *x <= *y.
template <typename T, typename U>
constexpr auto operator<=(const optional<T>& x, const optional<U>& y)
    -> decltype(optional_internal::convertible_to_bool(*x <= *y)) {
    return !x ? true : !y ? false : static_cast<bool>(*x <= *y);
}
// Returns: If !y, true; otherwise, if !x, false; otherwise *x >= *y.
template <typename T, typename U>
constexpr auto operator>=(const optional<T>& x, const optional<U>& y)
    -> decltype(optional_internal::convertible_to_bool(*x >= *y)) {
    return !y ? true : !x ? false : static_cast<bool>(*x >= *y);
}

// Comparison with nullopt [optional.nullops]
// The C++17 (N4606) "Returns:" statements are used directly here.
template <typename T>
constexpr bool operator==(const optional<T>& x, nullopt_t) noexcept {
    return !x;
}
template <typename T>
constexpr bool operator==(nullopt_t, const optional<T>& x) noexcept {
    return !x;
}
template <typename T>
constexpr bool operator!=(const optional<T>& x, nullopt_t) noexcept {
    return static_cast<bool>(x);
}
template <typename T>
constexpr bool operator!=(nullopt_t, const optional<T>& x) noexcept {
    return static_cast<bool>(x);
}
template <typename T>
constexpr bool operator<(const optional<T>&, nullopt_t) noexcept {
    return false;
}
template <typename T>
constexpr bool operator<(nullopt_t, const optional<T>& x) noexcept {
    return static_cast<bool>(x);
}
template <typename T>
constexpr bool operator<=(const optional<T>& x, nullopt_t) noexcept {
    return !x;
}
template <typename T>
constexpr bool operator<=(nullopt_t, const optional<T>&) noexcept {
    return true;
}
template <typename T>
constexpr bool operator>(const optional<T>& x, nullopt_t) noexcept {
    return static_cast<bool>(x);
}
template <typename T>
constexpr bool operator>(nullopt_t, const optional<T>&) noexcept {
    return false;
}
template <typename T>
constexpr bool operator>=(const optional<T>&, nullopt_t) noexcept {
    return true;
}
template <typename T>
constexpr bool operator>=(nullopt_t, const optional<T>& x) noexcept {
    return !x;
}

// Comparison with T [optional.comp_with_t]

// Requires: The expression, e.g. "*x == v" shall be well-formed and its result
// shall be convertible to bool.
// The C++17 (N4606) "Equivalent to:" statements are used directly here.
template <typename T, typename U>
constexpr auto operator==(const optional<T>& x, const U& v)
    -> decltype(optional_internal::convertible_to_bool(*x == v)) {
    return static_cast<bool>(x) ? static_cast<bool>(*x == v) : false;
}
template <typename T, typename U>
constexpr auto operator==(const U& v, const optional<T>& x)
    -> decltype(optional_internal::convertible_to_bool(v == *x)) {
    return static_cast<bool>(x) ? static_cast<bool>(v == *x) : false;
}
template <typename T, typename U>
constexpr auto operator!=(const optional<T>& x, const U& v)
    -> decltype(optional_internal::convertible_to_bool(*x != v)) {
    return static_cast<bool>(x) ? static_cast<bool>(*x != v) : true;
}
template <typename T, typename U>
constexpr auto operator!=(const U& v, const optional<T>& x)
    -> decltype(optional_internal::convertible_to_bool(v != *x)) {
    return static_cast<bool>(x) ? static_cast<bool>(v != *x) : true;
}
template <typename T, typename U>
constexpr auto operator<(const optional<T>& x, const U& v)
    -> decltype(optional_internal::convertible_to_bool(*x < v)) {
    return static_cast<bool>(x) ? static_cast<bool>(*x < v) : true;
}
template <typename T, typename U>
constexpr auto operator<(const U& v, const optional<T>& x)
    -> decltype(optional_internal::convertible_to_bool(v < *x)) {
    return static_cast<bool>(x) ? static_cast<bool>(v < *x) : false;
}
template <typename T, typename U>
constexpr auto operator<=(const optional<T>& x, const U& v)
    -> decltype(optional_internal::convertible_to_bool(*x <= v)) {
    return static_cast<bool>(x) ? static_cast<bool>(*x <= v) : true;
}
template <typename T, typename U>
constexpr auto operator<=(const U& v, const optional<T>& x)
    -> decltype(optional_internal::convertible_to_bool(v <= *x)) {
    return static_cast<bool>(x) ? static_cast<bool>(v <= *x) : false;
}
template <typename T, typename U>
constexpr auto operator>(const optional<T>& x, const U& v)
    -> decltype(optional_internal::convertible_to_bool(*x > v)) {
    return static_cast<bool>(x) ? static_cast<bool>(*x > v) : false;
}
template <typename T, typename U>
constexpr auto operator>(const U& v, const optional<T>& x)
    -> decltype(optional_internal::convertible_to_bool(v > *x)) {
    return static_cast<bool>(x) ? static_cast<bool>(v > *x) : true;
}
template <typename T, typename U>
constexpr auto operator>=(const optional<T>& x, const U& v)
    -> decltype(optional_internal::convertible_to_bool(*x >= v)) {
    return static_cast<bool>(x) ? static_cast<bool>(*x >= v) : false;
}
template <typename T, typename U>
constexpr auto operator>=(const U& v, const optional<T>& x)
    -> decltype(optional_internal::convertible_to_bool(v >= *x)) {
    return static_cast<bool>(x) ? static_cast<bool>(v >= *x) : true;
}

}  // namespace phmap

namespace std {

// std::hash specialization for phmap::optional.
template <typename T>
struct hash<phmap::optional<T> >
    : phmap::optional_internal::optional_hash_base<T> {};

}  // namespace std

#endif

// -----------------------------------------------------------------------------
//          common.h
// -----------------------------------------------------------------------------
namespace phmap {
namespace priv {

template <class, class = void>
struct IsTransparent : std::false_type {};
template <class T>
struct IsTransparent<T, phmap::void_t<typename T::is_transparent>>
    : std::true_type {};

template <bool is_transparent>
struct KeyArg 
{
    // Transparent. Forward `K`.
    template <typename K, typename key_type>
    using type = K;
};

template <>
struct KeyArg<false> 
{
    // Not transparent. Always use `key_type`.
    template <typename K, typename key_type>
    using type = key_type;
};

#ifdef _MSC_VER
    #pragma warning(push)  
    //  warning C4820: '6' bytes padding added after data member
    #pragma warning(disable : 4820)
#endif

// The node_handle concept from C++17.
// We specialize node_handle for sets and maps. node_handle_base holds the
// common API of both.
// -----------------------------------------------------------------------
template <typename PolicyTraits, typename Alloc>
class node_handle_base 
{
protected:
    using slot_type = typename PolicyTraits::slot_type;

public:
    using allocator_type = Alloc;

    constexpr node_handle_base() {}

    node_handle_base(node_handle_base&& other) noexcept {
        *this = std::move(other);
    }

    ~node_handle_base() { destroy(); }

    node_handle_base& operator=(node_handle_base&& other) noexcept {
        destroy();
        if (!other.empty()) {
            if (other.alloc_) {
               alloc_.emplace(other.alloc_.value());
            }
            PolicyTraits::transfer(alloc(), slot(), other.slot());
            other.reset();
        }
        return *this;
    }

    bool empty() const noexcept { return !alloc_; }
    explicit operator bool() const noexcept { return !empty(); }
    allocator_type get_allocator() const { return *alloc_; }

protected:
    friend struct CommonAccess;

    struct transfer_tag_t {};
    node_handle_base(transfer_tag_t, const allocator_type& a, slot_type* s)
        : alloc_(a) {
        PolicyTraits::transfer(alloc(), slot(), s);
    }
    
    struct move_tag_t {};
    node_handle_base(move_tag_t, const allocator_type& a, slot_type* s)
        : alloc_(a) {
        PolicyTraits::construct(alloc(), slot(), s);
    }

    node_handle_base(const allocator_type& a, slot_type* s) : alloc_(a) {
        PolicyTraits::transfer(alloc(), slot(), s);
    }

    //node_handle_base(const node_handle_base&) = delete;
    //node_handle_base& operator=(const node_handle_base&) = delete;

    void destroy() {
        if (!empty()) {
            PolicyTraits::destroy(alloc(), slot());
            reset();
        }
    }

    void reset() {
        assert(alloc_.has_value());
        alloc_ = phmap::nullopt;
    }

    slot_type* slot() const {
        assert(!empty());
        return reinterpret_cast<slot_type*>(std::addressof(slot_space_));
    }

    allocator_type* alloc() { return std::addressof(*alloc_); }

private:
    phmap::optional<allocator_type> alloc_;
    mutable phmap::aligned_storage_t<sizeof(slot_type), alignof(slot_type)> slot_space_;
};

#ifdef _MSC_VER
     #pragma warning(pop)  
#endif

// For sets.
// ---------
template <typename Policy, typename PolicyTraits, typename Alloc,
          typename = void>
class node_handle : public node_handle_base<PolicyTraits, Alloc> 
{
    using Base = node_handle_base<PolicyTraits, Alloc>;

public:
    using value_type = typename PolicyTraits::value_type;

    constexpr node_handle() {}

    value_type& value() const { return PolicyTraits::element(this->slot()); }

    value_type& key() const { return PolicyTraits::element(this->slot()); }

private:
    friend struct CommonAccess;

    using Base::Base;
};

// For maps.
// ---------
template <typename Policy, typename PolicyTraits, typename Alloc>
class node_handle<Policy, PolicyTraits, Alloc,
                  phmap::void_t<typename Policy::mapped_type>>
    : public node_handle_base<PolicyTraits, Alloc> 
{
    using Base = node_handle_base<PolicyTraits, Alloc>;
    using slot_type = typename PolicyTraits::slot_type;

public:
    using key_type = typename Policy::key_type;
    using mapped_type = typename Policy::mapped_type;

    constexpr node_handle() {}

    auto key() const -> decltype(PolicyTraits::key(this->slot())) {
        return PolicyTraits::key(this->slot());
    }

    mapped_type& mapped() const {
        return PolicyTraits::value(&PolicyTraits::element(this->slot()));
    }

private:
    friend struct CommonAccess;

    using Base::Base;
};

// Provide access to non-public node-handle functions.
struct CommonAccess 
{
    template <typename Node>
    static auto GetSlot(const Node& node) -> decltype(node.slot()) {
        return node.slot();
    }

    template <typename Node>
    static void Destroy(Node* node) {
        node->destroy();
    }

    template <typename Node>
    static void Reset(Node* node) {
        node->reset();
    }

    template <typename T, typename... Args>
    static T Make(Args&&... args) {
        return T(std::forward<Args>(args)...);
    }

    template <typename T, typename... Args>
    static T Transfer(Args&&... args) {
        return T(typename T::transfer_tag_t{}, std::forward<Args>(args)...);
    }

    template <typename T, typename... Args>
    static T Move(Args&&... args) {
        return T(typename T::move_tag_t{}, std::forward<Args>(args)...);
    }
};

// Implement the insert_return_type<> concept of C++17.
template <class Iterator, class NodeType>
struct InsertReturnType 
{
    Iterator position;
    bool inserted;
    NodeType node;
};

}  // namespace priv
}  // namespace phmap


#ifdef ADDRESS_SANITIZER
    #include <sanitizer/asan_interface.h>
#endif

// ---------------------------------------------------------------------------
//  span.h
// ---------------------------------------------------------------------------

namespace phmap {

template <typename T>
class Span;

namespace span_internal {
// A constexpr min function
constexpr size_t Min(size_t a, size_t b) noexcept { return a < b ? a : b; }

// Wrappers for access to container data pointers.
template <typename C>
constexpr auto GetDataImpl(C& c, char) noexcept  // NOLINT(runtime/references)
    -> decltype(c.data()) {
  return c.data();
}

// Before C++17, std::string::data returns a const char* in all cases.
inline char* GetDataImpl(std::string& s,  // NOLINT(runtime/references)
                         int) noexcept {
  return &s[0];
}

template <typename C>
constexpr auto GetData(C& c) noexcept  // NOLINT(runtime/references)
    -> decltype(GetDataImpl(c, 0)) {
  return GetDataImpl(c, 0);
}

// Detection idioms for size() and data().
template <typename C>
using HasSize =
    std::is_integral<phmap::decay_t<decltype(std::declval<C&>().size())>>;

// We want to enable conversion from vector<T*> to Span<const T* const> but
// disable conversion from vector<Derived> to Span<Base>. Here we use
// the fact that U** is convertible to Q* const* if and only if Q is the same
// type or a more cv-qualified version of U.  We also decay the result type of
// data() to avoid problems with classes which have a member function data()
// which returns a reference.
template <typename T, typename C>
using HasData =
    std::is_convertible<phmap::decay_t<decltype(GetData(std::declval<C&>()))>*,
                        T* const*>;

// Extracts value type from a Container
template <typename C>
struct ElementType {
  using type = typename phmap::remove_reference_t<C>::value_type;
};

template <typename T, size_t N>
struct ElementType<T (&)[N]> {
  using type = T;
};

template <typename C>
using ElementT = typename ElementType<C>::type;

template <typename T>
using EnableIfMutable =
    typename std::enable_if<!std::is_const<T>::value, int>::type;

template <typename T>
bool EqualImpl(Span<T> a, Span<T> b) {
  static_assert(std::is_const<T>::value, "");
  return std::equal(a.begin(), a.end(), b.begin(), b.end());
}

template <typename T>
bool LessThanImpl(Span<T> a, Span<T> b) {
  static_assert(std::is_const<T>::value, "");
  return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end());
}

// The `IsConvertible` classes here are needed because of the
// `std::is_convertible` bug in libcxx when compiled with GCC. This build
// configuration is used by Android NDK toolchain. Reference link:
// https://bugs.llvm.org/show_bug.cgi?id=27538.
template <typename From, typename To>
struct IsConvertibleHelper {
  static std::true_type testval(To);
  static std::false_type testval(...);

  using type = decltype(testval(std::declval<From>()));
};

template <typename From, typename To>
struct IsConvertible : IsConvertibleHelper<From, To>::type {};

// TODO(zhangxy): replace `IsConvertible` with `std::is_convertible` once the
// older version of libcxx is not supported.
template <typename From, typename To>
using EnableIfConvertibleToSpanConst =
    typename std::enable_if<IsConvertible<From, Span<const To>>::value>::type;
}  // namespace span_internal

//------------------------------------------------------------------------------
// Span
//------------------------------------------------------------------------------
//
// A `Span` is an "array view" type for holding a view of a contiguous data
// array; the `Span` object does not and cannot own such data itself. A span
// provides an easy way to provide overloads for anything operating on
// contiguous sequences without needing to manage pointers and array lengths
// manually.

// A span is conceptually a pointer (ptr) and a length (size) into an already
// existing array of contiguous memory; the array it represents references the
// elements "ptr[0] .. ptr[size-1]". Passing a properly-constructed `Span`
// instead of raw pointers avoids many issues related to index out of bounds
// errors.
//
// Spans may also be constructed from containers holding contiguous sequences.
// Such containers must supply `data()` and `size() const` methods (e.g
// `std::vector<T>`, `phmap::InlinedVector<T, N>`). All implicit conversions to
// `phmap::Span` from such containers will create spans of type `const T`;
// spans which can mutate their values (of type `T`) must use explicit
// constructors.
//
// A `Span<T>` is somewhat analogous to an `phmap::string_view`, but for an array
// of elements of type `T`. A user of `Span` must ensure that the data being
// pointed to outlives the `Span` itself.
//
// You can construct a `Span<T>` in several ways:
//
//   * Explicitly from a reference to a container type
//   * Explicitly from a pointer and size
//   * Implicitly from a container type (but only for spans of type `const T`)
//   * Using the `MakeSpan()` or `MakeConstSpan()` factory functions.
//
// Examples:
//
//   // Construct a Span explicitly from a container:
//   std::vector<int> v = {1, 2, 3, 4, 5};
//   auto span = phmap::Span<const int>(v);
//
//   // Construct a Span explicitly from a C-style array:
//   int a[5] =  {1, 2, 3, 4, 5};
//   auto span = phmap::Span<const int>(a);
//
//   // Construct a Span implicitly from a container
//   void MyRoutine(phmap::Span<const int> a) {
//     ...
//   }
//   std::vector v = {1,2,3,4,5};
//   MyRoutine(v)                     // convert to Span<const T>
//
// Note that `Span` objects, in addition to requiring that the memory they
// point to remains alive, must also ensure that such memory does not get
// reallocated. Therefore, to avoid undefined behavior, containers with
// associated span views should not invoke operations that may reallocate memory
// (such as resizing) or invalidate iterators into the container.
//
// One common use for a `Span` is when passing arguments to a routine that can
// accept a variety of array types (e.g. a `std::vector`, `phmap::InlinedVector`,
// a C-style array, etc.). Instead of creating overloads for each case, you
// can simply specify a `Span` as the argument to such a routine.
//
// Example:
//
//   void MyRoutine(phmap::Span<const int> a) {
//     ...
//   }
//
//   std::vector v = {1,2,3,4,5};
//   MyRoutine(v);
//
//   phmap::InlinedVector<int, 4> my_inline_vector;
//   MyRoutine(my_inline_vector);
//
//   // Explicit constructor from pointer,size
//   int* my_array = new int[10];
//   MyRoutine(phmap::Span<const int>(my_array, 10));
template <typename T>
class Span 
{
private:
    // Used to determine whether a Span can be constructed from a container of
    // type C.
    template <typename C>
    using EnableIfConvertibleFrom =
        typename std::enable_if<span_internal::HasData<T, C>::value &&
                                span_internal::HasSize<C>::value>::type;

    // Used to SFINAE-enable a function when the slice elements are const.
    template <typename U>
    using EnableIfConstView =
        typename std::enable_if<std::is_const<T>::value, U>::type;

    // Used to SFINAE-enable a function when the slice elements are mutable.
    template <typename U>
    using EnableIfMutableView =
        typename std::enable_if<!std::is_const<T>::value, U>::type;

public:
    using value_type = phmap::remove_cv_t<T>;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using iterator = pointer;
    using const_iterator = const_pointer;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;
    using size_type = size_t;
    using difference_type = ptrdiff_t;

    static const size_type npos = ~(size_type(0));

    constexpr Span() noexcept : Span(nullptr, 0) {}
    constexpr Span(pointer array, size_type lgth) noexcept
        : ptr_(array), len_(lgth) {}

    // Implicit conversion constructors
    template <size_t N>
    constexpr Span(T (&a)[N]) noexcept  // NOLINT(runtime/explicit)
        : Span(a, N) {}

    // Explicit reference constructor for a mutable `Span<T>` type. Can be
    // replaced with MakeSpan() to infer the type parameter.
    template <typename V, typename = EnableIfConvertibleFrom<V>,
              typename = EnableIfMutableView<V>>
        explicit Span(V& v) noexcept  // NOLINT(runtime/references)
        : Span(span_internal::GetData(v), v.size()) {}

    // Implicit reference constructor for a read-only `Span<const T>` type
    template <typename V, typename = EnableIfConvertibleFrom<V>,
              typename = EnableIfConstView<V>>
        constexpr Span(const V& v) noexcept  // NOLINT(runtime/explicit)
        : Span(span_internal::GetData(v), v.size()) {}

    // Implicit constructor from an initializer list, making it possible to pass a
    // brace-enclosed initializer list to a function expecting a `Span`. Such
    // spans constructed from an initializer list must be of type `Span<const T>`.
    //
    //   void Process(phmap::Span<const int> x);
    //   Process({1, 2, 3});
    //
    // Note that as always the array referenced by the span must outlive the span.
    // Since an initializer list constructor acts as if it is fed a temporary
    // array (cf. C++ standard [dcl.init.list]/5), it's safe to use this
    // constructor only when the `std::initializer_list` itself outlives the span.
    // In order to meet this requirement it's sufficient to ensure that neither
    // the span nor a copy of it is used outside of the expression in which it's
    // created:
    //
    //   // Assume that this function uses the array directly, not retaining any
    //   // copy of the span or pointer to any of its elements.
    //   void Process(phmap::Span<const int> ints);
    //
    //   // Okay: the std::initializer_list<int> will reference a temporary array
    //   // that isn't destroyed until after the call to Process returns.
    //   Process({ 17, 19 });
    //
    //   // Not okay: the storage used by the std::initializer_list<int> is not
    //   // allowed to be referenced after the first line.
    //   phmap::Span<const int> ints = { 17, 19 };
    //   Process(ints);
    //
    //   // Not okay for the same reason as above: even when the elements of the
    //   // initializer list expression are not temporaries the underlying array
    //   // is, so the initializer list must still outlive the span.
    //   const int foo = 17;
    //   phmap::Span<const int> ints = { foo };
    //   Process(ints);
    //
    template <typename LazyT = T,
              typename = EnableIfConstView<LazyT>>
        Span(
            std::initializer_list<value_type> v) noexcept  // NOLINT(runtime/explicit)
        : Span(v.begin(), v.size()) {}

    // Accessors

    // Span::data()
    //
    // Returns a pointer to the span's underlying array of data (which is held
    // outside the span).
    constexpr pointer data() const noexcept { return ptr_; }

    // Span::size()
    //
    // Returns the size of this span.
    constexpr size_type size() const noexcept { return len_; }

    // Span::length()
    //
    // Returns the length (size) of this span.
    constexpr size_type length() const noexcept { return size(); }

    // Span::empty()
    //
    // Returns a boolean indicating whether or not this span is considered empty.
    constexpr bool empty() const noexcept { return size() == 0; }

    // Span::operator[]
    //
    // Returns a reference to the i'th element of this span.
    constexpr reference operator[](size_type i) const noexcept {
        // MSVC 2015 accepts this as constexpr, but not ptr_[i]
        return *(data() + i);
    }

    // Span::at()
    //
    // Returns a reference to the i'th element of this span.
    constexpr reference at(size_type i) const {
        return PHMAP_PREDICT_TRUE(i < size())  //
            ? *(data() + i)
            : (base_internal::ThrowStdOutOfRange(
                   "Span::at failed bounds check"),
               *(data() + i));
    }

    // Span::front()
    //
    // Returns a reference to the first element of this span.
    constexpr reference front() const noexcept {
        return PHMAP_ASSERT(size() > 0), *data();
    }

    // Span::back()
    //
    // Returns a reference to the last element of this span.
    constexpr reference back() const noexcept {
        return PHMAP_ASSERT(size() > 0), *(data() + size() - 1);
    }

    // Span::begin()
    //
    // Returns an iterator to the first element of this span.
    constexpr iterator begin() const noexcept { return data(); }

    // Span::cbegin()
    //
    // Returns a const iterator to the first element of this span.
    constexpr const_iterator cbegin() const noexcept { return begin(); }

    // Span::end()
    //
    // Returns an iterator to the last element of this span.
    constexpr iterator end() const noexcept { return data() + size(); }

    // Span::cend()
    //
    // Returns a const iterator to the last element of this span.
    constexpr const_iterator cend() const noexcept { return end(); }

    // Span::rbegin()
    //
    // Returns a reverse iterator starting at the last element of this span.
    constexpr reverse_iterator rbegin() const noexcept {
        return reverse_iterator(end());
    }

    // Span::crbegin()
    //
    // Returns a reverse const iterator starting at the last element of this span.
    constexpr const_reverse_iterator crbegin() const noexcept { return rbegin(); }

    // Span::rend()
    //
    // Returns a reverse iterator starting at the first element of this span.
    constexpr reverse_iterator rend() const noexcept {
        return reverse_iterator(begin());
    }

    // Span::crend()
    //
    // Returns a reverse iterator starting at the first element of this span.
    constexpr const_reverse_iterator crend() const noexcept { return rend(); }

    // Span mutations

    // Span::remove_prefix()
    //
    // Removes the first `n` elements from the span.
    void remove_prefix(size_type n) noexcept {
        assert(size() >= n);
        ptr_ += n;
        len_ -= n;
    }

    // Span::remove_suffix()
    //
    // Removes the last `n` elements from the span.
    void remove_suffix(size_type n) noexcept {
        assert(size() >= n);
        len_ -= n;
    }

    // Span::subspan()
    //
    // Returns a `Span` starting at element `pos` and of length `len`. Both `pos`
    // and `len` are of type `size_type` and thus non-negative. Parameter `pos`
    // must be <= size(). Any `len` value that points past the end of the span
    // will be trimmed to at most size() - `pos`. A default `len` value of `npos`
    // ensures the returned subspan continues until the end of the span.
    //
    // Examples:
    //
    //   std::vector<int> vec = {10, 11, 12, 13};
    //   phmap::MakeSpan(vec).subspan(1, 2);  // {11, 12}
    //   phmap::MakeSpan(vec).subspan(2, 8);  // {12, 13}
    //   phmap::MakeSpan(vec).subspan(1);     // {11, 12, 13}
    //   phmap::MakeSpan(vec).subspan(4);     // {}
    //   phmap::MakeSpan(vec).subspan(5);     // throws std::out_of_range
    constexpr Span subspan(size_type pos = 0, size_type len = npos) const {
        return (pos <= size())
            ? Span(data() + pos, span_internal::Min(size() - pos, len))
            : (base_internal::ThrowStdOutOfRange("pos > size()"), Span());
    }

    // Span::first()
    //
    // Returns a `Span` containing first `len` elements. Parameter `len` is of
    // type `size_type` and thus non-negative. `len` value must be <= size().
    //
    // Examples:
    //
    //   std::vector<int> vec = {10, 11, 12, 13};
    //   phmap::MakeSpan(vec).first(1);  // {10}
    //   phmap::MakeSpan(vec).first(3);  // {10, 11, 12}
    //   phmap::MakeSpan(vec).first(5);  // throws std::out_of_range
    constexpr Span first(size_type len) const {
        return (len <= size())
            ? Span(data(), len)
            : (base_internal::ThrowStdOutOfRange("len > size()"), Span());
    }

    // Span::last()
    //
    // Returns a `Span` containing last `len` elements. Parameter `len` is of
    // type `size_type` and thus non-negative. `len` value must be <= size().
    //
    // Examples:
    //
    //   std::vector<int> vec = {10, 11, 12, 13};
    //   phmap::MakeSpan(vec).last(1);  // {13}
    //   phmap::MakeSpan(vec).last(3);  // {11, 12, 13}
    //   phmap::MakeSpan(vec).last(5);  // throws std::out_of_range
    constexpr Span last(size_type len) const {
        return (len <= size())
            ? Span(size() - len + data(), len)
            : (base_internal::ThrowStdOutOfRange("len > size()"), Span());
    }

    // Support for phmap::Hash.
    template <typename H>
    friend H AbslHashValue(H h, Span v) {
        return H::combine(H::combine_contiguous(std::move(h), v.data(), v.size()),
                          v.size());
    }

private:
    pointer ptr_;
    size_type len_;
};

template <typename T>
const typename Span<T>::size_type Span<T>::npos;

// Span relationals

// Equality is compared element-by-element, while ordering is lexicographical.
// We provide three overloads for each operator to cover any combination on the
// left or right hand side of mutable Span<T>, read-only Span<const T>, and
// convertible-to-read-only Span<T>.
// TODO(zhangxy): Due to MSVC overload resolution bug with partial ordering
// template functions, 5 overloads per operator is needed as a workaround. We
// should update them to 3 overloads per operator using non-deduced context like
// string_view, i.e.
// - (Span<T>, Span<T>)
// - (Span<T>, non_deduced<Span<const T>>)
// - (non_deduced<Span<const T>>, Span<T>)

// operator==
template <typename T>
bool operator==(Span<T> a, Span<T> b) {
  return span_internal::EqualImpl<const T>(a, b);
}

template <typename T>
bool operator==(Span<const T> a, Span<T> b) {
  return span_internal::EqualImpl<const T>(a, b);
}

template <typename T>
bool operator==(Span<T> a, Span<const T> b) {
  return span_internal::EqualImpl<const T>(a, b);
}

template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator==(const U& a, Span<T> b) {
  return span_internal::EqualImpl<const T>(a, b);
}

template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator==(Span<T> a, const U& b) {
  return span_internal::EqualImpl<const T>(a, b);
}

// operator!=
template <typename T>
bool operator!=(Span<T> a, Span<T> b) {
  return !(a == b);
}

template <typename T>
bool operator!=(Span<const T> a, Span<T> b) {
  return !(a == b);
}

template <typename T>
bool operator!=(Span<T> a, Span<const T> b) {
  return !(a == b);
}

template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator!=(const U& a, Span<T> b) {
  return !(a == b);
}

template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator!=(Span<T> a, const U& b) {
  return !(a == b);
}

// operator<
template <typename T>
bool operator<(Span<T> a, Span<T> b) {
  return span_internal::LessThanImpl<const T>(a, b);
}

template <typename T>
bool operator<(Span<const T> a, Span<T> b) {
  return span_internal::LessThanImpl<const T>(a, b);
}

template <typename T>
bool operator<(Span<T> a, Span<const T> b) {
  return span_internal::LessThanImpl<const T>(a, b);
}

template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator<(const U& a, Span<T> b) {
  return span_internal::LessThanImpl<const T>(a, b);
}

template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator<(Span<T> a, const U& b) {
  return span_internal::LessThanImpl<const T>(a, b);
}

// operator>
template <typename T>
bool operator>(Span<T> a, Span<T> b) {
  return b < a;
}

template <typename T>
bool operator>(Span<const T> a, Span<T> b) {
  return b < a;
}

template <typename T>
bool operator>(Span<T> a, Span<const T> b) {
  return b < a;
}

template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator>(const U& a, Span<T> b) {
  return b < a;
}

template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator>(Span<T> a, const U& b) {
  return b < a;
}

// operator<=
template <typename T>
bool operator<=(Span<T> a, Span<T> b) {
  return !(b < a);
}

template <typename T>
bool operator<=(Span<const T> a, Span<T> b) {
  return !(b < a);
}

template <typename T>
bool operator<=(Span<T> a, Span<const T> b) {
  return !(b < a);
}

template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator<=(const U& a, Span<T> b) {
  return !(b < a);
}

template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator<=(Span<T> a, const U& b) {
  return !(b < a);
}

// operator>=
template <typename T>
bool operator>=(Span<T> a, Span<T> b) {
  return !(a < b);
}

template <typename T>
bool operator>=(Span<const T> a, Span<T> b) {
  return !(a < b);
}

template <typename T>
bool operator>=(Span<T> a, Span<const T> b) {
  return !(a < b);
}

template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator>=(const U& a, Span<T> b) {
  return !(a < b);
}

template <typename T, typename U,
          typename = span_internal::EnableIfConvertibleToSpanConst<U, T>>
bool operator>=(Span<T> a, const U& b) {
  return !(a < b);
}

// MakeSpan()
//
// Constructs a mutable `Span<T>`, deducing `T` automatically from either a
// container or pointer+size.
//
// Because a read-only `Span<const T>` is implicitly constructed from container
// types regardless of whether the container itself is a const container,
// constructing mutable spans of type `Span<T>` from containers requires
// explicit constructors. The container-accepting version of `MakeSpan()`
// deduces the type of `T` by the constness of the pointer received from the
// container's `data()` member. Similarly, the pointer-accepting version returns
// a `Span<const T>` if `T` is `const`, and a `Span<T>` otherwise.
//
// Examples:
//
//   void MyRoutine(phmap::Span<MyComplicatedType> a) {
//     ...
//   };
//   // my_vector is a container of non-const types
//   std::vector<MyComplicatedType> my_vector;
//
//   // Constructing a Span implicitly attempts to create a Span of type
//   // `Span<const T>`
//   MyRoutine(my_vector);                // error, type mismatch
//
//   // Explicitly constructing the Span is verbose
//   MyRoutine(phmap::Span<MyComplicatedType>(my_vector));
//
//   // Use MakeSpan() to make an phmap::Span<T>
//   MyRoutine(phmap::MakeSpan(my_vector));
//
//   // Construct a span from an array ptr+size
//   phmap::Span<T> my_span() {
//     return phmap::MakeSpan(&array[0], num_elements_);
//   }
//
template <int&... ExplicitArgumentBarrier, typename T>
constexpr Span<T> MakeSpan(T* ptr, size_t size) noexcept {
  return Span<T>(ptr, size);
}

template <int&... ExplicitArgumentBarrier, typename T>
Span<T> MakeSpan(T* begin, T* end) noexcept {
  return PHMAP_ASSERT(begin <= end), Span<T>(begin, end - begin);
}

template <int&... ExplicitArgumentBarrier, typename C>
constexpr auto MakeSpan(C& c) noexcept  // NOLINT(runtime/references)
    -> decltype(phmap::MakeSpan(span_internal::GetData(c), c.size())) {
  return MakeSpan(span_internal::GetData(c), c.size());
}

template <int&... ExplicitArgumentBarrier, typename T, size_t N>
constexpr Span<T> MakeSpan(T (&array)[N]) noexcept {
  return Span<T>(array, N);
}

// MakeConstSpan()
//
// Constructs a `Span<const T>` as with `MakeSpan`, deducing `T` automatically,
// but always returning a `Span<const T>`.
//
// Examples:
//
//   void ProcessInts(phmap::Span<const int> some_ints);
//
//   // Call with a pointer and size.
//   int array[3] = { 0, 0, 0 };
//   ProcessInts(phmap::MakeConstSpan(&array[0], 3));
//
//   // Call with a [begin, end) pair.
//   ProcessInts(phmap::MakeConstSpan(&array[0], &array[3]));
//
//   // Call directly with an array.
//   ProcessInts(phmap::MakeConstSpan(array));
//
//   // Call with a contiguous container.
//   std::vector<int> some_ints = ...;
//   ProcessInts(phmap::MakeConstSpan(some_ints));
//   ProcessInts(phmap::MakeConstSpan(std::vector<int>{ 0, 0, 0 }));
//
template <int&... ExplicitArgumentBarrier, typename T>
constexpr Span<const T> MakeConstSpan(T* ptr, size_t size) noexcept {
  return Span<const T>(ptr, size);
}

template <int&... ExplicitArgumentBarrier, typename T>
Span<const T> MakeConstSpan(T* begin, T* end) noexcept {
  return PHMAP_ASSERT(begin <= end), Span<const T>(begin, end - begin);
}

template <int&... ExplicitArgumentBarrier, typename C>
constexpr auto MakeConstSpan(const C& c) noexcept -> decltype(MakeSpan(c)) {
  return MakeSpan(c);
}

template <int&... ExplicitArgumentBarrier, typename T, size_t N>
constexpr Span<const T> MakeConstSpan(const T (&array)[N]) noexcept {
  return Span<const T>(array, N);
}
}  // namespace phmap

// ---------------------------------------------------------------------------
//  layout.h
// ---------------------------------------------------------------------------
namespace phmap {
namespace priv {

// A type wrapper that instructs `Layout` to use the specific alignment for the
// array. `Layout<..., Aligned<T, N>, ...>` has exactly the same API
// and behavior as `Layout<..., T, ...>` except that the first element of the
// array of `T` is aligned to `N` (the rest of the elements follow without
// padding).
//
// Requires: `N >= alignof(T)` and `N` is a power of 2.
template <class T, size_t N>
struct Aligned;

namespace internal_layout {

template <class T>
struct NotAligned {};

template <class T, size_t N>
struct NotAligned<const Aligned<T, N>> {
  static_assert(sizeof(T) == 0, "Aligned<T, N> cannot be const-qualified");
};

template <size_t>
using IntToSize = size_t;

template <class>
using TypeToSize = size_t;

template <class T>
struct Type : NotAligned<T> {
    using type = T;
};

template <class T, size_t N>
struct Type<Aligned<T, N>> {
    using type = T;
};

template <class T>
struct SizeOf : NotAligned<T>, std::integral_constant<size_t, sizeof(T)> {};

template <class T, size_t N>
struct SizeOf<Aligned<T, N>> : std::integral_constant<size_t, sizeof(T)> {};

// Note: workaround for https://gcc.gnu.org/PR88115
template <class T>
struct AlignOf : NotAligned<T> {
    static constexpr size_t value = alignof(T);
};

template <class T, size_t N>
struct AlignOf<Aligned<T, N>> {
    static_assert(N % alignof(T) == 0,
                  "Custom alignment can't be lower than the type's alignment");
    static constexpr size_t value = N;
};

// Does `Ts...` contain `T`?
template <class T, class... Ts>
using Contains = phmap::disjunction<std::is_same<T, Ts>...>;

template <class From, class To>
using CopyConst =
    typename std::conditional<std::is_const<From>::value, const To, To>::type;

// Note: We're not qualifying this with phmap:: because it doesn't compile under
// MSVC.
template <class T>
using SliceType = Span<T>;

// This namespace contains no types. It prevents functions defined in it from
// being found by ADL.
namespace adl_barrier {

template <class Needle, class... Ts>
constexpr size_t Find(Needle, Needle, Ts...) {
    static_assert(!Contains<Needle, Ts...>(), "Duplicate element type");
    return 0;
}

template <class Needle, class T, class... Ts>
constexpr size_t Find(Needle, T, Ts...) {
  return adl_barrier::Find(Needle(), Ts()...) + 1;
}

constexpr bool IsPow2(size_t n) { return !(n & (n - 1)); }

// Returns `q * m` for the smallest `q` such that `q * m >= n`.
// Requires: `m` is a power of two. It's enforced by IsLegalElementType below.
constexpr size_t Align(size_t n, size_t m) { return (n + m - 1) & ~(m - 1); }

constexpr size_t Min(size_t a, size_t b) { return b < a ? b : a; }

constexpr size_t Max(size_t a) { return a; }

template <class... Ts>
constexpr size_t Max(size_t a, size_t b, Ts... rest) {
    return adl_barrier::Max(b < a ? a : b, rest...);
}

}  // namespace adl_barrier

template <bool C>
using EnableIf = typename std::enable_if<C, int>::type;

// Can `T` be a template argument of `Layout`?
// ---------------------------------------------------------------------------
template <class T>
using IsLegalElementType = std::integral_constant<
    bool, !std::is_reference<T>::value && !std::is_volatile<T>::value &&
              !std::is_reference<typename Type<T>::type>::value &&
              !std::is_volatile<typename Type<T>::type>::value &&
              adl_barrier::IsPow2(AlignOf<T>::value)>;

template <class Elements, class SizeSeq, class OffsetSeq>
class LayoutImpl;

// ---------------------------------------------------------------------------
// Public base class of `Layout` and the result type of `Layout::Partial()`.
//
// `Elements...` contains all template arguments of `Layout` that created this
// instance.
//
// `SizeSeq...` is `[0, NumSizes)` where `NumSizes` is the number of arguments
// passed to `Layout::Partial()` or `Layout::Layout()`.
//
// `OffsetSeq...` is `[0, NumOffsets)` where `NumOffsets` is
// `Min(sizeof...(Elements), NumSizes + 1)` (the number of arrays for which we
// can compute offsets).
// ---------------------------------------------------------------------------
template <class... Elements, size_t... SizeSeq, size_t... OffsetSeq>
class LayoutImpl<std::tuple<Elements...>, phmap::index_sequence<SizeSeq...>,
                 phmap::index_sequence<OffsetSeq...>> 
{
private:
    static_assert(sizeof...(Elements) > 0, "At least one field is required");
    static_assert(phmap::conjunction<IsLegalElementType<Elements>...>::value,
                  "Invalid element type (see IsLegalElementType)");

    enum {
        NumTypes = sizeof...(Elements),
        NumSizes = sizeof...(SizeSeq),
        NumOffsets = sizeof...(OffsetSeq),
    };

    // These are guaranteed by `Layout`.
    static_assert(NumOffsets == adl_barrier::Min(NumTypes, NumSizes + 1),
                  "Internal error");
    static_assert(NumTypes > 0, "Internal error");

    // Returns the index of `T` in `Elements...`. Results in a compilation error
    // if `Elements...` doesn't contain exactly one instance of `T`.
    template <class T>
        static constexpr size_t ElementIndex() {
        static_assert(Contains<Type<T>, Type<typename Type<Elements>::type>...>(),
                      "Type not found");
        return adl_barrier::Find(Type<T>(),
                                 Type<typename Type<Elements>::type>()...);
    }

    template <size_t N>
        using ElementAlignment =
        AlignOf<typename std::tuple_element<N, std::tuple<Elements...>>::type>;

public:
    // Element types of all arrays packed in a tuple.
    using ElementTypes = std::tuple<typename Type<Elements>::type...>;

    // Element type of the Nth array.
    template <size_t N>
        using ElementType = typename std::tuple_element<N, ElementTypes>::type;

    constexpr explicit LayoutImpl(IntToSize<SizeSeq>... sizes)
        : size_{sizes...} {}

    // Alignment of the layout, equal to the strictest alignment of all elements.
    // All pointers passed to the methods of layout must be aligned to this value.
    static constexpr size_t Alignment() {
        return adl_barrier::Max(AlignOf<Elements>::value...);
    }

    // Offset in bytes of the Nth array.
    //
    //   // int[3], 4 bytes of padding, double[4].
    //   Layout<int, double> x(3, 4);
    //   assert(x.Offset<0>() == 0);   // The ints starts from 0.
    //   assert(x.Offset<1>() == 16);  // The doubles starts from 16.
    //
    // Requires: `N <= NumSizes && N < sizeof...(Ts)`.
    template <size_t N, EnableIf<N == 0> = 0>
        constexpr size_t Offset() const {
        return 0;
    }

    template <size_t N, EnableIf<N != 0> = 0>
        constexpr size_t Offset() const {
        static_assert(N < NumOffsets, "Index out of bounds");
        return adl_barrier::Align(
            Offset<N - 1>() + SizeOf<ElementType<N - 1>>::value * size_[N - 1],
            ElementAlignment<N>::value);
    }

    // Offset in bytes of the array with the specified element type. There must
    // be exactly one such array and its zero-based index must be at most
    // `NumSizes`.
    //
    //   // int[3], 4 bytes of padding, double[4].
    //   Layout<int, double> x(3, 4);
    //   assert(x.Offset<int>() == 0);      // The ints starts from 0.
    //   assert(x.Offset<double>() == 16);  // The doubles starts from 16.
    template <class T>
        constexpr size_t Offset() const {
        return Offset<ElementIndex<T>()>();
    }

    // Offsets in bytes of all arrays for which the offsets are known.
    constexpr std::array<size_t, NumOffsets> Offsets() const {
        return {{Offset<OffsetSeq>()...}};
    }

    // The number of elements in the Nth array. This is the Nth argument of
    // `Layout::Partial()` or `Layout::Layout()` (zero-based).
    //
    //   // int[3], 4 bytes of padding, double[4].
    //   Layout<int, double> x(3, 4);
    //   assert(x.Size<0>() == 3);
    //   assert(x.Size<1>() == 4);
    //
    // Requires: `N < NumSizes`.
    template <size_t N>
        constexpr size_t Size() const {
        static_assert(N < NumSizes, "Index out of bounds");
        return size_[N];
    }

    // The number of elements in the array with the specified element type.
    // There must be exactly one such array and its zero-based index must be
    // at most `NumSizes`.
    //
    //   // int[3], 4 bytes of padding, double[4].
    //   Layout<int, double> x(3, 4);
    //   assert(x.Size<int>() == 3);
    //   assert(x.Size<double>() == 4);
    template <class T>
        constexpr size_t Size() const {
        return Size<ElementIndex<T>()>();
    }

    // The number of elements of all arrays for which they are known.
    constexpr std::array<size_t, NumSizes> Sizes() const {
        return {{Size<SizeSeq>()...}};
    }

    // Pointer to the beginning of the Nth array.
    //
    // `Char` must be `[const] [signed|unsigned] char`.
    //
    //   // int[3], 4 bytes of padding, double[4].
    //   Layout<int, double> x(3, 4);
    //   unsigned char* p = new unsigned char[x.AllocSize()];
    //   int* ints = x.Pointer<0>(p);
    //   double* doubles = x.Pointer<1>(p);
    //
    // Requires: `N <= NumSizes && N < sizeof...(Ts)`.
    // Requires: `p` is aligned to `Alignment()`.
    template <size_t N, class Char>
        CopyConst<Char, ElementType<N>>* Pointer(Char* p) const {
        using C = typename std::remove_const<Char>::type;
        static_assert(
            std::is_same<C, char>() || std::is_same<C, unsigned char>() ||
            std::is_same<C, signed char>(),
            "The argument must be a pointer to [const] [signed|unsigned] char");
        constexpr size_t alignment = Alignment();
        (void)alignment;
        assert(reinterpret_cast<uintptr_t>(p) % alignment == 0);
        return reinterpret_cast<CopyConst<Char, ElementType<N>>*>(p + Offset<N>());
    }

    // Pointer to the beginning of the array with the specified element type.
    // There must be exactly one such array and its zero-based index must be at
    // most `NumSizes`.
    //
    // `Char` must be `[const] [signed|unsigned] char`.
    //
    //   // int[3], 4 bytes of padding, double[4].
    //   Layout<int, double> x(3, 4);
    //   unsigned char* p = new unsigned char[x.AllocSize()];
    //   int* ints = x.Pointer<int>(p);
    //   double* doubles = x.Pointer<double>(p);
    //
    // Requires: `p` is aligned to `Alignment()`.
    template <class T, class Char>
        CopyConst<Char, T>* Pointer(Char* p) const {
        return Pointer<ElementIndex<T>()>(p);
    }

    // Pointers to all arrays for which pointers are known.
    //
    // `Char` must be `[const] [signed|unsigned] char`.
    //
    //   // int[3], 4 bytes of padding, double[4].
    //   Layout<int, double> x(3, 4);
    //   unsigned char* p = new unsigned char[x.AllocSize()];
    //
    //   int* ints;
    //   double* doubles;
    //   std::tie(ints, doubles) = x.Pointers(p);
    //
    // Requires: `p` is aligned to `Alignment()`.
    //
    // Note: We're not using ElementType alias here because it does not compile
    // under MSVC.
    template <class Char>
        std::tuple<CopyConst<
                       Char, typename std::tuple_element<OffsetSeq, ElementTypes>::type>*...>
        Pointers(Char* p) const {
        return std::tuple<CopyConst<Char, ElementType<OffsetSeq>>*...>(
            Pointer<OffsetSeq>(p)...);
    }

    // The Nth array.
    //
    // `Char` must be `[const] [signed|unsigned] char`.
    //
    //   // int[3], 4 bytes of padding, double[4].
    //   Layout<int, double> x(3, 4);
    //   unsigned char* p = new unsigned char[x.AllocSize()];
    //   Span<int> ints = x.Slice<0>(p);
    //   Span<double> doubles = x.Slice<1>(p);
    //
    // Requires: `N < NumSizes`.
    // Requires: `p` is aligned to `Alignment()`.
    template <size_t N, class Char>
        SliceType<CopyConst<Char, ElementType<N>>> Slice(Char* p) const {
        return SliceType<CopyConst<Char, ElementType<N>>>(Pointer<N>(p), Size<N>());
    }

    // The array with the specified element type. There must be exactly one
    // such array and its zero-based index must be less than `NumSizes`.
    //
    // `Char` must be `[const] [signed|unsigned] char`.
    //
    //   // int[3], 4 bytes of padding, double[4].
    //   Layout<int, double> x(3, 4);
    //   unsigned char* p = new unsigned char[x.AllocSize()];
    //   Span<int> ints = x.Slice<int>(p);
    //   Span<double> doubles = x.Slice<double>(p);
    //
    // Requires: `p` is aligned to `Alignment()`.
    template <class T, class Char>
        SliceType<CopyConst<Char, T>> Slice(Char* p) const {
        return Slice<ElementIndex<T>()>(p);
    }

    // All arrays with known sizes.
    //
    // `Char` must be `[const] [signed|unsigned] char`.
    //
    //   // int[3], 4 bytes of padding, double[4].
    //   Layout<int, double> x(3, 4);
    //   unsigned char* p = new unsigned char[x.AllocSize()];
    //
    //   Span<int> ints;
    //   Span<double> doubles;
    //   std::tie(ints, doubles) = x.Slices(p);
    //
    // Requires: `p` is aligned to `Alignment()`.
    //
    // Note: We're not using ElementType alias here because it does not compile
    // under MSVC.
    template <class Char>
        std::tuple<SliceType<CopyConst<
                                 Char, typename std::tuple_element<SizeSeq, ElementTypes>::type>>...>
        Slices(Char* p) const {
        // Workaround for https://gcc.gnu.org/bugzilla/show_bug.cgi?id=63875 (fixed
        // in 6.1).
        (void)p;
        return std::tuple<SliceType<CopyConst<Char, ElementType<SizeSeq>>>...>(
            Slice<SizeSeq>(p)...);
    }

    // The size of the allocation that fits all arrays.
    //
    //   // int[3], 4 bytes of padding, double[4].
    //   Layout<int, double> x(3, 4);
    //   unsigned char* p = new unsigned char[x.AllocSize()];  // 48 bytes
    //
    // Requires: `NumSizes == sizeof...(Ts)`.
    constexpr size_t AllocSize() const {
        static_assert(NumTypes == NumSizes, "You must specify sizes of all fields");
        return Offset<NumTypes - 1>() +
            SizeOf<ElementType<NumTypes - 1>>::value * size_[NumTypes - 1];
    }

    // If built with --config=asan, poisons padding bytes (if any) in the
    // allocation. The pointer must point to a memory block at least
    // `AllocSize()` bytes in length.
    //
    // `Char` must be `[const] [signed|unsigned] char`.
    //
    // Requires: `p` is aligned to `Alignment()`.
    template <class Char, size_t N = NumOffsets - 1, EnableIf<N == 0> = 0>
        void PoisonPadding(const Char* p) const {
        Pointer<0>(p);  // verify the requirements on `Char` and `p`
    }

    template <class Char, size_t N = NumOffsets - 1, EnableIf<N != 0> = 0>
        void PoisonPadding(const Char* p) const {
        static_assert(N < NumOffsets, "Index out of bounds");
        (void)p;
#ifdef ADDRESS_SANITIZER
        PoisonPadding<Char, N - 1>(p);
        // The `if` is an optimization. It doesn't affect the observable behaviour.
        if (ElementAlignment<N - 1>::value % ElementAlignment<N>::value) {
            size_t start =
                Offset<N - 1>() + SizeOf<ElementType<N - 1>>::value * size_[N - 1];
            ASAN_POISON_MEMORY_REGION(p + start, Offset<N>() - start);
        }
#endif
    }

private:
    // Arguments of `Layout::Partial()` or `Layout::Layout()`.
    size_t size_[NumSizes > 0 ? NumSizes : 1];
};

template <size_t NumSizes, class... Ts>
using LayoutType = LayoutImpl<
    std::tuple<Ts...>, phmap::make_index_sequence<NumSizes>,
    phmap::make_index_sequence<adl_barrier::Min(sizeof...(Ts), NumSizes + 1)>>;

}  // namespace internal_layout

// ---------------------------------------------------------------------------
// Descriptor of arrays of various types and sizes laid out in memory one after
// another. See the top of the file for documentation.
//
// Check out the public API of internal_layout::LayoutImpl above. The type is
// internal to the library but its methods are public, and they are inherited
// by `Layout`.
// ---------------------------------------------------------------------------
template <class... Ts>
class Layout : public internal_layout::LayoutType<sizeof...(Ts), Ts...> 
{
public:
    static_assert(sizeof...(Ts) > 0, "At least one field is required");
    static_assert(
        phmap::conjunction<internal_layout::IsLegalElementType<Ts>...>::value,
        "Invalid element type (see IsLegalElementType)");

    template <size_t NumSizes>
    using PartialType = internal_layout::LayoutType<NumSizes, Ts...>;

    template <class... Sizes>
    static constexpr PartialType<sizeof...(Sizes)> Partial(Sizes&&... sizes) {
        static_assert(sizeof...(Sizes) <= sizeof...(Ts), "");
        return PartialType<sizeof...(Sizes)>(phmap::forward<Sizes>(sizes)...);
    }

    // Creates a layout with the sizes of all arrays specified. If you know
    // only the sizes of the first N arrays (where N can be zero), you can use
    // `Partial()` defined above. The constructor is essentially equivalent to
    // calling `Partial()` and passing in all array sizes; the constructor is
    // provided as a convenient abbreviation.
    //
    // Note: The sizes of the arrays must be specified in number of elements,
    // not in bytes.
    constexpr explicit Layout(internal_layout::TypeToSize<Ts>... sizes)
        : internal_layout::LayoutType<sizeof...(Ts), Ts...>(sizes...) {}
};


#ifdef _MSC_VER
    #pragma warning(push)  
    // warning warning C4324: structure was padded due to alignment specifier
    #pragma warning(disable : 4324)
#endif


// ----------------------------------------------------------------------------
// Allocates at least n bytes aligned to the specified alignment.
// Alignment must be a power of 2. It must be positive.
//
// Note that many allocators don't honor alignment requirements above certain
// threshold (usually either alignof(std::max_align_t) or alignof(void*)).
// Allocate() doesn't apply alignment corrections. If the underlying allocator
// returns insufficiently alignment pointer, that's what you are going to get.
// ----------------------------------------------------------------------------
template <size_t Alignment, class Alloc>
void* Allocate(Alloc* alloc, size_t n) {
  static_assert(Alignment > 0, "");
  assert(n && "n must be positive");
  struct alignas(Alignment) M {};
  using A = typename phmap::allocator_traits<Alloc>::template rebind_alloc<M>;
  using AT = typename phmap::allocator_traits<Alloc>::template rebind_traits<M>;
  A mem_alloc(*alloc);
  void* p = &*AT::allocate(mem_alloc, (n + sizeof(M) - 1) / sizeof(M)); // `&*` to support custom pointers such as boost offset_ptr.
  assert(reinterpret_cast<uintptr_t>(p) % Alignment == 0 &&
         "allocator does not respect alignment");
  return p;
}

// ----------------------------------------------------------------------------
// The pointer must have been previously obtained by calling
// Allocate<Alignment>(alloc, n).
// ----------------------------------------------------------------------------
template <size_t Alignment, class Alloc>
void Deallocate(Alloc* alloc, void* p, size_t n) {
  static_assert(Alignment > 0, "");
  assert(n && "n must be positive");
  struct alignas(Alignment) M {};
  using A = typename phmap::allocator_traits<Alloc>::template rebind_alloc<M>;
  using AT = typename phmap::allocator_traits<Alloc>::template rebind_traits<M>;
  A mem_alloc(*alloc);
  AT::deallocate(mem_alloc, static_cast<M*>(p),
                 (n + sizeof(M) - 1) / sizeof(M));
}

#ifdef _MSC_VER
     #pragma warning(pop)  
#endif

// Helper functions for asan and msan.
// ----------------------------------------------------------------------------
inline void SanitizerPoisonMemoryRegion(const void* m, size_t s) {
#ifdef ADDRESS_SANITIZER
    ASAN_POISON_MEMORY_REGION(m, s);
#endif
#ifdef MEMORY_SANITIZER
    __msan_poison(m, s);
#endif
    (void)m;
    (void)s;
}

inline void SanitizerUnpoisonMemoryRegion(const void* m, size_t s) {
#ifdef ADDRESS_SANITIZER
    ASAN_UNPOISON_MEMORY_REGION(m, s);
#endif
#ifdef MEMORY_SANITIZER
    __msan_unpoison(m, s);
#endif
    (void)m;
    (void)s;
}

template <typename T>
inline void SanitizerPoisonObject(const T* object) {
    SanitizerPoisonMemoryRegion(object, sizeof(T));
}

template <typename T>
inline void SanitizerUnpoisonObject(const T* object) {
    SanitizerUnpoisonMemoryRegion(object, sizeof(T));
}

}  // namespace priv
}  // namespace phmap


// ---------------------------------------------------------------------------
//  thread_annotations.h
// ---------------------------------------------------------------------------

#if defined(__clang__)
    #define PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(x)   __attribute__((x))
#else
    #define PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(x)   // no-op
#endif

#define PHMAP_GUARDED_BY(x) PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(guarded_by(x))
#define PHMAP_PT_GUARDED_BY(x) PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(pt_guarded_by(x))

#define PHMAP_ACQUIRED_AFTER(...) \
  PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(acquired_after(__VA_ARGS__))

#define PHMAP_ACQUIRED_BEFORE(...) \
  PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(acquired_before(__VA_ARGS__))

#define PHMAP_EXCLUSIVE_LOCKS_REQUIRED(...) \
  PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(exclusive_locks_required(__VA_ARGS__))

#define PHMAP_SHARED_LOCKS_REQUIRED(...) \
  PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(shared_locks_required(__VA_ARGS__))

#define PHMAP_LOCKS_EXCLUDED(...) \
  PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(locks_excluded(__VA_ARGS__))

#define PHMAP_LOCK_RETURNED(x) \
  PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(lock_returned(x))

#define PHMAP_LOCKABLE \
  PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(lockable)

#define PHMAP_SCOPED_LOCKABLE \
  PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(scoped_lockable)

#define PHMAP_EXCLUSIVE_LOCK_FUNCTION(...) \
  PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(exclusive_lock_function(__VA_ARGS__))

#define PHMAP_SHARED_LOCK_FUNCTION(...) \
  PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(shared_lock_function(__VA_ARGS__))

#define PHMAP_UNLOCK_FUNCTION(...) \
  PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(unlock_function(__VA_ARGS__))

#define PHMAP_EXCLUSIVE_TRYLOCK_FUNCTION(...) \
  PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(exclusive_trylock_function(__VA_ARGS__))

#define PHMAP_SHARED_TRYLOCK_FUNCTION(...) \
  PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(shared_trylock_function(__VA_ARGS__))

#define PHMAP_ASSERT_EXCLUSIVE_LOCK(...) \
  PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(assert_exclusive_lock(__VA_ARGS__))

#define PHMAP_ASSERT_SHARED_LOCK(...) \
  PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(assert_shared_lock(__VA_ARGS__))

#define PHMAP_NO_THREAD_SAFETY_ANALYSIS \
  PHMAP_THREAD_ANNOTATION_ATTRIBUTE__(no_thread_safety_analysis)

//------------------------------------------------------------------------------
// Tool-Supplied Annotations
//------------------------------------------------------------------------------

// TS_UNCHECKED should be placed around lock expressions that are not valid
// C++ syntax, but which are present for documentation purposes.  These
// annotations will be ignored by the analysis.
#define PHMAP_TS_UNCHECKED(x) ""

// TS_FIXME is used to mark lock expressions that are not valid C++ syntax.
// It is used by automated tools to mark and disable invalid expressions.
// The annotation should either be fixed, or changed to TS_UNCHECKED.
#define PHMAP_TS_FIXME(x) ""

// Like NO_THREAD_SAFETY_ANALYSIS, this turns off checking within the body of
// a particular function.  However, this attribute is used to mark functions
// that are incorrect and need to be fixed.  It is used by automated tools to
// avoid breaking the build when the analysis is updated.
// Code owners are expected to eventually fix the routine.
#define PHMAP_NO_THREAD_SAFETY_ANALYSIS_FIXME  PHMAP_NO_THREAD_SAFETY_ANALYSIS

// Similar to NO_THREAD_SAFETY_ANALYSIS_FIXME, this macro marks a GUARDED_BY
// annotation that needs to be fixed, because it is producing thread safety
// warning.  It disables the GUARDED_BY.
#define PHMAP_GUARDED_BY_FIXME(x)

// Disables warnings for a single read operation.  This can be used to avoid
// warnings when it is known that the read is not actually involved in a race,
// but the compiler cannot confirm that.
#define PHMAP_TS_UNCHECKED_READ(x) thread_safety_analysis::ts_unchecked_read(x)


namespace phmap {
namespace thread_safety_analysis {

// Takes a reference to a guarded data member, and returns an unguarded
// reference.
template <typename T>
inline const T& ts_unchecked_read(const T& v) PHMAP_NO_THREAD_SAFETY_ANALYSIS {
    return v;
}

template <typename T>
inline T& ts_unchecked_read(T& v) PHMAP_NO_THREAD_SAFETY_ANALYSIS {
    return v;
}

}  // namespace thread_safety_analysis

namespace priv {

namespace memory_internal {

// ----------------------------------------------------------------------------
// If Pair is a standard-layout type, OffsetOf<Pair>::kFirst and
// OffsetOf<Pair>::kSecond are equivalent to offsetof(Pair, first) and
// offsetof(Pair, second) respectively. Otherwise they are -1.
//
// The purpose of OffsetOf is to avoid calling offsetof() on non-standard-layout
// type, which is non-portable.
// ----------------------------------------------------------------------------
template <class Pair, class = std::true_type>
struct OffsetOf {
   static constexpr size_t kFirst  = static_cast<size_t>(-1);
   static constexpr size_t kSecond = static_cast<size_t>(-1);
};

template <class Pair>
struct OffsetOf<Pair, typename std::is_standard_layout<Pair>::type> 
{
    static constexpr size_t kFirst  = offsetof(Pair, first);
    static constexpr size_t kSecond = offsetof(Pair, second);
};

// ----------------------------------------------------------------------------
template <class K, class V>
struct IsLayoutCompatible 
{
private:
    struct Pair {
        K first;
        V second;
    };

    // Is P layout-compatible with Pair?
    template <class P>
    static constexpr bool LayoutCompatible() {
        return std::is_standard_layout<P>() && sizeof(P) == sizeof(Pair) &&
            alignof(P) == alignof(Pair) &&
            memory_internal::OffsetOf<P>::kFirst ==
            memory_internal::OffsetOf<Pair>::kFirst &&
            memory_internal::OffsetOf<P>::kSecond ==
            memory_internal::OffsetOf<Pair>::kSecond;
    }

public:
    // Whether pair<const K, V> and pair<K, V> are layout-compatible. If they are,
    // then it is safe to store them in a union and read from either.
    static constexpr bool value = std::is_standard_layout<K>() &&
        std::is_standard_layout<Pair>() &&
        memory_internal::OffsetOf<Pair>::kFirst == 0 &&
        LayoutCompatible<std::pair<K, V>>() &&
        LayoutCompatible<std::pair<const K, V>>();
};

}  // namespace memory_internal

// ----------------------------------------------------------------------------
// The internal storage type for key-value containers like flat_hash_map.
//
// It is convenient for the value_type of a flat_hash_map<K, V> to be
// pair<const K, V>; the "const K" prevents accidental modification of the key
// when dealing with the reference returned from find() and similar methods.
// However, this creates other problems; we want to be able to emplace(K, V)
// efficiently with move operations, and similarly be able to move a
// pair<K, V> in insert().
//
// The solution is this union, which aliases the const and non-const versions
// of the pair. This also allows flat_hash_map<const K, V> to work, even though
// that has the same efficiency issues with move in emplace() and insert() -
// but people do it anyway.
//
// If kMutableKeys is false, only the value member can be accessed.
//
// If kMutableKeys is true, key can be accessed through all slots while value
// and mutable_value must be accessed only via INITIALIZED slots. Slots are
// created and destroyed via mutable_value so that the key can be moved later.
//
// Accessing one of the union fields while the other is active is safe as
// long as they are layout-compatible, which is guaranteed by the definition of
// kMutableKeys. For C++11, the relevant section of the standard is
// https://timsong-cpp.github.io/cppwp/n3337/class.mem#19 (9.2.19)
// ----------------------------------------------------------------------------
template <class K, class V>
union map_slot_type 
{
    map_slot_type() {}
    ~map_slot_type() = delete;
    map_slot_type(const map_slot_type&) = delete;
    map_slot_type& operator=(const map_slot_type&) = delete;

    using value_type = std::pair<const K, V>;
    using mutable_value_type = std::pair<K, V>;

    value_type value;
    mutable_value_type mutable_value;
    K key;
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
template <class K, class V>
struct map_slot_policy 
{
    using slot_type = map_slot_type<K, V>;
    using value_type = std::pair<const K, V>;
    using mutable_value_type = std::pair<K, V>;

private:
    static void emplace(slot_type* slot) {
        // The construction of union doesn't do anything at runtime but it allows us
        // to access its members without violating aliasing rules.
        new (slot) slot_type;
    }
    // If pair<const K, V> and pair<K, V> are layout-compatible, we can accept one
    // or the other via slot_type. We are also free to access the key via
    // slot_type::key in this case.
    using kMutableKeys = memory_internal::IsLayoutCompatible<K, V>;

public:
    static value_type& element(slot_type* slot) { return slot->value; }
    static const value_type& element(const slot_type* slot) {
        return slot->value;
    }

    static const K& key(const slot_type* slot) {
        return kMutableKeys::value ? slot->key : slot->value.first;
    }

    template <class Allocator, class... Args>
    static void construct(Allocator* alloc, slot_type* slot, Args&&... args) {
        emplace(slot);
        if (kMutableKeys::value) {
            phmap::allocator_traits<Allocator>::construct(*alloc, &slot->mutable_value,
                                                         std::forward<Args>(args)...);
        } else {
            phmap::allocator_traits<Allocator>::construct(*alloc, &slot->value,
                                                         std::forward<Args>(args)...);
        }
    }

    // Construct this slot by moving from another slot.
    template <class Allocator>
    static void construct(Allocator* alloc, slot_type* slot, slot_type* other) {
        emplace(slot);
        if (kMutableKeys::value) {
            phmap::allocator_traits<Allocator>::construct(
                *alloc, &slot->mutable_value, std::move(other->mutable_value));
        } else {
            phmap::allocator_traits<Allocator>::construct(*alloc, &slot->value,
                                                         std::move(other->value));
        }
    }

    template <class Allocator>
    static void destroy(Allocator* alloc, slot_type* slot) {
        if (kMutableKeys::value) {
            phmap::allocator_traits<Allocator>::destroy(*alloc, &slot->mutable_value);
        } else {
            phmap::allocator_traits<Allocator>::destroy(*alloc, &slot->value);
        }
    }

    template <class Allocator>
    static void transfer(Allocator* alloc, slot_type* new_slot,
                         slot_type* old_slot) {
        emplace(new_slot);
        if (kMutableKeys::value) {
            phmap::allocator_traits<Allocator>::construct(
                *alloc, &new_slot->mutable_value, std::move(old_slot->mutable_value));
        } else {
            phmap::allocator_traits<Allocator>::construct(*alloc, &new_slot->value,
                                                         std::move(old_slot->value));
        }
        destroy(alloc, old_slot);
    }

    template <class Allocator>
    static void swap(Allocator* alloc, slot_type* a, slot_type* b) {
        if (kMutableKeys::value) {
            using std::swap;
            swap(a->mutable_value, b->mutable_value);
        } else {
            value_type tmp = std::move(a->value);
            phmap::allocator_traits<Allocator>::destroy(*alloc, &a->value);
            phmap::allocator_traits<Allocator>::construct(*alloc, &a->value,
                                                         std::move(b->value));
            phmap::allocator_traits<Allocator>::destroy(*alloc, &b->value);
            phmap::allocator_traits<Allocator>::construct(*alloc, &b->value,
                                                         std::move(tmp));
        }
    }

    template <class Allocator>
    static void move(Allocator* alloc, slot_type* src, slot_type* dest) {
        if (kMutableKeys::value) {
            dest->mutable_value = std::move(src->mutable_value);
        } else {
            phmap::allocator_traits<Allocator>::destroy(*alloc, &dest->value);
            phmap::allocator_traits<Allocator>::construct(*alloc, &dest->value,
                                                          std::move(src->value));
        }
    }

    template <class Allocator>
    static void move(Allocator* alloc, slot_type* first, slot_type* last,
                     slot_type* result) {
        for (slot_type *src = first, *dest = result; src != last; ++src, ++dest)
            move(alloc, src, dest);
    }
};

}  // namespace priv
}  // phmap


namespace phmap {

#ifdef BOOST_THREAD_LOCK_OPTIONS_HPP
    using defer_lock_t  = boost::defer_lock_t;
    using try_to_lock_t = boost::try_to_lock_t;
    using adopt_lock_t  = boost::adopt_lock_t;
#else
    struct adopt_lock_t  { explicit adopt_lock_t() = default; };
    struct defer_lock_t  { explicit defer_lock_t() = default; };
    struct try_to_lock_t { explicit try_to_lock_t() = default; };
#endif

// -----------------------------------------------------------------------------
// NullMutex
// -----------------------------------------------------------------------------
// A class that implements the Mutex interface, but does nothing. This is to be 
// used as a default template parameters for classes who provide optional 
// internal locking (like phmap::parallel_flat_hash_map).
// -----------------------------------------------------------------------------
class NullMutex {
public:
    NullMutex() {}
    ~NullMutex() {}
    void lock() {}
    void unlock() {}
    bool try_lock() { return true; }
    void lock_shared() {}
    void unlock_shared() {}
    bool try_lock_shared() { return true; }
};

// ------------------------ lockable object used internally -------------------------
template <class MutexType>
class LockableBaseImpl 
{
public:
    // ----------------------------------------------------
    struct DoNothing
    {
        using mutex_type = MutexType;  
        DoNothing() noexcept {}
        explicit DoNothing(mutex_type& ) noexcept {}
        explicit DoNothing(mutex_type& , mutex_type&) noexcept {}
        DoNothing(mutex_type&, phmap::adopt_lock_t) noexcept {}
        DoNothing(mutex_type&, phmap::defer_lock_t) noexcept {}
        DoNothing(mutex_type&, phmap::try_to_lock_t) {}
        template<class T> explicit DoNothing(T&&) {}
        DoNothing& operator=(const DoNothing&) { return *this; }
        DoNothing& operator=(DoNothing&&) noexcept { return *this; }
        void swap(DoNothing &)  noexcept {}
        bool owns_lock() const noexcept { return true; }
        void lock() {}
        void unlock() {}
        void lock_shared() {}
        void unlock_shared() {}
        bool switch_to_unique() { return false; }
    };

    // ----------------------------------------------------
    class WriteLock
    {
    public:
        using mutex_type = MutexType;

        WriteLock() :  m_(nullptr), locked_(false)  {}

        explicit WriteLock(mutex_type &m) : m_(&m) { 
            m_->lock(); 
            locked_ = true; 
        }

        WriteLock(mutex_type& m, adopt_lock_t) noexcept :
            m_(&m), locked_(true) 
        {}

        WriteLock(mutex_type& m, defer_lock_t) noexcept :
            m_(&m), locked_(false) 
        {}

        WriteLock(mutex_type& m, try_to_lock_t)  :
            m_(&m), locked_(false) { 
            m_->try_lock(); 
        }

        WriteLock(WriteLock &&o) noexcept :
            m_(std::move(o.m_)), locked_(std::move(o.locked_)) {
            o.locked_ = false;
            o.m_      = nullptr;
        }

        WriteLock& operator=(WriteLock&& other) noexcept {
            WriteLock temp(std::move(other));
            swap(temp);
            return *this;
        }

        ~WriteLock() {
            if (locked_) 
                m_->unlock(); 
        }

        void lock() { 
            if (!locked_) { 
                m_->lock(); 
                locked_ = true; 
            }
        }

        void unlock() { 
            if (locked_) {
                m_->unlock(); 
                locked_ = false;
            }
        } 

        bool try_lock() { 
            if (locked_)
                return true;
            locked_ = m_->try_lock(); 
            return locked_;
        }
        
        bool owns_lock() const noexcept { return locked_; }

        void swap(WriteLock &o) noexcept { 
            std::swap(m_, o.m_);
            std::swap(locked_, o.locked_);
        }

        mutex_type *mutex() const noexcept { return m_; }
        
        bool switch_to_unique() { return false; }

    private:
        mutex_type *m_;
        bool        locked_;
    };

    // ----------------------------------------------------
    class ReadLock
    {
    public:
        using mutex_type = MutexType;

        ReadLock() :  m_(nullptr), locked_(false)  {}

        explicit ReadLock(mutex_type &m) : m_(&m) { 
            m_->lock_shared(); 
            locked_ = true; 
        }

        ReadLock(mutex_type& m, adopt_lock_t) noexcept :
            m_(&m), locked_(true) 
        {}

        ReadLock(mutex_type& m, defer_lock_t) noexcept :
            m_(&m), locked_(false) 
        {}

        ReadLock(mutex_type& m, try_to_lock_t)  :
            m_(&m), locked_(false) { 
            m_->try_lock_shared(); 
        }

        ReadLock(ReadLock &&o) noexcept :
            m_(std::move(o.m_)), locked_(std::move(o.locked_)) {
            o.locked_ = false;
            o.m_      = nullptr;
        }

        ReadLock& operator=(ReadLock&& other) noexcept {
            ReadLock temp(std::move(other));
            swap(temp);
            return *this;
        }

        ~ReadLock() {
            if (locked_) 
                m_->unlock_shared(); 
        }

        void lock() { 
            if (!locked_) { 
                m_->lock_shared(); 
                locked_ = true; 
            }
        }

        void unlock() { 
            if (locked_) {
                m_->unlock_shared(); 
                locked_ = false;
            }
        } 

        bool try_lock() { 
            if (locked_)
                return true;
            locked_ = m_->try_lock_shared(); 
            return locked_;
        }
        
        bool owns_lock() const noexcept { return locked_; }

        void swap(ReadLock &o) noexcept { 
            std::swap(m_, o.m_);
            std::swap(locked_, o.locked_);
        }

        mutex_type *mutex() const noexcept { return m_; }

        bool switch_to_unique() { return false; }

    private:
        mutex_type *m_;
        bool        locked_;
    };

    // ----------------------------------------------------
    class ReadWriteLock
    {
    public:
        using mutex_type = MutexType;

        ReadWriteLock() :  m_(nullptr), locked_(false), locked_shared_(false)  {}

        explicit ReadWriteLock(mutex_type &m) : m_(&m), locked_(false), locked_shared_(true)  {
            m_->lock_shared(); 
        }

        ReadWriteLock(mutex_type& m, defer_lock_t) noexcept :
            m_(&m), locked_(false), locked_shared_(false)
        {}

        ReadWriteLock(ReadWriteLock &&o) noexcept :
            m_(std::move(o.m_)), locked_(o.locked_), locked_shared_(o.locked_shared_) {
            o.locked_        = false;
            o.locked_shared_ = false;
            o.m_             = nullptr;
        }

        ReadWriteLock& operator=(ReadWriteLock&& other) noexcept {
            ReadWriteLock temp(std::move(other));
            swap(temp);
            return *this;
        }

        ~ReadWriteLock() {
            if (locked_shared_) 
                m_->unlock_shared();
            else if (locked_) 
                m_->unlock();
        }

        void lock_shared() {
            assert(!locked_);
            if (!locked_shared_) { 
                m_->lock_shared(); 
                locked_shared_ = true; 
            }
        }

        void unlock_shared() { 
            if (locked_shared_) {
                m_->unlock_shared(); 
                locked_shared_ = false;
            }
        } 

        void lock() {
            assert(!locked_shared_);
            if (!locked_) { 
                m_->lock(); 
                locked_ = true; 
            }
        }

        void unlock() { 
            if (locked_) {
                m_->unlock(); 
                locked_ = false;
            }
        } 

        bool owns_lock() const noexcept { return locked_; }
        bool owns_shared_lock() const noexcept { return locked_shared_; }

        void swap(ReadWriteLock &o) noexcept { 
            std::swap(m_, o.m_);
            std::swap(locked_, o.locked_);
            std::swap(locked_shared_, o.locked_shared_);
        }

        mutex_type *mutex() const noexcept { return m_; }

        bool switch_to_unique() {
            assert(locked_shared_);
            unlock_shared();
            lock();
            return true;
        }

    private:
        mutex_type *m_;
        bool        locked_;
        bool        locked_shared_;
    };

    // ----------------------------------------------------
    class WriteLocks
    {
    public:
        using mutex_type = MutexType;  

        explicit WriteLocks(mutex_type& m1, mutex_type& m2) : 
            _m1(m1), _m2(m2)
        { 
            std::lock(m1, m2); 
        }

        WriteLocks(adopt_lock_t, mutex_type& m1, mutex_type& m2) :
            _m1(m1), _m2(m2)
        { // adopt means we already own the mutexes
        }

        ~WriteLocks()
        {
            _m1.unlock();
            _m2.unlock();
        }

        WriteLocks(WriteLocks const&) = delete;
        WriteLocks& operator=(WriteLocks const&) = delete;
    private:
        mutex_type& _m1;
        mutex_type& _m2;
    };

    // ----------------------------------------------------
    class ReadLocks
    {
    public:
        using mutex_type = MutexType;  

        explicit ReadLocks(mutex_type& m1, mutex_type& m2) : 
            _m1(m1), _m2(m2)
        { 
            _m1.lock_shared(); 
            _m2.lock_shared(); 
        }

        ReadLocks(adopt_lock_t, mutex_type& m1, mutex_type& m2) :
            _m1(m1), _m2(m2)
        { // adopt means we already own the mutexes
        }

        ~ReadLocks()
        {
            _m1.unlock_shared();
            _m2.unlock_shared();
        }

        ReadLocks(ReadLocks const&) = delete;
        ReadLocks& operator=(ReadLocks const&) = delete;
    private:
        mutex_type& _m1;
        mutex_type& _m2;
    };
};

// ------------------------ holds a mutex ------------------------------------
// Default implementation for Lockable, should work fine for std::mutex 
// -----------------------------------
// use as:
//    using Lockable = phmap::LockableImpl<mutex_type>;
//    Lockable m;
//  
//    Lockable::ReadWriteLock read_lock(m); // take a lock (read if supported, otherwise write)
//    ... do something
// 
//    m.switch_to_unique(); // returns true if we had a read lock and switched to write
//    // now locked for write
//
// ---------------------------------------------------------------------------
//         Generic mutex support (always write locks)
// --------------------------------------------------------------------------
template <class Mtx_>
class LockableImpl : public Mtx_
{
public:
    using mutex_type      = Mtx_;
    using Base            = LockableBaseImpl<Mtx_>;
    using SharedLock      = typename Base::WriteLock;
    using UniqueLock      = typename Base::WriteLock;
    using ReadWriteLock   = typename Base::WriteLock;
    using SharedLocks     = typename Base::WriteLocks;
    using UniqueLocks     = typename Base::WriteLocks;
};

// ---------------------------------------------------------------------------
//          Null mutex (no-op) - when we don't want internal synchronization
// ---------------------------------------------------------------------------
template <>
class  LockableImpl<phmap::NullMutex>: public phmap::NullMutex
{
public:
    using mutex_type      = phmap::NullMutex;
    using Base            = LockableBaseImpl<phmap::NullMutex>;
    using SharedLock      = typename Base::DoNothing; 
    using ReadWriteLock   = typename Base::DoNothing;
    using UniqueLock      = typename Base::DoNothing; 
    using SharedLocks     = typename Base::DoNothing;
    using UniqueLocks     = typename Base::DoNothing;
};

// --------------------------------------------------------------------------
//         Abseil Mutex support (read and write lock support)
//         use: `phmap::AbslMutex` instead of `std::mutex`
// --------------------------------------------------------------------------
#ifdef ABSL_SYNCHRONIZATION_MUTEX_H_
    
    struct AbslMutex : protected absl::Mutex
    {
        void lock()            ABSL_EXCLUSIVE_LOCK_FUNCTION()        { this->Lock(); }
        void unlock()          ABSL_UNLOCK_FUNCTION()                { this->Unlock(); }
        void try_lock()        ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true) { this->TryLock(); }
        void lock_shared()     ABSL_SHARED_LOCK_FUNCTION()           { this->ReaderLock(); }
        void unlock_shared()   ABSL_UNLOCK_FUNCTION()                { this->ReaderUnlock(); }
        void try_lock_shared() ABSL_SHARED_TRYLOCK_FUNCTION(true)    { this->ReaderTryLock(); }
    };
    
    template <>
    class  LockableImpl<absl::Mutex> : public AbslMutex
    {
    public:
        using mutex_type      = phmap::AbslMutex;
        using Base            = LockableBaseImpl<phmap::AbslMutex>;
        using SharedLock      = typename Base::ReadLock;
        using ReadWriteLock   = typename Base::ReadWriteLock;
        using UniqueLock      = typename Base::WriteLock;
        using SharedLocks     = typename Base::ReadLocks;
        using UniqueLocks     = typename Base::WriteLocks;
    };

#endif

// --------------------------------------------------------------------------
//         Microsoft SRWLOCK support (read and write lock support)
//         use: `phmap::srwlock` instead of `std::mutex`
// --------------------------------------------------------------------------
#if defined(_MSC_VER) && defined(SRWLOCK_INIT)

    class srwlock {
        SRWLOCK _lock;
    public:
        srwlock()              { InitializeSRWLock(&_lock); }
        void lock()            { AcquireSRWLockExclusive(&_lock); }
        void unlock()          { ReleaseSRWLockExclusive(&_lock); }
        bool try_lock()        { return !!TryAcquireSRWLockExclusive(&_lock); }
        void lock_shared()     { AcquireSRWLockShared(&_lock); }
        void unlock_shared()   { ReleaseSRWLockShared(&_lock); }
        bool try_lock_shared() { return !!TryAcquireSRWLockShared(&_lock); }
    };


    template<>
    class LockableImpl<srwlock> : public srwlock
    {
    public:
        using mutex_type    = srwlock;
        using Base          = LockableBaseImpl<srwlock>;
        using SharedLock    = typename Base::ReadLock;
        using ReadWriteLock = typename Base::ReadWriteLock;
        using UniqueLock    = typename Base::WriteLock;
        using SharedLocks   = typename Base::ReadLocks;
        using UniqueLocks   = typename Base::WriteLocks;
    };

#endif

// --------------------------------------------------------------------------
//         Boost shared_mutex support (read and write lock support)
// --------------------------------------------------------------------------
#ifdef BOOST_THREAD_SHARED_MUTEX_HPP

    // ---------------------------------------------------------------------------
    template <>
    class  LockableImpl<boost::shared_mutex> : public boost::shared_mutex
    {
    public:
        using mutex_type      = boost::shared_mutex;
        using Base            = LockableBaseImpl<boost::shared_mutex>;
        using SharedLock      = boost::shared_lock<mutex_type>;
        using ReadWriteLock   = typename Base::ReadWriteLock;
        using UniqueLock      = boost::unique_lock<mutex_type>;
        using SharedLocks     = typename Base::ReadLocks;
        using UniqueLocks     = typename Base::WriteLocks;
    };

#endif // BOOST_THREAD_SHARED_MUTEX_HPP

// --------------------------------------------------------------------------
//         std::shared_mutex support (read and write lock support)
// --------------------------------------------------------------------------
#ifdef PHMAP_HAVE_SHARED_MUTEX

    // ---------------------------------------------------------------------------
    template <>
    class  LockableImpl<std::shared_mutex> : public std::shared_mutex
    {
    public:
        using mutex_type      = std::shared_mutex;
        using Base            = LockableBaseImpl<std::shared_mutex>;
        using SharedLock      = std::shared_lock<mutex_type>;
        using ReadWriteLock   = typename Base::ReadWriteLock;
        using UniqueLock      = std::unique_lock<mutex_type>;
        using SharedLocks     = typename Base::ReadLocks;
        using UniqueLocks     = typename Base::WriteLocks;
    };
#endif // PHMAP_HAVE_SHARED_MUTEX


}  // phmap

#ifdef _MSC_VER
     #pragma warning(pop)  
#endif


#endif // phmap_base_h_guard_
