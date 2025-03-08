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

#ifndef PHMAP_BTREE_BTREE_CONTAINER_H_
#define PHMAP_BTREE_BTREE_CONTAINER_H_

#ifdef _MSC_VER
    #pragma warning(push)  

    #pragma warning(disable : 4127) // conditional expression is constant
    #pragma warning(disable : 4324) // structure was padded due to alignment specifier
    #pragma warning(disable : 4355) // 'this': used in base member initializer list
    #pragma warning(disable : 4365) // conversion from 'int' to 'const unsigned __int64', signed/unsigned mismatch
    #pragma warning(disable : 4514) // unreferenced inline function has been removed
    #pragma warning(disable : 4623) // default constructor was implicitly defined as deleted
    #pragma warning(disable : 4625) // copy constructor was implicitly defined as deleted
    #pragma warning(disable : 4626) // assignment operator was implicitly defined as deleted
    #pragma warning(disable : 4710) // function not inlined
    #pragma warning(disable : 4711) //  selected for automatic inline expansion
    #pragma warning(disable : 4820) // '6' bytes padding added after data member
    #pragma warning(disable : 4868) // compiler may not enforce left-to-right evaluation order in braced initializer list
    #pragma warning(disable : 5026) // move constructor was implicitly defined as deleted
    #pragma warning(disable : 5027) // move assignment operator was implicitly defined as deleted
    #pragma warning(disable : 5045) // Compiler will insert Spectre mitigation for memory load if /Qspectre switch specified
#endif


#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>
#include <type_traits>

#include "phmap_fwd_decl.h"
#include "phmap_base.h"

#if PHMAP_HAVE_STD_STRING_VIEW
    #include <string_view>
#endif

// MSVC constructibility traits do not detect destructor properties and so our
// implementations should not use them as a source-of-truth.
#if defined(_MSC_VER) && !defined(__clang__) && !defined(__GNUC__)
    #define PHMAP_META_INTERNAL_STD_CONSTRUCTION_TRAITS_DONT_CHECK_DESTRUCTION 1
#endif

namespace phmap {

    namespace type_traits_internal {

        // Silence MSVC warnings about the destructor being defined as deleted.
#if defined(_MSC_VER) && !defined(__GNUC__)
    #pragma warning(push)
    #pragma warning(disable : 4624)
#endif  // defined(_MSC_VER) && !defined(__GNUC__)

        template <class T>
        union SingleMemberUnion {
            T t;
        };

        // Restore the state of the destructor warning that was silenced above.
#if defined(_MSC_VER) && !defined(__GNUC__)
    #pragma warning(pop)
#endif  // defined(_MSC_VER) && !defined(__GNUC__)

        template <class T>
        struct IsTriviallyMoveConstructibleObject
            : std::integral_constant<
            bool, std::is_move_constructible<
                      type_traits_internal::SingleMemberUnion<T>>::value &&
            std::is_trivially_destructible<T>::value> {};

        template <class T>
        struct IsTriviallyCopyConstructibleObject
            : std::integral_constant<
            bool, std::is_copy_constructible<
                      type_traits_internal::SingleMemberUnion<T>>::value &&
            std::is_trivially_destructible<T>::value> {};
#if 0
        template <class T>
        struct IsTriviallyMoveAssignableReference : std::false_type {};

        template <class T>
        struct IsTriviallyMoveAssignableReference<T&>
            : std::is_trivially_move_assignable<T>::type {};

        template <class T>
        struct IsTriviallyMoveAssignableReference<T&&>
            : std::is_trivially_move_assignable<T>::type {};
#endif
    }  // namespace type_traits_internal


    template <typename... Ts>
    using void_t = typename type_traits_internal::VoidTImpl<Ts...>::type;


    template <typename T>
    struct is_function
        : std::integral_constant<
        bool, !(std::is_reference<T>::value ||
                std::is_const<typename std::add_const<T>::type>::value)> {};


    namespace type_traits_internal {

        template <typename T>
        class is_trivially_copyable_impl {
            using ExtentsRemoved = typename std::remove_all_extents<T>::type;
            static constexpr bool kIsCopyOrMoveConstructible =
                std::is_copy_constructible<ExtentsRemoved>::value ||
                std::is_move_constructible<ExtentsRemoved>::value;
            static constexpr bool kIsCopyOrMoveAssignable =
                phmap::is_copy_assignable<ExtentsRemoved>::value ||
                phmap::is_move_assignable<ExtentsRemoved>::value;

        public:
            static constexpr bool kValue =
                (phmap::is_trivially_copyable<ExtentsRemoved>::value || !kIsCopyOrMoveConstructible) &&
                (phmap::is_trivially_copy_assignable<ExtentsRemoved>::value || !kIsCopyOrMoveAssignable) &&
                (kIsCopyOrMoveConstructible || kIsCopyOrMoveAssignable) &&
                std::is_trivially_destructible<ExtentsRemoved>::value &&
                // We need to check for this explicitly because otherwise we'll say
                // references are trivial copyable when compiled by MSVC.
                !std::is_reference<ExtentsRemoved>::value;
        };

        template <typename T>
        struct is_trivially_copyable
            : std::integral_constant<
            bool, type_traits_internal::is_trivially_copyable_impl<T>::kValue> {};
    }  // namespace type_traits_internal

    namespace swap_internal {

        // Necessary for the traits.
        using std::swap;

        // This declaration prevents global `swap` and `phmap::swap` overloads from being
        // considered unless ADL picks them up.
        void swap();

        template <class T>
        using IsSwappableImpl = decltype(swap(std::declval<T&>(), std::declval<T&>()));

        // NOTE: This dance with the default template parameter is for MSVC.
        template <class T,
                  class IsNoexcept = std::integral_constant<
                      bool, noexcept(swap(std::declval<T&>(), std::declval<T&>()))>>
            using IsNothrowSwappableImpl = typename std::enable_if<IsNoexcept::value>::type;

        template <class T>
        struct IsSwappable
            : phmap::type_traits_internal::is_detected<IsSwappableImpl, T> {};

        template <class T>
        struct IsNothrowSwappable
            : phmap::type_traits_internal::is_detected<IsNothrowSwappableImpl, T> {};

        template <class T, phmap::enable_if_t<IsSwappable<T>::value, int> = 0>
        void Swap(T& lhs, T& rhs) noexcept(IsNothrowSwappable<T>::value) {
            swap(lhs, rhs);
        }

       using StdSwapIsUnconstrained = IsSwappable<void()>;

    }  // namespace swap_internal

    namespace type_traits_internal {

        // Make the swap-related traits/function accessible from this namespace.
        using swap_internal::IsNothrowSwappable;
        using swap_internal::IsSwappable;
        using swap_internal::Swap;
        using swap_internal::StdSwapIsUnconstrained;

    }  // namespace type_traits_internal

    namespace compare_internal {

        using value_type = int8_t;

        template <typename T>
        struct Fail {
            static_assert(sizeof(T) < 0, "Only literal `0` is allowed.");
        };

        template <typename NullPtrT = std::nullptr_t>
        struct OnlyLiteralZero {
            constexpr OnlyLiteralZero(NullPtrT) noexcept {}  // NOLINT

            template <
                typename T,
                typename = typename std::enable_if<
                    std::is_same<T, std::nullptr_t>::value ||
                    (std::is_integral<T>::value && !std::is_same<T, int>::value)>::type,
                typename = typename Fail<T>::type>
                OnlyLiteralZero(T);  // NOLINT
        };

        enum class eq : value_type {
            equal = 0,
                equivalent = equal,
                nonequal = 1,
                nonequivalent = nonequal,
                };

        enum class ord : value_type { less = -1, greater = 1 };

        enum class ncmp : value_type { unordered = -127 };

#if defined(__cpp_inline_variables) && !defined(_MSC_VER)

#define PHMAP_COMPARE_INLINE_BASECLASS_DECL(name)

#define PHMAP_COMPARE_INLINE_SUBCLASS_DECL(type, name)  \
        static const type name;

#define PHMAP_COMPARE_INLINE_INIT(type, name, init) \
        inline constexpr type type::name(init)

#else  // __cpp_inline_variables

#define PHMAP_COMPARE_INLINE_BASECLASS_DECL(name)   \
        static const T name;

#define PHMAP_COMPARE_INLINE_SUBCLASS_DECL(type, name)

#define PHMAP_COMPARE_INLINE_INIT(type, name, init)             \
        template <typename T>                                   \
        const T compare_internal::type##_base<T>::name(init)

#endif  // __cpp_inline_variables

        // These template base classes allow for defining the values of the constants
        // in the header file (for performance) without using inline variables (which
        // aren't available in C++11).
        template <typename T>
        struct weak_equality_base {
            PHMAP_COMPARE_INLINE_BASECLASS_DECL(equivalent)
            PHMAP_COMPARE_INLINE_BASECLASS_DECL(nonequivalent)
        };

        template <typename T>
        struct strong_equality_base {
            PHMAP_COMPARE_INLINE_BASECLASS_DECL(equal)
            PHMAP_COMPARE_INLINE_BASECLASS_DECL(nonequal)
            PHMAP_COMPARE_INLINE_BASECLASS_DECL(equivalent)
            PHMAP_COMPARE_INLINE_BASECLASS_DECL(nonequivalent)
        };

        template <typename T>
        struct partial_ordering_base {
            PHMAP_COMPARE_INLINE_BASECLASS_DECL(less)
            PHMAP_COMPARE_INLINE_BASECLASS_DECL(equivalent)
            PHMAP_COMPARE_INLINE_BASECLASS_DECL(greater)
            PHMAP_COMPARE_INLINE_BASECLASS_DECL(unordered)
        };

        template <typename T>
        struct weak_ordering_base {
            PHMAP_COMPARE_INLINE_BASECLASS_DECL(less)
            PHMAP_COMPARE_INLINE_BASECLASS_DECL(equivalent)
            PHMAP_COMPARE_INLINE_BASECLASS_DECL(greater)
        };

        template <typename T>
        struct strong_ordering_base {
            PHMAP_COMPARE_INLINE_BASECLASS_DECL(less)
            PHMAP_COMPARE_INLINE_BASECLASS_DECL(equal)
            PHMAP_COMPARE_INLINE_BASECLASS_DECL(equivalent)
            PHMAP_COMPARE_INLINE_BASECLASS_DECL(greater)
        };

    }  // namespace compare_internal

    class weak_equality
        : public compare_internal::weak_equality_base<weak_equality> {
        explicit constexpr weak_equality(compare_internal::eq v) noexcept
            : value_(static_cast<compare_internal::value_type>(v)) {}
        friend struct compare_internal::weak_equality_base<weak_equality>;

    public:
        PHMAP_COMPARE_INLINE_SUBCLASS_DECL(weak_equality, equivalent)
        PHMAP_COMPARE_INLINE_SUBCLASS_DECL(weak_equality, nonequivalent)

        // Comparisons
        friend constexpr bool operator==(
            weak_equality v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.value_ == 0;
        }
        friend constexpr bool operator!=(
            weak_equality v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.value_ != 0;
        }
        friend constexpr bool operator==(compare_internal::OnlyLiteralZero<>,
                                         weak_equality v) noexcept {
            return 0 == v.value_;
        }
        friend constexpr bool operator!=(compare_internal::OnlyLiteralZero<>,
                                         weak_equality v) noexcept {
            return 0 != v.value_;
        }

    private:
        compare_internal::value_type value_;
    };
    PHMAP_COMPARE_INLINE_INIT(weak_equality, equivalent,
                              compare_internal::eq::equivalent);
    PHMAP_COMPARE_INLINE_INIT(weak_equality, nonequivalent,
                              compare_internal::eq::nonequivalent);

    class strong_equality
        : public compare_internal::strong_equality_base<strong_equality> {
        explicit constexpr strong_equality(compare_internal::eq v) noexcept
            : value_(static_cast<compare_internal::value_type>(v)) {}
        friend struct compare_internal::strong_equality_base<strong_equality>;

    public:
        PHMAP_COMPARE_INLINE_SUBCLASS_DECL(strong_equality, equal)
        PHMAP_COMPARE_INLINE_SUBCLASS_DECL(strong_equality, nonequal)
        PHMAP_COMPARE_INLINE_SUBCLASS_DECL(strong_equality, equivalent)
        PHMAP_COMPARE_INLINE_SUBCLASS_DECL(strong_equality, nonequivalent)

        // Conversion
        constexpr operator weak_equality() const noexcept {  // NOLINT
            return value_ == 0 ? weak_equality::equivalent
                : weak_equality::nonequivalent;
        }
        // Comparisons
        friend constexpr bool operator==(
            strong_equality v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.value_ == 0;
        }
        friend constexpr bool operator!=(
            strong_equality v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.value_ != 0;
        }
        friend constexpr bool operator==(compare_internal::OnlyLiteralZero<>,
                                         strong_equality v) noexcept {
            return 0 == v.value_;
        }
        friend constexpr bool operator!=(compare_internal::OnlyLiteralZero<>,
                                         strong_equality v) noexcept {
            return 0 != v.value_;
        }

    private:
        compare_internal::value_type value_;
    };

    PHMAP_COMPARE_INLINE_INIT(strong_equality, equal, compare_internal::eq::equal);
    PHMAP_COMPARE_INLINE_INIT(strong_equality, nonequal,
                              compare_internal::eq::nonequal);
    PHMAP_COMPARE_INLINE_INIT(strong_equality, equivalent,
                              compare_internal::eq::equivalent);
    PHMAP_COMPARE_INLINE_INIT(strong_equality, nonequivalent,
                              compare_internal::eq::nonequivalent);

    class partial_ordering
        : public compare_internal::partial_ordering_base<partial_ordering> {
        explicit constexpr partial_ordering(compare_internal::eq v) noexcept
            : value_(static_cast<compare_internal::value_type>(v)) {}
        explicit constexpr partial_ordering(compare_internal::ord v) noexcept
            : value_(static_cast<compare_internal::value_type>(v)) {}
        explicit constexpr partial_ordering(compare_internal::ncmp v) noexcept
            : value_(static_cast<compare_internal::value_type>(v)) {}
        friend struct compare_internal::partial_ordering_base<partial_ordering>;

        constexpr bool is_ordered() const noexcept {
            return value_ !=
                compare_internal::value_type(compare_internal::ncmp::unordered);
        }

    public:
        PHMAP_COMPARE_INLINE_SUBCLASS_DECL(partial_ordering, less)
        PHMAP_COMPARE_INLINE_SUBCLASS_DECL(partial_ordering, equivalent)
        PHMAP_COMPARE_INLINE_SUBCLASS_DECL(partial_ordering, greater)
        PHMAP_COMPARE_INLINE_SUBCLASS_DECL(partial_ordering, unordered)

        // Conversion
        constexpr operator weak_equality() const noexcept {  // NOLINT
            return value_ == 0 ? weak_equality::equivalent
                : weak_equality::nonequivalent;
        }
        // Comparisons
        friend constexpr bool operator==(
            partial_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.is_ordered() && v.value_ == 0;
        }
        friend constexpr bool operator!=(
            partial_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return !v.is_ordered() || v.value_ != 0;
        }
        friend constexpr bool operator<(
            partial_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.is_ordered() && v.value_ < 0;
        }
        friend constexpr bool operator<=(
            partial_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.is_ordered() && v.value_ <= 0;
        }
        friend constexpr bool operator>(
            partial_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.is_ordered() && v.value_ > 0;
        }
        friend constexpr bool operator>=(
            partial_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.is_ordered() && v.value_ >= 0;
        }
        friend constexpr bool operator==(compare_internal::OnlyLiteralZero<>,
                                         partial_ordering v) noexcept {
            return v.is_ordered() && 0 == v.value_;
        }
        friend constexpr bool operator!=(compare_internal::OnlyLiteralZero<>,
                                         partial_ordering v) noexcept {
            return !v.is_ordered() || 0 != v.value_;
        }
        friend constexpr bool operator<(compare_internal::OnlyLiteralZero<>,
                                        partial_ordering v) noexcept {
            return v.is_ordered() && 0 < v.value_;
        }
        friend constexpr bool operator<=(compare_internal::OnlyLiteralZero<>,
                                         partial_ordering v) noexcept {
            return v.is_ordered() && 0 <= v.value_;
        }
        friend constexpr bool operator>(compare_internal::OnlyLiteralZero<>,
                                        partial_ordering v) noexcept {
            return v.is_ordered() && 0 > v.value_;
        }
        friend constexpr bool operator>=(compare_internal::OnlyLiteralZero<>,
                                         partial_ordering v) noexcept {
            return v.is_ordered() && 0 >= v.value_;
        }

    private:
        compare_internal::value_type value_;
    };

    PHMAP_COMPARE_INLINE_INIT(partial_ordering, less, compare_internal::ord::less);
    PHMAP_COMPARE_INLINE_INIT(partial_ordering, equivalent,
                              compare_internal::eq::equivalent);
    PHMAP_COMPARE_INLINE_INIT(partial_ordering, greater,
                              compare_internal::ord::greater);
    PHMAP_COMPARE_INLINE_INIT(partial_ordering, unordered,
                              compare_internal::ncmp::unordered);

    class weak_ordering
        : public compare_internal::weak_ordering_base<weak_ordering> {
        explicit constexpr weak_ordering(compare_internal::eq v) noexcept
            : value_(static_cast<compare_internal::value_type>(v)) {}
        explicit constexpr weak_ordering(compare_internal::ord v) noexcept
            : value_(static_cast<compare_internal::value_type>(v)) {}
        friend struct compare_internal::weak_ordering_base<weak_ordering>;

    public:
        PHMAP_COMPARE_INLINE_SUBCLASS_DECL(weak_ordering, less)
        PHMAP_COMPARE_INLINE_SUBCLASS_DECL(weak_ordering, equivalent)
        PHMAP_COMPARE_INLINE_SUBCLASS_DECL(weak_ordering, greater)

        // Conversions
        constexpr operator weak_equality() const noexcept {  // NOLINT
            return value_ == 0 ? weak_equality::equivalent
                : weak_equality::nonequivalent;
        }
        constexpr operator partial_ordering() const noexcept {  // NOLINT
            return value_ == 0 ? partial_ordering::equivalent
                : (value_ < 0 ? partial_ordering::less
                   : partial_ordering::greater);
        }
        // Comparisons
        friend constexpr bool operator==(
            weak_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.value_ == 0;
        }
        friend constexpr bool operator!=(
            weak_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.value_ != 0;
        }
        friend constexpr bool operator<(
            weak_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.value_ < 0;
        }
        friend constexpr bool operator<=(
            weak_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.value_ <= 0;
        }
        friend constexpr bool operator>(
            weak_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.value_ > 0;
        }
        friend constexpr bool operator>=(
            weak_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.value_ >= 0;
        }
        friend constexpr bool operator==(compare_internal::OnlyLiteralZero<>,
                                         weak_ordering v) noexcept {
            return 0 == v.value_;
        }
        friend constexpr bool operator!=(compare_internal::OnlyLiteralZero<>,
                                         weak_ordering v) noexcept {
            return 0 != v.value_;
        }
        friend constexpr bool operator<(compare_internal::OnlyLiteralZero<>,
                                        weak_ordering v) noexcept {
            return 0 < v.value_;
        }
        friend constexpr bool operator<=(compare_internal::OnlyLiteralZero<>,
                                         weak_ordering v) noexcept {
            return 0 <= v.value_;
        }
        friend constexpr bool operator>(compare_internal::OnlyLiteralZero<>,
                                        weak_ordering v) noexcept {
            return 0 > v.value_;
        }
        friend constexpr bool operator>=(compare_internal::OnlyLiteralZero<>,
                                         weak_ordering v) noexcept {
            return 0 >= v.value_;
        }

    private:
        compare_internal::value_type value_;
    };

    PHMAP_COMPARE_INLINE_INIT(weak_ordering, less, compare_internal::ord::less);
    PHMAP_COMPARE_INLINE_INIT(weak_ordering, equivalent,
                              compare_internal::eq::equivalent);
    PHMAP_COMPARE_INLINE_INIT(weak_ordering, greater,
                              compare_internal::ord::greater);

    class strong_ordering
        : public compare_internal::strong_ordering_base<strong_ordering> {
        explicit constexpr strong_ordering(compare_internal::eq v) noexcept
            : value_(static_cast<compare_internal::value_type>(v)) {}
        explicit constexpr strong_ordering(compare_internal::ord v) noexcept
            : value_(static_cast<compare_internal::value_type>(v)) {}
        friend struct compare_internal::strong_ordering_base<strong_ordering>;

    public:
        PHMAP_COMPARE_INLINE_SUBCLASS_DECL(strong_ordering, less)
        PHMAP_COMPARE_INLINE_SUBCLASS_DECL(strong_ordering, equal)
        PHMAP_COMPARE_INLINE_SUBCLASS_DECL(strong_ordering, equivalent)
        PHMAP_COMPARE_INLINE_SUBCLASS_DECL(strong_ordering, greater)

        // Conversions
        constexpr operator weak_equality() const noexcept {  // NOLINT
            return value_ == 0 ? weak_equality::equivalent
                : weak_equality::nonequivalent;
        }
        constexpr operator strong_equality() const noexcept {  // NOLINT
            return value_ == 0 ? strong_equality::equal : strong_equality::nonequal;
        }
        constexpr operator partial_ordering() const noexcept {  // NOLINT
            return value_ == 0 ? partial_ordering::equivalent
                : (value_ < 0 ? partial_ordering::less
                   : partial_ordering::greater);
        }
        constexpr operator weak_ordering() const noexcept {  // NOLINT
            return value_ == 0
                ? weak_ordering::equivalent
                : (value_ < 0 ? weak_ordering::less : weak_ordering::greater);
        }
        // Comparisons
        friend constexpr bool operator==(
            strong_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.value_ == 0;
        }
        friend constexpr bool operator!=(
            strong_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.value_ != 0;
        }
        friend constexpr bool operator<(
            strong_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.value_ < 0;
        }
        friend constexpr bool operator<=(
            strong_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.value_ <= 0;
        }
        friend constexpr bool operator>(
            strong_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.value_ > 0;
        }
        friend constexpr bool operator>=(
            strong_ordering v, compare_internal::OnlyLiteralZero<>) noexcept {
            return v.value_ >= 0;
        }
        friend constexpr bool operator==(compare_internal::OnlyLiteralZero<>,
                                         strong_ordering v) noexcept {
            return 0 == v.value_;
        }
        friend constexpr bool operator!=(compare_internal::OnlyLiteralZero<>,
                                         strong_ordering v) noexcept {
            return 0 != v.value_;
        }
        friend constexpr bool operator<(compare_internal::OnlyLiteralZero<>,
                                        strong_ordering v) noexcept {
            return 0 < v.value_;
        }
        friend constexpr bool operator<=(compare_internal::OnlyLiteralZero<>,
                                         strong_ordering v) noexcept {
            return 0 <= v.value_;
        }
        friend constexpr bool operator>(compare_internal::OnlyLiteralZero<>,
                                        strong_ordering v) noexcept {
            return 0 > v.value_;
        }
        friend constexpr bool operator>=(compare_internal::OnlyLiteralZero<>,
                                         strong_ordering v) noexcept {
            return 0 >= v.value_;
        }

    private:
        compare_internal::value_type value_;
    };
    PHMAP_COMPARE_INLINE_INIT(strong_ordering, less, compare_internal::ord::less);
    PHMAP_COMPARE_INLINE_INIT(strong_ordering, equal, compare_internal::eq::equal);
    PHMAP_COMPARE_INLINE_INIT(strong_ordering, equivalent,
                              compare_internal::eq::equivalent);
    PHMAP_COMPARE_INLINE_INIT(strong_ordering, greater,
                              compare_internal::ord::greater);

#undef PHMAP_COMPARE_INLINE_BASECLASS_DECL
#undef PHMAP_COMPARE_INLINE_SUBCLASS_DECL
#undef PHMAP_COMPARE_INLINE_INIT

    namespace compare_internal {
        // We also provide these comparator adapter functions for internal phmap use.

        // Helper functions to do a boolean comparison of two keys given a boolean
        // or three-way comparator.
        // SFINAE prevents implicit conversions to bool (such as from int).
        template <typename BoolType,
                  phmap::enable_if_t<std::is_same<bool, BoolType>::value, int> = 0>
        constexpr bool compare_result_as_less_than(const BoolType r) { return r; }
        constexpr bool compare_result_as_less_than(const phmap::weak_ordering r) {
            return r < 0;
        }

        template <typename Compare, typename K, typename LK>
        constexpr bool do_less_than_comparison(const Compare &compare, const K &x,
                                               const LK &y) {
            return compare_result_as_less_than(compare(x, y));
        }

        // Helper functions to do a three-way comparison of two keys given a boolean or
        // three-way comparator.
        // SFINAE prevents implicit conversions to int (such as from bool).
        template <typename Int,
                  phmap::enable_if_t<std::is_same<int, Int>::value, int> = 0>
        constexpr phmap::weak_ordering compare_result_as_ordering(const Int c) {
            return c < 0 ? phmap::weak_ordering::less
                       : c == 0 ? phmap::weak_ordering::equivalent
                       : phmap::weak_ordering::greater;
        }
        constexpr phmap::weak_ordering compare_result_as_ordering(
            const phmap::weak_ordering c) {
            return c;
        }

        template <
            typename Compare, typename K, typename LK,
            phmap::enable_if_t<!std::is_same<bool, phmap::invoke_result_t<
                                                       Compare, const K &, const LK &>>::value,
                               int> = 0>
            constexpr phmap::weak_ordering do_three_way_comparison(const Compare &compare,
                                                                   const K &x, const LK &y) {
            return compare_result_as_ordering(compare(x, y));
        }
        template <
            typename Compare, typename K, typename LK,
            phmap::enable_if_t<std::is_same<bool, phmap::invoke_result_t<Compare,
            const K &, const LK &>>::value,
                               int> = 0>
            constexpr phmap::weak_ordering do_three_way_comparison(const Compare &compare,
                                                                   const K &x, const LK &y) {
            return compare(x, y) ? phmap::weak_ordering::less
                : compare(y, x) ? phmap::weak_ordering::greater
                : phmap::weak_ordering::equivalent;
        }

    }  // namespace compare_internal
}


namespace phmap {

namespace priv {

    // A helper class that indicates if the Compare parameter is a key-compare-to
    // comparator.
    template <typename Compare, typename T>
    using btree_is_key_compare_to =
        std::is_convertible<phmap::invoke_result_t<Compare, const T &, const T &>,
                            phmap::weak_ordering>;

    struct StringBtreeDefaultLess {
        using is_transparent = void;

        StringBtreeDefaultLess() = default;

        // Compatibility constructor.
        StringBtreeDefaultLess(std::less<std::string>) {}       // NOLINT
#if PHMAP_HAVE_STD_STRING_VIEW
        StringBtreeDefaultLess(std::less<std::string_view>) {}  // NOLINT
        StringBtreeDefaultLess(phmap::Less<std::string_view>) {}  // NOLINT

        phmap::weak_ordering operator()(const std::string_view &lhs,
                                        const std::string_view &rhs) const {
            return compare_internal::compare_result_as_ordering(lhs.compare(rhs));
        }
#else
        phmap::weak_ordering operator()(const std::string &lhs,
                                        const std::string &rhs) const {
            return compare_internal::compare_result_as_ordering(lhs.compare(rhs));
        }
#endif
    };

    struct StringBtreeDefaultGreater {
        using is_transparent = void;

        StringBtreeDefaultGreater() = default;

        StringBtreeDefaultGreater(std::greater<std::string>) {}       // NOLINT
#if PHMAP_HAVE_STD_STRING_VIEW
        StringBtreeDefaultGreater(std::greater<std::string_view>) {}  // NOLINT

        phmap::weak_ordering operator()(std::string_view lhs,
                                        std::string_view rhs) const {
            return compare_internal::compare_result_as_ordering(rhs.compare(lhs));
        }
#else
        phmap::weak_ordering operator()(const std::string &lhs,
                                        const std::string &rhs) const {
            return compare_internal::compare_result_as_ordering(rhs.compare(lhs));
        }
#endif
    };

    // A helper class to convert a boolean comparison into a three-way "compare-to"
    // comparison that returns a negative value to indicate less-than, zero to
    // indicate equality and a positive value to indicate greater-than. This helper
    // class is specialized for less<std::string>, greater<std::string>,
    // less<std::string_view>, and greater<std::string_view>.
    //
    // key_compare_to_adapter is provided so that btree users
    // automatically get the more efficient compare-to code when using common
    // google string types with common comparison functors.
    // These string-like specializations also turn on heterogeneous lookup by
    // default.
    template <typename Compare>
    struct key_compare_to_adapter {
        using type = Compare;
    };

    template <>
    struct key_compare_to_adapter<std::less<std::string>> {
        using type = StringBtreeDefaultLess;
    };

    template <>
    struct key_compare_to_adapter<phmap::Less<std::string>> {
        using type = StringBtreeDefaultLess;
    };

    template <>
    struct key_compare_to_adapter<std::greater<std::string>> {
        using type = StringBtreeDefaultGreater;
    };

#if PHMAP_HAVE_STD_STRING_VIEW
    template <>
    struct key_compare_to_adapter<std::less<std::string_view>> {
        using type = StringBtreeDefaultLess;
    };

    template <>
    struct key_compare_to_adapter<phmap::Less<std::string_view>> {
        using type = StringBtreeDefaultLess;
    };

    template <>
    struct key_compare_to_adapter<std::greater<std::string_view>> {
        using type = StringBtreeDefaultGreater;
    };
#endif

    template <typename Key, typename Compare, typename Alloc, int TargetNodeSize,
              bool Multi, typename SlotPolicy>
    struct common_params {
        // If Compare is a common comparator for a std::string-like type, then we adapt it
        // to use heterogeneous lookup and to be a key-compare-to comparator.
        using key_compare = typename key_compare_to_adapter<Compare>::type;
        // A type which indicates if we have a key-compare-to functor or a plain old
        // key-compare functor.
        using is_key_compare_to = btree_is_key_compare_to<key_compare, Key>;

        using allocator_type = Alloc;
        using key_type = Key;
        using size_type = std::size_t ;
        using difference_type = ptrdiff_t;

        // True if this is a multiset or multimap.
        using is_multi_container = std::integral_constant<bool, Multi>;

        using slot_policy = SlotPolicy;
        using slot_type = typename slot_policy::slot_type;
        using value_type = typename slot_policy::value_type;
        using init_type = typename slot_policy::mutable_value_type;
        using pointer = value_type *;
        using const_pointer = const value_type *;
        using reference = value_type &;
        using const_reference = const value_type &;

        enum {
            kTargetNodeSize = TargetNodeSize,

            // Upper bound for the available space for values. This is largest for leaf
            // nodes, which have overhead of at least a pointer + 4 bytes (for storing
            // 3 field_types and an enum).
            kNodeSlotSpace =
                TargetNodeSize - /*minimum overhead=*/(sizeof(void *) + 4),
        };

        // This is an integral type large enough to hold as many
        // ValueSize-values as will fit a node of TargetNodeSize bytes.
        using node_count_type =
            phmap::conditional_t<(kNodeSlotSpace / sizeof(slot_type) >
                                   (std::numeric_limits<uint8_t>::max)()),
            uint16_t, uint8_t>;  // NOLINT

        // The following methods are necessary for passing this struct as PolicyTraits
        // for node_handle and/or are used within btree.
        static value_type &element(slot_type *slot) {
            return slot_policy::element(slot);
        }
        static const value_type &element(const slot_type *slot) {
            return slot_policy::element(slot);
        }
        template <class... Args>
        static void construct(Alloc *alloc, slot_type *slot, Args &&... args) {
            slot_policy::construct(alloc, slot, std::forward<Args>(args)...);
        }
        static void construct(Alloc *alloc, slot_type *slot, slot_type *other) {
            slot_policy::construct(alloc, slot, other);
        }
        static void destroy(Alloc *alloc, slot_type *slot) {
            slot_policy::destroy(alloc, slot);
        }
        static void transfer(Alloc *alloc, slot_type *new_slot, slot_type *old_slot) {
            construct(alloc, new_slot, old_slot);
            destroy(alloc, old_slot);
        }
        static void swap(Alloc *alloc, slot_type *a, slot_type *b) {
            slot_policy::swap(alloc, a, b);
        }
        static void move(Alloc *alloc, slot_type *src, slot_type *dest) {
            slot_policy::move(alloc, src, dest);
        }
        static void move(Alloc *alloc, slot_type *first, slot_type *last,
                         slot_type *result) {
            slot_policy::move(alloc, first, last, result);
        }
    };

    // A parameters structure for holding the type parameters for a btree_map.
    // Compare and Alloc should be nothrow copy-constructible.
    template <typename Key, typename Data, typename Compare, typename Alloc,
              int TargetNodeSize, bool Multi>
    struct map_params : common_params<Key, Compare, Alloc, TargetNodeSize, Multi,
                                      phmap::priv::map_slot_policy<Key, Data>> {
        using super_type = typename map_params::common_params;
        using mapped_type = Data;
        // This type allows us to move keys when it is safe to do so. It is safe
        // for maps in which value_type and mutable_value_type are layout compatible.
        using slot_policy = typename super_type::slot_policy;
        using slot_type = typename super_type::slot_type;
        using value_type = typename super_type::value_type;
        using init_type = typename super_type::init_type;

        using key_compare = typename super_type::key_compare;
        // Inherit from key_compare for empty base class optimization.
        struct value_compare : private key_compare {
            value_compare() = default;
            explicit value_compare(const key_compare &cmp) : key_compare(cmp) {}

            template <typename T, typename U>
            auto operator()(const T &left, const U &right) const
                -> decltype(std::declval<key_compare>()(left.first, right.first)) {
                return key_compare::operator()(left.first, right.first);
            }
        };
        using is_map_container = std::true_type;

        static const Key &key(const value_type &x) { return x.first; }
        static const Key &key(const init_type &x) { return x.first; }
        static const Key &key(const slot_type *x) { return slot_policy::key(x); }
        static mapped_type &value(value_type *value) { return value->second; }
    };

    // This type implements the necessary functions from the
    // btree::priv::slot_type interface.
    template <typename Key>
    struct set_slot_policy {
        using slot_type = Key;
        using value_type = Key;
        using mutable_value_type = Key;

        static value_type &element(slot_type *slot) { return *slot; }
        static const value_type &element(const slot_type *slot) { return *slot; }

        template <typename Alloc, class... Args>
        static void construct(Alloc *alloc, slot_type *slot, Args &&... args) {
            phmap::allocator_traits<Alloc>::construct(*alloc, slot,
                                                       std::forward<Args>(args)...);
        }

        template <typename Alloc>
        static void construct(Alloc *alloc, slot_type *slot, slot_type *other) {
            phmap::allocator_traits<Alloc>::construct(*alloc, slot, std::move(*other));
        }

        template <typename Alloc>
        static void destroy(Alloc *alloc, slot_type *slot) {
            phmap::allocator_traits<Alloc>::destroy(*alloc, slot);
        }

        template <typename Alloc>
        static void swap(Alloc * /*alloc*/, slot_type *a, slot_type *b) {
            using std::swap;
            swap(*a, *b);
        }

        template <typename Alloc>
        static void move(Alloc * /*alloc*/, slot_type *src, slot_type *dest) {
            *dest = std::move(*src);
        }

        template <typename Alloc>
        static void move(Alloc *alloc, slot_type *first, slot_type *last,
                         slot_type *result) {
            for (slot_type *src = first, *dest = result; src != last; ++src, ++dest)
                move(alloc, src, dest);
        }
    };

    // A parameters structure for holding the type parameters for a btree_set.
    // Compare and Alloc should be nothrow copy-constructible.
    template <typename Key, typename Compare, typename Alloc, int TargetNodeSize,
              bool Multi>
    struct set_params : common_params<Key, Compare, Alloc, TargetNodeSize, Multi,
                                      set_slot_policy<Key>> {
        using value_type = Key;
        using slot_type = typename set_params::common_params::slot_type;
        using value_compare = typename set_params::common_params::key_compare;
        using is_map_container = std::false_type;

        static const Key &key(const value_type &x) { return x; }
        static const Key &key(const slot_type *x) { return *x; }
    };

    // An adapter class that converts a lower-bound compare into an upper-bound
    // compare. Note: there is no need to make a version of this adapter specialized
    // for key-compare-to functors because the upper-bound (the first value greater
    // than the input) is never an exact match.
    template <typename Compare>
    struct upper_bound_adapter {
        explicit upper_bound_adapter(const Compare &c) : comp(c) {}
        template <typename K, typename LK>
        bool operator()(const K &a, const LK &b) const {
            // Returns true when a is not greater than b.
            return !phmap::compare_internal::compare_result_as_less_than(comp(b, a));
        }

    private:
        Compare comp;
    };

    enum class MatchKind : uint8_t { kEq, kNe };

    template <typename V, bool IsCompareTo>
    struct SearchResult {
        V value;
        MatchKind match;

        static constexpr bool HasMatch() { return true; }
        bool IsEq() const { return match == MatchKind::kEq; }
    };

    // When we don't use CompareTo, `match` is not present.
    // This ensures that callers can't use it accidentally when it provides no
    // useful information.
    template <typename V>
    struct SearchResult<V, false> {
        V value;

        static constexpr bool HasMatch() { return false; }
        static constexpr bool IsEq() { return false; }
    };

    // A node in the btree holding. The same node type is used for both internal
    // and leaf nodes in the btree, though the nodes are allocated in such a way
    // that the children array is only valid in internal nodes.
    template <typename Params>
    class btree_node {
        using is_key_compare_to = typename Params::is_key_compare_to;
        using is_multi_container = typename Params::is_multi_container;
        using field_type = typename Params::node_count_type;
        using allocator_type = typename Params::allocator_type;
        using slot_type = typename Params::slot_type;

    public:
        using params_type = Params;
        using key_type = typename Params::key_type;
        using value_type = typename Params::value_type;
        using pointer = typename Params::pointer;
        using const_pointer = typename Params::const_pointer;
        using reference = typename Params::reference;
        using const_reference = typename Params::const_reference;
        using key_compare = typename Params::key_compare;
        using size_type = typename Params::size_type;
        using difference_type = typename Params::difference_type;

        // Btree decides whether to use linear node search as follows:
        //   - If the key is arithmetic and the comparator is std::less or
        //     std::greater, choose linear.
        //   - Otherwise, choose binary.
        // TODO(ezb): Might make sense to add condition(s) based on node-size.
        using use_linear_search = std::integral_constant<
            bool,
            std::is_arithmetic<key_type>::value &&
            (std::is_same<phmap::Less<key_type>, key_compare>::value ||
             std::is_same<std::less<key_type>, key_compare>::value ||
             std::is_same<std::greater<key_type>, key_compare>::value)>;


        ~btree_node() = default;
        btree_node(btree_node const &) = delete;
        btree_node &operator=(btree_node const &) = delete;

        // Public for EmptyNodeType.
        constexpr static size_type Alignment() {
            static_assert(LeafLayout(1).Alignment() == InternalLayout().Alignment(),
                          "Alignment of all nodes must be equal.");
            return (size_type)InternalLayout().Alignment();
        }

    protected:
        btree_node() = default;

    private:
        using layout_type = phmap::priv::Layout<btree_node *, field_type,
                                                               slot_type, btree_node *>;
        constexpr static size_type SizeWithNValues(size_type n) {
            return (size_type)layout_type(/*parent*/ 1,
                               /*position, start, count, max_count*/ 4,
                               /*values*/ (size_t)n,
                               /*children*/ 0)
                .AllocSize();
        }
        // A lower bound for the overhead of fields other than values in a leaf node.
        constexpr static size_type MinimumOverhead() {
            return (size_type)(SizeWithNValues(1) - sizeof(value_type));
        }

        // Compute how many values we can fit onto a leaf node taking into account
        // padding.
        constexpr static size_type NodeTargetValues(const int begin, const int end) {
            return begin == end ? begin
                : SizeWithNValues((begin + end) / 2 + 1) >
                params_type::kTargetNodeSize
                ? NodeTargetValues(begin, (begin + end) / 2)
                : NodeTargetValues((begin + end) / 2 + 1, end);
        }

        enum {
            kTargetNodeSize = params_type::kTargetNodeSize,
            kNodeTargetValues = NodeTargetValues(0, params_type::kTargetNodeSize),

            // We need a minimum of 3 values per internal node in order to perform
            // splitting (1 value for the two nodes involved in the split and 1 value
            // propagated to the parent as the delimiter for the split).
            kNodeValues = kNodeTargetValues >= 3 ? kNodeTargetValues : 3,

            // The node is internal (i.e. is not a leaf node) if and only if `max_count`
            // has this value.
            kInternalNodeMaxCount = 0,
        };

        // Leaves can have less than kNodeValues values.
        constexpr static layout_type LeafLayout(const int max_values = kNodeValues) {
            return layout_type(/*parent*/ 1,
                               /*position, start, count, max_count*/ 4,
                               /*values*/ (size_t)max_values,
                               /*children*/ 0);
        }
        constexpr static layout_type InternalLayout() {
            return layout_type(/*parent*/ 1,
                               /*position, start, count, max_count*/ 4,
                               /*values*/ kNodeValues,
                               /*children*/ kNodeValues + 1);
        }
        constexpr static size_type LeafSize(const int max_values = kNodeValues) {
            return (size_type)LeafLayout(max_values).AllocSize();
        }
        constexpr static size_type InternalSize() {
            return (size_type)InternalLayout().AllocSize();
        }

        // N is the index of the type in the Layout definition.
        // ElementType<N> is the Nth type in the Layout definition.
        template <size_type N>
        inline typename layout_type::template ElementType<N> *GetField() {
            // We assert that we don't read from values that aren't there.
            assert(N < 3 || !leaf());
            return InternalLayout().template Pointer<N>(reinterpret_cast<char *>(this));
        }

        template <size_type N>
        inline const typename layout_type::template ElementType<N> *GetField() const {
            assert(N < 3 || !leaf());
            return InternalLayout().template Pointer<N>(
                reinterpret_cast<const char *>(this));
        }

        void set_parent(btree_node *p)     { *GetField<0>() = p; }
        field_type &mutable_count()        { return GetField<1>()[2]; }
        slot_type *slot(size_type i)       { return &GetField<2>()[i]; }
        const slot_type *slot(size_type i) const { return &GetField<2>()[i]; }
        void set_position(field_type v)    { GetField<1>()[0] = v; }
        void set_start(field_type v)       { GetField<1>()[1] = v; }
        void set_count(field_type v)       { GetField<1>()[2] = v; }
        void set_max_count(field_type v)   { GetField<1>()[3] = v; }

    public:
        // Whether this is a leaf node or not. This value doesn't change after the
        // node is created.
        bool leaf() const { return GetField<1>()[3] != kInternalNodeMaxCount; }

        // Getter for the position of this node in its parent.
        field_type position() const { return GetField<1>()[0]; }

        // Getter for the offset of the first value in the `values` array.
        field_type start() const { return GetField<1>()[1]; }

        // Getters for the number of values stored in this node.
        field_type count() const { return GetField<1>()[2]; }
        field_type max_count() const {
            // Internal nodes have max_count==kInternalNodeMaxCount.
            // Leaf nodes have max_count in [1, kNodeValues].
            const field_type max_cnt = GetField<1>()[3];
            return max_cnt == field_type{kInternalNodeMaxCount}
            ? field_type{kNodeValues}
            : max_cnt;
        }

        // Getter for the parent of this node.
        btree_node *parent() const { return *GetField<0>(); }
        // Getter for whether the node is the root of the tree. The parent of the
        // root of the tree is the leftmost node in the tree which is guaranteed to
        // be a leaf.
        bool is_root() const { return parent()->leaf(); }
        void make_root() {
            assert(parent()->is_root());
            set_parent(parent()->parent());
        }

        // Getters for the key/value at position i in the node.
        const key_type &key(size_type i) const { return params_type::key(slot(i)); }
        reference value(size_type i) { return params_type::element(slot(i)); }
        const_reference value(size_type i) const { return params_type::element(slot(i)); }

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#endif
        // Getters/setter for the child at position i in the node.
        btree_node *child(size_type i) const { return GetField<3>()[i]; }
        btree_node *&mutable_child(size_type i) { return GetField<3>()[i]; }
        void clear_child(size_type i) {
            phmap::priv::SanitizerPoisonObject(&mutable_child(i));
        }
        void set_child(size_type i, btree_node *c) {
            phmap::priv::SanitizerUnpoisonObject(&mutable_child(i));
            mutable_child(i) = c;
            c->set_position((field_type)i);
        }
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
        void init_child(int i, btree_node *c) {
            set_child(i, c);
            c->set_parent(this);
        }

        // Returns the position of the first value whose key is not less than k.
        template <typename K>
        SearchResult<int, is_key_compare_to::value> lower_bound(
            const K &k, const key_compare &comp) const {
            return use_linear_search::value ? linear_search(k, comp)
                : binary_search(k, comp);
        }
        // Returns the position of the first value whose key is greater than k.
        template <typename K>
        int upper_bound(const K &k, const key_compare &comp) const {
            auto upper_compare = upper_bound_adapter<key_compare>(comp);
            return use_linear_search::value ? linear_search(k, upper_compare).value
                : binary_search(k, upper_compare).value;
        }

        template <typename K, typename Compare>
        SearchResult<int, btree_is_key_compare_to<Compare, key_type>::value>
        linear_search(const K &k, const Compare &comp) const {
            return linear_search_impl(k, 0, count(), comp,
                                      btree_is_key_compare_to<Compare, key_type>());
        }

        template <typename K, typename Compare>
        SearchResult<int, btree_is_key_compare_to<Compare, key_type>::value>
        binary_search(const K &k, const Compare &comp) const {
            return binary_search_impl(k, 0, count(), comp,
                                      btree_is_key_compare_to<Compare, key_type>());
        }

        // Returns the position of the first value whose key is not less than k using
        // linear search performed using plain compare.
        template <typename K, typename Compare>
        SearchResult<int, false> linear_search_impl(
            const K &k, int s, const int e, const Compare &comp,
            std::false_type /* IsCompareTo */) const {
            while (s < e) {
                if (!comp(key(s), k)) {
                    break;
                }
                ++s;
            }
            return {s};
        }

        // Returns the position of the first value whose key is not less than k using
        // linear search performed using compare-to.
        template <typename K, typename Compare>
        SearchResult<int, true> linear_search_impl(
            const K &k, int s, const int e, const Compare &comp,
            std::true_type /* IsCompareTo */) const {
            while (s < e) {
                const phmap::weak_ordering c = comp(key(s), k);
                if (c == 0) {
                    return {s, MatchKind::kEq};
                } else if (c > 0) {
                    break;
                }
                ++s;
            }
            return {s, MatchKind::kNe};
        }

        // Returns the position of the first value whose key is not less than k using
        // binary search performed using plain compare.
        template <typename K, typename Compare>
        SearchResult<int, false> binary_search_impl(
            const K &k, int s, int e, const Compare &comp,
            std::false_type /* IsCompareTo */) const {
            while (s != e) {
                const int mid = (s + e) >> 1;
                if (comp(key(mid), k)) {
                    s = mid + 1;
                } else {
                    e = mid;
                }
            }
            return {s};
        }

        // Returns the position of the first value whose key is not less than k using
        // binary search performed using compare-to.
        template <typename K, typename CompareTo>
        SearchResult<int, true> binary_search_impl(
            const K &k, int s, int e, const CompareTo &comp,
            std::true_type /* IsCompareTo */) const {
            if (is_multi_container::value) {
                MatchKind exact_match = MatchKind::kNe;
                while (s != e) {
                    const int mid = (s + e) >> 1;
                    const phmap::weak_ordering c = comp(key(mid), k);
                    if (c < 0) {
                        s = mid + 1;
                    } else {
                        e = mid;
                        if (c == 0) {
                            // Need to return the first value whose key is not less than k,
                            // which requires continuing the binary search if this is a
                            // multi-container.
                            exact_match = MatchKind::kEq;
                        }
                    }
                }
                return {s, exact_match};
            } else {  // Not a multi-container.
                while (s != e) {
                    const int mid = (s + e) >> 1;
                    const phmap::weak_ordering c = comp(key(mid), k);
                    if (c < 0) {
                        s = mid + 1;
                    } else if (c > 0) {
                        e = mid;
                    } else {
                        return {mid, MatchKind::kEq};
                    }
                }
                return {s, MatchKind::kNe};
            }
        }

        // Emplaces a value at position i, shifting all existing values and
        // children at positions >= i to the right by 1.
        template <typename... Args>
        void emplace_value(size_type i, allocator_type *alloc, Args &&... args);

        // Removes the value at position i, shifting all existing values and children
        // at positions > i to the left by 1.
        void remove_value(int i, allocator_type *alloc);

        // Removes the values at positions [i, i + to_erase), shifting all values
        // after that range to the left by to_erase. Does not change children at all.
        void remove_values_ignore_children(int i, size_type to_erase,
                                           allocator_type *alloc);

        // Rebalances a node with its right sibling.
        void rebalance_right_to_left(int to_move, btree_node *right,
                                     allocator_type *alloc);
        void rebalance_left_to_right(int to_move, btree_node *right,
                                     allocator_type *alloc);

        // Splits a node, moving a portion of the node's values to its right sibling.
        void split(int insert_position, btree_node *dest, allocator_type *alloc);

        // Merges a node with its right sibling, moving all of the values and the
        // delimiting key in the parent node onto itself.
        void merge(btree_node *sibling, allocator_type *alloc);

        // Swap the contents of "this" and "src".
        void swap(btree_node *src, allocator_type *alloc);

        // Node allocation/deletion routines.
        static btree_node *init_leaf(btree_node *n, btree_node *parent,
                                     int max_cnt) {
            n->set_parent(parent);
            n->set_position(0);
            n->set_start(0);
            n->set_count(0);
            n->set_max_count((field_type)max_cnt);
            phmap::priv::SanitizerPoisonMemoryRegion(
                n->slot(0), max_cnt * sizeof(slot_type));
            return n;
        }
        static btree_node *init_internal(btree_node *n, btree_node *parent) {
            init_leaf(n, parent, kNodeValues);
            // Set `max_count` to a sentinel value to indicate that this node is
            // internal.
            n->set_max_count(kInternalNodeMaxCount);
            phmap::priv::SanitizerPoisonMemoryRegion(
                &n->mutable_child(0), (kNodeValues + 1) * sizeof(btree_node *));
            return n;
        }
        void destroy(allocator_type *alloc) {
            for (int i = 0; i < count(); ++i) {
                value_destroy(i, alloc);
            }
        }

    public:
        // Exposed only for tests.
        static bool testonly_uses_linear_node_search() {
            return use_linear_search::value;
        }

    private:
        template <typename... Args>
        void value_init(const size_type i, allocator_type *alloc, Args &&... args) {
            phmap::priv::SanitizerUnpoisonObject(slot(i));
            params_type::construct(alloc, slot(i), std::forward<Args>(args)...);
        }
        void value_destroy(const size_type i, allocator_type *alloc) {
            params_type::destroy(alloc, slot(i));
            phmap::priv::SanitizerPoisonObject(slot(i));
        }

        // Move n values starting at value i in this node into the values starting at
        // value j in node x.
        void uninitialized_move_n(const size_type n, const size_type i,
                                  const size_type j, btree_node *x,
                                  allocator_type *alloc) {
            phmap::priv::SanitizerUnpoisonMemoryRegion(
                x->slot(j), n * sizeof(slot_type));
            for (slot_type *src = slot(i), *end = src + n, *dest = x->slot(j);
                 src != end; ++src, ++dest) {
                params_type::construct(alloc, dest, src);
            }
        }

        // Destroys a range of n values, starting at index i.
        void value_destroy_n(const size_type i, const size_type n,
                             allocator_type *alloc) {
            for (size_type j = 0; j < n; ++j) {
                value_destroy(i + j, alloc);
            }
        }

        template <typename P>
        friend class btree;
        template <typename N, typename R, typename P>
        friend struct btree_iterator;
        friend class BtreeNodePeer;
    };

    template <typename Node, typename Reference, typename Pointer>
    struct btree_iterator {
    private:
        using key_type = typename Node::key_type;
        using size_type = typename Node::size_type;
        using params_type = typename Node::params_type;

        using node_type = Node;
        using normal_node = typename std::remove_const<Node>::type;
        using const_node = const Node;
        using normal_pointer = typename params_type::pointer;
        using normal_reference = typename params_type::reference;
        using const_pointer = typename params_type::const_pointer;
        using const_reference = typename params_type::const_reference;
        using slot_type = typename params_type::slot_type;

        using iterator =
            btree_iterator<normal_node, normal_reference, normal_pointer>;
        using const_iterator =
            btree_iterator<const_node, const_reference, const_pointer>;

    public:
        // These aliases are public for std::iterator_traits.
        using difference_type = typename Node::difference_type;
        using value_type = typename params_type::value_type;
        using pointer = Pointer;
        using reference = Reference;
        using iterator_category = std::bidirectional_iterator_tag;

        btree_iterator() : node(nullptr), position(-1) {}
        btree_iterator(Node *n, int p) : node(n), position(p) {}

        // NOTE: this SFINAE allows for implicit conversions from iterator to
        // const_iterator, but it specifically avoids defining copy constructors so
        // that btree_iterator can be trivially copyable. This is for performance and
        // binary size reasons.
        template <typename N, typename R, typename P,
                  phmap::enable_if_t<
                      std::is_same<btree_iterator<N, R, P>, iterator>::value &&
                      std::is_same<btree_iterator, const_iterator>::value,
                      int> = 0>
            btree_iterator(const btree_iterator<N, R, P> &x)  // NOLINT
            : node(x.node), position(x.position) {}

    private:
        // This SFINAE allows explicit conversions from const_iterator to
        // iterator, but also avoids defining a copy constructor.
        // NOTE: the const_cast is safe because this constructor is only called by
        // non-const methods and the container owns the nodes.
        template <typename N, typename R, typename P,
                  phmap::enable_if_t<
                      std::is_same<btree_iterator<N, R, P>, const_iterator>::value &&
                      std::is_same<btree_iterator, iterator>::value,
                      int> = 0>
            explicit btree_iterator(const btree_iterator<N, R, P> &x)
            : node(const_cast<node_type *>(x.node)), position(x.position) {}

        // Increment/decrement the iterator.
        void increment() {
            if (node->leaf() && ++position < node->count()) {
                return;
            }
            increment_slow();
        }
        void increment_slow();

        void decrement() {
            if (node->leaf() && --position >= 0) {
                return;
            }
            decrement_slow();
        }
        void decrement_slow();

    public:
        bool operator==(const const_iterator &x) const {
            return node == x.node && position == x.position;
        }
        bool operator!=(const const_iterator &x) const {
            return node != x.node || position != x.position;
        }
        bool operator==(const iterator &x) const {
            return node == x.node && position == x.position;
        }
        bool operator!=(const iterator &x) const {
            return node != x.node || position != x.position;
        }

        // Accessors for the key/value the iterator is pointing at.
        reference operator*() const {
            return node->value(position);
        }
        pointer operator->() const {
            return &node->value(position);
        }

        btree_iterator& operator++() {
            increment();
            return *this;
        }
        btree_iterator& operator--() {
            decrement();
            return *this;
        }
        btree_iterator operator++(int) {
            btree_iterator tmp = *this;
            ++*this;
            return tmp;
        }
        btree_iterator operator--(int) {
            btree_iterator tmp = *this;
            --*this;
            return tmp;
        }

    private:
        template <typename Params>
        friend class btree;
        template <typename Tree>
        friend class btree_container;
        template <typename Tree>
        friend class btree_set_container;
        template <typename Tree>
        friend class btree_map_container;
        template <typename Tree>
        friend class btree_multiset_container;
        template <typename N, typename R, typename P>
        friend struct btree_iterator;
        template <typename TreeType, typename CheckerType>
        friend class base_checker;

        const key_type &key() const { return node->key(position); }
        slot_type *slot() { return node->slot(position); }

        // The node in the tree the iterator is pointing at.
        Node *node;
        // The position within the node of the tree the iterator is pointing at.
        // TODO(ezb): make this a field_type
        int position;
    };

    template <typename Params>
    class btree {
        using node_type = btree_node<Params>;
        using is_key_compare_to = typename Params::is_key_compare_to;

        // We use a static empty node for the root/leftmost/rightmost of empty btrees
        // in order to avoid branching in begin()/end().
        struct alignas(node_type::Alignment()) EmptyNodeType : node_type {
            using field_type = typename node_type::field_type;
            node_type *parent;
            field_type position = 0;
            field_type start = 0;
            field_type count = 0;
            // max_count must be != kInternalNodeMaxCount (so that this node is regarded
            // as a leaf node). max_count() is never called when the tree is empty.
            field_type max_count = node_type::kInternalNodeMaxCount + 1;

#ifdef _MSC_VER
            // MSVC has constexpr code generations bugs here.
            EmptyNodeType() : parent(this) {}
#else
            constexpr EmptyNodeType(node_type *p) : parent(p) {}
#endif
        };

        static node_type *EmptyNode() {
#ifdef _MSC_VER
            static EmptyNodeType empty_node;
            // This assert fails on some other construction methods.
            assert(empty_node.parent == &empty_node);
            return &empty_node;
#else
            static constexpr EmptyNodeType empty_node(
                const_cast<EmptyNodeType *>(&empty_node));
            return const_cast<EmptyNodeType *>(&empty_node);
#endif
        }

        enum {
            kNodeValues = node_type::kNodeValues,
            kMinNodeValues = kNodeValues / 2,
        };

        struct node_stats {
            using size_type = typename Params::size_type;

            node_stats(size_type l, size_type i)
                : leaf_nodes(l),
                  internal_nodes(i) {
            }

            node_stats& operator+=(const node_stats &x) {
                leaf_nodes += x.leaf_nodes;
                internal_nodes += x.internal_nodes;
                return *this;
            }

            size_type leaf_nodes;
            size_type internal_nodes;
        };

    public:
        using key_type = typename Params::key_type;
        using value_type = typename Params::value_type;
        using size_type = typename Params::size_type;
        using difference_type = typename Params::difference_type;
        using key_compare = typename Params::key_compare;
        using value_compare = typename Params::value_compare;
        using allocator_type = typename Params::allocator_type;
        using reference = typename Params::reference;
        using const_reference = typename Params::const_reference;
        using pointer = typename Params::pointer;
        using const_pointer = typename Params::const_pointer;
        using iterator = btree_iterator<node_type, reference, pointer>;
        using const_iterator = typename iterator::const_iterator;
        using reverse_iterator = std::reverse_iterator<iterator>;
        using const_reverse_iterator = std::reverse_iterator<const_iterator>;
        using node_handle_type = node_handle<Params, Params, allocator_type>;

        // Internal types made public for use by btree_container types.
        using params_type = Params;
        using slot_type = typename Params::slot_type;

    private:
        // For use in copy_or_move_values_in_order.
        const value_type &maybe_move_from_iterator(const_iterator x) { return *x; }
        value_type &&maybe_move_from_iterator(iterator x) { return std::move(*x); }

        // Copies or moves (depending on the template parameter) the values in
        // x into this btree in their order in x. This btree must be empty before this
        // method is called. This method is used in copy construction, copy
        // assignment, and move assignment.
        template <typename Btree>
        void copy_or_move_values_in_order(Btree *x);

        // Validates that various assumptions/requirements are true at compile time.
        constexpr static bool static_assert_validation();

    public:
        btree(const key_compare &comp, const allocator_type &alloc);

        btree(const btree &x);
        btree(btree &&x) noexcept
            : root_(std::move(x.root_)),
            rightmost_(phmap::exchange(x.rightmost_, EmptyNode())),
            size_(phmap::exchange(x.size_, 0)) {
            x.mutable_root() = EmptyNode();
        }

        ~btree() {
            // Put static_asserts in destructor to avoid triggering them before the type
            // is complete.
            static_assert(static_assert_validation(), "This call must be elided.");
            clear();
        }

        // Assign the contents of x to *this.
        btree &operator=(const btree &x);
        btree &operator=(btree &&x) noexcept;

        iterator begin() {
            return iterator(leftmost(), 0);
        }
        const_iterator begin() const {
            return const_iterator(leftmost(), 0);
        }
        iterator end() { return iterator(rightmost_, rightmost_->count()); }
        const_iterator end() const {
            return const_iterator(rightmost_, rightmost_->count());
        }
        reverse_iterator rbegin() {
            return reverse_iterator(end());
        }
        const_reverse_iterator rbegin() const {
            return const_reverse_iterator(end());
        }
        reverse_iterator rend() {
            return reverse_iterator(begin());
        }
        const_reverse_iterator rend() const {
            return const_reverse_iterator(begin());
        }

        // Finds the first element whose key is not less than key.
        template <typename K>
        iterator lower_bound(const K &key) {
            return internal_end(internal_lower_bound(key));
        }
        template <typename K>
        const_iterator lower_bound(const K &key) const {
            return internal_end(internal_lower_bound(key));
        }

        // Finds the first element whose key is greater than key.
        template <typename K>
        iterator upper_bound(const K &key) {
            return internal_end(internal_upper_bound(key));
        }
        template <typename K>
        const_iterator upper_bound(const K &key) const {
            return internal_end(internal_upper_bound(key));
        }

        // Finds the range of values which compare equal to key. The first member of
        // the returned pair is equal to lower_bound(key). The second member pair of
        // the pair is equal to upper_bound(key).
        template <typename K>
        std::pair<iterator, iterator> equal_range(const K &key) {
            return {lower_bound(key), upper_bound(key)};
        }
        template <typename K>
        std::pair<const_iterator, const_iterator> equal_range(const K &key) const {
            return {lower_bound(key), upper_bound(key)};
        }

        // Inserts a value into the btree only if it does not already exist. The
        // boolean return value indicates whether insertion succeeded or failed.
        // Requirement: if `key` already exists in the btree, does not consume `args`.
        // Requirement: `key` is never referenced after consuming `args`.
        template <typename... Args>
        std::pair<iterator, bool> insert_unique(const key_type &key, Args &&... args);

        // Inserts with hint. Checks to see if the value should be placed immediately
        // before `position` in the tree. If so, then the insertion will take
        // amortized constant time. If not, the insertion will take amortized
        // logarithmic time as if a call to insert_unique() were made.
        // Requirement: if `key` already exists in the btree, does not consume `args`.
        // Requirement: `key` is never referenced after consuming `args`.
        template <typename... Args>
        std::pair<iterator, bool> insert_hint_unique(iterator position,
                                                     const key_type &key,
                                                     Args &&... args);

        // Insert a range of values into the btree.
        template <typename InputIterator>
        void insert_iterator_unique(InputIterator b, InputIterator e);

        // Inserts a value into the btree.
        template <typename ValueType>
        iterator insert_multi(const key_type &key, ValueType &&v);

        // Inserts a value into the btree.
        template <typename ValueType>
        iterator insert_multi(ValueType &&v) {
            return insert_multi(params_type::key(v), std::forward<ValueType>(v));
        }

        // Insert with hint. Check to see if the value should be placed immediately
        // before position in the tree. If it does, then the insertion will take
        // amortized constant time. If not, the insertion will take amortized
        // logarithmic time as if a call to insert_multi(v) were made.
        template <typename ValueType>
        iterator insert_hint_multi(iterator position, ValueType &&v);

        // Insert a range of values into the btree.
        template <typename InputIterator>
        void insert_iterator_multi(InputIterator b, InputIterator e);

        // Erase the specified iterator from the btree. The iterator must be valid
        // (i.e. not equal to end()).  Return an iterator pointing to the node after
        // the one that was erased (or end() if none exists).
        // Requirement: does not read the value at `*iter`.
        iterator erase(iterator iter);

        // Erases range. Returns the number of keys erased and an iterator pointing
        // to the element after the last erased element.
        std::pair<size_type, iterator> erase(iterator begin, iterator end);

        // Erases the specified key from the btree. Returns 1 if an element was
        // erased and 0 otherwise.
        template <typename K>
        size_type erase_unique(const K &key);

        // Erases all of the entries matching the specified key from the
        // btree. Returns the number of elements erased.
        template <typename K>
        size_type erase_multi(const K &key);

        // Finds the iterator corresponding to a key or returns end() if the key is
        // not present.
        template <typename K>
        iterator find(const K &key) {
            return internal_end(internal_find(key));
        }
        template <typename K>
        const_iterator find(const K &key) const {
            return internal_end(internal_find(key));
        }

        // Returns a count of the number of times the key appears in the btree.
        template <typename K>
        size_type count_unique(const K &key) const {
            const iterator beg = internal_find(key);
            if (beg.node == nullptr) {
                // The key doesn't exist in the tree.
                return 0;
            }
            return 1;
        }
        // Returns a count of the number of times the key appears in the btree.
        template <typename K>
        size_type count_multi(const K &key) const {
            const auto range = equal_range(key);
            return std::distance(range.first, range.second);
        }

        // Clear the btree, deleting all of the values it contains.
        void clear();

        // Swap the contents of *this and x.
        void swap(btree &x);

        const key_compare &key_comp() const noexcept {
            return std::get<0>(root_);
        }
        template <typename K, typename LK>
        bool compare_keys(const K &x, const LK &y) const {
            return compare_internal::compare_result_as_less_than(key_comp()(x, y));
        }

        value_compare value_comp() const { return value_compare(key_comp()); }

        // Verifies the structure of the btree.
        void verify() const;

        // Size routines.
        size_type size() const { return size_; }
        size_type max_size() const { return (std::numeric_limits<size_type>::max)(); }
        bool empty() const { return size_ == 0; }

        // The height of the btree. An empty tree will have height 0.
        size_type height() const {
            size_type h = 0;
            if (!empty()) {
                // Count the length of the chain from the leftmost node up to the
                // root. We actually count from the root back around to the level below
                // the root, but the calculation is the same because of the circularity
                // of that traversal.
                const node_type *n = root();
                do {
                    ++h;
                    n = n->parent();
                } while (n != root());
            }
            return h;
        }

        // The number of internal, leaf and total nodes used by the btree.
        size_type leaf_nodes() const {
            return internal_stats(root()).leaf_nodes;
        }
        size_type internal_nodes() const {
            return internal_stats(root()).internal_nodes;
        }
        size_type nodes() const {
            node_stats stats = internal_stats(root());
            return stats.leaf_nodes + stats.internal_nodes;
        }

        // The total number of bytes used by the btree.
        size_type bytes_used() const {
            node_stats stats = internal_stats(root());
            if (stats.leaf_nodes == 1 && stats.internal_nodes == 0) {
                return sizeof(*this) +
                    node_type::LeafSize(root()->max_count());
            } else {
                return sizeof(*this) +
                    stats.leaf_nodes * node_type::LeafSize() +
                    stats.internal_nodes * node_type::InternalSize();
            }
        }

        // The average number of bytes used per value stored in the btree.
        static double average_bytes_per_value() {
            // Returns the number of bytes per value on a leaf node that is 75%
            // full. Experimentally, this matches up nicely with the computed number of
            // bytes per value in trees that had their values inserted in random order.
            return node_type::LeafSize() / (kNodeValues * 0.75);
        }

        // The fullness of the btree. Computed as the number of elements in the btree
        // divided by the maximum number of elements a tree with the current number
        // of nodes could hold. A value of 1 indicates perfect space
        // utilization. Smaller values indicate space wastage.
        // Returns 0 for empty trees.
        double fullness() const {
            if (empty()) return 0.0;
            return static_cast<double>(size()) / (nodes() * kNodeValues);
        }
        // The overhead of the btree structure in bytes per node. Computed as the
        // total number of bytes used by the btree minus the number of bytes used for
        // storing elements divided by the number of elements.
        // Returns 0 for empty trees.
        double overhead() const {
            if (empty()) return 0.0;
            return (bytes_used() - size() * sizeof(value_type)) /
                static_cast<double>(size());
        }

        // The allocator used by the btree.
        allocator_type get_allocator() const {
            return allocator();
        }

    private:
        // Internal accessor routines.
        node_type *root() { return std::get<2>(root_); }
        const node_type *root() const { return std::get<2>(root_); }
        node_type *&mutable_root() noexcept { return std::get<2>(root_); }
        key_compare *mutable_key_comp() noexcept { return &std::get<0>(root_); }

        // The leftmost node is stored as the parent of the root node.
        node_type *leftmost() { return root()->parent(); }
        const node_type *leftmost() const { return root()->parent(); }

        // Allocator routines.
        allocator_type *mutable_allocator() noexcept {
            return &std::get<1>(root_);
        }
        const allocator_type &allocator() const noexcept {
            return std::get<1>(root_);
        }

        // Allocates a correctly aligned node of at least size bytes using the
        // allocator.
        node_type *allocate(const size_type sz) {
            return reinterpret_cast<node_type *>(
                phmap::priv::Allocate<node_type::Alignment()>(
                    mutable_allocator(), (size_t)sz));
        }

        // Node creation/deletion routines.
        node_type* new_internal_node(node_type *parent) {
            node_type *p = allocate(node_type::InternalSize());
            return node_type::init_internal(p, parent);
        }
        node_type* new_leaf_node(node_type *parent) {
            node_type *p = allocate(node_type::LeafSize());
            return node_type::init_leaf(p, parent, kNodeValues);
        }
        node_type *new_leaf_root_node(const int max_count) {
            node_type *p = allocate(node_type::LeafSize(max_count));
            return node_type::init_leaf(p, p, max_count);
        }

        // Deletion helper routines.
        void erase_same_node(iterator begin, iterator end);
        iterator erase_from_leaf_node(iterator begin, size_type to_erase);
        iterator rebalance_after_delete(iterator iter);

        // Deallocates a node of a certain size in bytes using the allocator.
        void deallocate(const size_type sz, node_type *node) {
            phmap::priv::Deallocate<node_type::Alignment()>(
                mutable_allocator(), node, (size_t)sz);
        }

        void delete_internal_node(node_type *node) {
            node->destroy(mutable_allocator());
            deallocate(node_type::InternalSize(), node);
        }
        void delete_leaf_node(node_type *node) {
            node->destroy(mutable_allocator());
            deallocate(node_type::LeafSize(node->max_count()), node);
        }

        // Rebalances or splits the node iter points to.
        void rebalance_or_split(iterator *iter);

        // Merges the values of left, right and the delimiting key on their parent
        // onto left, removing the delimiting key and deleting right.
        void merge_nodes(node_type *left, node_type *right);

        // Tries to merge node with its left or right sibling, and failing that,
        // rebalance with its left or right sibling. Returns true if a merge
        // occurred, at which point it is no longer valid to access node. Returns
        // false if no merging took place.
        bool try_merge_or_rebalance(iterator *iter);

        // Tries to shrink the height of the tree by 1.
        void try_shrink();

        iterator internal_end(iterator iter) {
            return iter.node != nullptr ? iter : end();
        }
        const_iterator internal_end(const_iterator iter) const {
            return iter.node != nullptr ? iter : end();
        }

        // Emplaces a value into the btree immediately before iter. Requires that
        // key(v) <= iter.key() and (--iter).key() <= key(v).
        template <typename... Args>
        iterator internal_emplace(iterator iter, Args &&... args);

        // Returns an iterator pointing to the first value >= the value "iter" is
        // pointing at. Note that "iter" might be pointing to an invalid location as
        // iter.position == iter.node->count(). This routine simply moves iter up in
        // the tree to a valid location.
        // Requires: iter.node is non-null.
        template <typename IterType>
        static IterType internal_last(IterType iter);

        // Returns an iterator pointing to the leaf position at which key would
        // reside in the tree. We provide 2 versions of internal_locate. The first
        // version uses a less-than comparator and is incapable of distinguishing when
        // there is an exact match. The second version is for the key-compare-to
        // specialization and distinguishes exact matches. The key-compare-to
        // specialization allows the caller to avoid a subsequent comparison to
        // determine if an exact match was made, which is important for keys with
        // expensive comparison, such as strings.
        template <typename K>
        SearchResult<iterator, is_key_compare_to::value> internal_locate(
            const K &key) const;

        template <typename K>
        SearchResult<iterator, false> internal_locate_impl(
            const K &key, std::false_type /* IsCompareTo */) const;

        template <typename K>
        SearchResult<iterator, true> internal_locate_impl(
            const K &key, std::true_type /* IsCompareTo */) const;

        // Internal routine which implements lower_bound().
        template <typename K>
        iterator internal_lower_bound(const K &key) const;

        // Internal routine which implements upper_bound().
        template <typename K>
        iterator internal_upper_bound(const K &key) const;

        // Internal routine which implements find().
        template <typename K>
        iterator internal_find(const K &key) const;

        // Deletes a node and all of its children.
        void internal_clear(node_type *node);

        // Verifies the tree structure of node.
        size_type internal_verify(const node_type *node,
                                  const key_type *lo, const key_type *hi) const;

        node_stats internal_stats(const node_type *node) const {
            // The root can be a static empty node.
            if (node == nullptr || (node == root() && empty())) {
                return node_stats(0, 0);
            }
            if (node->leaf()) {
                return node_stats(1, 0);
            }
            node_stats res(0, 1);
            for (int i = 0; i <= node->count(); ++i) {
                res += internal_stats(node->child(i));
            }
            return res;
        }

    public:
        // Exposed only for tests.
        static bool testonly_uses_linear_node_search() {
            return node_type::testonly_uses_linear_node_search();
        }

    private:
        std::tuple<key_compare, allocator_type, node_type *> root_;

        // A pointer to the rightmost node. Note that the leftmost node is stored as
        // the root's parent.
        node_type *rightmost_;

        // Number of values.
        size_type size_;
    };

    ////
    // btree_node methods
    template <typename P>
    template <typename... Args>
    inline void btree_node<P>::emplace_value(const size_type i,
                                             allocator_type *alloc,
                                             Args &&... args) {
        assert(i <= count());
        // Shift old values to create space for new value and then construct it in
        // place.
        if (i < count()) {
            value_init(count(), alloc, slot(count() - 1));
            for (size_type j = count() - 1; j > i; --j)
                params_type::move(alloc, slot(j - 1), slot(j));
            value_destroy(i, alloc);
        }
        value_init(i, alloc, std::forward<Args>(args)...);
        set_count((field_type)(count() + 1));

        if (!leaf() && count() > i + 1) {
            for (int j = count(); j > (int)(i + 1); --j) {
                set_child(j, child(j - 1));
            }
            clear_child(i + 1);
        }
    }

    template <typename P>
    inline void btree_node<P>::remove_value(const int i, allocator_type *alloc) {
        if (!leaf() && count() > i + 1) {
            assert(child(i + 1)->count() == 0);
            for (size_type j = i + 1; j < count(); ++j) {
                set_child(j, child(j + 1));
            }
            clear_child(count());
        }

        remove_values_ignore_children(i, /*to_erase=*/1, alloc);
    }

    template <typename P>
    inline void btree_node<P>::remove_values_ignore_children(
        int i, size_type to_erase, allocator_type *alloc) {
        params_type::move(alloc, slot(i + to_erase), slot(count()), slot(i));
        value_destroy_n(count() - to_erase, to_erase, alloc);
        set_count((field_type)(count() - to_erase));
    }

    template <typename P>
    void btree_node<P>::rebalance_right_to_left(const int to_move,
                                                btree_node *right,
                                                allocator_type *alloc) {
        assert(parent() == right->parent());
        assert(position() + 1 == right->position());
        assert(right->count() >= count());
        assert(to_move >= 1);
        assert(to_move <= right->count());

        // 1) Move the delimiting value in the parent to the left node.
        value_init(count(), alloc, parent()->slot(position()));

        // 2) Move the (to_move - 1) values from the right node to the left node.
        right->uninitialized_move_n(to_move - 1, 0, count() + 1, this, alloc);

        // 3) Move the new delimiting value to the parent from the right node.
        params_type::move(alloc, right->slot(to_move - 1),
                          parent()->slot(position()));

        // 4) Shift the values in the right node to their correct position.
        params_type::move(alloc, right->slot(to_move), right->slot(right->count()),
                          right->slot(0));

        // 5) Destroy the now-empty to_move entries in the right node.
        right->value_destroy_n(right->count() - to_move, to_move, alloc);

        if (!leaf()) {
            // Move the child pointers from the right to the left node.
            for (int i = 0; i < to_move; ++i) {
                init_child(count() + i + 1, right->child(i));
            }
            for (int i = 0; i <= right->count() - to_move; ++i) {
                assert(i + to_move <= right->max_count());
                right->init_child(i, right->child(i + to_move));
                right->clear_child(i + to_move);
            }
        }

        // Fixup the counts on the left and right nodes.
        set_count((field_type)(count() + to_move));
        right->set_count((field_type)(right->count() - to_move));
    }

    template <typename P>
    void btree_node<P>::rebalance_left_to_right(const int to_move,
                                                btree_node *right,
                                                allocator_type *alloc) {
        assert(parent() == right->parent());
        assert(position() + 1 == right->position());
        assert(count() >= right->count());
        assert(to_move >= 1);
        assert(to_move <= count());

        // Values in the right node are shifted to the right to make room for the
        // new to_move values. Then, the delimiting value in the parent and the
        // other (to_move - 1) values in the left node are moved into the right node.
        // Lastly, a new delimiting value is moved from the left node into the
        // parent, and the remaining empty left node entries are destroyed.

        if (right->count() >= to_move) {
            // The original location of the right->count() values are sufficient to hold
            // the new to_move entries from the parent and left node.

            // 1) Shift existing values in the right node to their correct positions.
            right->uninitialized_move_n(to_move, right->count() - to_move,
                                        right->count(), right, alloc);
            if (right->count() > to_move) {
                for (slot_type *src = right->slot(right->count() - to_move - 1),
                         *dest = right->slot(right->count() - 1),
                         *end = right->slot(0);
                     src >= end; --src, --dest) {
                    params_type::move(alloc, src, dest);
                }
            }

            // 2) Move the delimiting value in the parent to the right node.
            params_type::move(alloc, parent()->slot(position()),
                              right->slot(to_move - 1));

            // 3) Move the (to_move - 1) values from the left node to the right node.
            params_type::move(alloc, slot(count() - (to_move - 1)), slot(count()),
                              right->slot(0));
        } else {
            // The right node does not have enough initialized space to hold the new
            // to_move entries, so part of them will move to uninitialized space.

            // 1) Shift existing values in the right node to their correct positions.
            right->uninitialized_move_n(right->count(), 0, to_move, right, alloc);

            // 2) Move the delimiting value in the parent to the right node.
            right->value_init(to_move - 1, alloc, parent()->slot(position()));

            // 3) Move the (to_move - 1) values from the left node to the right node.
            const size_type uninitialized_remaining = to_move - right->count() - 1;
            uninitialized_move_n(uninitialized_remaining,
                                 count() - uninitialized_remaining, right->count(),
                                 right, alloc);
            params_type::move(alloc, slot(count() - (to_move - 1)),
                              slot(count() - uninitialized_remaining), right->slot(0));
        }

        // 4) Move the new delimiting value to the parent from the left node.
        params_type::move(alloc, slot(count() - to_move), parent()->slot(position()));

        // 5) Destroy the now-empty to_move entries in the left node.
        value_destroy_n(count() - to_move, to_move, alloc);

        if (!leaf()) {
            // Move the child pointers from the left to the right node.
            for (int i = right->count(); i >= 0; --i) {
                right->init_child(i + to_move, right->child(i));
                right->clear_child(i);
            }
            for (int i = 1; i <= to_move; ++i) {
                right->init_child(i - 1, child(count() - to_move + i));
                clear_child(count() - to_move + i);
            }
        }

        // Fixup the counts on the left and right nodes.
        set_count((field_type)(count() - to_move));
        right->set_count((field_type)(right->count() + to_move));
    }

    template <typename P>
    void btree_node<P>::split(const int insert_position, btree_node *dest,
                              allocator_type *alloc) {
        assert(dest->count() == 0);
        assert(max_count() == kNodeValues);

        // We bias the split based on the position being inserted. If we're
        // inserting at the beginning of the left node then bias the split to put
        // more values on the right node. If we're inserting at the end of the
        // right node then bias the split to put more values on the left node.
        if (insert_position == 0) {
            dest->set_count((field_type)(count() - 1));
        } else if (insert_position == kNodeValues) {
            dest->set_count(0);
        } else {
            dest->set_count((field_type)(count() / 2));
        }
        set_count((field_type)(count() - dest->count()));
        assert(count() >= 1);

        // Move values from the left sibling to the right sibling.
        uninitialized_move_n(dest->count(), count(), 0, dest, alloc);

        // Destroy the now-empty entries in the left node.
        value_destroy_n(count(), dest->count(), alloc);

        // The split key is the largest value in the left sibling.
        set_count((field_type)(count() - 1));
        parent()->emplace_value(position(), alloc, slot(count()));
        value_destroy(count(), alloc);
        parent()->init_child(position() + 1, dest);

        if (!leaf()) {
            for (int i = 0; i <= dest->count(); ++i) {
                assert(child(count() + i + 1) != nullptr);
                dest->init_child(i, child(count() + i + 1));
                clear_child(count() + i + 1);
            }
        }
    }

    template <typename P>
    void btree_node<P>::merge(btree_node *src, allocator_type *alloc) {
        assert(parent() == src->parent());
        assert(position() + 1 == src->position());

        // Move the delimiting value to the left node.
        value_init(count(), alloc, parent()->slot(position()));

        // Move the values from the right to the left node.
        src->uninitialized_move_n(src->count(), 0, count() + 1, this, alloc);

        // Destroy the now-empty entries in the right node.
        src->value_destroy_n(0, src->count(), alloc);

        if (!leaf()) {
            // Move the child pointers from the right to the left node.
            for (int i = 0; i <= src->count(); ++i) {
                init_child(count() + i + 1, src->child(i));
                src->clear_child(i);
            }
        }

        // Fixup the counts on the src and dest nodes.
        set_count((field_type)(1 + count() + src->count()));
        src->set_count(0);

        // Remove the value on the parent node.
        parent()->remove_value(position(), alloc);
    }

    template <typename P>
    void btree_node<P>::swap(btree_node *x, allocator_type *alloc) {
        using std::swap;
        assert(leaf() == x->leaf());

        // Determine which is the smaller/larger node.
        btree_node *smaller = this, *larger = x;
        if (smaller->count() > larger->count()) {
            swap(smaller, larger);
        }

        // Swap the values.
        for (slot_type *a = smaller->slot(0), *b = larger->slot(0),
                 *end = a + smaller->count();
             a != end; ++a, ++b) {
            params_type::swap(alloc, a, b);
        }

        // Move values that can't be swapped.
        const size_type to_move = larger->count() - smaller->count();
        larger->uninitialized_move_n(to_move, smaller->count(), smaller->count(),
                                     smaller, alloc);
        larger->value_destroy_n(smaller->count(), to_move, alloc);

        if (!leaf()) {
            // Swap the child pointers.
            std::swap_ranges(&smaller->mutable_child(0),
                             &smaller->mutable_child(smaller->count() + 1),
                             &larger->mutable_child(0));
            // Update swapped children's parent pointers.
            int i = 0;
            for (; i <= smaller->count(); ++i) {
                smaller->child(i)->set_parent(smaller);
                larger->child(i)->set_parent(larger);
            }
            // Move the child pointers that couldn't be swapped.
            for (; i <= larger->count(); ++i) {
                smaller->init_child(i, larger->child(i));
                larger->clear_child(i);
            }
        }

        // Swap the counts.
        swap(mutable_count(), x->mutable_count());
    }

    ////
    // btree_iterator methods
    template <typename N, typename R, typename P>
    void btree_iterator<N, R, P>::increment_slow() {
        if (node->leaf()) {
            assert(position >= node->count());
            btree_iterator save(*this);
            while (position == node->count() && !node->is_root()) {
                assert(node->parent()->child(node->position()) == node);
                position = node->position();
                node = node->parent();
            }
            if (position == node->count()) {
                *this = save;
            }
        } else {
            assert(position < node->count());
            node = node->child(position + 1);
            while (!node->leaf()) {
                node = node->child(0);
            }
            position = 0;
        }
    }

    template <typename N, typename R, typename P>
    void btree_iterator<N, R, P>::decrement_slow() {
        if (node->leaf()) {
            assert(position <= -1);
            btree_iterator save(*this);
            while (position < 0 && !node->is_root()) {
                assert(node->parent()->child(node->position()) == node);
                position = node->position() - 1;
                node = node->parent();
            }
            if (position < 0) {
                *this = save;
            }
        } else {
            assert(position >= 0);
            node = node->child(position);
            while (!node->leaf()) {
                node = node->child(node->count());
            }
            position = node->count() - 1;
        }
    }

    ////
    // btree methods
    template <typename P>
    template <typename Btree>
    void btree<P>::copy_or_move_values_in_order(Btree *x) {
        static_assert(std::is_same<btree, Btree>::value ||
                      std::is_same<const btree, Btree>::value,
                      "Btree type must be same or const.");
        assert(empty());

        // We can avoid key comparisons because we know the order of the
        // values is the same order we'll store them in.
        auto iter = x->begin();
        if (iter == x->end()) return;
        insert_multi(maybe_move_from_iterator(iter));
        ++iter;
        for (; iter != x->end(); ++iter) {
            // If the btree is not empty, we can just insert the new value at the end
            // of the tree.
            internal_emplace(end(), maybe_move_from_iterator(iter));
        }
    }

    template <typename P>
    constexpr bool btree<P>::static_assert_validation() {
        static_assert(std::is_nothrow_copy_constructible<key_compare>::value,
                      "Key comparison must be nothrow copy constructible");
        static_assert(std::is_nothrow_copy_constructible<allocator_type>::value,
                      "Allocator must be nothrow copy constructible");
        static_assert(type_traits_internal::is_trivially_copyable<iterator>::value,
                      "iterator not trivially copyable.");

        // Note: We assert that kTargetValues, which is computed from
        // Params::kTargetNodeSize, must fit the node_type::field_type.
        static_assert(
            kNodeValues < (1 << (8 * sizeof(typename node_type::field_type))),
            "target node size too large");

        // Verify that key_compare returns an phmap::{weak,strong}_ordering or bool.
        using compare_result_type =
            phmap::invoke_result_t<key_compare, key_type, key_type>;
        static_assert(
            std::is_same<compare_result_type, bool>::value ||
            std::is_convertible<compare_result_type, phmap::weak_ordering>::value,
            "key comparison function must return phmap::{weak,strong}_ordering or "
            "bool.");

        // Test the assumption made in setting kNodeSlotSpace.
        static_assert(node_type::MinimumOverhead() >= sizeof(void *) + 4,
                      "node space assumption incorrect");

        return true;
    }

    template <typename P>
    btree<P>::btree(const key_compare &comp, const allocator_type &alloc)
        : root_(comp, alloc, EmptyNode()), rightmost_(EmptyNode()), size_(0) {}

    template <typename P>
    btree<P>::btree(const btree &x) : btree(x.key_comp(), x.allocator()) {
        copy_or_move_values_in_order(&x);
    }

    template <typename P>
    template <typename... Args>
    auto btree<P>::insert_unique(const key_type &key, Args &&... args)
        -> std::pair<iterator, bool> {
        if (empty()) {
            mutable_root() = rightmost_ = new_leaf_root_node(1);
        }

        auto res = internal_locate(key);
        iterator &iter = res.value;

        if (res.HasMatch()) {
            if (res.IsEq()) {
                // The key already exists in the tree, do nothing.
                return {iter, false};
            }
        } else {
            iterator last = internal_last(iter);
            if (last.node && !compare_keys(key, last.key())) {
                // The key already exists in the tree, do nothing.
                return {last, false};
            }
        }
        return {internal_emplace(iter, std::forward<Args>(args)...), true};
    }

    template <typename P>
    template <typename... Args>
    inline auto btree<P>::insert_hint_unique(iterator position, const key_type &key,
                                             Args &&... args)
        -> std::pair<iterator, bool> {
        if (!empty()) {
            if (position == end() || compare_keys(key, position.key())) {
                iterator prev = position;
                if (position == begin() || compare_keys((--prev).key(), key)) {
                    // prev.key() < key < position.key()
                    return {internal_emplace(position, std::forward<Args>(args)...), true};
                }
            } else if (compare_keys(position.key(), key)) {
                ++position;
                if (position == end() || compare_keys(key, position.key())) {
                    // {original `position`}.key() < key < {current `position`}.key()
                    return {internal_emplace(position, std::forward<Args>(args)...), true};
                }
            } else {
                // position.key() == key
                return {position, false};
            }
        }
        return insert_unique(key, std::forward<Args>(args)...);
    }

    template <typename P>
    template <typename InputIterator>
    void btree<P>::insert_iterator_unique(InputIterator b, InputIterator e) {
        for (; b != e; ++b) {
            insert_hint_unique(end(), params_type::key(*b), *b);
        }
    }

    template <typename P>
    template <typename ValueType>
    auto btree<P>::insert_multi(const key_type &key, ValueType &&v) -> iterator {
        if (empty()) {
            mutable_root() = rightmost_ = new_leaf_root_node(1);
        }

        iterator iter = internal_upper_bound(key);
        if (iter.node == nullptr) {
            iter = end();
        }
        return internal_emplace(iter, std::forward<ValueType>(v));
    }

    template <typename P>
    template <typename ValueType>
    auto btree<P>::insert_hint_multi(iterator position, ValueType &&v) -> iterator {
        if (!empty()) {
            const key_type &key = params_type::key(v);
            if (position == end() || !compare_keys(position.key(), key)) {
                iterator prev = position;
                if (position == begin() || !compare_keys(key, (--prev).key())) {
                    // prev.key() <= key <= position.key()
                    return internal_emplace(position, std::forward<ValueType>(v));
                }
            } else {
                iterator next = position;
                ++next;
                if (next == end() || !compare_keys(next.key(), key)) {
                    // position.key() < key <= next.key()
                    return internal_emplace(next, std::forward<ValueType>(v));
                }
            }
        }
        return insert_multi(std::forward<ValueType>(v));
    }

    template <typename P>
    template <typename InputIterator>
    void btree<P>::insert_iterator_multi(InputIterator b, InputIterator e) {
        for (; b != e; ++b) {
            insert_hint_multi(end(), *b);
        }
    }

    template <typename P>
    auto btree<P>::operator=(const btree &x) -> btree & {
        if (this != &x) {
            clear();

            *mutable_key_comp() = x.key_comp();
            if (phmap::allocator_traits<
                allocator_type>::propagate_on_container_copy_assignment::value) {
                *mutable_allocator() = x.allocator();
            }

            copy_or_move_values_in_order(&x);
        }
        return *this;
    }

    template <typename P>
    auto btree<P>::operator=(btree &&x) noexcept -> btree & {
        if (this != &x) {
            clear();

            using std::swap;
            if (phmap::allocator_traits<
                allocator_type>::propagate_on_container_copy_assignment::value) {
                // Note: `root_` also contains the allocator and the key comparator.
                swap(root_, x.root_);
                swap(rightmost_, x.rightmost_);
                swap(size_, x.size_);
            } else {
                if (allocator() == x.allocator()) {
                    swap(mutable_root(), x.mutable_root());
                    swap(*mutable_key_comp(), *x.mutable_key_comp());
                    swap(rightmost_, x.rightmost_);
                    swap(size_, x.size_);
                } else {
                    // We aren't allowed to propagate the allocator and the allocator is
                    // different so we can't take over its memory. We must move each element
                    // individually. We need both `x` and `this` to have `x`s key comparator
                    // while moving the values so we can't swap the key comparators.
                    *mutable_key_comp() = x.key_comp();
                    copy_or_move_values_in_order(&x);
                }
            }
        }
        return *this;
    }

    template <typename P>
    auto btree<P>::erase(iterator iter) -> iterator {
        bool internal_delete = false;
        if (!iter.node->leaf()) {
            // Deletion of a value on an internal node. First, move the largest value
            // from our left child here, then delete that position (in remove_value()
            // below). We can get to the largest value from our left child by
            // decrementing iter.
            iterator internal_iter(iter);
            --iter;
            assert(iter.node->leaf());
            params_type::move(mutable_allocator(), iter.node->slot(iter.position),
                              internal_iter.node->slot(internal_iter.position));
            internal_delete = true;
        }

        // Delete the key from the leaf.
        iter.node->remove_value(iter.position, mutable_allocator());
        --size_;

        // We want to return the next value after the one we just erased. If we
        // erased from an internal node (internal_delete == true), then the next
        // value is ++(++iter). If we erased from a leaf node (internal_delete ==
        // false) then the next value is ++iter. Note that ++iter may point to an
        // internal node and the value in the internal node may move to a leaf node
        // (iter.node) when rebalancing is performed at the leaf level.

        iterator res = rebalance_after_delete(iter);

        // If we erased from an internal node, advance the iterator.
        if (internal_delete) {
            ++res;
        }
        return res;
    }

    template <typename P>
    auto btree<P>::rebalance_after_delete(iterator iter) -> iterator {
        // Merge/rebalance as we walk back up the tree.
        iterator res(iter);
        bool first_iteration = true;
        for (;;) {
            if (iter.node == root()) {
                try_shrink();
                if (empty()) {
                    return end();
                }
                break;
            }
            if (iter.node->count() >= kMinNodeValues) {
                break;
            }
            bool merged = try_merge_or_rebalance(&iter);
            // On the first iteration, we should update `res` with `iter` because `res`
            // may have been invalidated.
            if (first_iteration) {
                res = iter;
                first_iteration = false;
            }
            if (!merged) {
                break;
            }
            iter.position = iter.node->position();
            iter.node = iter.node->parent();
        }

        // Adjust our return value. If we're pointing at the end of a node, advance
        // the iterator.
        if (res.position == res.node->count()) {
            res.position = res.node->count() - 1;
            ++res;
        }

        return res;
    }

    template <typename P>
    auto btree<P>::erase(iterator _begin, iterator _end)
        -> std::pair<size_type, iterator> {
        difference_type count = std::distance(_begin, _end);
        assert(count >= 0);

        if (count == 0) {
            return {0, _begin};
        }

        if (count == (difference_type)size_) {
            clear();
            return {count, this->end()};
        }

        if (_begin.node == _end.node) {
            erase_same_node(_begin, _end);
            size_ -= count;
            return {count, rebalance_after_delete(_begin)};
        }

        const size_type target_size = size_ - count;
        while (size_ > target_size) {
            if (_begin.node->leaf()) {
                const size_type remaining_to_erase = size_ - target_size;
                const size_type remaining_in_node = _begin.node->count() - _begin.position;
                _begin = erase_from_leaf_node(
                    _begin, (std::min)(remaining_to_erase, remaining_in_node));
            } else {
                _begin = erase(_begin);
            }
        }
        return {count, _begin};
    }

    template <typename P>
    void btree<P>::erase_same_node(iterator _begin, iterator _end) {
        assert(_begin.node == _end.node);
        assert(_end.position > _begin.position);

        node_type *node = _begin.node;
        size_type to_erase = _end.position - _begin.position;
        if (!node->leaf()) {
            // Delete all children between _begin and _end.
            for (size_type i = 0; i < to_erase; ++i) {
                internal_clear(node->child(_begin.position + i + 1));
            }
            // Rotate children after _end into new positions.
            for (size_type i = _begin.position + to_erase + 1; i <= node->count(); ++i) {
                node->set_child(i - to_erase, node->child(i));
                node->clear_child(i);
            }
        }
        node->remove_values_ignore_children(_begin.position, to_erase,
                                            mutable_allocator());

        // Do not need to update rightmost_, because
        // * either _end == this->end(), and therefore node == rightmost_, and still
        //   exists
        // * or _end != this->end(), and therefore rightmost_ hasn't been erased, since
        //   it wasn't covered in [_begin, _end)
    }

    template <typename P>
    auto btree<P>::erase_from_leaf_node(iterator _begin, size_type to_erase)
        -> iterator {
        node_type *node = _begin.node;
        assert(node->leaf());
        assert(node->count() > _begin.position);
        assert(_begin.position + to_erase <= node->count());

        node->remove_values_ignore_children(_begin.position, to_erase,
                                            mutable_allocator());

        size_ -= to_erase;

        return rebalance_after_delete(_begin);
    }

    template <typename P>
    template <typename K>
    auto btree<P>::erase_unique(const K &key) -> size_type {
        const iterator iter = internal_find(key);
        if (iter.node == nullptr) {
            // The key doesn't exist in the tree, return nothing done.
            return 0;
        }
        erase(iter);
        return 1;
    }

    template <typename P>
    template <typename K>
    auto btree<P>::erase_multi(const K &key) -> size_type {
        const iterator _begin = internal_lower_bound(key);
        if (_begin.node == nullptr) {
            // The key doesn't exist in the tree, return nothing done.
            return 0;
        }
        // Delete all of the keys between _begin and upper_bound(key).
        const iterator _end = internal_end(internal_upper_bound(key));
        return erase(_begin, _end).first;
    }

    template <typename P>
    void btree<P>::clear() {
        if (!empty()) {
            internal_clear(root());
        }
        mutable_root() = EmptyNode();
        rightmost_ = EmptyNode();
        size_ = 0;
    }

    template <typename P>
    void btree<P>::swap(btree &x) {
        using std::swap;
        if (phmap::allocator_traits<
            allocator_type>::propagate_on_container_swap::value) {
            // Note: `root_` also contains the allocator and the key comparator.
            swap(root_, x.root_);
        } else {
            // It's undefined behavior if the allocators are unequal here.
            assert(allocator() == x.allocator());
            swap(mutable_root(), x.mutable_root());
            swap(*mutable_key_comp(), *x.mutable_key_comp());
        }
        swap(rightmost_, x.rightmost_);
        swap(size_, x.size_);
    }

    template <typename P>
    void btree<P>::verify() const {
        assert(root() != nullptr);
        assert(leftmost() != nullptr);
        assert(rightmost_ != nullptr);
        assert(empty() || size() == internal_verify(root(), nullptr, nullptr));
        assert(leftmost() == (++const_iterator(root(), -1)).node);
        assert(rightmost_ == (--const_iterator(root(), root()->count())).node);
        assert(leftmost()->leaf());
        assert(rightmost_->leaf());
    }

    template <typename P>
    void btree<P>::rebalance_or_split(iterator *iter) {
        node_type *&node = iter->node;
        int &insert_position = iter->position;
        assert(node->count() == node->max_count());
        assert(kNodeValues == node->max_count());

        // First try to make room on the node by rebalancing.
        node_type *parent = node->parent();
        if (node != root()) {
            if (node->position() > 0) {
                // Try rebalancing with our left sibling.
                node_type *left = parent->child(node->position() - 1);
                assert(left->max_count() == kNodeValues);
                if (left->count() < kNodeValues) {
                    // We bias rebalancing based on the position being inserted. If we're
                    // inserting at the end of the right node then we bias rebalancing to
                    // fill up the left node.
                    int to_move = (kNodeValues - left->count()) /
                        (1 + (insert_position < kNodeValues));
                    to_move = (std::max)(1, to_move);

                    if (((insert_position - to_move) >= 0) ||
                        ((left->count() + to_move) < kNodeValues)) {
                        left->rebalance_right_to_left(to_move, node, mutable_allocator());

                        assert(node->max_count() - node->count() == to_move);
                        insert_position = insert_position - to_move;
                        if (insert_position < 0) {
                            insert_position = insert_position + left->count() + 1;
                            node = left;
                        }

                        assert(node->count() < node->max_count());
                        return;
                    }
                }
            }

            if (node->position() < parent->count()) {
                // Try rebalancing with our right sibling.
                node_type *right = parent->child(node->position() + 1);
                assert(right->max_count() == kNodeValues);
                if (right->count() < kNodeValues) {
                    // We bias rebalancing based on the position being inserted. If we're
                    // inserting at the _beginning of the left node then we bias rebalancing
                    // to fill up the right node.
                    int to_move =
                        (kNodeValues - right->count()) / (1 + (insert_position > 0));
                    to_move = (std::max)(1, to_move);

                    if ((insert_position <= (node->count() - to_move)) ||
                        ((right->count() + to_move) < kNodeValues)) {
                        node->rebalance_left_to_right(to_move, right, mutable_allocator());

                        if (insert_position > node->count()) {
                            insert_position = insert_position - node->count() - 1;
                            node = right;
                        }

                        assert(node->count() < node->max_count());
                        return;
                    }
                }
            }

            // Rebalancing failed, make sure there is room on the parent node for a new
            // value.
            assert(parent->max_count() == kNodeValues);
            if (parent->count() == kNodeValues) {
                iterator parent_iter(node->parent(), node->position());
                rebalance_or_split(&parent_iter);
            }
        } else {
            // Rebalancing not possible because this is the root node.
            // Create a new root node and set the current root node as the child of the
            // new root.
            parent = new_internal_node(parent);
            parent->init_child(0, root());
            mutable_root() = parent;
            // If the former root was a leaf node, then it's now the rightmost node.
            assert(!parent->child(0)->leaf() || parent->child(0) == rightmost_);
        }

        // Split the node.
        node_type *split_node;
        if (node->leaf()) {
            split_node = new_leaf_node(parent);
            node->split(insert_position, split_node, mutable_allocator());
            if (rightmost_ == node) rightmost_ = split_node;
        } else {
            split_node = new_internal_node(parent);
            node->split(insert_position, split_node, mutable_allocator());
        }

        if (insert_position > node->count()) {
            insert_position = insert_position - node->count() - 1;
            node = split_node;
        }
    }

    template <typename P>
    void btree<P>::merge_nodes(node_type *left, node_type *right) {
        left->merge(right, mutable_allocator());
        if (right->leaf()) {
            if (rightmost_ == right) rightmost_ = left;
            delete_leaf_node(right);
        } else {
            delete_internal_node(right);
        }
    }

    template <typename P>
    bool btree<P>::try_merge_or_rebalance(iterator *iter) {
        node_type *parent = iter->node->parent();
        if (iter->node->position() > 0) {
            // Try merging with our left sibling.
            node_type *left = parent->child(iter->node->position() - 1);
            assert(left->max_count() == kNodeValues);
            if ((1 + left->count() + iter->node->count()) <= kNodeValues) {
                iter->position += 1 + left->count();
                merge_nodes(left, iter->node);
                iter->node = left;
                return true;
            }
        }
        if (iter->node->position() < parent->count()) {
            // Try merging with our right sibling.
            node_type *right = parent->child(iter->node->position() + 1);
            assert(right->max_count() == kNodeValues);
            if ((1 + iter->node->count() + right->count()) <= kNodeValues) {
                merge_nodes(iter->node, right);
                return true;
            }
            // Try rebalancing with our right sibling. We don't perform rebalancing if
            // we deleted the first element from iter->node and the node is not
            // empty. This is a small optimization for the common pattern of deleting
            // from the front of the tree.
            if ((right->count() > kMinNodeValues) &&
                ((iter->node->count() == 0) ||
                 (iter->position > 0))) {
                int to_move = (right->count() - iter->node->count()) / 2;
                to_move = (std::min)(to_move, right->count() - 1);
                iter->node->rebalance_right_to_left(to_move, right, mutable_allocator());
                return false;
            }
        }
        if (iter->node->position() > 0) {
            // Try rebalancing with our left sibling. We don't perform rebalancing if
            // we deleted the last element from iter->node and the node is not
            // empty. This is a small optimization for the common pattern of deleting
            // from the back of the tree.
            node_type *left = parent->child(iter->node->position() - 1);
            if ((left->count() > kMinNodeValues) &&
                ((iter->node->count() == 0) ||
                 (iter->position < iter->node->count()))) {
                int to_move = (left->count() - iter->node->count()) / 2;
                to_move = (std::min)(to_move, left->count() - 1);
                left->rebalance_left_to_right(to_move, iter->node, mutable_allocator());
                iter->position += to_move;
                return false;
            }
        }
        return false;
    }

    template <typename P>
    void btree<P>::try_shrink() {
        if (root()->count() > 0) {
            return;
        }
        // Deleted the last item on the root node, shrink the height of the tree.
        if (root()->leaf()) {
            assert(size() == 0);
            delete_leaf_node(root());
            mutable_root() = EmptyNode();
            rightmost_ = EmptyNode();
        } else {
            node_type *child = root()->child(0);
            child->make_root();
            delete_internal_node(root());
            mutable_root() = child;
        }
    }

    template <typename P>
    template <typename IterType>
    inline IterType btree<P>::internal_last(IterType iter) {
        assert(iter.node != nullptr);
        while (iter.position == iter.node->count()) {
            iter.position = iter.node->position();
            iter.node = iter.node->parent();
            if (iter.node->leaf()) {
                iter.node = nullptr;
                break;
            }
        }
        return iter;
    }

    template <typename P>
    template <typename... Args>
    inline auto btree<P>::internal_emplace(iterator iter, Args &&... args)
        -> iterator {
        if (!iter.node->leaf()) {
            // We can't insert on an internal node. Instead, we'll insert after the
            // previous value which is guaranteed to be on a leaf node.
            --iter;
            ++iter.position;
        }
        const int max_count = iter.node->max_count();
        if (iter.node->count() == max_count) {
            // Make room in the leaf for the new item.
            if (max_count < kNodeValues) {
                // Insertion into the root where the root is smaller than the full node
                // size. Simply grow the size of the root node.
                assert(iter.node == root());
                iter.node =
                    new_leaf_root_node((std::min<int>)(kNodeValues, 2 * max_count));
                iter.node->swap(root(), mutable_allocator());
                delete_leaf_node(root());
                mutable_root() = iter.node;
                rightmost_ = iter.node;
            } else {
                rebalance_or_split(&iter);
            }
        }
        iter.node->emplace_value(iter.position, mutable_allocator(),
                                 std::forward<Args>(args)...);
        ++size_;
        return iter;
    }

    template <typename P>
    template <typename K>
    inline auto btree<P>::internal_locate(const K &key) const
        -> SearchResult<iterator, is_key_compare_to::value> {
        return internal_locate_impl(key, is_key_compare_to());
    }

    template <typename P>
    template <typename K>
    inline auto btree<P>::internal_locate_impl(
        const K &key, std::false_type /* IsCompareTo */) const
        -> SearchResult<iterator, false> {
        iterator iter(const_cast<node_type *>(root()), 0);
        for (;;) {
            iter.position = iter.node->lower_bound(key, key_comp()).value;
            // NOTE: we don't need to walk all the way down the tree if the keys are
            // equal, but determining equality would require doing an extra comparison
            // on each node on the way down, and we will need to go all the way to the
            // leaf node in the expected case.
            if (iter.node->leaf()) {
                break;
            }
            iter.node = iter.node->child(iter.position);
        }
        return {iter};
    }

    template <typename P>
    template <typename K>
    inline auto btree<P>::internal_locate_impl(
        const K &key, std::true_type /* IsCompareTo */) const
        -> SearchResult<iterator, true> {
        iterator iter(const_cast<node_type *>(root()), 0);
        for (;;) {
            SearchResult<int, true> res = iter.node->lower_bound(key, key_comp());
            iter.position = res.value;
            if (res.match == MatchKind::kEq) {
                return {iter, MatchKind::kEq};
            }
            if (iter.node->leaf()) {
                break;
            }
            iter.node = iter.node->child(iter.position);
        }
        return {iter, MatchKind::kNe};
    }

    template <typename P>
    template <typename K>
    auto btree<P>::internal_lower_bound(const K &key) const -> iterator {
        iterator iter(const_cast<node_type *>(root()), 0);
        for (;;) {
            iter.position = iter.node->lower_bound(key, key_comp()).value;
            if (iter.node->leaf()) {
                break;
            }
            iter.node = iter.node->child(iter.position);
        }
        return internal_last(iter);
    }

    template <typename P>
    template <typename K>
    auto btree<P>::internal_upper_bound(const K &key) const -> iterator {
        iterator iter(const_cast<node_type *>(root()), 0);
        for (;;) {
            iter.position = iter.node->upper_bound(key, key_comp());
            if (iter.node->leaf()) {
                break;
            }
            iter.node = iter.node->child(iter.position);
        }
        return internal_last(iter);
    }

    template <typename P>
    template <typename K>
    auto btree<P>::internal_find(const K &key) const -> iterator {
        auto res = internal_locate(key);
        if (res.HasMatch()) {
            if (res.IsEq()) {
                return res.value;
            }
        } else {
            const iterator iter = internal_last(res.value);
            if (iter.node != nullptr && !compare_keys(key, iter.key())) {
                return iter;
            }
        }
        return {nullptr, 0};
    }

    template <typename P>
    void btree<P>::internal_clear(node_type *node) {
        if (!node->leaf()) {
            for (int i = 0; i <= node->count(); ++i) {
                internal_clear(node->child(i));
            }
            delete_internal_node(node);
        } else {
            delete_leaf_node(node);
        }
    }

    template <typename P>
    typename btree<P>::size_type btree<P>::internal_verify(
        const node_type *node, const key_type *lo, const key_type *hi) const {
        assert(node->count() > 0);
        assert(node->count() <= node->max_count());
        if (lo) {
            assert(!compare_keys(node->key(0), *lo));
        }
        if (hi) {
            assert(!compare_keys(*hi, node->key(node->count() - 1)));
        }
        for (int i = 1; i < node->count(); ++i) {
            assert(!compare_keys(node->key(i), node->key(i - 1)));
        }
        size_type count = node->count();
        if (!node->leaf()) {
            for (int i = 0; i <= node->count(); ++i) {
                assert(node->child(i) != nullptr);
                assert(node->child(i)->parent() == node);
                assert(node->child(i)->position() == i);
                count += internal_verify(
                    node->child(i),
                    (i == 0) ? lo : &node->key(i - 1),
                    (i == node->count()) ? hi : &node->key(i));
            }
        }
        return count;
    }

    // A common base class for btree_set, btree_map, btree_multiset, and btree_multimap.
    // ---------------------------------------------------------------------------------
    template <typename Tree>
    class btree_container {
        using params_type = typename Tree::params_type;

    protected:
        // Alias used for heterogeneous lookup functions.
        // `key_arg<K>` evaluates to `K` when the functors are transparent and to
        // `key_type` otherwise. It permits template argument deduction on `K` for the
        // transparent case.
        template <class K>
        using key_arg =
            typename KeyArg<IsTransparent<typename Tree::key_compare>::value>::
            template type<K, typename Tree::key_type>;

    public:
        using key_type = typename Tree::key_type;
        using value_type = typename Tree::value_type;
        using size_type = typename Tree::size_type;
        using difference_type = typename Tree::difference_type;
        using key_compare = typename Tree::key_compare;
        using value_compare = typename Tree::value_compare;
        using allocator_type = typename Tree::allocator_type;
        using reference = typename Tree::reference;
        using const_reference = typename Tree::const_reference;
        using pointer = typename Tree::pointer;
        using const_pointer = typename Tree::const_pointer;
        using iterator = typename Tree::iterator;
        using const_iterator = typename Tree::const_iterator;
        using reverse_iterator = typename Tree::reverse_iterator;
        using const_reverse_iterator = typename Tree::const_reverse_iterator;
        using node_type = typename Tree::node_handle_type;

        // Constructors/assignments.
        btree_container() : tree_(key_compare(), allocator_type()) {}
        explicit btree_container(const key_compare &comp,
                                 const allocator_type &alloc = allocator_type())
            : tree_(comp, alloc) {}
        btree_container(const btree_container &x) = default;
        btree_container(btree_container &&x) noexcept = default;
        btree_container &operator=(const btree_container &x) = default;
        btree_container &operator=(btree_container &&x) noexcept(
            std::is_nothrow_move_assignable<Tree>::value) = default;

        // Iterator routines.
        iterator begin()                       { return tree_.begin(); }
        const_iterator begin() const           { return tree_.begin(); }
        const_iterator cbegin() const          { return tree_.begin(); }
        iterator end()                         { return tree_.end(); }
        const_iterator end() const             { return tree_.end(); }
        const_iterator cend() const            { return tree_.end(); }
        reverse_iterator rbegin()              { return tree_.rbegin(); }
        const_reverse_iterator rbegin() const  { return tree_.rbegin(); }
        const_reverse_iterator crbegin() const { return tree_.rbegin(); }
        reverse_iterator rend()                { return tree_.rend(); }
        const_reverse_iterator rend() const    { return tree_.rend(); }
        const_reverse_iterator crend() const   { return tree_.rend(); }

        // Lookup routines.
        // ----------------
        template <typename K = key_type>
        size_type count(const key_arg<K> &key) const {
            auto er = this->equal_range(key);
            return std::distance(er.first, er.second);
        }
        template <typename K = key_type>
        iterator find(const key_arg<K> &key) {
            return tree_.find(key);
        }
        template <typename K = key_type>
        const_iterator find(const key_arg<K> &key) const { return tree_.find(key); }

        template <typename K = key_type>
        bool contains(const key_arg<K> &key) const { return find(key) != end(); }

        template <typename K = key_type>
        iterator lower_bound(const key_arg<K> &key) { return tree_.lower_bound(key); }

        template <typename K = key_type>
        const_iterator lower_bound(const key_arg<K> &key) const { return tree_.lower_bound(key); }

        template <typename K = key_type>
        iterator upper_bound(const key_arg<K> &key) { return tree_.upper_bound(key); }

        template <typename K = key_type>
        const_iterator upper_bound(const key_arg<K> &key) const { return tree_.upper_bound(key); }

        template <typename K = key_type>
        std::pair<iterator, iterator> equal_range(const key_arg<K> &key) { return tree_.equal_range(key); }

        template <typename K = key_type>
        std::pair<const_iterator, const_iterator> equal_range(
            const key_arg<K> &key) const {
            return tree_.equal_range(key);
        }

        iterator erase(const_iterator iter) { return tree_.erase(iterator(iter)); }
        iterator erase(iterator iter)       { return tree_.erase(iter); }
        iterator erase(const_iterator first, const_iterator last) {
            return tree_.erase(iterator(first), iterator(last)).second;
        }
        template <typename K = key_type>
        size_type erase(const key_arg<K> &key) {
            auto er = this->equal_range(key);
            return tree_.erase_range(er.first, er.second).first;
        }
        node_type extract(iterator position) {
            // Use Move instead of Transfer, because the rebalancing code expects to
            // have a valid object to scribble metadata bits on top of.
            auto node = CommonAccess::Move<node_type>(get_allocator(), position.slot());
            erase(position);
            return node;
        }

        node_type extract(const_iterator position) {
            return extract(iterator(position));
        }

    public:
        void clear() { tree_.clear(); }
        void swap(btree_container &x) { tree_.swap(x.tree_); }
        void verify() const { tree_.verify(); }

        size_type size() const { return tree_.size(); }
        size_type max_size() const { return tree_.max_size(); }
        bool empty() const { return tree_.empty(); }

        friend bool operator==(const btree_container &x, const btree_container &y) {
            if (x.size() != y.size()) return false;
            return std::equal(x.begin(), x.end(), y.begin());
        }

        friend bool operator!=(const btree_container &x, const btree_container &y) { return !(x == y); }

        friend bool operator<(const btree_container &x, const btree_container &y) {
            return std::lexicographical_compare(x.begin(), x.end(), y.begin(), y.end());
        }

        friend bool operator>(const btree_container &x, const btree_container &y) { return y < x; }

        friend bool operator<=(const btree_container &x, const btree_container &y) { return !(y < x); }

        friend bool operator>=(const btree_container &x, const btree_container &y) { return !(x < y); }

        // The allocator used by the btree.
        allocator_type get_allocator() const { return tree_.get_allocator(); }

        // The key comparator used by the btree.
        key_compare key_comp() const { return tree_.key_comp(); }
        value_compare value_comp() const { return tree_.value_comp(); }

        // Support absl::Hash.
        template <typename State>
        friend State AbslHashValue(State h, const btree_container &b) {
            for (const auto &v : b) {
                h = State::combine(std::move(h), v);
            }
            return State::combine(std::move(h), b.size());
        }

    protected:
        Tree tree_;
    };

    // A common base class for btree_set and btree_map.
    // -----------------------------------------------
    template <typename Tree>
    class btree_set_container : public btree_container<Tree> {
        using super_type = btree_container<Tree>;
        using params_type = typename Tree::params_type;
        using init_type = typename params_type::init_type;
        using is_key_compare_to = typename params_type::is_key_compare_to;
        friend class BtreeNodePeer;

    protected:
        template <class K>
        using key_arg = typename super_type::template key_arg<K>;

    public:
        using key_type = typename Tree::key_type;
        using value_type = typename Tree::value_type;
        using size_type = typename Tree::size_type;
        using key_compare = typename Tree::key_compare;
        using allocator_type = typename Tree::allocator_type;
        using iterator = typename Tree::iterator;
        using const_iterator = typename Tree::const_iterator;
        using node_type = typename super_type::node_type;
        using insert_return_type = InsertReturnType<iterator, node_type>;
        using super_type::super_type;
        btree_set_container() {}

        template <class InputIterator>
        btree_set_container(InputIterator b, InputIterator e,
                            const key_compare &comp = key_compare(),
                            const allocator_type &alloc = allocator_type())
            : super_type(comp, alloc) {
            insert(b, e);
        }

        btree_set_container(std::initializer_list<init_type> init,
                            const key_compare &comp = key_compare(),
                            const allocator_type &alloc = allocator_type())
            : btree_set_container(init.begin(), init.end(), comp, alloc) {}

        btree_set_container(std::initializer_list<init_type> init,
                            const allocator_type &alloc)
            : btree_set_container(init.begin(), init.end(), alloc) {}

        // Lookup routines.
        template <typename K = key_type>
        size_type count(const key_arg<K> &key) const {
            return this->tree_.count_unique(key);
        }

        // Insertion routines.
        std::pair<iterator, bool> insert(const value_type &x) {
            return this->tree_.insert_unique(params_type::key(x), x);
        }
        std::pair<iterator, bool> insert(value_type &&x) {
            return this->tree_.insert_unique(params_type::key(x), std::move(x));
        }
        template <typename... Args>
        std::pair<iterator, bool> emplace(Args &&... args) {
            init_type v(std::forward<Args>(args)...);
            return this->tree_.insert_unique(params_type::key(v), std::move(v));
        }
        iterator insert(const_iterator hint, const value_type &x) {
            return this->tree_
                .insert_hint_unique(iterator(hint), params_type::key(x), x)
                .first;
        }
        iterator insert(const_iterator hint, value_type &&x) {
            return this->tree_
                .insert_hint_unique(iterator(hint), params_type::key(x),
                                    std::move(x))
                .first;
        }

        template <typename... Args>
        iterator emplace_hint(const_iterator hint, Args &&... args) {
            init_type v(std::forward<Args>(args)...);
            return this->tree_
                .insert_hint_unique(iterator(hint), params_type::key(v),
                                    std::move(v))
                .first;
        }

        template <typename InputIterator>
        void insert(InputIterator b, InputIterator e) {
            this->tree_.insert_iterator_unique(b, e);
        }

        void insert(std::initializer_list<init_type> init) {
            this->tree_.insert_iterator_unique(init.begin(), init.end());
        }

        insert_return_type insert(node_type &&node) {
            if (!node) return {this->end(), false, node_type()};
            std::pair<iterator, bool> res =
                this->tree_.insert_unique(params_type::key(CommonAccess::GetSlot(node)),
                                          CommonAccess::GetSlot(node));
            if (res.second) {
                CommonAccess::Destroy(&node);
                return {res.first, true, node_type()};
            } else {
                return {res.first, false, std::move(node)};
            }
        }

        iterator insert(const_iterator hint, node_type &&node) {
            if (!node) return this->end();
            std::pair<iterator, bool> res = this->tree_.insert_hint_unique(
                iterator(hint), params_type::key(CommonAccess::GetSlot(node)),
                CommonAccess::GetSlot(node));
            if (res.second) CommonAccess::Destroy(&node);
            return res.first;
        }

        template <typename K = key_type>
        size_type erase(const key_arg<K> &key) { return this->tree_.erase_unique(key); }
        using super_type::erase;

        template <typename K = key_type>
        node_type extract(const key_arg<K> &key) {
            auto it = this->find(key);
            return it == this->end() ? node_type() : extract(it);
        }

        using super_type::extract;

        // Merge routines.
        // Moves elements from `src` into `this`. If the element already exists in
        // `this`, it is left unmodified in `src`.
        template <
            typename T,
            typename phmap::enable_if_t<
                phmap::conjunction<
                    std::is_same<value_type, typename T::value_type>,
                    std::is_same<allocator_type, typename T::allocator_type>,
                    std::is_same<typename params_type::is_map_container,
                                 typename T::params_type::is_map_container>>::value,
                int> = 0>
            void merge(btree_container<T> &src) {  // NOLINT
            for (auto src_it = src.begin(); src_it != src.end();) {
                if (insert(std::move(*src_it)).second) {
                    src_it = src.erase(src_it);
                } else {
                    ++src_it;
                }
            }
        }

        template <
            typename T,
            typename phmap::enable_if_t<
                phmap::conjunction<
                    std::is_same<value_type, typename T::value_type>,
                    std::is_same<allocator_type, typename T::allocator_type>,
                    std::is_same<typename params_type::is_map_container,
                                 typename T::params_type::is_map_container>>::value,
                int> = 0>
            void merge(btree_container<T> &&src) {
            merge(src);
        }
    };

    // Base class for btree_map.
    // -------------------------
    template <typename Tree>
    class btree_map_container : public btree_set_container<Tree> {
        using super_type = btree_set_container<Tree>;
        using params_type = typename Tree::params_type;

    protected:
        template <class K>
        using key_arg = typename super_type::template key_arg<K>;

    public:
        using key_type = typename Tree::key_type;
        using mapped_type = typename params_type::mapped_type;
        using value_type = typename Tree::value_type;
        using key_compare = typename Tree::key_compare;
        using allocator_type = typename Tree::allocator_type;
        using iterator = typename Tree::iterator;
        using const_iterator = typename Tree::const_iterator;

        // Inherit constructors.
        using super_type::super_type;
        btree_map_container() {}

        // Insertion routines.
        template <typename... Args>
        std::pair<iterator, bool> try_emplace(const key_type &k, Args &&... args) {
            return this->tree_.insert_unique(
                k, std::piecewise_construct, std::forward_as_tuple(k),
                std::forward_as_tuple(std::forward<Args>(args)...));
        }
        template <typename... Args>
        std::pair<iterator, bool> try_emplace(key_type &&k, Args &&... args) {
            // Note: `key_ref` exists to avoid a ClangTidy warning about moving from `k`
            // and then using `k` unsequenced. This is safe because the move is into a
            // forwarding reference and insert_unique guarantees that `key` is never
            // referenced after consuming `args`.
            const key_type& key_ref = k;
            return this->tree_.insert_unique(
                key_ref, std::piecewise_construct, std::forward_as_tuple(std::move(k)),
                std::forward_as_tuple(std::forward<Args>(args)...));
        }
        template <typename... Args>
        iterator try_emplace(const_iterator hint, const key_type &k,
                             Args &&... args) {
            return this->tree_
                .insert_hint_unique(iterator(hint), k, std::piecewise_construct,
                                    std::forward_as_tuple(k),
                                    std::forward_as_tuple(std::forward<Args>(args)...))
                .first;
        }
        template <typename... Args>
        iterator try_emplace(const_iterator hint, key_type &&k, Args &&... args) {
            // Note: `key_ref` exists to avoid a ClangTidy warning about moving from `k`
            // and then using `k` unsequenced. This is safe because the move is into a
            // forwarding reference and insert_hint_unique guarantees that `key` is
            // never referenced after consuming `args`.
            const key_type& key_ref = k;
            return this->tree_
                .insert_hint_unique(iterator(hint), key_ref, std::piecewise_construct,
                                    std::forward_as_tuple(std::move(k)),
                                    std::forward_as_tuple(std::forward<Args>(args)...))
                .first;
        }
        mapped_type &operator[](const key_type &k) {
            return try_emplace(k).first->second;
        }
        mapped_type &operator[](key_type &&k) {
            return try_emplace(std::move(k)).first->second;
        }

        template <typename K = key_type>
        mapped_type &at(const key_arg<K> &key) {
            auto it = this->find(key);
            if (it == this->end())
                base_internal::ThrowStdOutOfRange("phmap::btree_map::at");
            return it->second;
        }
        template <typename K = key_type>
        const mapped_type &at(const key_arg<K> &key) const {
            auto it = this->find(key);
            if (it == this->end())
                base_internal::ThrowStdOutOfRange("phmap::btree_map::at");
            return it->second;
        }
    };

    // A common base class for btree_multiset and btree_multimap.
    template <typename Tree>
    class btree_multiset_container : public btree_container<Tree> {
        using super_type = btree_container<Tree>;
        using params_type = typename Tree::params_type;
        using init_type = typename params_type::init_type;
        using is_key_compare_to = typename params_type::is_key_compare_to;

        template <class K>
        using key_arg = typename super_type::template key_arg<K>;

    public:
        using key_type = typename Tree::key_type;
        using value_type = typename Tree::value_type;
        using size_type = typename Tree::size_type;
        using key_compare = typename Tree::key_compare;
        using allocator_type = typename Tree::allocator_type;
        using iterator = typename Tree::iterator;
        using const_iterator = typename Tree::const_iterator;
        using node_type = typename super_type::node_type;

        // Inherit constructors.
        using super_type::super_type;
        btree_multiset_container() {}

        // Range constructor.
        template <class InputIterator>
        btree_multiset_container(InputIterator b, InputIterator e,
                                 const key_compare &comp = key_compare(),
                                 const allocator_type &alloc = allocator_type())
            : super_type(comp, alloc) {
            insert(b, e);
        }

        // Initializer list constructor.
        btree_multiset_container(std::initializer_list<init_type> init,
                                 const key_compare &comp = key_compare(),
                                 const allocator_type &alloc = allocator_type())
            : btree_multiset_container(init.begin(), init.end(), comp, alloc) {}

        // Lookup routines.
        template <typename K = key_type>
        size_type count(const key_arg<K> &key) const {
            return this->tree_.count_multi(key);
        }

        // Insertion routines.
        iterator insert(const value_type &x) { return this->tree_.insert_multi(x); }
        iterator insert(value_type &&x) {
            return this->tree_.insert_multi(std::move(x));
        }
        iterator insert(const_iterator hint, const value_type &x) {
            return this->tree_.insert_hint_multi(iterator(hint), x);
        }
        iterator insert(const_iterator hint, value_type &&x) {
            return this->tree_.insert_hint_multi(iterator(hint), std::move(x));
        }
        template <typename InputIterator>
        void insert(InputIterator b, InputIterator e) {
            this->tree_.insert_iterator_multi(b, e);
        }
        void insert(std::initializer_list<init_type> init) {
            this->tree_.insert_iterator_multi(init.begin(), init.end());
        }
        template <typename... Args>
        iterator emplace(Args &&... args) {
            return this->tree_.insert_multi(init_type(std::forward<Args>(args)...));
        }
        template <typename... Args>
        iterator emplace_hint(const_iterator hint, Args &&... args) {
            return this->tree_.insert_hint_multi(
                iterator(hint), init_type(std::forward<Args>(args)...));
        }
        iterator insert(node_type &&node) {
            if (!node) return this->end();
            iterator res =
                this->tree_.insert_multi(params_type::key(CommonAccess::GetSlot(node)),
                                         CommonAccess::GetSlot(node));
            CommonAccess::Destroy(&node);
            return res;
        }
        iterator insert(const_iterator hint, node_type &&node) {
            if (!node) return this->end();
            iterator res = this->tree_.insert_hint_multi(
                iterator(hint),
                std::move(params_type::element(CommonAccess::GetSlot(node))));
            CommonAccess::Destroy(&node);
            return res;
        }

        // Deletion routines.
        template <typename K = key_type>
        size_type erase(const key_arg<K> &key) {
            return this->tree_.erase_multi(key);
        }
        using super_type::erase;

        // Node extraction routines.
        template <typename K = key_type>
        node_type extract(const key_arg<K> &key) {
            auto it = this->find(key);
            return it == this->end() ? node_type() : extract(it);
        }
        using super_type::extract;

        // Merge routines.
        // Moves all elements from `src` into `this`.
        template <
            typename T,
            typename phmap::enable_if_t<
                phmap::conjunction<
                    std::is_same<value_type, typename T::value_type>,
                    std::is_same<allocator_type, typename T::allocator_type>,
                    std::is_same<typename params_type::is_map_container,
                                 typename T::params_type::is_map_container>>::value,
                int> = 0>
        void merge(btree_container<T> &src) {  // NOLINT
            insert(std::make_move_iterator(src.begin()),
                   std::make_move_iterator(src.end()));
            src.clear();
        }

        template <
            typename T,
            typename phmap::enable_if_t<
                phmap::conjunction<
                    std::is_same<value_type, typename T::value_type>,
                    std::is_same<allocator_type, typename T::allocator_type>,
                    std::is_same<typename params_type::is_map_container,
                                 typename T::params_type::is_map_container>>::value,
                int> = 0>
        void merge(btree_container<T> &&src) {
            merge(src);
        }
    };

    // A base class for btree_multimap.
    template <typename Tree>
    class btree_multimap_container : public btree_multiset_container<Tree> {
        using super_type = btree_multiset_container<Tree>;
        using params_type = typename Tree::params_type;

    public:
        using mapped_type = typename params_type::mapped_type;

        // Inherit constructors.
        using super_type::super_type;
        btree_multimap_container() {}
    };

}  // namespace priv



    // ----------------------------------------------------------------------
    //  btree_set - default values in phmap_fwd_decl.h
    // ----------------------------------------------------------------------
    template <typename Key, typename Compare, typename Alloc>
    class btree_set : public priv::btree_set_container<
        priv::btree<priv::set_params<
            Key, Compare, Alloc, /*TargetNodeSize=*/ 256, /*Multi=*/ false>>> 
    {
        using Base = typename btree_set::btree_set_container;

    public:
        btree_set() {}
        using Base::Base;
        using Base::begin;
        using Base::cbegin;
        using Base::end;
        using Base::cend;
        using Base::empty;
        using Base::max_size;
        using Base::size;
        using Base::clear;
        using Base::erase;
        using Base::insert;
        using Base::emplace;
        using Base::emplace_hint;
        using Base::extract;
        using Base::merge;
        using Base::swap;
        using Base::contains;
        using Base::count;
        using Base::equal_range;
        using Base::lower_bound;
        using Base::upper_bound;
        using Base::find;
        using Base::get_allocator;
        using Base::key_comp;
        using Base::value_comp;
    };

    // Swaps the contents of two `phmap::btree_set` containers.
    // -------------------------------------------------------
    template <typename K, typename C, typename A>
    void swap(btree_set<K, C, A> &x, btree_set<K, C, A> &y) {
        return x.swap(y);
    }

    // Erases all elements that satisfy the predicate pred from the container.
    // ----------------------------------------------------------------------
    template <typename K, typename C, typename A, typename Pred>
    void erase_if(btree_set<K, C, A> &set, Pred pred) {
        for (auto it = set.begin(); it != set.end();) {
            if (pred(*it)) {
                it = set.erase(it);
            } else {
                ++it;
            }
        }
    }

    // ----------------------------------------------------------------------
    //  btree_multiset - default values in phmap_fwd_decl.h
    // ----------------------------------------------------------------------
    template <typename Key, typename Compare,  typename Alloc>
        class btree_multiset : public priv::btree_multiset_container<
        priv::btree<priv::set_params<
             Key, Compare, Alloc, /*TargetNodeSize=*/ 256, /*Multi=*/ true>>> 
    {
        using Base = typename btree_multiset::btree_multiset_container;
        
    public:
        btree_multiset() {}
        using Base::Base;
        using Base::begin;
        using Base::cbegin;
        using Base::end;
        using Base::cend;
        using Base::empty;
        using Base::max_size;
        using Base::size;
        using Base::clear;
        using Base::erase;
        using Base::insert;
        using Base::emplace;
        using Base::emplace_hint;
        using Base::extract;
        using Base::merge;
        using Base::swap;
        using Base::contains;
        using Base::count;
        using Base::equal_range;
        using Base::lower_bound;
        using Base::upper_bound;
        using Base::find;
        using Base::get_allocator;
        using Base::key_comp;
        using Base::value_comp;
    };

    // Swaps the contents of two `phmap::btree_multiset` containers.
    // ------------------------------------------------------------
    template <typename K, typename C, typename A>
    void swap(btree_multiset<K, C, A> &x, btree_multiset<K, C, A> &y) {
        return x.swap(y);
    }
    
    // Erases all elements that satisfy the predicate pred from the container.
    // ----------------------------------------------------------------------
    template <typename K, typename C, typename A, typename Pred>
    void erase_if(btree_multiset<K, C, A> &set, Pred pred) {
        for (auto it = set.begin(); it != set.end();) {
            if (pred(*it)) {
                it = set.erase(it);
            } else {
                ++it;
            }
        }
    }


    // ----------------------------------------------------------------------
    //  btree_map - default values in phmap_fwd_decl.h
    // ----------------------------------------------------------------------
    template <typename Key, typename Value, typename Compare,  typename Alloc>
        class btree_map : public priv::btree_map_container<
        priv::btree<priv::map_params<
             Key, Value, Compare, Alloc, /*TargetNodeSize=*/ 256, /*Multi=*/ false>>> 
    {
        using Base = typename btree_map::btree_map_container;

    public:
        btree_map() {}
        using Base::Base;
        using Base::begin;
        using Base::cbegin;
        using Base::end;
        using Base::cend;
        using Base::empty;
        using Base::max_size;
        using Base::size;
        using Base::clear;
        using Base::erase;
        using Base::insert;
        using Base::emplace;
        using Base::emplace_hint;
        using Base::try_emplace;
        using Base::extract;
        using Base::merge;
        using Base::swap;
        using Base::at;
        using Base::contains;
        using Base::count;
        using Base::equal_range;
        using Base::lower_bound;
        using Base::upper_bound;
        using Base::find;
        using Base::operator[];
        using Base::get_allocator;
        using Base::key_comp;
        using Base::value_comp;
    };

    // Swaps the contents of two `phmap::btree_map` containers.
    // -------------------------------------------------------
    template <typename K, typename V, typename C, typename A>
    void swap(btree_map<K, V, C, A> &x, btree_map<K, V, C, A> &y) {
        return x.swap(y);
    }

    // ----------------------------------------------------------------------
    template <typename K, typename V, typename C, typename A, typename Pred>
    void erase_if(btree_map<K, V, C, A> &map, Pred pred) {
        for (auto it = map.begin(); it != map.end();) {
            if (pred(*it)) {
                it = map.erase(it);
            } else {
                ++it;
            }
        }
    }

    // ----------------------------------------------------------------------
    //  btree_multimap - default values in phmap_fwd_decl.h
    // ----------------------------------------------------------------------
    template <typename Key, typename Value, typename Compare, typename Alloc>
        class btree_multimap : public priv::btree_multimap_container<
        priv::btree<priv::map_params<
              Key, Value, Compare, Alloc, /*TargetNodeSize=*/ 256, /*Multi=*/ true>>> 
    {
        using Base = typename btree_multimap::btree_multimap_container;

    public:
        btree_multimap() {}
        using Base::Base;
        using Base::begin;
        using Base::cbegin;
        using Base::end;
        using Base::cend;
        using Base::empty;
        using Base::max_size;
        using Base::size;
        using Base::clear;
        using Base::erase;
        using Base::insert;
        using Base::emplace;
        using Base::emplace_hint;
        using Base::extract;
        using Base::merge;
        using Base::swap;
        using Base::contains;
        using Base::count;
        using Base::equal_range;
        using Base::lower_bound;
        using Base::upper_bound;
        using Base::find;
        using Base::get_allocator;
        using Base::key_comp;
        using Base::value_comp;
    };

    // Swaps the contents of two `phmap::btree_multimap` containers.
    // ------------------------------------------------------------
    template <typename K, typename V, typename C, typename A>
    void swap(btree_multimap<K, V, C, A> &x, btree_multimap<K, V, C, A> &y) {
        return x.swap(y);
    }

    // Erases all elements that satisfy the predicate pred from the container.
    // ----------------------------------------------------------------------
    template <typename K, typename V, typename C, typename A, typename Pred>
    void erase_if(btree_multimap<K, V, C, A> &map, Pred pred) {
        for (auto it = map.begin(); it != map.end();) {
            if (pred(*it)) {
                it = map.erase(it);
            } else {
                ++it;
            }
        }
    }


}  // namespace btree

#ifdef _MSC_VER
     #pragma warning(pop)  
#endif


#endif  // PHMAP_BTREE_BTREE_CONTAINER_H_
