/*
 * Copyright 2017 Google Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FLATBUFFERS_STL_EMULATION_H_
#define FLATBUFFERS_STL_EMULATION_H_

// clang-format off
#include "flatbuffers/base.h"

#include <string>
#include <type_traits>
#include <vector>
#include <memory>
#include <limits>

#if defined(_STLPORT_VERSION) && !defined(FLATBUFFERS_CPP98_STL)
  #define FLATBUFFERS_CPP98_STL
#endif  // defined(_STLPORT_VERSION) && !defined(FLATBUFFERS_CPP98_STL)

#if defined(FLATBUFFERS_CPP98_STL)
  #include <cctype>
#endif  // defined(FLATBUFFERS_CPP98_STL)

// Detect C++17 compatible compiler.
// __cplusplus >= 201703L - a compiler has support of 'static inline' variables.
#if defined(FLATBUFFERS_USE_STD_OPTIONAL) \
    || (defined(__cplusplus) && __cplusplus >= 201703L) \
    || (defined(_MSVC_LANG) &&  (_MSVC_LANG >= 201703L))
  #include <optional>
  #ifndef FLATBUFFERS_USE_STD_OPTIONAL
    #define FLATBUFFERS_USE_STD_OPTIONAL
  #endif
#endif // defined(FLATBUFFERS_USE_STD_OPTIONAL) ...

// The __cpp_lib_span is the predefined feature macro.
#if defined(FLATBUFFERS_USE_STD_SPAN)
    #include <span>
#elif defined(__cpp_lib_span) && defined(__has_include)
  #if __has_include(<span>)
    #include <span>
    #define FLATBUFFERS_USE_STD_SPAN
  #endif
#else
  // Disable non-trivial ctors if FLATBUFFERS_SPAN_MINIMAL defined.
  #if !defined(FLATBUFFERS_TEMPLATES_ALIASES) || defined(FLATBUFFERS_CPP98_STL)
    #define FLATBUFFERS_SPAN_MINIMAL
  #else
    // Enable implicit construction of a span<T,N> from a std::array<T,N>.
    #include <array>
  #endif
#endif // defined(FLATBUFFERS_USE_STD_SPAN)

// This header provides backwards compatibility for C++98 STLs like stlport.
namespace flatbuffers {

// Retrieve ::back() from a string in a way that is compatible with pre C++11
// STLs (e.g stlport).
inline char& string_back(std::string &value) {
  return value[value.length() - 1];
}

inline char string_back(const std::string &value) {
  return value[value.length() - 1];
}

// Helper method that retrieves ::data() from a vector in a way that is
// compatible with pre C++11 STLs (e.g stlport).
template <typename T> inline T *vector_data(std::vector<T> &vector) {
  // In some debug environments, operator[] does bounds checking, so &vector[0]
  // can't be used.
  return vector.empty() ? nullptr : &vector[0];
}

template <typename T> inline const T *vector_data(
    const std::vector<T> &vector) {
  return vector.empty() ? nullptr : &vector[0];
}

template <typename T, typename V>
inline void vector_emplace_back(std::vector<T> *vector, V &&data) {
  #if defined(FLATBUFFERS_CPP98_STL)
    vector->push_back(data);
  #else
    vector->emplace_back(std::forward<V>(data));
  #endif  // defined(FLATBUFFERS_CPP98_STL)
}

#ifndef FLATBUFFERS_CPP98_STL
  #if defined(FLATBUFFERS_TEMPLATES_ALIASES)
    template <typename T>
    using numeric_limits = std::numeric_limits<T>;
  #else
    template <typename T> class numeric_limits :
      public std::numeric_limits<T> {};
  #endif  // defined(FLATBUFFERS_TEMPLATES_ALIASES)
#else
  template <typename T> class numeric_limits :
      public std::numeric_limits<T> {
    public:
      // Android NDK fix.
      static T lowest() {
        return std::numeric_limits<T>::min();
      }
  };

  template <> class numeric_limits<float> :
      public std::numeric_limits<float> {
    public:
      static float lowest() { return -FLT_MAX; }
  };

  template <> class numeric_limits<double> :
      public std::numeric_limits<double> {
    public:
      static double lowest() { return -DBL_MAX; }
  };

  template <> class numeric_limits<unsigned long long> {
   public:
    static unsigned long long min() { return 0ULL; }
    static unsigned long long max() { return ~0ULL; }
    static unsigned long long lowest() {
      return numeric_limits<unsigned long long>::min();
    }
  };

  template <> class numeric_limits<long long> {
   public:
    static long long min() {
      return static_cast<long long>(1ULL << ((sizeof(long long) << 3) - 1));
    }
    static long long max() {
      return static_cast<long long>(
          (1ULL << ((sizeof(long long) << 3) - 1)) - 1);
    }
    static long long lowest() {
      return numeric_limits<long long>::min();
    }
  };
#endif  // FLATBUFFERS_CPP98_STL

#if defined(FLATBUFFERS_TEMPLATES_ALIASES)
  #ifndef FLATBUFFERS_CPP98_STL
    template <typename T> using is_scalar = std::is_scalar<T>;
    template <typename T, typename U> using is_same = std::is_same<T,U>;
    template <typename T> using is_floating_point = std::is_floating_point<T>;
    template <typename T> using is_unsigned = std::is_unsigned<T>;
    template <typename T> using is_enum = std::is_enum<T>;
    template <typename T> using make_unsigned = std::make_unsigned<T>;
    template<bool B, class T, class F>
    using conditional = std::conditional<B, T, F>;
    template<class T, T v>
    using integral_constant = std::integral_constant<T, v>;
    template <bool B>
    using bool_constant = integral_constant<bool, B>;
  #else
    // Map C++ TR1 templates defined by stlport.
    template <typename T> using is_scalar = std::tr1::is_scalar<T>;
    template <typename T, typename U> using is_same = std::tr1::is_same<T,U>;
    template <typename T> using is_floating_point =
        std::tr1::is_floating_point<T>;
    template <typename T> using is_unsigned = std::tr1::is_unsigned<T>;
    template <typename T> using is_enum = std::tr1::is_enum<T>;
    // Android NDK doesn't have std::make_unsigned or std::tr1::make_unsigned.
    template<typename T> struct make_unsigned {
      static_assert(is_unsigned<T>::value, "Specialization not implemented!");
      using type = T;
    };
    template<> struct make_unsigned<char> { using type = unsigned char; };
    template<> struct make_unsigned<short> { using type = unsigned short; };
    template<> struct make_unsigned<int> { using type = unsigned int; };
    template<> struct make_unsigned<long> { using type = unsigned long; };
    template<>
    struct make_unsigned<long long> { using type = unsigned long long; };
    template<bool B, class T, class F>
    using conditional = std::tr1::conditional<B, T, F>;
    template<class T, T v>
    using integral_constant = std::tr1::integral_constant<T, v>;
    template <bool B>
    using bool_constant = integral_constant<bool, B>;
  #endif  // !FLATBUFFERS_CPP98_STL
#else
  // MSVC 2010 doesn't support C++11 aliases.
  template <typename T> struct is_scalar : public std::is_scalar<T> {};
  template <typename T, typename U> struct is_same : public std::is_same<T,U> {};
  template <typename T> struct is_floating_point :
        public std::is_floating_point<T> {};
  template <typename T> struct is_unsigned : public std::is_unsigned<T> {};
  template <typename T> struct is_enum : public std::is_enum<T> {};
  template <typename T> struct make_unsigned : public std::make_unsigned<T> {};
  template<bool B, class T, class F>
  struct conditional : public std::conditional<B, T, F> {};
  template<class T, T v>
  struct integral_constant : public std::integral_constant<T, v> {};
  template <bool B>
  struct bool_constant : public integral_constant<bool, B> {};
#endif  // defined(FLATBUFFERS_TEMPLATES_ALIASES)

#ifndef FLATBUFFERS_CPP98_STL
  #if defined(FLATBUFFERS_TEMPLATES_ALIASES)
    template <class T> using unique_ptr = std::unique_ptr<T>;
  #else
    // MSVC 2010 doesn't support C++11 aliases.
    // We're manually "aliasing" the class here as we want to bring unique_ptr
    // into the flatbuffers namespace.  We have unique_ptr in the flatbuffers
    // namespace we have a completely independent implementation (see below)
    // for C++98 STL implementations.
    template <class T> class unique_ptr : public std::unique_ptr<T> {
     public:
      unique_ptr() {}
      explicit unique_ptr(T* p) : std::unique_ptr<T>(p) {}
      unique_ptr(std::unique_ptr<T>&& u) { *this = std::move(u); }
      unique_ptr(unique_ptr&& u) { *this = std::move(u); }
      unique_ptr& operator=(std::unique_ptr<T>&& u) {
        std::unique_ptr<T>::reset(u.release());
        return *this;
      }
      unique_ptr& operator=(unique_ptr&& u) {
        std::unique_ptr<T>::reset(u.release());
        return *this;
      }
      unique_ptr& operator=(T* p) {
        return std::unique_ptr<T>::operator=(p);
      }
    };
  #endif  // defined(FLATBUFFERS_TEMPLATES_ALIASES)
#else
  // Very limited implementation of unique_ptr.
  // This is provided simply to allow the C++ code generated from the default
  // settings to function in C++98 environments with no modifications.
  template <class T> class unique_ptr {
   public:
    typedef T element_type;

    unique_ptr() : ptr_(nullptr) {}
    explicit unique_ptr(T* p) : ptr_(p) {}
    unique_ptr(unique_ptr&& u) : ptr_(nullptr) { reset(u.release()); }
    unique_ptr(const unique_ptr& u) : ptr_(nullptr) {
      reset(const_cast<unique_ptr*>(&u)->release());
    }
    ~unique_ptr() { reset(); }

    unique_ptr& operator=(const unique_ptr& u) {
      reset(const_cast<unique_ptr*>(&u)->release());
      return *this;
    }

    unique_ptr& operator=(unique_ptr&& u) {
      reset(u.release());
      return *this;
    }

    unique_ptr& operator=(T* p) {
      reset(p);
      return *this;
    }

    const T& operator*() const { return *ptr_; }
    T* operator->() const { return ptr_; }
    T* get() const noexcept { return ptr_; }
    explicit operator bool() const { return ptr_ != nullptr; }

    // modifiers
    T* release() {
      T* value = ptr_;
      ptr_ = nullptr;
      return value;
    }

    void reset(T* p = nullptr) {
      T* value = ptr_;
      ptr_ = p;
      if (value) delete value;
    }

    void swap(unique_ptr& u) {
      T* temp_ptr = ptr_;
      ptr_ = u.ptr_;
      u.ptr_ = temp_ptr;
    }

   private:
    T* ptr_;
  };

  template <class T> bool operator==(const unique_ptr<T>& x,
                                     const unique_ptr<T>& y) {
    return x.get() == y.get();
  }

  template <class T, class D> bool operator==(const unique_ptr<T>& x,
                                              const D* y) {
    return static_cast<D*>(x.get()) == y;
  }

  template <class T> bool operator==(const unique_ptr<T>& x, intptr_t y) {
    return reinterpret_cast<intptr_t>(x.get()) == y;
  }

  template <class T> bool operator!=(const unique_ptr<T>& x, decltype(nullptr)) {
    return !!x;
  }

  template <class T> bool operator!=(decltype(nullptr), const unique_ptr<T>& x) {
    return !!x;
  }

  template <class T> bool operator==(const unique_ptr<T>& x, decltype(nullptr)) {
    return !x;
  }

  template <class T> bool operator==(decltype(nullptr), const unique_ptr<T>& x) {
    return !x;
  }

#endif  // !FLATBUFFERS_CPP98_STL

#ifdef FLATBUFFERS_USE_STD_OPTIONAL
template<class T>
using Optional = std::optional<T>;
using nullopt_t = std::nullopt_t;
inline constexpr nullopt_t nullopt = std::nullopt;

#else
// Limited implementation of Optional<T> type for a scalar T.
// This implementation limited by trivial types compatible with
// std::is_arithmetic<T> or std::is_enum<T> type traits.

// A tag to indicate an empty flatbuffers::optional<T>.
struct nullopt_t {
  explicit FLATBUFFERS_CONSTEXPR_CPP11 nullopt_t(int) {}
};

#if defined(FLATBUFFERS_CONSTEXPR_DEFINED)
  namespace internal {
    template <class> struct nullopt_holder {
      static constexpr nullopt_t instance_ = nullopt_t(0);
    };
    template<class Dummy>
    constexpr nullopt_t nullopt_holder<Dummy>::instance_;
  }
  static constexpr const nullopt_t &nullopt = internal::nullopt_holder<void>::instance_;

#else
  namespace internal {
    template <class> struct nullopt_holder {
      static const nullopt_t instance_;
    };
    template<class Dummy>
    const nullopt_t nullopt_holder<Dummy>::instance_  = nullopt_t(0);
  }
  static const nullopt_t &nullopt = internal::nullopt_holder<void>::instance_;

#endif

template<class T>
class Optional FLATBUFFERS_FINAL_CLASS {
  // Non-scalar 'T' would extremely complicated Optional<T>.
  // Use is_scalar<T> checking because flatbuffers flatbuffers::is_arithmetic<T>
  // isn't implemented.
  static_assert(flatbuffers::is_scalar<T>::value, "unexpected type T");

 public:
  ~Optional() {}

  FLATBUFFERS_CONSTEXPR_CPP11 Optional() FLATBUFFERS_NOEXCEPT
    : value_(), has_value_(false) {}

  FLATBUFFERS_CONSTEXPR_CPP11 Optional(nullopt_t) FLATBUFFERS_NOEXCEPT
    : value_(), has_value_(false) {}

  FLATBUFFERS_CONSTEXPR_CPP11 Optional(T val) FLATBUFFERS_NOEXCEPT
    : value_(val), has_value_(true) {}

  FLATBUFFERS_CONSTEXPR_CPP11 Optional(const Optional &other) FLATBUFFERS_NOEXCEPT
    : value_(other.value_), has_value_(other.has_value_) {}

  FLATBUFFERS_CONSTEXPR_CPP14 Optional &operator=(const Optional &other) FLATBUFFERS_NOEXCEPT {
    value_ = other.value_;
    has_value_ = other.has_value_;
    return *this;
  }

  FLATBUFFERS_CONSTEXPR_CPP14 Optional &operator=(nullopt_t) FLATBUFFERS_NOEXCEPT {
    value_ = T();
    has_value_ = false;
    return *this;
  }

  FLATBUFFERS_CONSTEXPR_CPP14 Optional &operator=(T val) FLATBUFFERS_NOEXCEPT {
    value_ = val;
    has_value_ = true;
    return *this;
  }

  void reset() FLATBUFFERS_NOEXCEPT {
    *this = nullopt;
  }

  void swap(Optional &other) FLATBUFFERS_NOEXCEPT {
    std::swap(value_, other.value_);
    std::swap(has_value_, other.has_value_);
  }

  FLATBUFFERS_CONSTEXPR_CPP11 FLATBUFFERS_EXPLICIT_CPP11 operator bool() const FLATBUFFERS_NOEXCEPT {
    return has_value_;
  }

  FLATBUFFERS_CONSTEXPR_CPP11 bool has_value() const FLATBUFFERS_NOEXCEPT {
    return has_value_;
  }

  FLATBUFFERS_CONSTEXPR_CPP11 const T& operator*() const FLATBUFFERS_NOEXCEPT {
    return value_;
  }

  const T& value() const {
    FLATBUFFERS_ASSERT(has_value());
    return value_;
  }

  T value_or(T default_value) const FLATBUFFERS_NOEXCEPT {
    return has_value() ? value_ : default_value;
  }

 private:
  T value_;
  bool has_value_;
};

template<class T>
FLATBUFFERS_CONSTEXPR_CPP11 bool operator==(const Optional<T>& opt, nullopt_t) FLATBUFFERS_NOEXCEPT {
  return !opt;
}
template<class T>
FLATBUFFERS_CONSTEXPR_CPP11 bool operator==(nullopt_t, const Optional<T>& opt) FLATBUFFERS_NOEXCEPT {
  return !opt;
}

template<class T, class U>
FLATBUFFERS_CONSTEXPR_CPP11 bool operator==(const Optional<T>& lhs, const U& rhs) FLATBUFFERS_NOEXCEPT {
  return static_cast<bool>(lhs) && (*lhs == rhs);
}

template<class T, class U>
FLATBUFFERS_CONSTEXPR_CPP11 bool operator==(const T& lhs, const Optional<U>& rhs) FLATBUFFERS_NOEXCEPT {
  return static_cast<bool>(rhs) && (lhs == *rhs);
}

template<class T, class U>
FLATBUFFERS_CONSTEXPR_CPP11 bool operator==(const Optional<T>& lhs, const Optional<U>& rhs) FLATBUFFERS_NOEXCEPT {
  return static_cast<bool>(lhs) != static_cast<bool>(rhs)
              ? false
              : !static_cast<bool>(lhs) ? false : (*lhs == *rhs);
}
#endif // FLATBUFFERS_USE_STD_OPTIONAL


// Very limited and naive partial implementation of C++20 std::span<T,Extent>.
#if defined(FLATBUFFERS_USE_STD_SPAN)
  inline constexpr std::size_t dynamic_extent = std::dynamic_extent;
  template<class T, std::size_t Extent = std::dynamic_extent>
  using span = std::span<T, Extent>;

#else // !defined(FLATBUFFERS_USE_STD_SPAN)
FLATBUFFERS_CONSTEXPR std::size_t dynamic_extent = static_cast<std::size_t>(-1);

// Exclude this code if MSVC2010 or non-STL Android is active.
// The non-STL Android doesn't have `std::is_convertible` required for SFINAE.
#if !defined(FLATBUFFERS_SPAN_MINIMAL)
namespace internal {
  // This is SFINAE helper class for checking of a common condition:
  // > This overload only participates in overload resolution
  // > Check whether a pointer to an array of U can be converted
  // > to a pointer to an array of E.
  // This helper is used for checking of 'U -> const U'.
  template<class E, std::size_t Extent, class U, std::size_t N>
  struct is_span_convertable {
    using type =
      typename std::conditional<std::is_convertible<U (*)[], E (*)[]>::value
                                && (Extent == dynamic_extent || N == Extent),
                                int, void>::type;
  };

}  // namespace internal
#endif  // !defined(FLATBUFFERS_SPAN_MINIMAL)

// T - element type; must be a complete type that is not an abstract
// class type.
// Extent - the number of elements in the sequence, or dynamic.
template<class T, std::size_t Extent = dynamic_extent>
class span FLATBUFFERS_FINAL_CLASS {
 public:
  typedef T element_type;
  typedef T& reference;
  typedef const T& const_reference;
  typedef T* pointer;
  typedef const T* const_pointer;
  typedef std::size_t size_type;

  static FLATBUFFERS_CONSTEXPR size_type extent = Extent;

  // Returns the number of elements in the span.
  FLATBUFFERS_CONSTEXPR_CPP11 size_type size() const FLATBUFFERS_NOEXCEPT {
    return count_;
  }

  // Returns the size of the sequence in bytes.
  FLATBUFFERS_CONSTEXPR_CPP11
  size_type size_bytes() const FLATBUFFERS_NOEXCEPT {
    return size() * sizeof(element_type);
  }

  // Checks if the span is empty.
  FLATBUFFERS_CONSTEXPR_CPP11 bool empty() const FLATBUFFERS_NOEXCEPT {
    return size() == 0;
  }

  // Returns a pointer to the beginning of the sequence.
  FLATBUFFERS_CONSTEXPR_CPP11 pointer data() const FLATBUFFERS_NOEXCEPT {
    return data_;
  }

  // Returns a reference to the idx-th element of the sequence.
  // The behavior is undefined if the idx is greater than or equal to size().
  FLATBUFFERS_CONSTEXPR_CPP11 reference operator[](size_type idx) const {
    return data()[idx];
  }

  FLATBUFFERS_CONSTEXPR_CPP11 span(const span &other) FLATBUFFERS_NOEXCEPT
      : data_(other.data_), count_(other.count_) {}

  FLATBUFFERS_CONSTEXPR_CPP14 span &operator=(const span &other)
      FLATBUFFERS_NOEXCEPT {
    data_ = other.data_;
    count_ = other.count_;
  }

  // Limited implementation of
  // `template <class It> constexpr std::span(It first, size_type count);`.
  //
  // Constructs a span that is a view over the range [first, first + count);
  // the resulting span has: data() == first and size() == count.
  // The behavior is undefined if [first, first + count) is not a valid range,
  // or if (extent != flatbuffers::dynamic_extent && count != extent).
  FLATBUFFERS_CONSTEXPR_CPP11
  explicit span(pointer first, size_type count) FLATBUFFERS_NOEXCEPT
    : data_ (Extent == dynamic_extent ? first : (Extent == count ? first : nullptr)),
      count_(Extent == dynamic_extent ? count : (Extent == count ? Extent : 0)) {
      // Make span empty if the count argument is incompatible with span<T,N>.
  }

  // Exclude this code if MSVC2010 is active. The MSVC2010 isn't C++11
  // compliant, it doesn't support default template arguments for functions.
  #if defined(FLATBUFFERS_SPAN_MINIMAL)
  FLATBUFFERS_CONSTEXPR_CPP11 span() FLATBUFFERS_NOEXCEPT : data_(nullptr),
                                                            count_(0) {
    static_assert(extent == 0 || extent == dynamic_extent, "invalid span");
  }

  #else
  // Constructs an empty span whose data() == nullptr and size() == 0.
  // This overload only participates in overload resolution if
  // extent == 0 || extent == flatbuffers::dynamic_extent.
  // A dummy template argument N is need dependency for SFINAE.
  template<std::size_t N = 0,
    typename internal::is_span_convertable<element_type, Extent, element_type, (N - N)>::type = 0>
  FLATBUFFERS_CONSTEXPR_CPP11 span() FLATBUFFERS_NOEXCEPT : data_(nullptr),
                                                            count_(0) {
    static_assert(extent == 0 || extent == dynamic_extent, "invalid span");
  }

  // Constructs a span that is a view over the array arr; the resulting span
  // has size() == N and data() == std::data(arr). These overloads only
  // participate in overload resolution if
  // extent == std::dynamic_extent || N == extent is true and
  // std::remove_pointer_t<decltype(std::data(arr))>(*)[]
  // is convertible to element_type (*)[].
  template<std::size_t N,
    typename internal::is_span_convertable<element_type, Extent, element_type, N>::type = 0>
  FLATBUFFERS_CONSTEXPR_CPP11 span(element_type (&arr)[N]) FLATBUFFERS_NOEXCEPT
      : data_(arr), count_(N) {}

  template<class U, std::size_t N,
    typename internal::is_span_convertable<element_type, Extent, U, N>::type = 0>
  FLATBUFFERS_CONSTEXPR_CPP11 span(std::array<U, N> &arr) FLATBUFFERS_NOEXCEPT
     : data_(arr.data()), count_(N) {}

  //template<class U, std::size_t N,
  //  int = 0>
  //FLATBUFFERS_CONSTEXPR_CPP11 span(std::array<U, N> &arr) FLATBUFFERS_NOEXCEPT
  //   : data_(arr.data()), count_(N) {}

  template<class U, std::size_t N,
    typename internal::is_span_convertable<element_type, Extent, U, N>::type = 0>
  FLATBUFFERS_CONSTEXPR_CPP11 span(const std::array<U, N> &arr) FLATBUFFERS_NOEXCEPT
    : data_(arr.data()), count_(N) {}

  // Converting constructor from another span s;
  // the resulting span has size() == s.size() and data() == s.data().
  // This overload only participates in overload resolution
  // if extent == std::dynamic_extent || N == extent is true and U (*)[]
  // is convertible to element_type (*)[].
  template<class U, std::size_t N,
    typename internal::is_span_convertable<element_type, Extent, U, N>::type = 0>
  FLATBUFFERS_CONSTEXPR_CPP11 span(const flatbuffers::span<U, N> &s) FLATBUFFERS_NOEXCEPT
      : span(s.data(), s.size()) {
  }

  #endif  // !defined(FLATBUFFERS_SPAN_MINIMAL)

 private:
  // This is a naive implementation with 'count_' member even if (Extent != dynamic_extent).
  pointer const data_;
  const size_type count_;
};

 #if !defined(FLATBUFFERS_SPAN_MINIMAL)
  template<class U, std::size_t N>
  FLATBUFFERS_CONSTEXPR_CPP11
  flatbuffers::span<U, N> make_span(U(&arr)[N]) FLATBUFFERS_NOEXCEPT {
    return span<U, N>(arr);
  }

  template<class U, std::size_t N>
  FLATBUFFERS_CONSTEXPR_CPP11
  flatbuffers::span<const U, N> make_span(const U(&arr)[N]) FLATBUFFERS_NOEXCEPT {
    return span<const U, N>(arr);
  }

  template<class U, std::size_t N>
  FLATBUFFERS_CONSTEXPR_CPP11
  flatbuffers::span<U, N> make_span(std::array<U, N> &arr) FLATBUFFERS_NOEXCEPT {
    return span<U, N>(arr);
  }

  template<class U, std::size_t N>
  FLATBUFFERS_CONSTEXPR_CPP11
  flatbuffers::span<const U, N> make_span(const std::array<U, N> &arr) FLATBUFFERS_NOEXCEPT {
    return span<const U, N>(arr);
  }

  template<class U, std::size_t N>
  FLATBUFFERS_CONSTEXPR_CPP11
  flatbuffers::span<U, dynamic_extent> make_span(U *first, std::size_t count) FLATBUFFERS_NOEXCEPT {
    return span<U, dynamic_extent>(first, count);
  }

  template<class U, std::size_t N>
  FLATBUFFERS_CONSTEXPR_CPP11
  flatbuffers::span<const U, dynamic_extent> make_span(const U *first, std::size_t count) FLATBUFFERS_NOEXCEPT {
    return span<const U, dynamic_extent>(first, count);
  }
#endif

#endif  // defined(FLATBUFFERS_USE_STD_SPAN)

}  // namespace flatbuffers

#endif  // FLATBUFFERS_STL_EMULATION_H_
