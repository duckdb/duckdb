// cpp11 version: 0.4.2
// vendored on: 2022-01-10
#pragma once

#include <csetjmp>    // for longjmp, setjmp, jmp_buf
#include <exception>  // for exception
#include <stdexcept>  // for std::runtime_error
#include <string>     // for string, basic_string
#include <tuple>      // for tuple, make_tuple

// NB: cpp11/R.hpp must precede R_ext/Error.h to ensure R_NO_REMAP is defined
#include "cpp11/R.hpp"  // for SEXP, SEXPREC, CDR, R_NilValue, CAR, R_Pres...

#include "R_ext/Boolean.h"  // for Rboolean
#include "R_ext/Error.h"    // for Rf_error, Rf_warning
#include "R_ext/Print.h"    // for REprintf
#include "R_ext/Utils.h"    // for R_CheckUserInterrupt
#include "Rversion.h"       // for R_VERSION, R_Version

#if defined(R_VERSION) && R_VERSION >= R_Version(3, 5, 0)
#define HAS_UNWIND_PROTECT
#endif

#ifdef CPP11_USE_FMT
#define FMT_HEADER_ONLY
#include "fmt/core.h"
#endif

namespace cpp11 {
class unwind_exception : public std::exception {
 public:
  SEXP token;
  unwind_exception(SEXP token_) : token(token_) {}
};

namespace detail {
// We deliberately avoid using safe[] in the below code, as this code runs
// when the shared library is loaded and will not be wrapped by
// `CPP11_UNWIND`, so if an error occurs we will not catch the C++ exception
// that safe emits.
inline void set_option(SEXP name, SEXP value) {
  static SEXP opt = SYMVALUE(Rf_install(".Options"));
  SEXP t = opt;
  while (CDR(t) != R_NilValue) {
    if (TAG(CDR(t)) == name) {
      opt = CDR(t);
      SET_TAG(opt, name);
      SETCAR(opt, value);
      return;
    }
    t = CDR(t);
  }
  SETCDR(t, Rf_allocList(1));
  opt = CDR(t);
  SET_TAG(opt, name);
  SETCAR(opt, value);
}

inline Rboolean& get_should_unwind_protect() {
  SEXP should_unwind_protect_sym = Rf_install("cpp11_should_unwind_protect");
  SEXP should_unwind_protect_sexp = Rf_GetOption1(should_unwind_protect_sym);
  if (should_unwind_protect_sexp == R_NilValue) {
    should_unwind_protect_sexp = PROTECT(Rf_allocVector(LGLSXP, 1));
    detail::set_option(should_unwind_protect_sym, should_unwind_protect_sexp);
    UNPROTECT(1);
  }

  Rboolean* should_unwind_protect =
      reinterpret_cast<Rboolean*>(LOGICAL(should_unwind_protect_sexp));
  should_unwind_protect[0] = TRUE;

  return should_unwind_protect[0];
}

}  // namespace detail

#ifdef HAS_UNWIND_PROTECT

/// Unwind Protection from C longjmp's, like those used in R error handling
///
/// @param code The code to which needs to be protected, as a nullary callable
template <typename Fun, typename = typename std::enable_if<std::is_same<
                            decltype(std::declval<Fun&&>()()), SEXP>::value>::type>
SEXP unwind_protect(Fun&& code) {
  static auto should_unwind_protect = detail::get_should_unwind_protect();
  if (should_unwind_protect == FALSE) {
    return std::forward<Fun>(code)();
  }

  should_unwind_protect = FALSE;

  static SEXP token = [] {
    SEXP res = R_MakeUnwindCont();
    R_PreserveObject(res);
    return res;
  }();

  std::jmp_buf jmpbuf;
  if (setjmp(jmpbuf)) {
    should_unwind_protect = TRUE;
    throw unwind_exception(token);
  }

  SEXP res = R_UnwindProtect(
      [](void* data) -> SEXP {
        auto callback = static_cast<decltype(&code)>(data);
        return static_cast<Fun&&>(*callback)();
      },
      &code,
      [](void* jmpbuf, Rboolean jump) {
        if (jump == TRUE) {
          // We need to first jump back into the C++ stacks because you can't safely
          // throw exceptions from C stack frames.
          longjmp(*static_cast<std::jmp_buf*>(jmpbuf), 1);
        }
      },
      &jmpbuf, token);

  // R_UnwindProtect adds the result to the CAR of the continuation token,
  // which implicitly protects the result. However if there is no error and
  // R_UwindProtect does a normal exit the memory shouldn't be protected, so we
  // unset it here before returning the value ourselves.
  SETCAR(token, R_NilValue);

  should_unwind_protect = TRUE;

  return res;
}

template <typename Fun, typename = typename std::enable_if<std::is_same<
                            decltype(std::declval<Fun&&>()()), void>::value>::type>
void unwind_protect(Fun&& code) {
  (void)unwind_protect([&] {
    std::forward<Fun>(code)();
    return R_NilValue;
  });
}

template <typename Fun, typename R = decltype(std::declval<Fun&&>()())>
typename std::enable_if<!std::is_same<R, SEXP>::value && !std::is_same<R, void>::value,
                        R>::type
unwind_protect(Fun&& code) {
  R out;
  (void)unwind_protect([&] {
    out = std::forward<Fun>(code)();
    return R_NilValue;
  });
  return out;
}

#else
// Don't do anything if we don't have unwind protect. This will leak C++ resources,
// including those held by cpp11 objects, but the other alternatives are also not great.
template <typename Fun>
decltype(std::declval<Fun&&>()()) unwind_protect(Fun&& code) {
  return std::forward<Fun>(code)();
}
#endif

namespace detail {

template <size_t...>
struct index_sequence {
  using type = index_sequence;
};

template <typename, size_t>
struct appended_sequence;

template <std::size_t... I, std::size_t J>
struct appended_sequence<index_sequence<I...>, J> : index_sequence<I..., J> {};

template <size_t N>
struct make_index_sequence
    : appended_sequence<typename make_index_sequence<N - 1>::type, N - 1> {};

template <>
struct make_index_sequence<0> : index_sequence<> {};

template <typename F, typename... Aref, size_t... I>
decltype(std::declval<F&&>()(std::declval<Aref>()...)) apply(
    F&& f, std::tuple<Aref...>&& a, const index_sequence<I...>&) {
  return std::forward<F>(f)(std::get<I>(std::move(a))...);
}

template <typename F, typename... Aref>
decltype(std::declval<F&&>()(std::declval<Aref>()...)) apply(F&& f,
                                                             std::tuple<Aref...>&& a) {
  return apply(std::forward<F>(f), std::move(a), make_index_sequence<sizeof...(Aref)>{});
}

// overload to silence a compiler warning that the (empty) tuple parameter is set but
// unused
template <typename F>
decltype(std::declval<F&&>()()) apply(F&& f, std::tuple<>&&) {
  return std::forward<F>(f)();
}

template <typename F, typename... Aref>
struct closure {
  decltype(std::declval<F*>()(std::declval<Aref>()...)) operator()() && {
    return apply(ptr_, std::move(arefs_));
  }
  F* ptr_;
  std::tuple<Aref...> arefs_;
};

}  // namespace detail

struct protect {
  template <typename F>
  struct function {
    template <typename... A>
    decltype(std::declval<F*>()(std::declval<A&&>()...)) operator()(A&&... a) const {
      // workaround to support gcc4.8, which can't capture a parameter pack
      return unwind_protect(
          detail::closure<F, A&&...>{ptr_, std::forward_as_tuple(std::forward<A>(a)...)});
    }

    F* ptr_;
  };

  /// May not be applied to a function bearing attributes, which interfere with linkage on
  /// some compilers; use an appropriately attributed alternative. (For example, Rf_error
  /// bears the [[noreturn]] attribute and must be protected with safe.noreturn rather
  /// than safe.operator[]).
  template <typename F>
  constexpr function<F> operator[](F* raw) const {
    return {raw};
  }

  template <typename F>
  struct noreturn_function {
    template <typename... A>
    void operator() [[noreturn]] (A&&... a) const {
      // workaround to support gcc4.8, which can't capture a parameter pack
      unwind_protect(
          detail::closure<F, A&&...>{ptr_, std::forward_as_tuple(std::forward<A>(a)...)});
      // Compiler hint to allow [[noreturn]] attribute; this is never executed since
      // the above call will not return.
      throw std::runtime_error("[[noreturn]]");
    }
    F* ptr_;
  };

  template <typename F>
  constexpr noreturn_function<F> noreturn(F* raw) const {
    return {raw};
  }
};
constexpr struct protect safe = {};

inline void check_user_interrupt() { safe[R_CheckUserInterrupt](); }

#ifdef CPP11_USE_FMT
template <typename... Args>
void stop [[noreturn]] (const char* fmt_arg, Args&&... args) {
  std::string msg = fmt::format(fmt_arg, std::forward<Args>(args)...);
  safe.noreturn(Rf_errorcall)(R_NilValue, "%s", msg.c_str());
}

template <typename... Args>
void stop [[noreturn]] (const std::string& fmt_arg, Args&&... args) {
  std::string msg = fmt::format(fmt_arg, std::forward<Args>(args)...);
  safe.noreturn(Rf_errorcall)(R_NilValue, "%s", msg.c_str());
}

template <typename... Args>
void warning(const char* fmt_arg, Args&&... args) {
  std::string msg = fmt::format(fmt_arg, std::forward<Args>(args)...);
  safe[Rf_warningcall](R_NilValue, "%s", msg.c_str());
}

template <typename... Args>
void warning(const std::string& fmt_arg, Args&&... args) {
  std::string msg = fmt::format(fmt_arg, std::forward<Args>(args)...);
  safe[Rf_warningcall](R_NilValue, "%s", msg.c_str());
}
#else
template <typename... Args>
void stop [[noreturn]] (const char* fmt, Args... args) {
  safe.noreturn(Rf_errorcall)(R_NilValue, fmt, args...);
}

template <typename... Args>
void stop [[noreturn]] (const std::string& fmt, Args... args) {
  safe.noreturn(Rf_errorcall)(R_NilValue, fmt.c_str(), args...);
}

template <typename... Args>
void warning(const char* fmt, Args... args) {
  safe[Rf_warningcall](R_NilValue, fmt, args...);
}

template <typename... Args>
void warning(const std::string& fmt, Args... args) {
  safe[Rf_warningcall](R_NilValue, fmt.c_str(), args...);
}
#endif

/// A doubly-linked list of preserved objects, allowing O(1) insertion/release of
/// objects compared to O(N preserved) with R_PreserveObject.
static struct {
  SEXP insert(SEXP obj) {
    if (obj == R_NilValue) {
      return R_NilValue;
    }

#ifdef CPP11_USE_PRESERVE_OBJECT
    PROTECT(obj);
    R_PreserveObject(obj);
    UNPROTECT(1);
    return obj;
#endif

    PROTECT(obj);

    static SEXP list_ = get_preserve_list();

    // Add a new cell that points to the previous end.
    SEXP cell = PROTECT(Rf_cons(list_, CDR(list_)));

    SET_TAG(cell, obj);

    SETCDR(list_, cell);

    if (CDR(cell) != R_NilValue) {
      SETCAR(CDR(cell), cell);
    }

    UNPROTECT(2);

    return cell;
  }

  void print() {
    static SEXP list_ = get_preserve_list();
    for (SEXP head = list_; head != R_NilValue; head = CDR(head)) {
      REprintf("%x CAR: %x CDR: %x TAG: %x\n", head, CAR(head), CDR(head), TAG(head));
    }
    REprintf("---\n");
  }

  // This is currently unused, but client packages could use it to free leaked resources
  // in older R versions if needed
  void release_all() {
#if !defined(CPP11_USE_PRESERVE_OBJECT)
    static SEXP list_ = get_preserve_list();
    SEXP first = CDR(list_);
    if (first != R_NilValue) {
      SETCAR(first, R_NilValue);
      SETCDR(list_, R_NilValue);
    }
#endif
  }

  void release(SEXP token) {
    if (token == R_NilValue) {
      return;
    }

#ifdef CPP11_USE_PRESERVE_OBJECT
    R_ReleaseObject(token);
    return;
#endif

    SEXP before = CAR(token);

    SEXP after = CDR(token);

    if (before == R_NilValue && after == R_NilValue) {
      Rf_error("should never happen");
    }

    SETCDR(before, after);

    if (after != R_NilValue) {
      SETCAR(after, before);
    }
  }

 private:
  // The preserved list singleton is stored in a XPtr within an R global option.
  //
  // It is not constructed as a static variable directly since many
  // translation units may be compiled, resulting in unrelated instances of each
  // static variable.
  //
  // We cannot store it in the cpp11 namespace, as cpp11 likely will not be loaded by
  // packages.
  // We cannot store it in R's global environment, as that is against CRAN
  // policies.
  // We instead store it as an XPtr in the global options, which avoids issues
  // both copying and serializing.
  static SEXP get_preserve_xptr_addr() {
    static SEXP preserve_xptr_sym = Rf_install("cpp11_preserve_xptr");
    SEXP preserve_xptr = Rf_GetOption1(preserve_xptr_sym);

    if (TYPEOF(preserve_xptr) != EXTPTRSXP) {
      return R_NilValue;
    }
    auto addr = R_ExternalPtrAddr(preserve_xptr);
    if (addr == nullptr) {
      return R_NilValue;
    }
    return static_cast<SEXP>(addr);
  }

  static void set_preserve_xptr(SEXP value) {
    static SEXP preserve_xptr_sym = Rf_install("cpp11_preserve_xptr");

    SEXP xptr = PROTECT(R_MakeExternalPtr(value, R_NilValue, R_NilValue));
    detail::set_option(preserve_xptr_sym, xptr);
    UNPROTECT(1);
  }

  static SEXP get_preserve_list() {
    static SEXP preserve_list = R_NilValue;

    if (TYPEOF(preserve_list) != LISTSXP) {
      preserve_list = get_preserve_xptr_addr();
      if (TYPEOF(preserve_list) != LISTSXP) {
        preserve_list = Rf_cons(R_NilValue, R_NilValue);
        R_PreserveObject(preserve_list);
        set_preserve_xptr(preserve_list);
      }
    }

    return preserve_list;
  }
}  // namespace cpp11
preserved;
}  // namespace cpp11
