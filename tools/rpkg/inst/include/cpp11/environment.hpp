// cpp11 version: 0.4.2
// vendored on: 2022-01-10
#pragma once

#include <string>  // for string, basic_string

#include "Rversion.h"         // for R_VERSION, R_Version
#include "cpp11/R.hpp"        // for SEXP, SEXPREC, Rf_install, Rf_findVarIn...
#include "cpp11/as.hpp"       // for as_sexp
#include "cpp11/protect.hpp"  // for protect, protect::function, safe, unwin...
#include "cpp11/sexp.hpp"     // for sexp

#if R_VERSION >= R_Version(4, 0, 0)
#define HAS_REMOVE_VAR_FROM_FRAME
#endif

#ifndef HAS_REMOVE_VAR_FROM_FRAME
#include "cpp11/function.hpp"
#endif

namespace cpp11 {

class environment {
 private:
  sexp env_;

  class proxy {
    SEXP parent_;
    SEXP name_;

   public:
    proxy(SEXP parent, SEXP name) : parent_(parent), name_(name) {}

    template <typename T>
    proxy& operator=(T value) {
      safe[Rf_defineVar](name_, as_sexp(value), parent_);
      return *this;
    }
    operator SEXP() const { return safe[Rf_findVarInFrame3](parent_, name_, TRUE); };
    operator sexp() const { return SEXP(); };
  };

 public:
  environment(SEXP env) : env_(env) {}
  environment(sexp env) : env_(env) {}
  proxy operator[](const SEXP name) const { return {env_, name}; }
  proxy operator[](const char* name) const { return operator[](safe[Rf_install](name)); }
  proxy operator[](const std::string& name) const { return operator[](name.c_str()); }

  bool exists(SEXP name) const {
    SEXP res = safe[Rf_findVarInFrame3](env_, name, FALSE);
    return res != R_UnboundValue;
  }
  bool exists(const char* name) const { return exists(safe[Rf_install](name)); }

  bool exists(const std::string& name) const { return exists(name.c_str()); }

  void remove(SEXP name) {
    PROTECT(name);
#ifdef HAS_REMOVE_VAR_FROM_FRAME
    R_removeVarFromFrame(name, env_);
#else
    auto remove = package("base")["remove"];
    remove(name, "envir"_nm = env_);
#endif
    UNPROTECT(1);
  }

  void remove(const char* name) { remove(safe[Rf_install](name)); }

  R_xlen_t size() const { return Rf_xlength(env_); }

  operator SEXP() const { return env_; }
};

}  // namespace cpp11
