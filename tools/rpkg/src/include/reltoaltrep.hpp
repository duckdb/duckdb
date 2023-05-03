#pragma once

#include "rapi.hpp"

struct RelToAltrep {
	static void Initialize(DllInfo *dll);
	static R_xlen_t RownamesLength(SEXP x);
	static void *RownamesDataptr(SEXP x, Rboolean writeable);
	static Rboolean RownamesInspect(SEXP x, int pre, int deep, int pvec, void (*inspect_subtree)(SEXP, int, int, int));

	static R_xlen_t VectorLength(SEXP x);
	static void *VectorDataptr(SEXP x, Rboolean writeable);
	static Rboolean RelInspect(SEXP x, int pre, int deep, int pvec, void (*inspect_subtree)(SEXP, int, int, int));

	static SEXP VectorStringElt(SEXP x, R_xlen_t i);

	static R_altrep_class_t rownames_class;
	static R_altrep_class_t logical_class;
	static R_altrep_class_t int_class;
	static R_altrep_class_t real_class;
	static R_altrep_class_t string_class;
};
