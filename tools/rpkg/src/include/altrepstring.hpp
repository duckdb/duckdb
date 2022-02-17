#pragma once

#include "rapi.hpp"

struct AltrepString {

	static void Initialize(DllInfo *dll);

	static R_xlen_t Length(SEXP x);

	static Rboolean Inspect(SEXP x, int pre, int deep, int pvec, void (*inspect_subtree)(SEXP, int, int, int));

	static void *Dataptr(SEXP x, Rboolean writeable);
	static const void *DataptrOrNull(SEXP x);
	static SEXP Elt(SEXP x, R_xlen_t i);
	static void SetElt(SEXP x, R_xlen_t i, SEXP val);
	static int IsSorted(SEXP x);
	static int NoNA(SEXP x);

	static R_altrep_class_t rclass;
};
