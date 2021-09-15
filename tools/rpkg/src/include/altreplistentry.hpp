#pragma once

#include "rapi.hpp"

struct AltrepListEntry {

	static void Initialize(DllInfo *dll);
	static R_xlen_t Length(SEXP x);
	static void *Dataptr(SEXP x, Rboolean writeable);
	static R_altrep_class_t rclass;
};