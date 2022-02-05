#include "rapi.hpp"
#include "typesr.hpp"
#include "altrepstring.hpp"

using namespace duckdb;

R_altrep_class_t AltrepString::rclass;

void AltrepString::Initialize(DllInfo *dll) {
	rclass = R_make_altstring_class("duckdb_strings", "duckdb", dll);

	/* override ALTREP methods */
	R_set_altrep_Inspect_method(rclass, Inspect);
	R_set_altrep_Length_method(rclass, Length);

	/* override ALTVEC methods */
	R_set_altvec_Dataptr_method(rclass, Dataptr);
	R_set_altvec_Dataptr_or_null_method(rclass, DataptrOrNull);

	/* override ALTSTRING methods */
	R_set_altstring_Elt_method(rclass, Elt);
	R_set_altstring_Is_sorted_method(rclass, IsSorted);
	R_set_altstring_No_NA_method(rclass, NoNA);
	R_set_altstring_Set_elt_method(rclass, SetElt);
}

static DuckDBAltrepStringWrapper *duckdb_altrep_wrapper(SEXP x) {
	auto wrapper = (DuckDBAltrepStringWrapper *)R_ExternalPtrAddr(R_altrep_data1(x));
	if (!wrapper) {
		Rf_error("This looks like it has been freed");
	}
	return wrapper;
}

R_xlen_t AltrepString::Length(SEXP x) {
	return duckdb_altrep_wrapper(x)->length;
}

Rboolean AltrepString::Inspect(SEXP x, int pre, int deep, int pvec, void (*inspect_subtree)(SEXP, int, int, int)) {
	Rprintf("DUCKDB_STRING_COLUMN %llu\n", Length(x));
	return TRUE;
}

void *AltrepString::Dataptr(SEXP x, Rboolean writeable) {
	auto *wrapper = duckdb_altrep_wrapper(x);
	if (R_altrep_data2(x) == R_NilValue) {
		R_set_altrep_data2(x, NEW_STRING(wrapper->length));
		idx_t dest_offset = 0;
		for (auto &vec : wrapper->vectors) {
			auto src_ptr = FlatVector::GetData<string_t>(vec);
			auto &mask = FlatVector::Validity(vec);
			for (size_t row_idx = 0; row_idx < MinValue<idx_t>(STANDARD_VECTOR_SIZE, wrapper->length - dest_offset);
			     row_idx++) {
				if (!mask.RowIsValid(row_idx)) {
					SET_STRING_ELT(R_altrep_data2(x), dest_offset + row_idx, NA_STRING);
				} else {
					SET_STRING_ELT(
					    R_altrep_data2(x), dest_offset + row_idx,
					    Rf_mkCharLenCE(src_ptr[row_idx].GetDataUnsafe(), src_ptr[row_idx].GetSize(), CE_UTF8));
				}
			}
			dest_offset += STANDARD_VECTOR_SIZE;
		}
		wrapper->vectors.clear();
	}
	return CHARACTER_POINTER(R_altrep_data2(x));
}

const void *AltrepString::DataptrOrNull(SEXP x) {
	return nullptr;
}

SEXP AltrepString::Elt(SEXP x, R_xlen_t i) {
	auto *wrapper = duckdb_altrep_wrapper(x);
	if (R_altrep_data2(x) != R_NilValue) {
		return STRING_ELT(R_altrep_data2(x), i);
	}
	auto &vec = wrapper->vectors[i / STANDARD_VECTOR_SIZE];
	auto src_ptr = FlatVector::GetData<string_t>(vec);
	auto &mask = FlatVector::Validity(vec);
	auto vec_idx = i % STANDARD_VECTOR_SIZE;
	if (!mask.RowIsValid(vec_idx)) {
		return NA_STRING;
	}
	return Rf_mkCharLenCE(src_ptr[vec_idx].GetDataUnsafe(), src_ptr[vec_idx].GetSize(), CE_UTF8);
}

void AltrepString::SetElt(SEXP x, R_xlen_t i, SEXP val) {
	Dataptr(x, TRUE);
	SET_STRING_ELT(R_altrep_data2(x), i, val);
}

int AltrepString::IsSorted(SEXP x) {
	// we don't know
	return 0;
}

int AltrepString::NoNA(SEXP x) {
	// we kinda know but it matters little
	return 0;
}

// exception required as long as r-lib/decor#6 remains
// clang-format off
[[cpp11::init]] void AltrepString_Initialize(DllInfo* dll) {
	// clang-format on
	AltrepString::Initialize(dll);
}
