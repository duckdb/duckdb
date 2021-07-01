#include "rapi.hpp"
#include "typesr.hpp"

using namespace duckdb;

SEXP RApi::PointerToString(SEXP extptr) {
	if (TYPEOF(extptr) != EXTPTRSXP) {
		Rf_error("duckdb_ptr_to_str: Need external pointer parameter");
	}
	RProtector r;
	SEXP ret = r.Protect(NEW_STRING(1));
	SET_STRING_ELT(ret, 0, NA_STRING);
	void *ptr = R_ExternalPtrAddr(extptr);
	if (ptr != NULL) {
		char buf[100];
		snprintf(buf, 100, "%p", ptr);
		SET_STRING_ELT(ret, 0, Rf_mkChar(buf));
	}
	return ret;
}

static SEXP cstr_to_charsexp(const char *s) {
	return Rf_mkCharCE(s, CE_UTF8);
}

static SEXP cpp_str_to_charsexp(string s) {
	return cstr_to_charsexp(s.c_str());
}

SEXP RApi::StringsToSexp(vector<string> s) {
	RProtector r;
	SEXP retsexp = r.Protect(NEW_STRING(s.size()));
	for (idx_t i = 0; i < s.size(); i++) {
		SET_STRING_ELT(retsexp, i, cpp_str_to_charsexp(s[i]));
	}
	return retsexp;
}

RStrings::RStrings() {
	// allocate strings once
	RProtector r;

	SEXP out = r.Protect(Rf_allocVector(STRSXP, 5));
	SET_STRING_ELT(out, 0, secs = Rf_mkChar("secs"));
	SET_STRING_ELT(out, 1, mins = Rf_mkChar("mins"));
	SET_STRING_ELT(out, 2, hours = Rf_mkChar("hours"));
	SET_STRING_ELT(out, 3, days = Rf_mkChar("days"));
	SET_STRING_ELT(out, 4, weeks = Rf_mkChar("weeks"));
	R_PreserveObject(out);
	MARK_NOT_MUTABLE(out);
}

template <class SRC, class DST, class RTYPE>
static void AppendColumnSegment(SRC *source_data, Vector &result, idx_t count) {
	auto result_data = FlatVector::GetData<DST>(result);
	auto &result_mask = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto val = source_data[i];
		if (RTYPE::IsNull(val)) {
			result_mask.SetInvalid(i);
		} else {
			result_data[i] = RTYPE::Convert(val);
		}
	}
}
