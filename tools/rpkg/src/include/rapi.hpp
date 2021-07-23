#pragma once

#define R_NO_REMAP
#include <Rdefines.h>
#include <R_ext/Altrep.h>

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct RApi {

	static SEXP Startup(SEXP dbdirsexp, SEXP readonlysexp, SEXP configsexp);

	static SEXP Shutdown(SEXP dbsexp);

	static SEXP Connect(SEXP dbsexp);

	static SEXP Disconnect(SEXP connsexp);

	static SEXP Prepare(SEXP connsexp, SEXP querysexp);

	static SEXP Bind(SEXP stmtsexp, SEXP paramsexp);

	static SEXP Execute(SEXP stmtsexp, SEXP arrowsexp);

	static SEXP Release(SEXP stmtsexp);

	static SEXP RegisterDataFrame(SEXP connsexp, SEXP namesexp, SEXP valuesexp);

	static SEXP UnregisterDataFrame(SEXP connsexp, SEXP namesexp);

	static SEXP RegisterArrow(SEXP connsexp, SEXP namesexp, SEXP export_funsexp, SEXP valuesexp);

	static SEXP UnregisterArrow(SEXP connsexp, SEXP namesexp);

	static SEXP PointerToString(SEXP extptr);
	static SEXP StringsToSexp(vector<string> s);
};

struct RProtector {
	RProtector() : protect_count(0) {
	}
	~RProtector() {
		if (protect_count > 0) {
			UNPROTECT(protect_count);
		}
	}

	SEXP Protect(SEXP sexp) {
		protect_count++;
		return PROTECT(sexp);
	}

private:
	int protect_count;
};

struct DataFrameScanFunction : public TableFunction {
	DataFrameScanFunction();
};

struct RStrings {
	SEXP secs;
	SEXP mins;
	SEXP hours;
	SEXP days;
	SEXP weeks;

	static const RStrings &get() {
		// On demand
		static RStrings strings;
		return strings;
	}

private:
	RStrings();
};

} // namespace duckdb