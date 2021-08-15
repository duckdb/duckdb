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

	static SEXP DuckDBExecuteArrow(SEXP query_resultsexp, SEXP streamsexp, SEXP vector_per_chunksexp,
	                               SEXP return_tablesexp);

	static SEXP DuckDBRecordBatchR(SEXP query_resultsexp);

	static SEXP Release(SEXP stmtsexp);

	static SEXP RegisterDataFrame(SEXP connsexp, SEXP namesexp, SEXP valuesexp);

	static SEXP UnregisterDataFrame(SEXP connsexp, SEXP namesexp);

	static SEXP RegisterArrow(SEXP connsexp, SEXP namesexp, SEXP export_funsexp, SEXP valuesexp);

	static SEXP UnregisterArrow(SEXP connsexp, SEXP namesexp);

	static SEXP PointerToString(SEXP extptr);
	static SEXP StringsToSexp(vector<string> s);

	static SEXP REvalThrows(SEXP call, SEXP env = R_GlobalEnv);
	static SEXP REvalRerror(SEXP call, SEXP env = R_GlobalEnv);
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
	SEXP secs; // Rf_mkChar
	SEXP mins;
	SEXP hours;
	SEXP days;
	SEXP weeks;
	SEXP POSIXct;
	SEXP POSIXt;
	SEXP UTC_str; // Rf_mkString
	SEXP Date_str;
	SEXP difftime_str;
	SEXP arrow_str; // StringsToSexp
	SEXP POSIXct_POSIXt_str;
	SEXP str_ref_type_names_rtypes_n_param_str;
	SEXP _registered_df__sym; // Rf_install
	SEXP _registered_arrow__sym;
	SEXP tzone_sym;
	SEXP units_sym;
	SEXP getNamespace_sym;
	SEXP ImportSchema_sym;
	SEXP ImportRecordBatch_sym;
	SEXP ImportRecordBatchReader_sym;

	static const RStrings &get() {
		// On demand
		static RStrings strings;
		return strings;
	}

private:
	RStrings();
};

} // namespace duckdb