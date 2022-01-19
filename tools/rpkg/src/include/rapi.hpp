#pragma once

#define R_NO_REMAP
#include <Rdefines.h>

#include <R_ext/Altrep.h>

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

typedef unordered_map<std::string, SEXP> arrow_scans_t;

struct DBWrapper {
	unique_ptr<DuckDB> db;
	arrow_scans_t arrow_scans;
	mutex lock;
};

struct ConnWrapper {
	unique_ptr<Connection> conn;
	SEXP db_sexp;
};

struct RApi {

	static SEXP Startup(SEXP dbdirsexp, SEXP readonlysexp, SEXP configsexp);

	static void Shutdown(SEXP dbsexp);

	static SEXP Connect(SEXP dbsexp);

	static void Disconnect(SEXP connsexp);

	static SEXP Prepare(SEXP connsexp, SEXP querysexp);

	static SEXP Bind(SEXP stmtsexp, SEXP paramsexp, SEXP arrowsexp);

	static SEXP Execute(SEXP stmtsexp, SEXP arrowsexp);

	static SEXP DuckDBExecuteArrow(SEXP query_resultsexp, SEXP streamsexp, SEXP vector_per_chunksexp,
	                               SEXP return_tablesexp);

	static SEXP DuckDBRecordBatchR(SEXP query_resultsexp, SEXP approx_batch_sizeexp);

	static SEXP Release(SEXP stmtsexp);

	static void RegisterDataFrame(SEXP connsexp, SEXP namesexp, SEXP valuesexp);

	static void UnregisterDataFrame(SEXP connsexp, SEXP namesexp);

	static void RegisterArrow(SEXP connsexp, SEXP namesexp, SEXP export_funsexp, SEXP valuesexp);

	static void UnregisterArrow(SEXP connsexp, SEXP namesexp);

	static SEXP PointerToString(SEXP extptr);

	// internal
	static unique_ptr<TableFunctionRef> ArrowScanReplacement(const string &table_name, void *data);

	static SEXP StringsToSexp(vector<string> s);

	static SEXP ToUtf8(SEXP string_sexp);
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
	SEXP factor_str;
	SEXP difftime_str;
	SEXP secs_str;
	SEXP arrow_str; // StringsToSexp
	SEXP POSIXct_POSIXt_str;
	SEXP str_ref_type_names_rtypes_n_param_str;
	SEXP enc2utf8_sym; // Rf_install
	SEXP tzone_sym;
	SEXP units_sym;
	SEXP getNamespace_sym;
	SEXP Table__from_record_batches_sym;
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