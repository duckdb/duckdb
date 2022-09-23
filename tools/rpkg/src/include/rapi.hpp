#pragma once

#include "cpp11.hpp"

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

void DBDeleter(DBWrapper *);
typedef cpp11::external_pointer<DBWrapper, DBDeleter> db_eptr_t;

struct ConnWrapper {
	unique_ptr<Connection> conn;
	db_eptr_t db_eptr;
};

void ConnDeleter(ConnWrapper *);
typedef cpp11::external_pointer<ConnWrapper, ConnDeleter> conn_eptr_t;

struct RStatement {
	unique_ptr<PreparedStatement> stmt;
	vector<Value> parameters;
};

struct RelationWrapper {
	RelationWrapper(std::shared_ptr<Relation> rel_p) : rel(move(rel_p)) {
	}
	shared_ptr<Relation> rel;
};

typedef cpp11::external_pointer<ParsedExpression> expr_extptr_t;
typedef cpp11::external_pointer<RelationWrapper> rel_extptr_t;

typedef cpp11::external_pointer<RStatement> stmt_eptr_t;

struct RQueryResult {
	unique_ptr<QueryResult> result;
};

typedef cpp11::external_pointer<RQueryResult> rqry_eptr_t;

// internal
unique_ptr<TableFunctionRef> ArrowScanReplacement(ClientContext &context, const std::string &table_name,
                                                  ReplacementScanData *data);

struct ArrowScanReplacementData : public ReplacementScanData {
	DBWrapper *wrapper;
};

SEXP StringsToSexp(vector<std::string> s);

SEXP ToUtf8(SEXP string_sexp);

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
	SEXP dataframe_str;
	SEXP difftime_str;
	SEXP secs_str;
	SEXP arrow_str; // StringsToSexp
	SEXP POSIXct_POSIXt_str;
	SEXP integer64_str;
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

SEXP duckdb_execute_R_impl(MaterializedQueryResult *result, bool);

} // namespace duckdb

// moved out of duckdb namespace for the time being (r-lib/cpp11#262)

duckdb::db_eptr_t rapi_startup(std::string, bool, cpp11::list);

void rapi_shutdown(duckdb::db_eptr_t);

duckdb::conn_eptr_t rapi_connect(duckdb::db_eptr_t);

void rapi_disconnect(duckdb::conn_eptr_t);

cpp11::list rapi_prepare(duckdb::conn_eptr_t, std::string);

cpp11::list rapi_bind(duckdb::stmt_eptr_t, SEXP paramsexp, bool);

SEXP rapi_execute(duckdb::stmt_eptr_t, bool, bool);

void rapi_release(duckdb::stmt_eptr_t);

void rapi_register_df(duckdb::conn_eptr_t, std::string, cpp11::data_frame, bool);

void rapi_unregister_df(duckdb::conn_eptr_t, std::string);

void rapi_register_arrow(duckdb::conn_eptr_t, SEXP namesexp, SEXP export_funsexp, SEXP valuesexp);

void rapi_unregister_arrow(duckdb::conn_eptr_t, SEXP namesexp);

SEXP rapi_execute_arrow(duckdb::rqry_eptr_t, int);

SEXP rapi_record_batch(duckdb::rqry_eptr_t, int);

cpp11::r_string rapi_ptr_to_str(SEXP extptr);
