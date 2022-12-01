#include "rapi.hpp"
#include "typesr.hpp"

#include <R_ext/Utils.h>

#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"

#include "duckdb/parser/statement/relation_statement.hpp"

using namespace duckdb;
using namespace cpp11::literals;

[[cpp11::register]] void rapi_release(duckdb::stmt_eptr_t stmt) {
	auto stmt_ptr = stmt.release();
	if (stmt_ptr) {
		delete stmt_ptr;
	}
}

[[cpp11::register]] SEXP rapi_get_substrait(duckdb::conn_eptr_t conn, std::string query) {
	if (!conn || !conn.get() || !conn->conn) {
		cpp11::stop("rapi_get_substrait: Invalid connection");
	}

	auto rel = conn->conn->TableFunction("get_substrait", {Value(query)});
	auto res = rel->Execute();
	auto chunk = res->Fetch();
	auto blob_string = StringValue::Get(chunk->GetValue(0, 0));

	auto rawval = NEW_RAW(blob_string.size());
	if (!rawval) {
		throw std::bad_alloc();
	}
	memcpy(RAW_POINTER(rawval), blob_string.data(), blob_string.size());

	return rawval;
}

[[cpp11::register]] SEXP rapi_get_substrait_json(duckdb::conn_eptr_t conn, std::string query) {
	if (!conn || !conn.get() || !conn->conn) {
		cpp11::stop("rapi_get_substrait_json: Invalid connection");
	}

	auto rel = conn->conn->TableFunction("get_substrait_json", {Value(query)});
	auto res = rel->Execute();
	auto chunk = res->Fetch();
	auto json = StringValue::Get(chunk->GetValue(0, 0));

	return StringsToSexp({json});
}

static cpp11::list construct_retlist(unique_ptr<PreparedStatement> stmt, const string &query, idx_t n_param) {
	cpp11::writable::list retlist;
	retlist.reserve(6);
	retlist.push_back({"str"_nm = query});

	auto stmtholder = new RStatement();
	stmtholder->stmt = move(stmt);

	retlist.push_back({"ref"_nm = stmt_eptr_t(stmtholder)});
	retlist.push_back({"type"_nm = StatementTypeToString(stmtholder->stmt->GetStatementType())});
	retlist.push_back({"names"_nm = cpp11::as_sexp(stmtholder->stmt->GetNames())});

	cpp11::writable::strings rtypes;
	rtypes.reserve(stmtholder->stmt->GetTypes().size());

	for (auto &stype : stmtholder->stmt->GetTypes()) {
		string rtype = RApiTypes::DetectLogicalType(stype, "rapi_prepare");
		rtypes.push_back(rtype);
	}

	retlist.push_back({"rtypes"_nm = rtypes});
	retlist.push_back({"n_param"_nm = n_param});
	retlist.push_back(
	    {"return_type"_nm = StatementReturnTypeToString(stmtholder->stmt->GetStatementProperties().return_type)});

	return retlist;
}

[[cpp11::register]] cpp11::list rapi_prepare_substrait(duckdb::conn_eptr_t conn, cpp11::sexp query) {
	if (!conn || !conn.get() || !conn->conn) {
		cpp11::stop("rapi_prepare_substrait: Invalid connection");
	}

	if (!IS_RAW(query)) {
		cpp11::stop("rapi_prepare_substrait: Query is not a raw()/BLOB");
	}

	auto rel = conn->conn->TableFunction("from_substrait", {Value::BLOB(RAW_POINTER(query), LENGTH(query))});
	auto relation_stmt = make_unique<RelationStatement>(rel);
	relation_stmt->n_param = 0;
	relation_stmt->query = "";
	auto stmt = conn->conn->Prepare(move(relation_stmt));
	if (stmt->HasError()) {
		cpp11::stop("rapi_prepare_substrait: Failed to prepare query %s\nError: %s", stmt->error.Message().c_str());
	}

	return construct_retlist(move(stmt), "", 0);
}

[[cpp11::register]] cpp11::list rapi_prepare(duckdb::conn_eptr_t conn, std::string query) {
	if (!conn || !conn.get() || !conn->conn) {
		cpp11::stop("rapi_prepare: Invalid connection");
	}

	auto statements = conn->conn->ExtractStatements(query.c_str());
	if (statements.empty()) {
		// no statements to execute
		cpp11::stop("rapi_prepare: No statements to execute");
	}
	// if there are multiple statements, we directly execute the statements besides the last one
	// we only return the result of the last statement to the user, unless one of the previous statements fails
	for (idx_t i = 0; i + 1 < statements.size(); i++) {
		auto res = conn->conn->Query(move(statements[i]));
		if (res->HasError()) {
			cpp11::stop("rapi_prepare: Failed to execute statement %s\nError: %s", query.c_str(),
			            res->GetError().c_str());
		}
	}
	auto stmt = conn->conn->Prepare(move(statements.back()));
	if (stmt->HasError()) {
		cpp11::stop("rapi_prepare: Failed to prepare query %s\nError: %s", query.c_str(),
		            stmt->error.Message().c_str());
	}
	auto n_param = stmt->n_param;
	return construct_retlist(move(stmt), query, n_param);
}

[[cpp11::register]] cpp11::list rapi_bind(duckdb::stmt_eptr_t stmt, cpp11::list params, bool arrow, bool integer64) {
	if (!stmt || !stmt.get() || !stmt->stmt) {
		cpp11::stop("rapi_bind: Invalid statement");
	}

	stmt->parameters.clear();
	stmt->parameters.resize(stmt->stmt->n_param);

	if (stmt->stmt->n_param == 0) {
		cpp11::stop("rapi_bind: dbBind called but query takes no parameters");
	}

	if (params.size() != R_xlen_t(stmt->stmt->n_param)) {
		cpp11::stop("rapi_bind: Bind parameters need to be a list of length %i", stmt->stmt->n_param);
	}

	R_len_t n_rows = Rf_length(params[0]);

	for (auto param = std::next(params.begin()); param != params.end(); ++param) {
		if (Rf_length(*param) != n_rows) {
			cpp11::stop("rapi_bind: Bind parameter values need to have the same length");
		}
	}

	if (n_rows != 1 && arrow) {
		cpp11::stop("rapi_bind: Bind parameter values need to have length one for arrow queries");
	}

	cpp11::writable::list out;
	out.reserve(n_rows);

	for (idx_t row_idx = 0; row_idx < (size_t)n_rows; ++row_idx) {
		for (idx_t param_idx = 0; param_idx < (idx_t)params.size(); param_idx++) {
			SEXP valsexp = params[(size_t)param_idx];
			auto val = RApiTypes::SexpToValue(valsexp, row_idx);
			stmt->parameters[param_idx] = val;
		}

		// No protection, assigned immediately
		out.push_back(rapi_execute(stmt, arrow, integer64));
	}

	return out;
}

SEXP duckdb::duckdb_execute_R_impl(MaterializedQueryResult *result, bool integer64) {
	// step 2: create result data frame and allocate columns
	auto ncols = result->types.size();
	if (ncols == 0) {
		return Rf_ScalarReal(0); // no need for protection because no allocation can happen afterwards
	}

	auto nrows = result->RowCount();

	// Note we cannot use cpp11's data frame here as it tries to calculate the number of rows itself,
	// but gives the wrong answer if the first column is another data frame. So we set the necessary
	// attributes manually.
	cpp11::writable::list data_frame(NEW_LIST(ncols));
	data_frame.attr(R_ClassSymbol) = RStrings::get().dataframe_str;
	data_frame.attr(R_RowNamesSymbol) = {NA_INTEGER, -static_cast<int>(nrows)};
	SET_NAMES(data_frame, StringsToSexp(result->names));

	for (size_t col_idx = 0; col_idx < ncols; col_idx++) {
		RProtector r_varvalue;
		auto varvalue = r_varvalue.Protect(duckdb_r_allocate(result->types[col_idx], r_varvalue, nrows));
		duckdb_r_decorate(result->types[col_idx], varvalue, integer64);
		SET_VECTOR_ELT(data_frame, col_idx, varvalue);
	}

	// at this point data_frame is fully allocated and the only protected SEXP

	// step 3: set values from chunks
	idx_t dest_offset = 0;
	for (auto &chunk : result->Collection().Chunks()) {
		D_ASSERT(chunk.ColumnCount() == ncols);
		D_ASSERT(chunk.ColumnCount() == (idx_t)Rf_length(data_frame));
		for (size_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
			SEXP dest = VECTOR_ELT(data_frame, col_idx);
			duckdb_r_transform(chunk.data[col_idx], dest, dest_offset, chunk.size(), integer64);
		}
		dest_offset += chunk.size();
	}

	D_ASSERT(dest_offset == nrows);
	return data_frame;
}

struct AppendableRList {
	AppendableRList() {
		the_list = r.Protect(NEW_LIST(capacity));
	}
	void PrepAppend() {
		if (size >= capacity) {
			capacity = capacity * 2;
			SEXP new_list = r.Protect(NEW_LIST(capacity));
			D_ASSERT(new_list);
			for (idx_t i = 0; i < size; i++) {
				SET_VECTOR_ELT(new_list, i, VECTOR_ELT(the_list, i));
			}
			the_list = new_list;
		}
	}

	void Append(SEXP val) {
		D_ASSERT(size < capacity);
		D_ASSERT(the_list != R_NilValue);
		SET_VECTOR_ELT(the_list, size++, val);
	}
	SEXP the_list;
	idx_t capacity = 1000;
	idx_t size = 0;
	RProtector r;
};

bool FetchArrowChunk(QueryResult *result, AppendableRList &batches_list, ArrowArray &arrow_data,
                     ArrowSchema &arrow_schema, SEXP batch_import_from_c, SEXP arrow_namespace, idx_t chunk_size) {
	auto count = ArrowUtil::FetchChunk(result, chunk_size, &arrow_data);
	if (count == 0) {
		return false;
	}
	auto timezone_config = QueryResult::GetConfigTimezone(*result);
	ArrowConverter::ToArrowSchema(&arrow_schema, result->types, result->names, timezone_config);
	batches_list.PrepAppend();
	batches_list.Append(cpp11::safe[Rf_eval](batch_import_from_c, arrow_namespace));
	return true;
}

// Turn a DuckDB result set into an Arrow Table
[[cpp11::register]] SEXP rapi_execute_arrow(duckdb::rqry_eptr_t qry_res, int chunk_size) {
	auto result = qry_res->result.get();
	// somewhat dark magic below
	cpp11::function getNamespace = RStrings::get().getNamespace_sym;
	cpp11::sexp arrow_namespace(getNamespace(RStrings::get().arrow_str));

	// export schema setup
	ArrowSchema arrow_schema;
	cpp11::doubles schema_ptr_sexp(Rf_ScalarReal(static_cast<double>(reinterpret_cast<uintptr_t>(&arrow_schema))));
	cpp11::sexp schema_import_from_c(Rf_lang2(RStrings::get().ImportSchema_sym, schema_ptr_sexp));

	// export data setup
	ArrowArray arrow_data;
	cpp11::doubles data_ptr_sexp(Rf_ScalarReal(static_cast<double>(reinterpret_cast<uintptr_t>(&arrow_data))));
	cpp11::sexp batch_import_from_c(Rf_lang3(RStrings::get().ImportRecordBatch_sym, data_ptr_sexp, schema_ptr_sexp));
	// create data batches
	AppendableRList batches_list;

	while (FetchArrowChunk(result, batches_list, arrow_data, arrow_schema, batch_import_from_c, arrow_namespace,
	                       chunk_size)) {
	}

	SET_LENGTH(batches_list.the_list, batches_list.size);
	auto timezone_config = QueryResult::GetConfigTimezone(*result);
	ArrowConverter::ToArrowSchema(&arrow_schema, result->types, result->names, timezone_config);
	cpp11::sexp schema_arrow_obj(cpp11::safe[Rf_eval](schema_import_from_c, arrow_namespace));

	// create arrow::Table
	cpp11::sexp from_record_batches(
	    Rf_lang3(RStrings::get().Table__from_record_batches_sym, batches_list.the_list, schema_arrow_obj));
	return cpp11::safe[Rf_eval](from_record_batches, arrow_namespace);
}

// Turn a DuckDB result set into an RecordBatchReader
[[cpp11::register]] SEXP rapi_record_batch(duckdb::rqry_eptr_t qry_res, int chunk_size) {
	// somewhat dark magic below
	cpp11::function getNamespace = RStrings::get().getNamespace_sym;
	cpp11::sexp arrow_namespace(getNamespace(RStrings::get().arrow_str));

	auto result_stream = new ResultArrowArrayStreamWrapper(move(qry_res->result), chunk_size);
	cpp11::sexp stream_ptr_sexp(
	    Rf_ScalarReal(static_cast<double>(reinterpret_cast<uintptr_t>(&result_stream->stream))));
	cpp11::sexp record_batch_reader(Rf_lang2(RStrings::get().ImportRecordBatchReader_sym, stream_ptr_sexp));
	return cpp11::safe[Rf_eval](record_batch_reader, arrow_namespace);
}

[[cpp11::register]] SEXP rapi_execute(duckdb::stmt_eptr_t stmt, bool arrow, bool integer64) {
	if (!stmt || !stmt.get() || !stmt->stmt) {
		cpp11::stop("rapi_execute: Invalid statement");
	}
	auto pending_query = stmt->stmt->PendingQuery(stmt->parameters, arrow);
	duckdb::PendingExecutionResult execution_result;
	do {
		execution_result = pending_query->ExecuteTask();
		R_CheckUserInterrupt();
	} while (execution_result == PendingExecutionResult::RESULT_NOT_READY);
	if (execution_result == PendingExecutionResult::EXECUTION_ERROR) {
		cpp11::stop("rapi_execute: Failed to run query\nError: %s", pending_query->GetError().c_str());
	}
	auto generic_result = pending_query->Execute();
	if (generic_result->HasError()) {
		cpp11::stop("rapi_execute: Failed to run query\nError: %s", generic_result->GetError().c_str());
	}

	if (arrow) {
		auto query_result = new RQueryResult();
		query_result->result = move(generic_result);
		rqry_eptr_t query_resultsexp(query_result);
		return query_resultsexp;
	} else {
		D_ASSERT(generic_result->type == QueryResultType::MATERIALIZED_RESULT);
		auto result = (MaterializedQueryResult *)generic_result.get();
		return duckdb_execute_R_impl(result, integer64);
	}
}
