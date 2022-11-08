#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/common/preserved_error.hpp"

using duckdb::make_unique;
using duckdb::PendingExecutionResult;
using duckdb::PendingQueryResult;
using duckdb::PendingStatementWrapper;
using duckdb::PreparedStatementWrapper;

duckdb_state duckdb_pending_prepared(duckdb_prepared_statement prepared_statement, duckdb_pending_result *out_result) {
	if (!prepared_statement || !out_result) {
		return DuckDBError;
	}
	auto wrapper = (PreparedStatementWrapper *)prepared_statement;
	auto result = new PendingStatementWrapper();
	try {
		result->statement = wrapper->statement->PendingQuery(wrapper->values, false);
	} catch (const duckdb::Exception &ex) {
		result->statement = make_unique<PendingQueryResult>(duckdb::PreservedError(ex));
	} catch (std::exception &ex) {
		result->statement = make_unique<PendingQueryResult>(duckdb::PreservedError(ex));
	}
	duckdb_state return_value = !result->statement->HasError() ? DuckDBSuccess : DuckDBError;
	*out_result = (duckdb_pending_result)result;

	return return_value;
}

void duckdb_destroy_pending(duckdb_pending_result *pending_result) {
	if (!pending_result || !*pending_result) {
		return;
	}
	auto wrapper = (PendingStatementWrapper *)*pending_result;
	if (wrapper->statement) {
		wrapper->statement->Close();
	}
	delete wrapper;
	*pending_result = nullptr;
}

const char *duckdb_pending_error(duckdb_pending_result pending_result) {
	if (!pending_result) {
		return nullptr;
	}
	auto wrapper = (PendingStatementWrapper *)pending_result;
	if (!wrapper->statement) {
		return nullptr;
	}
	return wrapper->statement->GetError().c_str();
}

duckdb_pending_state duckdb_pending_execute_task(duckdb_pending_result pending_result) {
	if (!pending_result) {
		return DUCKDB_PENDING_ERROR;
	}
	auto wrapper = (PendingStatementWrapper *)pending_result;
	if (!wrapper->statement) {
		return DUCKDB_PENDING_ERROR;
	}
	if (wrapper->statement->HasError()) {
		return DUCKDB_PENDING_ERROR;
	}
	PendingExecutionResult return_value;
	try {
		return_value = wrapper->statement->ExecuteTask();
	} catch (const duckdb::Exception &ex) {
		wrapper->statement->SetError(duckdb::PreservedError(ex));
		return DUCKDB_PENDING_ERROR;
	} catch (std::exception &ex) {
		wrapper->statement->SetError(duckdb::PreservedError(ex));
		return DUCKDB_PENDING_ERROR;
	}
	switch (return_value) {
	case PendingExecutionResult::RESULT_READY:
		return DUCKDB_PENDING_RESULT_READY;
	case PendingExecutionResult::RESULT_NOT_READY:
		return DUCKDB_PENDING_RESULT_NOT_READY;
	default:
		return DUCKDB_PENDING_ERROR;
	}
}

duckdb_state duckdb_execute_pending(duckdb_pending_result pending_result, duckdb_result *out_result) {
	if (!pending_result || !out_result) {
		return DuckDBError;
	}
	auto wrapper = (PendingStatementWrapper *)pending_result;
	if (!wrapper->statement) {
		return DuckDBError;
	}
	auto result = wrapper->statement->Execute();
	wrapper->statement.reset();
	return duckdb_translate_result(move(result), out_result);
}
