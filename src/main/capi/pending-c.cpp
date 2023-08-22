#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/common/preserved_error.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional_ptr.hpp"

using duckdb::case_insensitive_map_t;
using duckdb::make_uniq;
using duckdb::optional_ptr;
using duckdb::PendingExecutionResult;
using duckdb::PendingQueryResult;
using duckdb::PendingStatementWrapper;
using duckdb::PreparedStatementWrapper;
using duckdb::Value;

duckdb_state duckdb_pending_prepared_internal(duckdb_prepared_statement prepared_statement,
                                              duckdb_pending_result *out_result, bool allow_streaming) {
	if (!prepared_statement || !out_result) {
		return DuckDBError;
	}
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	auto result = new PendingStatementWrapper();
	result->allow_streaming = allow_streaming;

	try {
		result->statement = wrapper->statement->PendingQuery(wrapper->values, allow_streaming);
	} catch (const duckdb::Exception &ex) {
		result->statement = make_uniq<PendingQueryResult>(duckdb::PreservedError(ex));
	} catch (std::exception &ex) {
		result->statement = make_uniq<PendingQueryResult>(duckdb::PreservedError(ex));
	}
	duckdb_state return_value = !result->statement->HasError() ? DuckDBSuccess : DuckDBError;
	*out_result = reinterpret_cast<duckdb_pending_result>(result);

	return return_value;
}

duckdb_state duckdb_pending_prepared(duckdb_prepared_statement prepared_statement, duckdb_pending_result *out_result) {
	return duckdb_pending_prepared_internal(prepared_statement, out_result, false);
}

duckdb_state duckdb_pending_prepared_streaming(duckdb_prepared_statement prepared_statement,
                                               duckdb_pending_result *out_result) {
	return duckdb_pending_prepared_internal(prepared_statement, out_result, true);
}

void duckdb_destroy_pending(duckdb_pending_result *pending_result) {
	if (!pending_result || !*pending_result) {
		return;
	}
	auto wrapper = reinterpret_cast<PendingStatementWrapper *>(*pending_result);
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
	auto wrapper = reinterpret_cast<PendingStatementWrapper *>(pending_result);
	if (!wrapper->statement) {
		return nullptr;
	}
	return wrapper->statement->GetError().c_str();
}

duckdb_pending_state duckdb_pending_execute_task(duckdb_pending_result pending_result) {
	if (!pending_result) {
		return DUCKDB_PENDING_ERROR;
	}
	auto wrapper = reinterpret_cast<PendingStatementWrapper *>(pending_result);
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
	case PendingExecutionResult::NO_TASKS_AVAILABLE:
		return DUCKDB_PENDING_NO_TASKS_AVAILABLE;
	case PendingExecutionResult::RESULT_NOT_READY:
		return DUCKDB_PENDING_RESULT_NOT_READY;
	default:
		return DUCKDB_PENDING_ERROR;
	}
}

bool duckdb_pending_execution_is_finished(duckdb_pending_state pending_state) {
	switch (pending_state) {
	case DUCKDB_PENDING_RESULT_READY:
		return PendingQueryResult::IsFinished(PendingExecutionResult::RESULT_READY);
	case DUCKDB_PENDING_NO_TASKS_AVAILABLE:
		return PendingQueryResult::IsFinished(PendingExecutionResult::NO_TASKS_AVAILABLE);
	case DUCKDB_PENDING_RESULT_NOT_READY:
		return PendingQueryResult::IsFinished(PendingExecutionResult::RESULT_NOT_READY);
	case DUCKDB_PENDING_ERROR:
		return PendingQueryResult::IsFinished(PendingExecutionResult::EXECUTION_ERROR);
	default:
		return PendingQueryResult::IsFinished(PendingExecutionResult::EXECUTION_ERROR);
	}
}

duckdb_state duckdb_execute_pending(duckdb_pending_result pending_result, duckdb_result *out_result) {
	if (!pending_result || !out_result) {
		return DuckDBError;
	}
	auto wrapper = reinterpret_cast<PendingStatementWrapper *>(pending_result);
	if (!wrapper->statement) {
		return DuckDBError;
	}

	duckdb::unique_ptr<duckdb::QueryResult> result;
	result = wrapper->statement->Execute();
	wrapper->statement.reset();
	return duckdb_translate_result(std::move(result), out_result);
}
