//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/capi_v2/capi_v2_result_internal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "duckdb/main/query_result_stream.hpp"

namespace duckdb::capiv2 {

struct ExecuteArgsV2 {
	identifier_map_t<BoundParameterData> param_values;
	ResultEagerness eagerness = ResultEagerness::AUTO;
};

auto Convert(ExecuteArgsV2 *args) -> duckdb_v2_execute_args_handle;
auto Convert(duckdb_v2_execute_args_handle args) -> ExecuteArgsV2 *;

struct ResultWrapperV2 {
	enum class State : uint8_t { RUNNING, STREAMING, FINISHED, CANCELLED, ERRORED };

	~ResultWrapperV2() {
		// Finalize() runs in duckdb_v2_result_destroy: a destructor must not drive locked engine
		// state behind a catch-all.
		stream.reset();
		handle.reset();
		principal.reset();
		ReleaseBusySlot();
	}

	State state = State::RUNNING;
	unique_ptr<QueryResult> handle;
	unique_ptr<QueryResult> principal;
	unique_ptr<QueryResultStream> stream;

	//! The connection handle may be destroyed while a result is live, so the session it shared with
	//! us stays alive here.
	shared_ptr<ClientContext> context;
	vector<unique_ptr<SQLStatement>> fragments;
	idx_t fragment_index = 0;
	//! Survives the clearing of `fragments` on a terminal transition.
	idx_t fragment_count = 0;
	//! Applied only to the first fragment: a parameterized statement never expands into a group.
	identifier_map_t<BoundParameterData> param_values;
	ResultEagerness eagerness = ResultEagerness::AUTO;
	bool principal_active = false;
	bool principal_seen = false;
	bool metadata_available = false;
	bool fragment_finished = false;
	//! Applied to the principal fragment, which may not have started when the request arrives.
	bool retain_requested = false;
	bool collection_taken = false;
	bool streamed = false;
	//! Whether preprocessing's injected BEGIN ... COMMIT is the bridge's to roll back. Captured at
	//! query time because the engine's auto_rollback flag is not observable from here.
	bool owns_wrapping_transaction = false;

	shared_ptr<ConnectionBusySlotV2> busy_slot;

	vector<LogicalType> types;
	vector<Identifier> names;
	StatementType statement_type = StatementType::INVALID_STATEMENT;
	StatementProperties properties;

	ErrorData error;

	//! Mirrors ClientContext::Query's chain-append loop: a group that cannot complete rolls back the
	//! transaction statement_execute injected to wrap it.
	void RollbackIncompleteGroup() {
		if (!owns_wrapping_transaction || !context) {
			return;
		}
		if (context->transaction.HasActiveTransaction()) {
			// Driven through the retained context so it still works after disconnect.
			auto result = context->Query("ROLLBACK", QueryParameters());
			if (result->HasError()) {
				result->ThrowError();
			}
		}
	}

	//! Closing an abandoned active query frees the executor, which breaks the ClientContext ref cycle.
	void Finalize() {
		if (stream) {
			stream->Close();
		} else if (handle) {
			handle->Close();
		}
		RollbackIncompleteGroup();
	}

	void ReleaseBusySlot() {
		if (busy_slot) {
			// Only the current owner may clear the slot: the connection may have moved on.
			void *expected = this;
			busy_slot->owner.compare_exchange_strong(expected, nullptr);
			busy_slot.reset();
		}
	}

	bool IsTerminalState() const {
		return state == State::FINISHED || state == State::CANCELLED || state == State::ERRORED;
	}

	void BeginPending(unique_ptr<QueryResult> handle, bool is_principal);
	void StartNextFragment();

	DUCKDB_V2_RESULT_STATUS Step();
	DUCKDB_V2_RESULT_STATUS Poll();
	void Wait();

	void RequestRetention();
	void RunToCompletion();
	unique_ptr<DataChunk> Fetch();
	ColumnDataCollection &Collection();
	unique_ptr<ColumnDataCollection> TakeCollection();
	bool CanStream();
	void OpenStream();

	unique_ptr<DataChunk> StreamFetch();
	DUCKDB_V2_RESULT_STATUS StreamTryFetch(unique_ptr<DataChunk> &out_chunk);
	unique_ptr<DataChunk> NextChunk();

	void RequireMetadata() const;
	//! Throws when the query was cancelled or has failed; returns otherwise.
	void RequireLive();

private:
	DUCKDB_V2_RESULT_STATUS ReportFragmentFinished();
	DUCKDB_V2_RESULT_STATUS FinishFragment();
	DUCKDB_V2_RESULT_STATUS EndStream();
	DUCKDB_V2_RESULT_STATUS Finished();
	void ReachPrincipal();
	[[noreturn]] void ThrowNoRows() const;
	DUCKDB_V2_RESULT_STATUS HandleExecutionError(ErrorData error_data);
};

auto Convert(ResultWrapperV2 *wrapper) -> duckdb_v2_result_handle;
auto Convert(duckdb_v2_result_handle handle) -> ResultWrapperV2 *;
auto ConvertStream(ResultWrapperV2 *wrapper) -> duckdb_v2_result_stream_handle;
auto Convert(duckdb_v2_result_stream_handle stream) -> ResultWrapperV2 *;

auto ExecutePreparedStatementV2(const shared_ptr<ClientContext> &context, PreparedStatement &prepared,
                                optional_ptr<ExecuteArgsV2> args) -> duckdb_v2_result_handle;

} // namespace duckdb::capiv2
