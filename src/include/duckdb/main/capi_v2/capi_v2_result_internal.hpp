//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/capi_v2/capi_v2_result_internal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

//! The result state machine, shared by the modules that build a result or consume one wholesale.
//! Not part of any public surface: only the V2 bridge's own translation units include this.
//! `capi_v2_result.cpp` owns it and defines the members declared here; `capi_v2_prepared_statement.cpp`
//! and `capi_v2_arrow.cpp` are the other consumers.

namespace duckdb::capiv2 {

struct ResultWrapperV2 {
	enum class State : uint8_t { PENDING, STREAMING, FINISHED, CANCELLED, ERRORED };

	~ResultWrapperV2() {
		// Finalize() (engine cleanup) runs in duckdb_v2_result_destroy, not here:
		// a destructor must not drive locked engine state behind a catch-all.
		pending.reset();
		result.reset();
		ReleaseBusySlot();
	}

	State state = State::PENDING;
	//! Live while state == PENDING.
	unique_ptr<PendingQueryResult> pending;
	//! Live while state == STREAMING.
	unique_ptr<QueryResult> result;

	//! Keeps the ClientContext alive for starting subsequent fragments and
	//! preserves the guarantee that an undrained result survives disconnect:
	//! the connection handle (a bare Connection *) may be destroyed while a
	//! result is live, but the context it shared with us stays alive here.
	shared_ptr<ClientContext> context;
	//! The preprocessed statement group; fragment_index points at the next
	//! fragment to start, fragment_count remembers the group size after
	//! fragments is cleared on terminal transitions.
	vector<unique_ptr<SQLStatement>> fragments;
	idx_t fragment_index = 0;
	idx_t fragment_count = 0;
	//! Positional parameter values for this execution, keyed by binding identifier
	//! ("1".."N"). Empty for an unparameterized statement (StartNextFragment takes
	//! the no-values path). Applied only to the first fragment (a parameterized
	//! statement does not expand into a group).
	identifier_map_t<BoundParameterData> param_values;
	//! True while the currently executing fragment is the principal one
	//! (its chunks are surfaced; other fragments' output is discarded).
	bool principal_active = false;
	//! True once a row-producing fragment has been selected as principal.
	bool principal_seen = false;
	//! True once the principal fragment's metadata has been captured.
	bool metadata_available = false;
	//! True when statement_execute injected its own wrapping transaction for
	//! this group (autocommit input that preprocessing expanded and wrapped
	//! in BEGIN ... COMMIT). Distinguishes a bridge-owned transaction, which
	//! must be rolled back on incomplete destroy, from a user-managed one,
	//! which must not be touched. Captured at query time because the engine's
	//! auto_rollback flag is not reliably observable from the bridge's
	//! fragment execution path.
	bool owns_wrapping_transaction = false;

	//! The owning connection's busy slot; released on terminal transition
	//! or destroy, whichever comes first.
	shared_ptr<ConnectionBusySlotV2> busy_slot;

	//! Principal fragment's metadata, valid once metadata_available.
	vector<LogicalType> types;
	vector<Identifier> names;
	StatementType statement_type = StatementType::INVALID_STATEMENT;
	StatementProperties properties;

	//! Sticky error, recorded when state == ERRORED.
	ErrorData error;

	//! Mirrors ClientContext::Query's error handling for expanded groups
	//! (client_context.cpp, chain-append loop): when the group cannot
	//! complete, roll back the transaction statement_execute injected to wrap
	//! it. A no-op for non-expanded statements, for groups that ran inside a
	//! user-managed transaction (which the bridge never wraps), and when the
	//! transaction is already gone.
	void RollbackIncompleteGroup() {
		if (!owns_wrapping_transaction || !context) {
			return;
		}
		if (context->transaction.HasActiveTransaction()) {
			// Mirrors Connection::Rollback (Query("ROLLBACK") + throw on error),
			// driven through the retained context so it works after disconnect.
			auto result = context->Query("ROLLBACK", QueryResultOutputType::FORCE_MATERIALIZED);
			if (result->HasError()) {
				result->ThrowError();
			}
		}
	}

	//! Close() the live engine result so an abandoned active query is cleaned
	//! up (freeing the executor, which breaks the ClientContext ref cycle),
	//! then roll back an injected group transaction. May throw; the terminal
	//! states leave pending/result null, so this is then a no-op.
	void Finalize() {
		if (pending) {
			pending->Close();
		} else if (result && result->GetResultType() == QueryResultType::STREAM_RESULT) {
			result->Cast<StreamQueryResult>().Close();
		}
		RollbackIncompleteGroup();
	}

	//! Frees the connection for its next query. Only the current owner can
	//! release the slot, so a release after the connection moved on is a
	//! no-op.
	void ReleaseBusySlot() {
		if (busy_slot) {
			void *expected = this;
			busy_slot->owner.compare_exchange_strong(expected, nullptr);
			busy_slot.reset();
		}
	}

	// State-machine entry points; defined in query_result-v2.cpp. All of
	// them throw DuckDB exceptions on failure (callers wrap in
	// WithErrorHandler) and record sticky errors before throwing.

	//! Adopts an already-produced pending query into the state machine: the single
	//! seam both the stateless (fragment) and prepared paths reach. When is_principal,
	//! captures its metadata and surfaces its chunks. Throws on a pending prepare error.
	void BeginPending(unique_ptr<PendingQueryResult> pending, bool is_principal);
	//! Starts the pending query for the next fragment, selecting it as
	//! principal per the engine-mirrored rule and adopting it via BeginPending.
	//! Throws on prepare errors.
	void StartNextFragment();
	//! Drives one unit of work; never blocks. On CHUNK, out_chunk holds the
	//! produced chunk; on every other status it is reset.
	DUCKDB_V2_RESULT_STEP_STATUS Step(unique_ptr<DataChunk> &out_chunk);
	//! Blocks until Step can make progress. No-op on terminal states.
	void Wait();
	//! Blocking convenience: steps/waits until a chunk is produced (returned)
	//! or the stream ends (nullptr). Cancellation throws InterruptException.
	unique_ptr<DataChunk> FetchChunkBlocking();
	//! Throws unless the principal fragment's metadata is available.
	void RequireMetadata() const;

private:
	//! Shared error sink: interrupts become the sticky CANCELLED state
	//! (returned as a status), everything else becomes the sticky ERRORED
	//! state and throws.
	DUCKDB_V2_RESULT_STEP_STATUS HandleExecutionError(ErrorData error_data);
};

auto Convert(ResultWrapperV2 *wrapper) -> duckdb_v2_result_handle;
auto Convert(duckdb_v2_result_handle handle) -> ResultWrapperV2 *;

//! Runs a prepared statement as a single-statement result, claiming the connection's
//! live-result slot first. `context` is the session the result holds on to, so it survives
//! disconnect. Throws ResourceInUseException when the connection already has a live result.
auto ExecutePreparedStatementV2(const shared_ptr<ClientContext> &context, PreparedStatement &prepared,
                                identifier_map_t<BoundParameterData> &values) -> duckdb_v2_result_handle;

} // namespace duckdb::capiv2
