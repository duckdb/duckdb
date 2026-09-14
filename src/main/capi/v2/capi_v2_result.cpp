#include "duckdb/main/capi_v2/capi_v2_result_internal.hpp"

#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/box_renderer_context.hpp"
#include "duckdb/common/column_data_collection_render_interface.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

#include "duckdb/common/enums/pending_execution_result.hpp"
#include "duckdb/common/enums/stream_execution_result.hpp"
#include "duckdb/parser/statement/transaction_statement.hpp"

namespace duckdb::capiv2 {
namespace {

// Map duckdb::StatementReturnType to DUCKDB_V2_RESULT_TYPE. Values are
// numerically identical by §4 of the V2 conventions (numeric enum-id
// round-trip); the switch is the explicit mapping that gives -Wswitch
// teeth and asserts in debug if core adds a variant the V2 enum does
// not yet surface.
DUCKDB_V2_RESULT_TYPE MapResultType(StatementReturnType t) {
	switch (t) {
	case StatementReturnType::QUERY_RESULT:
		return DUCKDB_V2_RESULT_TYPE_QUERY_RESULT;
	case StatementReturnType::CHANGED_ROWS:
		return DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS;
	case StatementReturnType::NOTHING:
		return DUCKDB_V2_RESULT_TYPE_NOTHING;
	}
	D_ASSERT(false); // unmapped StatementReturnType variant
	return DUCKDB_V2_RESULT_TYPE_QUERY_RESULT;
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// ResultWrapperV2 state machine
// ---------------------------------------------------------------------------

void ResultWrapperV2::BeginPending(unique_ptr<PendingQueryResult> next_pending, bool is_principal) {
	if (next_pending->HasError()) {
		// Re-throw the typed ErrorData so the exception's ExceptionType is
		// preserved and routed through GetErrorCodeFromExceptionType.
		next_pending->GetErrorObject().Throw();
	}
	principal_active = is_principal;
	if (is_principal) {
		types = next_pending->GetTypes();
		names = next_pending->GetNames();
		statement_type = next_pending->GetStatementType();
		properties = next_pending->GetStatementProperties();
		metadata_available = true;
	}
	pending = std::move(next_pending);
	state = State::PENDING;
}

void ResultWrapperV2::StartNextFragment() {
	D_ASSERT(fragment_index < fragments.size());
	if (fragment_index > 0 && context->IsInterrupted()) {
		// PendingQuery's InitialCleanup clears a pending interrupt flag;
		// without this check an interrupt landing exactly at a fragment
		// boundary would be silently swallowed and the group would keep
		// running. (At fragment 0 the clearing is desired: a stale
		// interrupt must not poison a new query.)
		throw InterruptException();
	}
	idx_t this_index = fragment_index;
	auto stmt = std::move(fragments[fragment_index++]);
	bool is_last = fragment_index == fragments.size();
	// Parameters bind only to the first fragment (statement_execute rejects them on
	// a statement that expands, so that fragment is the user's statement). Later
	// fragments take the no-values path.
	auto next_pending =
	    (this_index == 0 && !param_values.empty())
	        ? context->PendingQuery(std::move(stmt), param_values, QueryResultOutputType::ALLOW_STREAMING)
	        : context->PendingQuery(std::move(stmt), QueryResultOutputType::ALLOW_STREAMING);
	// Principal selection is a property of the fragment group; compute it here, then
	// hand the pending to the shared BeginPending seam. A HasError() pending is left
	// for BeginPending to raise (return_type is meaningless on it).
	bool has_result = !next_pending->HasError() &&
	                  next_pending->GetStatementProperties().return_type == StatementReturnType::QUERY_RESULT;
	if (principal_seen && has_result) {
		// ClientContext::Query would chain these as separate results with
		// separate schemas; a single stream cannot. No known expansion
		// produces two row-producing fragments.
		throw NotImplementedException(
		    "statement expands to multiple row-producing statements, which cannot be streamed as a single result");
	}
	// Result selection mirrors ClientContext::Query: the caller sees the
	// first row-producing fragment, or the last fragment when none
	// produces rows.
	bool is_principal = has_result || (is_last && !principal_seen);
	if (has_result) {
		principal_seen = true;
	}
	BeginPending(std::move(next_pending), is_principal);
}

void ResultWrapperV2::RequireMetadata() const {
	if (!metadata_available) {
		throw InvalidInputException("result metadata is not yet available: the statement expands to a group of "
		                            "statements and the result-producing one has not been prepared; step the result");
	}
}

DUCKDB_V2_RESULT_STEP_STATUS ResultWrapperV2::HandleExecutionError(ErrorData error_data) {
	// Only a consumer-initiated cancellation is a cancellation. An
	// engine-initiated interrupt that shares the INTERRUPT exception type
	// (e.g. a max_execution_time timeout) must surface as an error carrying the
	// engine's message, mirroring the eager ClientContext::Query path.
	bool user_cancelled = error_data.Type() == ExceptionType::INTERRUPT && busy_slot &&
	                      busy_slot->cancel_requested.load(std::memory_order_relaxed);
	pending.reset();
	result.reset();
	fragments.clear();
	try {
		RollbackIncompleteGroup();
	} catch (...) {
		// Best effort; never mask the original error.
	}
	ReleaseBusySlot();
	if (user_cancelled) {
		// In the step channel, cancellation is a status, not an error,
		// regardless of which phase the interrupt landed in. The status
		// channel carries no message, so the engine's error text is
		// deliberately dropped here.
		state = State::CANCELLED;
		return DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED;
	}
	state = State::ERRORED;
	error = std::move(error_data);
	error.Throw();
}

DUCKDB_V2_RESULT_STEP_STATUS ResultWrapperV2::Step(unique_ptr<DataChunk> &out_chunk) {
	out_chunk.reset();
	switch (state) {
	case State::FINISHED:
		return DUCKDB_V2_RESULT_STEP_STATUS_FINISHED;
	case State::CANCELLED:
		return DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED;
	case State::ERRORED:
		// Sticky: rethrow the recorded error without touching the (closed)
		// internal results.
		error.Throw();
	case State::PENDING: {
		PendingExecutionResult exec;
		try {
			exec = pending->ExecuteTask();
		} catch (std::exception &ex) {
			return HandleExecutionError(ErrorData(ex));
		}
		switch (exec) {
		case PendingExecutionResult::RESULT_NOT_READY:
		case PendingExecutionResult::BLOCKED:
		case PendingExecutionResult::NO_TASKS_AVAILABLE:
			return DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		case PendingExecutionResult::EXECUTION_ERROR:
			return HandleExecutionError(pending->GetErrorObject());
		case PendingExecutionResult::RESULT_READY:
		case PendingExecutionResult::EXECUTION_FINISHED: {
			// Transition to the streaming phase. Execute() is (mostly)
			// instant once the pending result is ready. Reporting WAITING
			// after the transition keeps the contract simple: one unit of
			// work per step; the next step hits the stream.
			unique_ptr<QueryResult> res;
			try {
				res = pending->Execute();
			} catch (std::exception &ex) {
				return HandleExecutionError(ErrorData(ex));
			}
			pending.reset();
			if (res->HasError()) {
				return HandleExecutionError(res->GetErrorObject());
			}
			result = std::move(res);
			state = State::STREAMING;
			return DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		}
		}
		D_ASSERT(false); // unmapped PendingExecutionResult variant
		return DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
	}
	case State::STREAMING: {
		if (result->GetResultType() == QueryResultType::STREAM_RESULT) {
			auto &stream = result->Cast<StreamQueryResult>();
			StreamExecutionResult exec;
			try {
				exec = stream.ExecuteTask();
			} catch (std::exception &ex) {
				// SimpleBufferedData surfaces a pending interrupt by
				// throwing InterruptException from ExecuteTask; route it
				// through the same sink as EXECUTION_CANCELLED.
				return HandleExecutionError(ErrorData(ex));
			}
			switch (exec) {
			case StreamExecutionResult::CHUNK_NOT_READY:
			case StreamExecutionResult::BLOCKED:
			case StreamExecutionResult::NO_TASKS_AVAILABLE:
				return DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
			case StreamExecutionResult::EXECUTION_CANCELLED:
				pending.reset();
				result.reset();
				fragments.clear();
				try {
					RollbackIncompleteGroup();
				} catch (...) {
					// Best effort; cancellation still wins.
				}
				ReleaseBusySlot();
				state = State::CANCELLED;
				return DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED;
			case StreamExecutionResult::EXECUTION_ERROR:
				return HandleExecutionError(stream.GetErrorObject());
			case StreamExecutionResult::CHUNK_READY:
			case StreamExecutionResult::EXECUTION_FINISHED:
				// Both states mean "Fetch without doing more execution
				// work": CHUNK_READY has a buffered chunk; after
				// EXECUTION_FINISHED the buffer may still hold trailing
				// chunks and Fetch drains them until it reports
				// end-of-stream.
				break;
			}
		}
		// Fetch the next buffered chunk. For the materialized fallback
		// (non-row statements can come back materialized even with
		// ALLOW_STREAMING) the data is fully available and every step
		// lands here directly.
		unique_ptr<DataChunk> chunk;
		try {
			chunk = result->Fetch();
		} catch (std::exception &ex) {
			return HandleExecutionError(ErrorData(ex));
		}
		if (result->HasError()) {
			// StreamQueryResult::Fetch reports late execution errors by
			// setting the error and returning null.
			return HandleExecutionError(result->GetErrorObject());
		}
		if (!chunk || chunk->size() == 0) {
			// Stream results already normalize end-of-stream to null (and
			// close) in FetchInternal; the size() == 0 arm is needed for the
			// materialized fallback, whose Fetch does not normalize.
			result.reset();
			if (fragment_index < fragments.size()) {
				// More fragments in the group: start the next one and keep
				// reporting WAITING. FINISHED only after the whole group
				// (including a trailing injected COMMIT) has executed.
				try {
					StartNextFragment();
				} catch (std::exception &ex) {
					return HandleExecutionError(ErrorData(ex));
				}
				return DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
			}
			state = State::FINISHED;
			ReleaseBusySlot();
			return DUCKDB_V2_RESULT_STEP_STATUS_FINISHED;
		}
		if (!principal_active) {
			// Output of a non-principal fragment (e.g. the internal UPDATE of
			// an expanded ALTER): discard it, exactly as ClientContext::Query
			// drops these results from its chain.
			return DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		}
		out_chunk = std::move(chunk);
		return DUCKDB_V2_RESULT_STEP_STATUS_CHUNK;
	}
	}
	D_ASSERT(false); // unreachable: all states handled above
	return DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
}

void ResultWrapperV2::Wait() {
	// Unlike ExecuteTask, the engine's WaitForTask performs no
	// executability check and dereferences the context's active query
	// unguarded; an unchecked wait on an invalidated result is UB. Verify
	// executability first, the same way the steps do.
	switch (state) {
	case State::PENDING: {
		// CheckPulse re-validates executability (throwing a clean error when
		// the result is closed) and reports the engine state without running
		// work. Its outcome decides whether blocking is safe: the engine may
		// already have processed an execution error and ended the query
		// internally (waiting then dereferences the closed query), and a
		// ready or finished query has no task to wake on.
		switch (pending->CheckPulse()) {
		case PendingExecutionResult::BLOCKED:
		case PendingExecutionResult::NO_TASKS_AVAILABLE:
			// No progress possible right now; this is the one case where
			// blocking is meaningful.
			pending->WaitForTask();
			return;
		case PendingExecutionResult::EXECUTION_ERROR:
			// The error (interrupts included) is already recorded on the
			// pending result and the engine closed the query. Transition the
			// state machine now: a later ExecuteTask would trip the engine's
			// closed-result check and misreport the error as INVALID_INPUT.
			HandleExecutionError(pending->GetErrorObject());
			return;
		default:
			// RESULT_NOT_READY / RESULT_READY / EXECUTION_FINISHED: the next
			// step makes progress without blocking.
			return;
		}
	}
	case State::STREAMING:
		if (result->GetResultType() == QueryResultType::STREAM_RESULT) {
			auto &stream = result->Cast<StreamQueryResult>();
			if (!stream.IsOpen()) {
				// Closed or invalidated: the next step returns without
				// blocking, so there is nothing to wait for.
				return;
			}
			stream.WaitForTask();
		}
		// Materialized fallback: every step makes progress; nothing to
		// wait for.
		return;
	default:
		// Terminal states: waiting is a no-op, never an error.
		return;
	}
}

unique_ptr<DataChunk> ResultWrapperV2::FetchChunkBlocking() {
	while (true) {
		unique_ptr<DataChunk> chunk;
		switch (Step(chunk)) {
		case DUCKDB_V2_RESULT_STEP_STATUS_CHUNK:
			return chunk;
		case DUCKDB_V2_RESULT_STEP_STATUS_FINISHED:
			return nullptr;
		case DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED:
			// fetch_chunk has no status channel, so cancellation surfaces
			// as ERROR_RUNTIME_INTERRUPT. The state stays CANCELLED (not
			// ERRORED), so steps keep reporting the status. Throwing a
			// fresh InterruptException means the error text is the generic
			// "Interrupted!", not the engine's message; deliberate, since
			// the CANCELLED state carries no message either.
			throw InterruptException();
		case DUCKDB_V2_RESULT_STEP_STATUS_WAITING:
			Wait();
			break;
		default:
			break;
		}
	}
}

auto Convert(ResultWrapperV2 *wrapper) -> duckdb_v2_result_handle {
	return reinterpret_cast<duckdb_v2_result_handle>(wrapper);
}
auto Convert(duckdb_v2_result_handle handle) -> ResultWrapperV2 * {
	return reinterpret_cast<ResultWrapperV2 *>(handle);
}

auto ExecutePreparedStatementV2(const shared_ptr<ClientContext> &context, PreparedStatement &prepared,
                                identifier_map_t<BoundParameterData> &values) -> duckdb_v2_result_handle {
	auto wrapper = make_uniq<ResultWrapperV2>();
	// One live result per connection, claimed the way statement_execute claims it and
	// before PendingQuery runs, which would otherwise cancel the live stream.
	auto busy_slot = GetBusySlot(*context);
	void *expected = nullptr;
	if (!busy_slot->owner.compare_exchange_strong(expected, wrapper.get())) {
		throw ResourceInUseException("connection has a live result; drain, destroy, or interrupt it before starting "
		                             "a new query (or open another connection)");
	}
	// On any failure below, the wrapper's destructor releases the slot.
	wrapper->busy_slot = std::move(busy_slot);
	wrapper->busy_slot->cancel_requested.store(false, std::memory_order_relaxed);
	wrapper->context = context;
	// A prepared statement is always one engine statement: preprocessing, expansion and
	// the wrapping transaction all happened at prepare time, so this bypasses the fragment
	// machinery and is always principal. fragment_count is 1 for metadata symmetry only.
	wrapper->fragment_count = 1;
	wrapper->BeginPending(prepared.PendingQuery(values, /*allow_stream_result=*/true), true);
	// The engine runs a prepared statement through an internal EXECUTE, whose statement type
	// would otherwise be what the result reports. Report the type of the statement that was
	// prepared instead, so a prepared result is indistinguishable from a stateless one.
	wrapper->statement_type = prepared.GetStatementType();
	return Convert(wrapper.release());
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public API
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_statement_execute(duckdb_v2_connection_handle conn, duckdb_v2_sql_statement_handle statement,
                                            const duckdb_v2_identifier_t *parameter_names,
                                            const duckdb_v2_value_handle *parameter_values, idx_t parameter_count,
                                            duckdb_v2_result_handle *out_result, duckdb_v2_error_info_handle *err) {
	// The refusals here never reach the engine and leave the statement intact
	// (the spec commits to this).
	DUCKDB_CHECK_ARG(out_result);
	*out_result = nullptr;
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(statement);
	if (parameter_count > 0 && !parameter_values) {
		return NullArgumentError(err, __func__, "parameter_values");
	}
	return WithErrorHandler(err, [&]() {
		auto *connection = Convert(conn);
		auto wrapper = duckdb::make_uniq<ResultWrapperV2>();
		// One live result per connection. The busy slot lives in the context's
		// registered-state map (so the connection handle stays a bare Connection *),
		// shared with this result. The busy check is a manual return path: no
		// ExceptionType maps to RESOURCE_IN_USE, so routing it through
		// WithErrorHandler would degrade the code. It must run before PendingQuery,
		// which would otherwise silently cancel the live stream.
		auto busy_slot = GetBusySlot(*connection->context);
		void *expected = nullptr;
		if (!busy_slot->owner.compare_exchange_strong(expected, wrapper.get())) {
			throw duckdb::ResourceInUseException(
			    "connection has a live result; drain, destroy, or interrupt it before starting "
			    "a new query (or open another connection)");
		}
		// On any failure below, the wrapper's destructor releases the slot.
		wrapper->busy_slot = std::move(busy_slot);
		// A fresh query starts uncancelled: clear any consumer-cancellation request
		// left over from before this result claimed the slot (mirrors the engine
		// clearing interrupt_state at query begin).
		wrapper->busy_slot->cancel_requested.store(false, std::memory_order_relaxed);

		// Borrowed, not consumed: execute a copy so the caller keeps the original.
		auto stmt = Convert(statement)->Copy();
		// Fold parameter values in as constants, keyed by name when parameter_names
		// supplies one and positionally ("1".."N") otherwise.
		BuildParameterMap(parameter_names, parameter_values, parameter_count, "duckdb_v2_statement_execute",
		                  wrapper->param_values);
		// Statement-level preprocessing (pragma reparsing, expansion
		// unpacking, transaction wrapping): one user statement can expand
		// into a group of engine statements that the wrapper executes in
		// order. parse_sql deliberately leaves this to statement_execute so
		// parsing stays binder-free and a group is never split across the
		// API boundary.
		wrapper->fragments.push_back(std::move(stmt));
		connection->context->PreprocessStatements(wrapper->fragments);
		if (wrapper->fragments.empty()) {
			throw duckdb::InvalidInputException("statement preprocessing yielded no executable statements");
		}
		wrapper->fragment_count = wrapper->fragments.size();
		// Reject parameters on a statement that expands into a group: the values would
		// bind to the first fragment, an injected BEGIN, not the user's statement. (A
		// statement can carry a parameter and still expand, e.g. a volatile DEFAULT.)
		if (!wrapper->param_values.empty() && wrapper->fragments.size() > 1) {
			throw duckdb::InvalidInputException(
			    "parameters are not supported for a statement that expands into multiple engine statements");
		}
		// Detect whether preprocessing wrapped this group in its own
		// transaction (autocommit input expanded to BEGIN ... COMMIT).
		// Preprocessing only injects the wrap for a multi-fragment group, as a
		// leading BEGIN paired with a trailing COMMIT; a lone user-issued BEGIN
		// is a single fragment the user owns and the bridge must not roll back.
		if (wrapper->fragments.size() > 1 &&
		    wrapper->fragments.front()->type == duckdb::StatementType::TRANSACTION_STATEMENT &&
		    wrapper->fragments.back()->type == duckdb::StatementType::TRANSACTION_STATEMENT) {
			auto &front_stmt = wrapper->fragments.front()->Cast<duckdb::TransactionStatement>();
			auto &back_stmt = wrapper->fragments.back()->Cast<duckdb::TransactionStatement>();
			wrapper->owns_wrapping_transaction = front_stmt.info->type == duckdb::TransactionType::BEGIN_TRANSACTION &&
			                                     back_stmt.info->type == duckdb::TransactionType::COMMIT;
		}
		wrapper->context = connection->context;
		// Prepare the first fragment. Lazy streaming execution: nothing
		// executes until the result is stepped; for non-expanding statements
		// (the common case) this also captures the metadata immediately.
		wrapper->StartNextFragment();
		*out_result = Convert(wrapper.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_result_drain(duckdb_v2_result_handle result, idx_t *out_rows_changed,
                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(result);
	DUCKDB_CHECK_ARG(out_rows_changed);
	return WithErrorHandler(err, [&]() {
		auto *wrapper = Convert(result);
		// Drain to completion: side effects are applied, rows of a
		// row-producing result are discarded. CHANGED_ROWS results stream a
		// single-row BIGINT Count chunk carrying the affected row count.
		// The principal fragment's metadata may only become available
		// mid-drain (expanding statements), so check per chunk: surfaced
		// chunks always belong to the principal fragment.
		idx_t rows_changed = 0;
		while (auto chunk = wrapper->FetchChunkBlocking()) {
			bool changed_rows = wrapper->metadata_available &&
			                    wrapper->properties.return_type == duckdb::StatementReturnType::CHANGED_ROWS;
			if (changed_rows && chunk->size() > 0) {
				rows_changed = static_cast<idx_t>(chunk->GetValue(0, 0).GetValue<int64_t>());
			}
		}
		*out_rows_changed = rows_changed;
	});
}

DUCKDB_V2_ERROR duckdb_v2_result_render_box(duckdb_v2_result_handle *result, idx_t max_rows, idx_t max_width,
                                            idx_t max_col_width, duckdb_v2_str null_value, idx_t render_mode,
                                            idx_t limit, duckdb_v2_text_sink_fn sink, void *user_data,
                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(result);
	DUCKDB_CHECK_ARG(*result);
	DUCKDB_CHECK_ARG(sink);
	// Validate the by-value arguments before consuming the result, so an
	// argument rejection leaves the caller's result intact (as the null-arg
	// rejections above do).
	DUCKDB_CHECK_ARG(null_value);
	return WithErrorHandler(err, [&]() {
		if (render_mode > 1) {
			throw duckdb::InvalidInputException("render_mode must be 0 (rows) or 1 (columns)");
		}
		// Adopt by transfer; consumed on success and failure alike.
		auto wrapper = duckdb::unique_ptr<ResultWrapperV2>(Convert(*result));
		*result = nullptr;
		// Names and types must be available before building the collection;
		// expanding statements may need stepping to the principal fragment.
		while (!wrapper->metadata_available) {
			duckdb::unique_ptr<duckdb::DataChunk> discard;
			auto status = wrapper->Step(discard);
			if (status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING) {
				wrapper->Wait();
				continue;
			}
			if (status == DUCKDB_V2_RESULT_STEP_STATUS_CHUNK) {
				throw duckdb::InternalException("render box: a row was produced before result metadata was available");
			}
			break; // FINISHED / CANCELLED: no row-producing fragment.
		}
		if (!wrapper->context) {
			throw duckdb::InvalidInputException("result is not associated with an active context");
		}
		auto &ctx = *wrapper->context;

		// Materialize the remainder; max_rows bounds display, not the read.
		duckdb::ColumnDataCollection collection(duckdb::Allocator::DefaultAllocator(), wrapper->types);
		while (auto chunk = wrapper->FetchChunkBlocking()) {
			collection.Append(*chunk);
		}

		duckdb::BoxRendererConfig config;
		if (max_rows != 0) {
			config.max_rows = max_rows;
		}
		if (max_width != 0) {
			config.max_width = max_width;
		}
		if (max_col_width != 0) {
			config.max_col_width = max_col_width;
		}
		if (null_value.len) {
			config.null_value = std::string(null_value.ptr, null_value.len);
		}
		config.render_mode = render_mode == 1 ? duckdb::RenderMode::COLUMNS : duckdb::RenderMode::ROWS;
		// The caller's query-side LIMIT: when the materialized result fills it, the
		// footer renders "? rows" since the true total is unknown.
		config.limit = limit;

		duckdb::ClientBoxRendererContext render_context(ctx);
		duckdb::BoxRenderer renderer(config);
		duckdb::ColumnDataCollectionWrapper data(collection);

		duckdb::vector<duckdb::string> names;
		for (auto &ident : wrapper->names) {
			names.push_back(ident.GetIdentifierName());
		}

		auto text = renderer.ToString(render_context, names, data);
		InvokeTextSink(sink, Convert(text), user_data);
	});
}

DUCKDB_V2_ERROR duckdb_v2_result_destroy(duckdb_v2_result_handle *result) {
	return WithErrorHandler(nullptr, [&]() {
		if (!result || !*result) {
			return;
		}
		// Adopt so the wrapper is freed even if Finalize() throws.
		duckdb::unique_ptr<ResultWrapperV2> wrapper(Convert(*result));
		*result = nullptr;
		wrapper->Finalize();
	});
}

// ---------------------------------------------------------------------------
// Streaming consumption
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_result_step(duckdb_v2_result_handle result, duckdb_v2_data_chunk_handle *out_chunk,
                                      DUCKDB_V2_RESULT_STEP_STATUS *out_status, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(result);
	DUCKDB_CHECK_ARG(out_chunk);
	DUCKDB_CHECK_ARG(out_status);
	*out_chunk = nullptr;
	return WithErrorHandler(err, [&]() {
		auto *wrapper = Convert(result);
		duckdb::unique_ptr<duckdb::DataChunk> chunk;
		auto status = wrapper->Step(chunk);
		*out_status = status;
		if (status == DUCKDB_V2_RESULT_STEP_STATUS_CHUNK) {
			*out_chunk = Convert(chunk.release());
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_result_fetch_chunk(duckdb_v2_result_handle result, duckdb_v2_data_chunk_handle *out_chunk,
                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(result);
	DUCKDB_CHECK_ARG(out_chunk);
	*out_chunk = nullptr;
	return WithErrorHandler(err, [&]() {
		auto *wrapper = Convert(result);
		auto chunk = wrapper->FetchChunkBlocking();
		if (chunk) {
			*out_chunk = Convert(chunk.release());
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_result_wait(duckdb_v2_result_handle result, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(result);
	return WithErrorHandler(err, [&]() { Convert(result)->Wait(); });
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_result_get_result_type(duckdb_v2_result_handle result, DUCKDB_V2_RESULT_TYPE *out_type,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(result);
	DUCKDB_CHECK_ARG(out_type);

	return WithErrorHandler(err, [&]() {
		auto *r = Convert(result);
		r->RequireMetadata();
		*out_type = MapResultType(r->properties.return_type);
	});
}

DUCKDB_V2_ERROR duckdb_v2_result_get_statement_type(duckdb_v2_result_handle result, DUCKDB_V2_STATEMENT_TYPE *out_type,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(result);
	DUCKDB_CHECK_ARG(out_type);

	return WithErrorHandler(err, [&]() {
		auto *r = Convert(result);
		r->RequireMetadata();
		*out_type = static_cast<DUCKDB_V2_STATEMENT_TYPE>(r->statement_type);
	});
}

DUCKDB_V2_ERROR duckdb_v2_result_get_schema(duckdb_v2_result_handle result, duckdb_v2_schema_handle *out_schema,
                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_schema);
	*out_schema = nullptr;

	DUCKDB_CHECK_ARG(result);

	return WithErrorHandler(err, [&]() {
		auto *r = Convert(result);
		r->RequireMetadata();
		auto schema = duckdb::make_uniq<CV2Schema>();
		for (duckdb::idx_t i = 0; i < r->types.size(); i++) {
			schema->fields.push_back({r->names[i].GetIdentifierName(), r->types[i]});
		}
		*out_schema = Convert(schema.release());
	});
}
