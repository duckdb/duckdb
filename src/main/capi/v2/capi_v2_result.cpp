#include "duckdb/main/capi_v2/capi_v2_result_internal.hpp"

#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/box_renderer_context.hpp"
#include "duckdb/common/column_data_collection_render_interface.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

#include "duckdb/common/enums/query_result_state.hpp"
#include "duckdb/main/buffered_data/buffered_data.hpp"
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

// The state machine turns the terminal engine states into its own, so they never reach here.
DUCKDB_V2_RESULT_STATUS MapProgressState(QueryResultState state) {
	switch (state) {
	case QueryResultState::NOT_READY:
		return DUCKDB_V2_RESULT_STATUS_NOT_READY;
	case QueryResultState::READY:
		return DUCKDB_V2_RESULT_STATUS_READY;
	case QueryResultState::BLOCKED:
		return DUCKDB_V2_RESULT_STATUS_BLOCKED;
	case QueryResultState::NO_TASKS_AVAILABLE:
		return DUCKDB_V2_RESULT_STATUS_NO_TASKS_AVAILABLE;
	default:
		D_ASSERT(false); // terminal states are handled by the caller
		return DUCKDB_V2_RESULT_STATUS_NOT_READY;
	}
}

ResultEagerness MapEagerness(DUCKDB_V2_RESULT_EAGERNESS eagerness) {
	switch (eagerness) {
	case DUCKDB_V2_RESULT_EAGERNESS_AUTO:
		return ResultEagerness::AUTO;
	case DUCKDB_V2_RESULT_EAGERNESS_FORCED:
		return ResultEagerness::FORCED;
	default:
		throw InvalidInputException("eagerness must be DUCKDB_V2_RESULT_EAGERNESS_AUTO or _FORCED");
	}
}

auto ConvertCollection(ColumnDataCollection *collection) -> duckdb_v2_column_data_collection_handle {
	return reinterpret_cast<duckdb_v2_column_data_collection_handle>(collection);
}

} // anonymous namespace

auto Convert(ExecuteArgsV2 *args) -> duckdb_v2_execute_args_handle {
	return reinterpret_cast<duckdb_v2_execute_args_handle>(args);
}
auto Convert(duckdb_v2_execute_args_handle args) -> ExecuteArgsV2 * {
	return reinterpret_cast<ExecuteArgsV2 *>(args);
}

auto Convert(ResultWrapperV2 *wrapper) -> duckdb_v2_result_handle {
	return reinterpret_cast<duckdb_v2_result_handle>(wrapper);
}
auto Convert(duckdb_v2_result_handle handle) -> ResultWrapperV2 * {
	return reinterpret_cast<ResultWrapperV2 *>(handle);
}
auto ConvertStream(ResultWrapperV2 *wrapper) -> duckdb_v2_result_stream_handle {
	return reinterpret_cast<duckdb_v2_result_stream_handle>(wrapper);
}
auto Convert(duckdb_v2_result_stream_handle stream) -> ResultWrapperV2 * {
	return reinterpret_cast<ResultWrapperV2 *>(stream);
}

void ResultWrapperV2::BeginPending(unique_ptr<QueryResult> next_handle, bool is_principal) {
	if (next_handle->HasError()) {
		// Re-throw the typed ErrorData so the exception's ExceptionType is
		// preserved and routed through GetErrorCodeFromExceptionType.
		next_handle->GetErrorObject().Throw();
	}
	principal_active = is_principal;
	if (is_principal) {
		types = next_handle->GetTypes();
		names = next_handle->GetNames();
		statement_type = next_handle->GetStatementType();
		properties = next_handle->GetStatementProperties();
		metadata_available = true;
	}
	// A fragment nobody consumes must never park for a retention decision.
	if (!is_principal || retain_requested) {
		next_handle->Materialize();
	}
	handle = std::move(next_handle);
	// A delegating result collector runs the query inside Submit and hands back a detached handle.
	fragment_finished = !handle->IsOpen();
	state = State::RUNNING;
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
	QueryParameters parameters;
	parameters.result_eagerness = eagerness;
	// Parameters bind only to the first fragment (statement_execute rejects them on
	// a statement that expands, so that fragment is the user's statement). Later
	// fragments take the no-values path.
	auto next_handle = (this_index == 0 && !param_values.empty())
	                       ? context->Submit(std::move(stmt), param_values, parameters)
	                       : context->Submit(std::move(stmt), parameters);
	// Principal selection is a property of the fragment group; compute it here, then
	// hand the handle to the shared BeginPending seam. A HasError() handle is left
	// for BeginPending to raise (return_type is meaningless on it).
	bool has_result = !next_handle->HasError() &&
	                  next_handle->GetStatementProperties().return_type == StatementReturnType::QUERY_RESULT;
	if (principal_seen && has_result) {
		// ClientContext::Query would chain these as separate results with
		// separate schemas; a single result handle cannot. No known expansion
		// produces two row-producing fragments.
		throw NotImplementedException(
		    "statement expands to multiple row-producing statements, which cannot be served as a single result");
	}
	// Result selection mirrors ClientContext::Query: the caller sees the
	// first row-producing fragment, or the last fragment when none
	// produces rows.
	bool is_principal = has_result || (is_last && !principal_seen);
	if (has_result) {
		principal_seen = true;
	}
	BeginPending(std::move(next_handle), is_principal);
}

void ResultWrapperV2::RequireMetadata() const {
	if (!metadata_available) {
		throw InvalidInputException("result metadata is not yet available: the statement expands to a group of "
		                            "statements and the result-producing one has not been prepared; step the result");
	}
}

DUCKDB_V2_RESULT_STATUS ResultWrapperV2::HandleExecutionError(ErrorData error_data) {
	// Only a consumer-initiated cancellation is a cancellation. An
	// engine-initiated interrupt that shares the INTERRUPT exception type
	// (e.g. a max_execution_time timeout) must surface as an error carrying the
	// engine's message, mirroring the eager ClientContext::Query path.
	bool user_cancelled = error_data.Type() == ExceptionType::INTERRUPT && busy_slot &&
	                      busy_slot->cancel_requested.load(std::memory_order_relaxed);
	stream.reset();
	handle.reset();
	principal.reset();
	fragments.clear();
	try {
		RollbackIncompleteGroup();
	} catch (...) {
		// Best effort; never mask the original error.
	}
	ReleaseBusySlot();
	if (user_cancelled) {
		// In the status channel, cancellation is a status, not an error,
		// regardless of which phase the interrupt landed in. The status
		// channel carries no message, so the engine's error text is
		// deliberately dropped here.
		state = State::CANCELLED;
		return DUCKDB_V2_RESULT_STATUS_CANCELLED;
	}
	state = State::ERRORED;
	error = std::move(error_data);
	error.Throw();
}

DUCKDB_V2_RESULT_STATUS ResultWrapperV2::ReportFragmentFinished() {
	fragment_finished = true;
	if (principal_active) {
		// Completing the principal fragment would settle its rows on being kept, which is the
		// consumer's choice to make.
		return DUCKDB_V2_RESULT_STATUS_FINISHED;
	}
	return FinishFragment();
}

DUCKDB_V2_RESULT_STATUS ResultWrapperV2::FinishFragment() {
	try {
		handle->Complete();
	} catch (std::exception &ex) {
		return HandleExecutionError(ErrorData(ex));
	}
	if (handle->HasError()) {
		return HandleExecutionError(handle->GetErrorObject());
	}
	if (principal_active) {
		principal = std::move(handle);
	}
	handle.reset();
	principal_active = false;
	fragment_finished = false;
	if (fragment_index < fragments.size()) {
		try {
			StartNextFragment();
		} catch (std::exception &ex) {
			return HandleExecutionError(ErrorData(ex));
		}
		return DUCKDB_V2_RESULT_STATUS_NOT_READY;
	}
	return Finished();
}

DUCKDB_V2_RESULT_STATUS ResultWrapperV2::EndStream() {
	if (stream) {
		// Closing commits an autocommit transaction, which can fail; the stream records that
		// without throwing.
		stream->Close();
		if (stream->HasError()) {
			return HandleExecutionError(stream->GetErrorObject());
		}
		stream.reset();
	}
	principal_active = false;
	fragment_finished = false;
	if (fragment_index < fragments.size()) {
		state = State::RUNNING;
		try {
			StartNextFragment();
		} catch (std::exception &ex) {
			return HandleExecutionError(ErrorData(ex));
		}
		return DUCKDB_V2_RESULT_STATUS_NOT_READY;
	}
	return Finished();
}

DUCKDB_V2_RESULT_STATUS ResultWrapperV2::Finished() {
	state = State::FINISHED;
	fragments.clear();
	// The group's own COMMIT has run, so there is nothing left for a later destroy to roll back.
	owns_wrapping_transaction = false;
	ReleaseBusySlot();
	return DUCKDB_V2_RESULT_STATUS_FINISHED;
}

DUCKDB_V2_RESULT_STATUS ResultWrapperV2::Step() {
	switch (state) {
	case State::FINISHED:
		return DUCKDB_V2_RESULT_STATUS_FINISHED;
	case State::CANCELLED:
		return DUCKDB_V2_RESULT_STATUS_CANCELLED;
	case State::ERRORED:
		error.Throw();
	case State::STREAMING: {
		QueryResultState exec;
		try {
			exec = stream->ExecuteTask();
		} catch (std::exception &ex) {
			// The stream records an interrupt as an error rather than throwing; this catches escapees.
			return HandleExecutionError(ErrorData(ex));
		}
		if (exec == QueryResultState::EXECUTION_ERROR) {
			return HandleExecutionError(stream->GetErrorObject());
		}
		if (exec == QueryResultState::FINISHED) {
			// Execution is done, but Poll still reports READY while a chunk is poppable.
			return Poll();
		}
		return MapProgressState(exec);
	}
	case State::RUNNING:
		break;
	}
	if (fragment_finished) {
		return ReportFragmentFinished();
	}
	QueryResultState exec;
	try {
		exec = handle->ExecuteTask();
	} catch (std::exception &ex) {
		return HandleExecutionError(ErrorData(ex));
	}
	if (exec == QueryResultState::EXECUTION_ERROR) {
		return HandleExecutionError(handle->GetErrorObject());
	}
	if (exec == QueryResultState::FINISHED) {
		return ReportFragmentFinished();
	}
	return MapProgressState(exec);
}

DUCKDB_V2_RESULT_STATUS ResultWrapperV2::Poll() {
	switch (state) {
	case State::FINISHED:
		return DUCKDB_V2_RESULT_STATUS_FINISHED;
	case State::CANCELLED:
		return DUCKDB_V2_RESULT_STATUS_CANCELLED;
	case State::ERRORED:
		error.Throw();
	case State::STREAMING: {
		QueryResultState exec;
		try {
			exec = stream->Poll();
		} catch (std::exception &ex) {
			return HandleExecutionError(ErrorData(ex));
		}
		if (exec == QueryResultState::EXECUTION_ERROR) {
			return HandleExecutionError(stream->GetErrorObject());
		}
		if (exec == QueryResultState::FINISHED) {
			return EndStream();
		}
		return MapProgressState(exec);
	}
	case State::RUNNING:
		break;
	}
	if (fragment_finished) {
		return ReportFragmentFinished();
	}
	QueryResultState exec;
	try {
		exec = handle->Poll();
	} catch (std::exception &ex) {
		return HandleExecutionError(ErrorData(ex));
	}
	if (exec == QueryResultState::EXECUTION_ERROR) {
		return HandleExecutionError(handle->GetErrorObject());
	}
	if (exec == QueryResultState::FINISHED) {
		return ReportFragmentFinished();
	}
	return MapProgressState(exec);
}

void ResultWrapperV2::Wait() {
	switch (state) {
	case State::RUNNING: {
		if (!handle || fragment_finished) {
			return;
		}
		// Polling first turns a failure that already closed the query into this wrapper's terminal
		// state, rather than an INVALID_INPUT from the next step.
		switch (handle->Poll()) {
		case QueryResultState::BLOCKED:
		case QueryResultState::NO_TASKS_AVAILABLE:
			// No progress possible right now; this is the one case where blocking is meaningful.
			handle->WaitForTask();
			return;
		case QueryResultState::EXECUTION_ERROR:
			// The engine already closed the query. Transition now: a later ExecuteTask would trip
			// the closed-result check and misreport the error as INVALID_INPUT.
			HandleExecutionError(handle->GetErrorObject());
			return;
		default:
			return;
		}
	}
	case State::STREAMING:
		if (stream && stream->IsOpen()) {
			stream->WaitForTask();
		}
		return;
	default:
		return;
	}
}

void ResultWrapperV2::ReachPrincipal() {
	while (state == State::RUNNING && !principal_active) {
		switch (Step()) {
		case DUCKDB_V2_RESULT_STATUS_FINISHED:
		case DUCKDB_V2_RESULT_STATUS_CANCELLED:
			return;
		case DUCKDB_V2_RESULT_STATUS_BLOCKED:
		case DUCKDB_V2_RESULT_STATUS_NO_TASKS_AVAILABLE:
			Wait();
			break;
		default:
			break;
		}
	}
}

void ResultWrapperV2::RequestRetention() {
	if (IsTerminalState() || state == State::STREAMING) {
		return;
	}
	retain_requested = true;
	if (principal_active && handle) {
		handle->Materialize();
	}
}

void ResultWrapperV2::RequireLive() {
	if (state == State::CANCELLED) {
		throw InterruptException();
	}
	if (state == State::ERRORED) {
		error.Throw();
	}
}

void ResultWrapperV2::RunToCompletion() {
	if (state == State::STREAMING) {
		// Stepping a stream nobody pops from saturates the buffer and then spins on READY forever.
		throw InvalidInputException("this result's rows are being drained as a stream");
	}
	RequestRetention();
	while (true) {
		RequireLive();
		if (state == State::FINISHED) {
			return;
		}
		if (fragment_finished) {
			// Step and poll leave the principal fragment for the calls that keep its rows.
			FinishFragment();
			continue;
		}
		switch (Step()) {
		case DUCKDB_V2_RESULT_STATUS_CANCELLED:
			throw InterruptException();
		case DUCKDB_V2_RESULT_STATUS_BLOCKED:
		case DUCKDB_V2_RESULT_STATUS_NO_TASKS_AVAILABLE:
		case DUCKDB_V2_RESULT_STATUS_READY:
			Wait();
			break;
		default:
			break;
		}
	}
}

void ResultWrapperV2::ThrowNoRows() const {
	throw InvalidInputException("this result no longer holds its rows: they were taken with "
	                            "duckdb_v2_result_take_collection");
}

unique_ptr<DataChunk> ResultWrapperV2::Fetch() {
	RunToCompletion();
	if (!principal || collection_taken) {
		return nullptr;
	}
	return principal->Fetch();
}

ColumnDataCollection &ResultWrapperV2::Collection() {
	RunToCompletion();
	if (!principal || collection_taken) {
		ThrowNoRows();
	}
	return principal->Collection();
}

unique_ptr<ColumnDataCollection> ResultWrapperV2::TakeCollection() {
	RunToCompletion();
	if (!principal || collection_taken) {
		ThrowNoRows();
	}
	auto taken = principal->TakeCollection();
	collection_taken = true;
	return taken;
}

bool ResultWrapperV2::CanStream() {
	if (IsTerminalState() || state == State::STREAMING) {
		return false;
	}
	RequireMetadata();
	if (retain_requested) {
		return false;
	}
	auto candidate = principal_active ? handle.get() : principal.get();
	if (!candidate || !candidate->HasBufferedData()) {
		// A delegating result collector produces its rows itself and never gets a streaming buffer.
		return false;
	}
	return candidate->GetBufferedData().Lifetime() == ResultLifetime::UNDECIDED;
}

void ResultWrapperV2::OpenStream() {
	if (IsTerminalState()) {
		throw InvalidInputException("this result cannot be streamed: it is finished, cancelled, or failed");
	}
	if (state == State::STREAMING || retain_requested) {
		throw InvalidInputException("this result cannot be streamed: its rows are already being kept or drained");
	}
	ReachPrincipal();
	if (state != State::RUNNING || !principal_active || !handle) {
		throw InvalidInputException("this result cannot be streamed: it produced no row-producing statement");
	}
	// The constructor destroys the handle it was given when it throws, so a refusal here leaves
	// nothing open behind it.
	stream = make_uniq<QueryResultStream>(std::move(handle));
	state = State::STREAMING;
	streamed = true;
}

DUCKDB_V2_RESULT_STATUS ResultWrapperV2::StreamTryFetch(unique_ptr<DataChunk> &out_chunk) {
	out_chunk.reset();
	if (state != State::STREAMING) {
		return Poll();
	}
	QueryResultState exec;
	unique_ptr<DataChunk> chunk;
	try {
		exec = stream->TryFetch(chunk);
	} catch (std::exception &ex) {
		return HandleExecutionError(ErrorData(ex));
	}
	if (exec == QueryResultState::EXECUTION_ERROR) {
		return HandleExecutionError(stream->GetErrorObject());
	}
	if (exec == QueryResultState::FINISHED) {
		return EndStream();
	}
	if (exec == QueryResultState::READY) {
		out_chunk = std::move(chunk);
		return DUCKDB_V2_RESULT_STATUS_READY;
	}
	return MapProgressState(exec);
}

unique_ptr<DataChunk> ResultWrapperV2::StreamFetch() {
	while (true) {
		switch (state) {
		case State::FINISHED:
			return nullptr;
		case State::CANCELLED:
			throw InterruptException();
		case State::ERRORED:
			error.Throw();
		case State::STREAMING: {
			unique_ptr<DataChunk> chunk;
			try {
				chunk = stream->Fetch();
			} catch (std::exception &ex) {
				HandleExecutionError(ErrorData(ex));
				break;
			}
			if (stream->HasError()) {
				// The stream reports late execution errors by setting the error and returning null.
				HandleExecutionError(stream->GetErrorObject());
				break;
			}
			if (chunk) {
				return chunk;
			}
			EndStream();
			break;
		}
		case State::RUNNING:
			// A fragment after the principal one still has to run before the stream may end.
			switch (Step()) {
			case DUCKDB_V2_RESULT_STATUS_BLOCKED:
			case DUCKDB_V2_RESULT_STATUS_NO_TASKS_AVAILABLE:
			case DUCKDB_V2_RESULT_STATUS_READY:
				Wait();
				break;
			default:
				break;
			}
			break;
		}
	}
}

unique_ptr<DataChunk> ResultWrapperV2::NextChunk() {
	return streamed ? StreamFetch() : Fetch();
}

auto ExecutePreparedStatementV2(const shared_ptr<ClientContext> &context, PreparedStatement &prepared,
                                optional_ptr<ExecuteArgsV2> args) -> duckdb_v2_result_handle {
	auto wrapper = make_uniq<ResultWrapperV2>();
	// One live result per connection, claimed the way statement_execute claims it and
	// before the submission runs, which would otherwise cancel the live result.
	auto busy_slot = GetBusySlot(*context);
	void *expected = nullptr;
	if (!busy_slot->owner.compare_exchange_strong(expected, wrapper.get())) {
		throw ResourceInUseException("connection has a live result; finish, destroy, or interrupt it before starting "
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
	QueryParameters parameters;
	if (args) {
		wrapper->param_values = args->param_values;
		wrapper->eagerness = args->eagerness;
		parameters.result_eagerness = args->eagerness;
	}
	wrapper->BeginPending(prepared.Submit(wrapper->param_values, parameters), true);
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

DUCKDB_V2_ERROR duckdb_v2_execute_args_create(duckdb_v2_execute_args_handle *out_args,
                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_args);
	*out_args = nullptr;
	return WithErrorHandler(err, [&]() { *out_args = Convert(duckdb::make_uniq<ExecuteArgsV2>().release()); });
}

DUCKDB_V2_ERROR duckdb_v2_execute_args_destroy(duckdb_v2_execute_args_handle *args) {
	return WithErrorHandler(nullptr, [&]() {
		if (!args || !*args) {
			return;
		}
		delete Convert(*args);
		*args = nullptr;
	});
}

DUCKDB_V2_ERROR duckdb_v2_execute_args_set_statement_params(duckdb_v2_execute_args_handle args,
                                                            const duckdb_v2_identifier_t *parameter_names,
                                                            const duckdb_v2_value_handle *parameter_values,
                                                            idx_t parameter_count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(args);
	if (parameter_count > 0 && !parameter_values) {
		return NullArgumentError(err, __func__, "parameter_values");
	}
	return WithErrorHandler(err, [&]() {
		// Built into a fresh map so a rejected set leaves the previous one in place.
		duckdb::identifier_map_t<duckdb::BoundParameterData> values;
		BuildParameterMap(parameter_names, parameter_values, parameter_count, __func__, values);
		Convert(args)->param_values = std::move(values);
	});
}

DUCKDB_V2_ERROR duckdb_v2_execute_args_set_eagerness(duckdb_v2_execute_args_handle args,
                                                     DUCKDB_V2_RESULT_EAGERNESS eagerness,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(args);
	return WithErrorHandler(err, [&]() { Convert(args)->eagerness = MapEagerness(eagerness); });
}

DUCKDB_V2_ERROR duckdb_v2_statement_execute(duckdb_v2_connection_handle conn, duckdb_v2_sql_statement_handle statement,
                                            duckdb_v2_execute_args_handle args, duckdb_v2_result_handle *out_result,
                                            duckdb_v2_error_info_handle *err) {
	// The refusals here never reach the engine and leave the statement intact
	// (the spec commits to this).
	DUCKDB_CHECK_ARG(out_result);
	*out_result = nullptr;
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(statement);
	return WithErrorHandler(err, [&]() {
		auto *connection = Convert(conn);
		auto wrapper = duckdb::make_uniq<ResultWrapperV2>();
		// One live result per connection. The busy slot lives in the context's
		// registered-state map (so the connection handle stays a bare Connection *),
		// shared with this result. The busy check is a manual return path: no
		// ExceptionType maps to RESOURCE_IN_USE, so routing it through
		// WithErrorHandler would degrade the code. It must run before the submission,
		// which would otherwise silently cancel the live result.
		auto busy_slot = GetBusySlot(*connection->context);
		void *expected = nullptr;
		if (!busy_slot->owner.compare_exchange_strong(expected, wrapper.get())) {
			throw duckdb::ResourceInUseException(
			    "connection has a live result; finish, destroy, or interrupt it before starting "
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
		if (args) {
			auto *execute_args = Convert(args);
			wrapper->param_values = execute_args->param_values;
			wrapper->eagerness = execute_args->eagerness;
		}
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
		wrapper->StartNextFragment();
		*out_result = Convert(wrapper.release());
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

DUCKDB_V2_ERROR duckdb_v2_result_step(duckdb_v2_result_handle result, DUCKDB_V2_RESULT_STATUS *out_status,
                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(result);
	DUCKDB_CHECK_ARG(out_status);
	return WithErrorHandler(err, [&]() { *out_status = Convert(result)->Step(); });
}

DUCKDB_V2_ERROR duckdb_v2_result_poll(duckdb_v2_result_handle result, DUCKDB_V2_RESULT_STATUS *out_status,
                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(result);
	DUCKDB_CHECK_ARG(out_status);
	return WithErrorHandler(err, [&]() { *out_status = Convert(result)->Poll(); });
}

DUCKDB_V2_ERROR duckdb_v2_result_wait(duckdb_v2_result_handle result, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(result);
	return WithErrorHandler(err, [&]() { Convert(result)->Wait(); });
}

DUCKDB_V2_ERROR duckdb_v2_result_materialize(duckdb_v2_result_handle result, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(result);
	return WithErrorHandler(err, [&]() { Convert(result)->RequestRetention(); });
}

DUCKDB_V2_ERROR duckdb_v2_result_complete(duckdb_v2_result_handle result, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(result);
	return WithErrorHandler(err, [&]() { Convert(result)->RunToCompletion(); });
}

DUCKDB_V2_ERROR duckdb_v2_result_fetch(duckdb_v2_result_handle result, duckdb_v2_data_chunk_handle *out_chunk,
                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(result);
	DUCKDB_CHECK_ARG(out_chunk);
	*out_chunk = nullptr;
	return WithErrorHandler(err, [&]() {
		auto chunk = Convert(result)->Fetch();
		if (chunk) {
			*out_chunk = Convert(chunk.release());
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_result_get_collection(duckdb_v2_result_handle result,
                                                duckdb_v2_column_data_collection_handle *out_collection,
                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(result);
	DUCKDB_CHECK_ARG(out_collection);
	*out_collection = nullptr;
	return WithErrorHandler(err, [&]() { *out_collection = ConvertCollection(&Convert(result)->Collection()); });
}

DUCKDB_V2_ERROR duckdb_v2_result_take_collection(duckdb_v2_result_handle result,
                                                 duckdb_v2_column_data_collection_handle *out_collection,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(result);
	DUCKDB_CHECK_ARG(out_collection);
	*out_collection = nullptr;
	return WithErrorHandler(
	    err, [&]() { *out_collection = ConvertCollection(Convert(result)->TakeCollection().release()); });
}

DUCKDB_V2_ERROR duckdb_v2_result_can_stream(duckdb_v2_result_handle result, bool *out_can_stream,
                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(result);
	DUCKDB_CHECK_ARG(out_can_stream);
	*out_can_stream = false;
	return WithErrorHandler(err, [&]() { *out_can_stream = Convert(result)->CanStream(); });
}

DUCKDB_V2_ERROR duckdb_v2_result_render_box(duckdb_v2_result_handle result, idx_t max_rows, idx_t max_width,
                                            idx_t max_col_width, duckdb_v2_str null_value, idx_t render_mode,
                                            idx_t limit, duckdb_v2_text_sink_fn sink, void *user_data,
                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(result);
	DUCKDB_CHECK_ARG(sink);
	DUCKDB_CHECK_ARG(null_value);
	return WithErrorHandler(err, [&]() {
		if (render_mode > 1) {
			throw duckdb::InvalidInputException("render_mode must be 0 (rows) or 1 (columns)");
		}
		auto *wrapper = Convert(result);
		auto &collection = wrapper->Collection();
		if (!wrapper->context) {
			throw duckdb::InvalidInputException("result is not associated with an active context");
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

		duckdb::ClientBoxRendererContext render_context(*wrapper->context);
		duckdb::BoxRenderer renderer(config);
		duckdb::ColumnDataCollectionWrapper data(collection);

		auto text = renderer.ToString(render_context, duckdb::IdentifiersToStrings(wrapper->names), data);
		InvokeTextSink(sink, Convert(text), user_data);
	});
}

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
		*out_type = Convert(r->statement_type);
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

DUCKDB_V2_ERROR duckdb_v2_result_stream_create(duckdb_v2_result_handle *result,
                                               duckdb_v2_result_stream_handle *out_stream,
                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_stream);
	*out_stream = nullptr;
	DUCKDB_CHECK_ARG(result);
	DUCKDB_CHECK_ARG(*result);
	return WithErrorHandler(err, [&]() {
		// Adopt by transfer; consumed on success and failure alike.
		duckdb::unique_ptr<ResultWrapperV2> wrapper(Convert(*result));
		*result = nullptr;
		try {
			wrapper->OpenStream();
		} catch (...) {
			try {
				wrapper->Finalize();
			} catch (...) { // NOLINT: never mask the original error
			}
			throw;
		}
		*out_stream = ConvertStream(wrapper.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_result_stream_destroy(duckdb_v2_result_stream_handle *stream) {
	return WithErrorHandler(nullptr, [&]() {
		if (!stream || !*stream) {
			return;
		}
		duckdb::unique_ptr<ResultWrapperV2> wrapper(Convert(*stream));
		*stream = nullptr;
		wrapper->Finalize();
	});
}

DUCKDB_V2_ERROR duckdb_v2_result_stream_step(duckdb_v2_result_stream_handle stream, DUCKDB_V2_RESULT_STATUS *out_status,
                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(stream);
	DUCKDB_CHECK_ARG(out_status);
	return WithErrorHandler(err, [&]() { *out_status = Convert(stream)->Step(); });
}

DUCKDB_V2_ERROR duckdb_v2_result_stream_poll(duckdb_v2_result_stream_handle stream, DUCKDB_V2_RESULT_STATUS *out_status,
                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(stream);
	DUCKDB_CHECK_ARG(out_status);
	return WithErrorHandler(err, [&]() { *out_status = Convert(stream)->Poll(); });
}

DUCKDB_V2_ERROR duckdb_v2_result_stream_wait(duckdb_v2_result_stream_handle stream, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(stream);
	return WithErrorHandler(err, [&]() { Convert(stream)->Wait(); });
}

DUCKDB_V2_ERROR duckdb_v2_result_stream_try_fetch(duckdb_v2_result_stream_handle stream,
                                                  duckdb_v2_data_chunk_handle *out_chunk,
                                                  DUCKDB_V2_RESULT_STATUS *out_status,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(stream);
	DUCKDB_CHECK_ARG(out_chunk);
	DUCKDB_CHECK_ARG(out_status);
	*out_chunk = nullptr;
	return WithErrorHandler(err, [&]() {
		duckdb::unique_ptr<duckdb::DataChunk> chunk;
		*out_status = Convert(stream)->StreamTryFetch(chunk);
		if (chunk) {
			*out_chunk = Convert(chunk.release());
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_result_stream_fetch(duckdb_v2_result_stream_handle stream,
                                              duckdb_v2_data_chunk_handle *out_chunk,
                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(stream);
	DUCKDB_CHECK_ARG(out_chunk);
	*out_chunk = nullptr;
	return WithErrorHandler(err, [&]() {
		auto chunk = Convert(stream)->StreamFetch();
		if (chunk) {
			*out_chunk = Convert(chunk.release());
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_result_stream_get_result_type(duckdb_v2_result_stream_handle stream,
                                                        DUCKDB_V2_RESULT_TYPE *out_type,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(stream);
	DUCKDB_CHECK_ARG(out_type);
	return WithErrorHandler(err, [&]() {
		auto *s = Convert(stream);
		s->RequireMetadata();
		*out_type = MapResultType(s->properties.return_type);
	});
}

DUCKDB_V2_ERROR duckdb_v2_result_stream_get_statement_type(duckdb_v2_result_stream_handle stream,
                                                           DUCKDB_V2_STATEMENT_TYPE *out_type,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(stream);
	DUCKDB_CHECK_ARG(out_type);
	return WithErrorHandler(err, [&]() {
		auto *s = Convert(stream);
		s->RequireMetadata();
		*out_type = Convert(s->statement_type);
	});
}

DUCKDB_V2_ERROR duckdb_v2_result_stream_get_schema(duckdb_v2_result_stream_handle stream,
                                                   duckdb_v2_schema_handle *out_schema,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_schema);
	*out_schema = nullptr;
	DUCKDB_CHECK_ARG(stream);
	return WithErrorHandler(err, [&]() {
		auto *s = Convert(stream);
		s->RequireMetadata();
		auto schema = duckdb::make_uniq<CV2Schema>();
		for (duckdb::idx_t i = 0; i < s->types.size(); i++) {
			schema->fields.push_back({s->names[i].GetIdentifierName(), s->types[i]});
		}
		*out_schema = Convert(schema.release());
	});
}
