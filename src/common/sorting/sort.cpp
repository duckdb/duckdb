#include "duckdb/common/sorting/sort.hpp"

#include "duckdb/common/sorting/sort_key.hpp"
#include "duckdb/common/sorting/sorted_run.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"

namespace duckdb {

Sort::Sort(ClientContext &context, const vector<BoundOrderByNode> &orders, const vector<LogicalType> &types,
           vector<idx_t> projection_map) {
	// Convert orders to a single "create_sort_key" expression
	// Copied from ordered_aggregate_optimizer.cpp, should be unified
	FunctionBinder binder(context);
	vector<unique_ptr<Expression>> sort_children;
	for (auto &order : orders) {
		sort_children.emplace_back(order.expression->Copy());
		string modifier;
		modifier += (order.type == OrderType::ASCENDING) ? "ASC" : "DESC";
		modifier += " NULLS";
		modifier += (order.null_order == OrderByNullType::NULLS_FIRST) ? " FIRST" : " LAST";
		sort_children.emplace_back(make_uniq<BoundConstantExpression>(Value(modifier)));
	}

	ErrorData error;
	key_expression = binder.BindScalarFunction(DEFAULT_SCHEMA, "create_sort_key", std::move(sort_children), error);
	if (!key_expression) {
		throw InternalException("Unable to bind create_sort_key in Sort::Sort");
	}

	// For convenience, we fill the projection map if it is empty
	if (projection_map.empty()) {
		projection_map.reserve(types.size());
		for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
			projection_map.push_back(col_idx);
		}
	}

	// We need to output this many columns, reserve
	output_projection_columns.reserve(projection_map.size());

	// Create mapping from input column to key (so we won't duplicate columns in key/payload)
	unordered_map<idx_t, idx_t> input_column_to_key;
	for (idx_t key_idx = 0; key_idx < orders.size(); key_idx++) {
		const auto &key_order_expr = *orders[key_idx].expression;
		if (key_order_expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
			input_column_to_key.emplace(key_order_expr.Cast<BoundReferenceExpression>().index, key_idx);
		}
	}

	// Construct payload layout (excluding columns that also appear as key)
	vector<LogicalType> payload_types;
	for (idx_t output_col_idx = 0; output_col_idx < projection_map.size(); output_col_idx++) {
		const auto &input_col_idx = projection_map[output_col_idx];
		const auto it = input_column_to_key.find(input_col_idx);
		if (it != input_column_to_key.end()) {
			// Projected column also appears as a key, just reference it
			output_projection_columns.push_back({false, it->second, output_col_idx});
		} else {
			// Projected column does not appear as a key, add to payload layout
			output_projection_columns.push_back({true, payload_types.size(), output_col_idx});
			payload_types.push_back(types[input_col_idx]);
			input_projection_map.push_back(input_col_idx);
		}
	}
	payload_layout.Initialize(payload_types);

	// Sort the output projection columns so we're gathering the columns in order
	std::sort(output_projection_columns.begin(), output_projection_columns.end(),
	          [](const SortProjectionColumn &lhs, const SortProjectionColumn &rhs) {
		          if (lhs.is_payload == rhs.is_payload) {
			          return lhs.layout_col_idx < rhs.layout_col_idx;
		          }
		          return lhs.is_payload < rhs.is_payload;
	          });

	// Finally, initialize the key layout (now that we know whether we have a payload)
	key_layout.Initialize(orders, key_expression->return_type, !payload_types.empty());
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class SortLocalSinkState : public LocalSinkState {
public:
	SortLocalSinkState(const Sort &sort, ClientContext &context)
	    : maximum_run_size(0), external(false), key_executor(context, *sort.key_expression) {
		key.Initialize(context, {sort.key_expression->return_type});
		payload.Initialize(context, sort.payload_layout.GetTypes());
	}

public:
	void InitializeSortedRun(const Sort &sort, ClientContext &context) {
		D_ASSERT(!sorted_run);
		auto &buffer_manager = BufferManager::GetBufferManager(context);
		sorted_run = make_uniq<SortedRun>(buffer_manager, sort.key_layout, sort.payload_layout);
	}

public:
	//! The sorted run that we're appending to
	unique_ptr<SortedRun> sorted_run;
	//! The current maximum run size (in bytes, retrieved from global state)
	idx_t maximum_run_size;
	//! Whether this is an external sort (retrieved from global state)
	bool external;

	ExpressionExecutor key_executor;
	DataChunk key;
	DataChunk payload;
};

class SortGlobalSinkState : public GlobalSinkState {
public:
	explicit SortGlobalSinkState(ClientContext &context)
	    : temporary_memory_state(TemporaryMemoryManager::Get(context).Register(context)), active_threads(0),
	      external(ClientConfig::GetConfig(context).force_external), any_combined(false), any_concatenated(false) {
	}

public:
	void UpdateLocalState(SortLocalSinkState &lstate) const {
		lstate.maximum_run_size = temporary_memory_state->GetReservation() / active_threads;
		lstate.external = external;
	}

	void TryIncreaseReservation(ClientContext &context, SortLocalSinkState &lstate, const unique_lock<mutex> &guard) {
		VerifyLock(guard);
		D_ASSERT(!external);

		// If we already got less than we requested last time, have to go external
		if (temporary_memory_state->GetReservation() < temporary_memory_state->GetRemainingSize()) {
			if (!any_combined) {
				external = true;
			}
			return;
		}

		// Double until it fits
		const auto required = active_threads * lstate.sorted_run->SizeInBytes();
		auto request = temporary_memory_state->GetReservation() * 2;
		while (request < required) {
			request *= 2;
		}

		// Send the request
		temporary_memory_state->SetRemainingSizeAndUpdateReservation(context, request);

		// If we got less than we required, we have to go external
		if (temporary_memory_state->GetReservation() < required) {
			if (!any_combined) {
				external = true;
			}
		}
	}

public:
	//! Temporary memory state for managing this sort's memory usage
	unique_ptr<TemporaryMemoryState> temporary_memory_state;
	//! Runs that have been sorted locally before being appended to this global state
	vector<unique_ptr<SortedRun>> sorted_runs;

	//! How many threads are active
	atomic<idx_t> active_threads;
	//! Whether this is an external sort
	bool external;
	//! Whether any thread has called Combined yet
	bool any_combined;
	//! Whether we have concatenated any sorted runs based on statistics (skipping the merge)
	bool any_concatenated;
};

unique_ptr<LocalSinkState> Sort::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<SortLocalSinkState>(*this, context.client);
}

unique_ptr<GlobalSinkState> Sort::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<SortGlobalSinkState>(context);
}

//! Returns true if the Sink call is done (either because run size is small or because run was finalized)
static bool TryFinishSink(SortGlobalSinkState &gstate, SortLocalSinkState &lstate, unique_lock<mutex> &guard) {
	// Check if we exceed the limit
	const auto sorted_run_size = lstate.sorted_run->SizeInBytes();
	if (sorted_run_size < lstate.maximum_run_size) {
		return true; // Sink is done
	}

	// Run size exceeds the limit. If external, the limit will never be updated, so we need to sort
	if (lstate.external) {
		// Finalize, i.e., sort, the run lock-free
		if (guard.owns_lock()) {
			guard.unlock();
		}
		lstate.sorted_run->Finalize(true);

		// Lock again to append to global state
		guard = gstate.Lock();
		gstate.sorted_runs.push_back(std::move(lstate.sorted_run));
		return true; // Sink is done
	}

	return false; // Sink is not done yet
}

SinkResultType Sort::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<SortGlobalSinkState>();
	auto &lstate = input.local_state.Cast<SortLocalSinkState>();

	if (!lstate.sorted_run) {
		++gstate.active_threads;
		lstate.InitializeSortedRun(*this, context.client);
		gstate.UpdateLocalState(lstate);
	}

	// Sink data into sorted run
	lstate.key.Reset();
	lstate.payload.Reset();
	lstate.key_executor.Execute(chunk, lstate.key);
	lstate.payload.ReferenceColumns(chunk, input_projection_map);
	lstate.sorted_run->Sink(lstate.key, lstate.payload);

	// Try to finish this call to Sink
	unique_lock<mutex> guard;
	if (TryFinishSink(gstate, lstate, guard)) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	// Grab the lock, update the local state, and see if we can finish now
	guard = gstate.Lock();
	gstate.UpdateLocalState(lstate);
	if (TryFinishSink(gstate, lstate, guard)) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	// Still no, this thread must try to increase the limit
	gstate.TryIncreaseReservation(context.client, lstate, guard);
	gstate.UpdateLocalState(lstate);
	guard.unlock(); // Can unlock now, local state is definitely up-to-date

	// We should always succeed this time
	const auto success = TryFinishSink(gstate, lstate, guard);
	if (!success) {
		throw InternalException("Unable to finish Sort::Sink");
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType Sort::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<SortGlobalSinkState>();
	auto &lstate = input.local_state.Cast<SortLocalSinkState>();

	if (!lstate.sorted_run) {
		return SinkCombineResultType::FINISHED;
	}

	// Set any_combined under lock
	auto guard = gstate.Lock();
	gstate.any_combined = true;
	guard.unlock();

	// Do the final local sort (lock-free)
	lstate.sorted_run->Finalize(gstate.external);

	// Append to global state under lock
	guard = gstate.Lock();
	gstate.sorted_runs.push_back(std::move(lstate.sorted_run));

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType Sort::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<SortGlobalSinkState>();

	if (gstate.sorted_runs.empty()) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// TODO: Keep a "Value" min/max per sorted run:
	//  1. Allows runs to be concatenated without merging
	//  2. Can identify sets of runs that overlap within the set, but the sets might not overlap with another set
	//    * For example, this could reduce one 100-ary merge into five 20-ary merges
	//    * This is probably going to be a really complicated algorithm (lots of trade-offs)
	//  3. Need C++ iterator over fixed-size blocks, use FastMod to reduce cost of modulo tuples per block

	return SinkFinalizeType::READY;
}

ProgressData Sort::GetSinkProgress(ClientContext &context, GlobalSinkState &gstate_p,
                                   const ProgressData source_progress) const {
	auto &gstate = gstate_p.Cast<SortGlobalSinkState>();

	throw NotImplementedException("Sort");

	return source_progress;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class SortGlobalSourceState : public GlobalSourceState {
public:
	SortGlobalSourceState(const Sort &sort, ClientContext &context, SortGlobalSinkState &sink_p) : sink(sink_p) {
	}

	idx_t MaxThreads() override {
		throw NotImplementedException("Sort");
	}

	SortGlobalSinkState &sink;
};

class SortLocalSourceState : public LocalSourceState {
public:
	SortLocalSourceState(const Sort &sort, ClientContext &context, SortGlobalSourceState &gstate) {
		throw NotImplementedException("Sort");
	}
};

unique_ptr<LocalSourceState> Sort::GetLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate) const {
	return make_uniq<SortLocalSourceState>(*this, context.client, gstate.Cast<SortGlobalSourceState>());
}

unique_ptr<GlobalSourceState> Sort::GetGlobalSourceState(ClientContext &context, GlobalSinkState &sink) const {
	return make_uniq<SortGlobalSourceState>(*this, context, sink.Cast<SortGlobalSinkState>());
}

SourceResultType Sort::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<SortGlobalSourceState>();
	auto &lstate = input.local_state.Cast<SortGlobalSourceState>();
	// return SourceResultType::BLOCKED as needed

	throw NotImplementedException("Sort");

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

ProgressData Sort::GetProgress(ClientContext &context, GlobalSourceState &gstate) const {
	throw NotImplementedException("Sort");
}

OperatorPartitionData Sort::GetPartitionData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                             LocalSourceState &lstate,
                                             const OperatorPartitionInfo &partition_info) const {
	throw NotImplementedException("Sort");
}

} // namespace duckdb
