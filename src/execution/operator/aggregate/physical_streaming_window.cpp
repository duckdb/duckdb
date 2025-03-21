#include "duckdb/execution/operator/aggregate/physical_streaming_window.hpp"

#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

PhysicalStreamingWindow::PhysicalStreamingWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                                                 idx_t estimated_cardinality, PhysicalOperatorType type)
    : PhysicalOperator(type, std::move(types), estimated_cardinality), select_list(std::move(select_list)) {
}

class StreamingWindowGlobalState : public GlobalOperatorState {
public:
	StreamingWindowGlobalState() : row_number(1) {
	}

	//! The next row number.
	std::atomic<int64_t> row_number;
};

class StreamingWindowState : public OperatorState {
public:
	struct AggregateState {
		AggregateState(ClientContext &client, BoundWindowExpression &wexpr, Allocator &allocator)
		    : wexpr(wexpr), arena_allocator(Allocator::DefaultAllocator()), executor(client), filter_executor(client),
		      statev(LogicalType::POINTER, data_ptr_cast(&state_ptr)), hashes(LogicalType::HASH),
		      addresses(LogicalType::POINTER) {
			D_ASSERT(wexpr.GetExpressionType() == ExpressionType::WINDOW_AGGREGATE);
			auto &aggregate = *wexpr.aggregate;
			bind_data = wexpr.bind_info.get();
			dtor = aggregate.destructor;
			state.resize(aggregate.state_size(aggregate));
			state_ptr = state.data();
			aggregate.initialize(aggregate, state.data());
			for (auto &child : wexpr.children) {
				arg_types.push_back(child->return_type);
				executor.AddExpression(*child);
			}
			if (!arg_types.empty()) {
				arg_chunk.Initialize(allocator, arg_types);
				arg_cursor.Initialize(allocator, arg_types);
			}
			if (wexpr.filter_expr) {
				filter_executor.AddExpression(*wexpr.filter_expr);
				filter_sel.Initialize();
			}
			if (wexpr.distinct) {
				distinct = make_uniq<GroupedAggregateHashTable>(client, allocator, arg_types);
				distinct_args.Initialize(allocator, arg_types);
				distinct_sel.Initialize();
			}
		}

		~AggregateState() {
			if (dtor) {
				AggregateInputData aggr_input_data(bind_data, arena_allocator);
				state_ptr = state.data();
				dtor(statev, aggr_input_data, 1);
			}
		}

		void Execute(ExecutionContext &context, DataChunk &input, Vector &result);

		//! The aggregate expression
		BoundWindowExpression &wexpr;
		//! The allocator to use for aggregate data structures
		ArenaAllocator arena_allocator;
		//! Reusable executor for the children
		ExpressionExecutor executor;
		//! Shared executor for FILTER clauses
		ExpressionExecutor filter_executor;
		//! The single aggregate state we update row-by-row
		vector<data_t> state;
		//! The pointer to the state stored in the state vector
		data_ptr_t state_ptr = nullptr;
		//! The state vector for the single state
		Vector statev;
		//! The aggregate binding data (if any)
		FunctionData *bind_data = nullptr;
		//! The aggregate state destructor (if any)
		aggregate_destructor_t dtor = nullptr;
		//! The inputs rows that pass the FILTER
		SelectionVector filter_sel;
		//! The number of unfiltered rows so far for COUNT(*)
		int64_t unfiltered = 0;
		//! Argument types
		vector<LogicalType> arg_types;
		//! Argument value buffer
		DataChunk arg_chunk;
		//! Argument cursor (a one element slice of arg_chunk)
		DataChunk arg_cursor;

		//! Hash table for accumulating the distinct values
		unique_ptr<GroupedAggregateHashTable> distinct;
		//! Filtered arguments for checking distinctness
		DataChunk distinct_args;
		//! Reusable hash vector
		Vector hashes;
		//! Rows that produced new distinct values
		SelectionVector distinct_sel;
		//! Pointers to groups in the hash table.
		Vector addresses;
	};

	struct LeadLagState {
		//	Fixed size
		static constexpr idx_t MAX_BUFFER = 2048U;

		static bool ComputeOffset(ClientContext &context, BoundWindowExpression &wexpr, int64_t &offset) {
			offset = 1;
			if (wexpr.offset_expr) {
				if (wexpr.offset_expr->HasParameter() || !wexpr.offset_expr->IsFoldable()) {
					return false;
				}
				auto offset_value = ExpressionExecutor::EvaluateScalar(context, *wexpr.offset_expr);
				if (offset_value.IsNull()) {
					return false;
				}
				Value bigint_value;
				if (!offset_value.DefaultTryCastAs(LogicalType::BIGINT, bigint_value, nullptr, false)) {
					return false;
				}
				offset = bigint_value.GetValue<int64_t>();
			}

			//	We can only support LEAD and LAG values within one standard vector
			if (wexpr.GetExpressionType() == ExpressionType::WINDOW_LEAD) {
				offset = -offset;
			}
			return idx_t(std::abs(offset)) < MAX_BUFFER;
		}

		static bool ComputeDefault(ClientContext &context, BoundWindowExpression &wexpr, Value &result) {
			if (!wexpr.default_expr) {
				result = Value(wexpr.return_type);
				return true;
			}

			if (wexpr.default_expr && (wexpr.default_expr->HasParameter() || !wexpr.default_expr->IsFoldable())) {
				return false;
			}
			auto dflt_value = ExpressionExecutor::EvaluateScalar(context, *wexpr.default_expr);
			return dflt_value.DefaultTryCastAs(wexpr.return_type, result, nullptr, false);
		}

		LeadLagState(ClientContext &context, BoundWindowExpression &wexpr)
		    : wexpr(wexpr), executor(context, *wexpr.children[0]), prev(wexpr.return_type), temp(wexpr.return_type) {
			ComputeOffset(context, wexpr, offset);
			ComputeDefault(context, wexpr, dflt);

			buffered = idx_t(std::abs(offset));
			prev.Reference(dflt);
			prev.Flatten(buffered);
			temp.Initialize(false, buffered);
		}

		void Execute(ExecutionContext &context, DataChunk &input, DataChunk &delayed, Vector &result) {
			if (!curr_chunk.ColumnCount()) {
				curr_chunk.Initialize(context.client, {result.GetType()}, delayed.GetCapacity());
			}

			if (offset >= 0) {
				ExecuteLag(context, input, result);
			} else {
				ExecuteLead(context, input, delayed, result);
			}
		}

		void ExecuteLag(ExecutionContext &context, DataChunk &input, Vector &result) {
			D_ASSERT(offset >= 0);
			auto &curr = curr_chunk.data[0];
			curr_chunk.Reset();
			executor.Execute(input, curr_chunk);
			const idx_t count = input.size();
			//	Copy prev[0, buffered] => result[0, buffered]
			idx_t source_count = MinValue<idx_t>(buffered, count);
			VectorOperations::Copy(prev, result, source_count, 0, 0);
			// Special case when we have buffered enough values for the output
			if (count < buffered) {
				//	Shift down incomplete buffers
				// 	Copy prev[buffered-count, buffered] => temp[0, count]
				source_count = buffered - count;
				FlatVector::Validity(temp).Reset();
				VectorOperations::Copy(prev, temp, buffered, source_count, 0);

				// 	Copy temp[0, count] => prev[0, count]
				FlatVector::Validity(prev).Reset();
				VectorOperations::Copy(temp, prev, count, 0, 0);
				// 	Copy curr[0, buffered-count] => prev[count, buffered]
				VectorOperations::Copy(curr, prev, source_count, 0, count);
			} else {
				//	Copy input values beyond what we have buffered
				source_count = count - buffered;
				//	Copy curr[0, count-buffered] => result[buffered, count]
				VectorOperations::Copy(curr, result, source_count, 0, buffered);
				// 	Copy curr[count-buffered, count] => prev[0, buffered]
				FlatVector::Validity(prev).Reset();
				VectorOperations::Copy(curr, prev, count, source_count, 0);
			}
		}

		void ExecuteLead(ExecutionContext &context, DataChunk &input, DataChunk &delayed, Vector &result) {
			//	We treat input || delayed as a logical unified buffer
			D_ASSERT(offset < 0);
			// Input has been set up with the number of rows we CAN produce.
			const idx_t count = input.size();
			auto &curr = curr_chunk.data[0];
			// Copy unified[buffered:count] => result[pos:]
			idx_t pos = 0;
			idx_t unified_offset = buffered;
			if (unified_offset < count) {
				Reset(curr_chunk);
				executor.Execute(input, curr_chunk);
				VectorOperations::Copy(curr, result, count, unified_offset, pos);
				pos += count - unified_offset;
				unified_offset = count;
			}
			// Copy unified[unified_offset:] => result[pos:]
			idx_t unified_count = count + delayed.size();
			if (unified_offset < unified_count) {
				Reset(curr_chunk);
				executor.Execute(delayed, curr_chunk);
				idx_t delayed_offset = unified_offset - count;
				// Only copy as many values as we need
				idx_t delayed_count = MinValue<idx_t>(delayed.size(), delayed_offset + (count - pos));
				VectorOperations::Copy(curr, result, delayed_count, delayed_offset, pos);
				pos += delayed_count - delayed_offset;
			}
			// Copy default[:count-pos] => result[pos:]
			if (pos < count) {
				const idx_t defaulted = count - pos;
				VectorOperations::Copy(prev, result, defaulted, 0, pos);
			}
		}

		//! The aggregate expression
		BoundWindowExpression &wexpr;
		//! Cache the executor to cut down on memory allocation
		ExpressionExecutor executor;
		//! The constant offset
		int64_t offset;
		//! The number of rows we have buffered
		idx_t buffered;
		//! The constant default value
		Value dflt;
		//! The current set of values
		DataChunk curr_chunk;
		//! The previous set of values
		Vector prev;
		//! The copy buffer
		Vector temp;
	};

	explicit StreamingWindowState(ClientContext &client) : initialized(false), allocator(Allocator::Get(client)) {
	}

	~StreamingWindowState() override {
	}

	void Initialize(ClientContext &context, DataChunk &input, const vector<unique_ptr<Expression>> &expressions) {
		const_vectors.resize(expressions.size());
		aggregate_states.resize(expressions.size());
		lead_lag_states.resize(expressions.size());

		for (idx_t expr_idx = 0; expr_idx < expressions.size(); expr_idx++) {
			auto &expr = *expressions[expr_idx];
			auto &wexpr = expr.Cast<BoundWindowExpression>();
			switch (expr.GetExpressionType()) {
			case ExpressionType::WINDOW_AGGREGATE:
				aggregate_states[expr_idx] = make_uniq<AggregateState>(context, wexpr, allocator);
				break;
			case ExpressionType::WINDOW_FIRST_VALUE: {
				// Just execute the expression once
				ExpressionExecutor executor(context);
				executor.AddExpression(*wexpr.children[0]);
				DataChunk result;
				result.Initialize(Allocator::Get(context), {wexpr.children[0]->return_type});
				executor.Execute(input, result);

				const_vectors[expr_idx] = make_uniq<Vector>(result.GetValue(0, 0));
				break;
			}
			case ExpressionType::WINDOW_PERCENT_RANK: {
				const_vectors[expr_idx] = make_uniq<Vector>(Value((double)0));
				break;
			}
			case ExpressionType::WINDOW_RANK:
			case ExpressionType::WINDOW_RANK_DENSE: {
				const_vectors[expr_idx] = make_uniq<Vector>(Value((int64_t)1));
				break;
			}
			case ExpressionType::WINDOW_LAG:
			case ExpressionType::WINDOW_LEAD: {
				lead_lag_states[expr_idx] = make_uniq<LeadLagState>(context, wexpr);
				const auto offset = lead_lag_states[expr_idx]->offset;
				if (offset < 0) {
					lead_count = MaxValue<idx_t>(idx_t(-offset), lead_count);
				}
				break;
			}
			default:
				break;
			}
		}
		if (lead_count) {
			delayed.Initialize(context, input.GetTypes(), lead_count + STANDARD_VECTOR_SIZE);
			shifted.Initialize(context, input.GetTypes(), lead_count + STANDARD_VECTOR_SIZE);
		}
		initialized = true;
	}

	static inline void Reset(DataChunk &chunk) {
		//	Reset trashes the capacity...
		const auto capacity = chunk.GetCapacity();
		chunk.Reset();
		chunk.SetCapacity(capacity);
	}

public:
	//! We can't initialise until we have an input chunk
	bool initialized;
	//! The values that are determined by the first row.
	vector<unique_ptr<Vector>> const_vectors;
	//! Aggregation states
	vector<unique_ptr<AggregateState>> aggregate_states;
	Allocator &allocator;
	//! Lead/Lag states
	vector<unique_ptr<LeadLagState>> lead_lag_states;
	//! The number of rows ahead to buffer for LEAD
	idx_t lead_count = 0;
	//! A buffer for delayed input
	DataChunk delayed;
	//! A buffer for shifting delayed input
	DataChunk shifted;
};

bool PhysicalStreamingWindow::IsStreamingFunction(ClientContext &context, unique_ptr<Expression> &expr) {
	auto &wexpr = expr->Cast<BoundWindowExpression>();
	if (!wexpr.partitions.empty() || !wexpr.orders.empty() || wexpr.ignore_nulls || !wexpr.arg_orders.empty() ||
	    wexpr.exclude_clause != WindowExcludeMode::NO_OTHER) {
		return false;
	}
	switch (wexpr.GetExpressionType()) {
	// TODO: add more expression types here?
	case ExpressionType::WINDOW_AGGREGATE:
		// We can stream aggregates if they are "running totals"
		return wexpr.start == WindowBoundary::UNBOUNDED_PRECEDING && wexpr.end == WindowBoundary::CURRENT_ROW_ROWS;
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_PERCENT_RANK:
	case ExpressionType::WINDOW_RANK:
	case ExpressionType::WINDOW_RANK_DENSE:
	case ExpressionType::WINDOW_ROW_NUMBER:
		return true;
	case ExpressionType::WINDOW_LAG:
	case ExpressionType::WINDOW_LEAD: {
		// We can stream LEAD/LAG if the arguments are constant and the delta is less than a block behind
		Value dflt;
		int64_t offset;
		return StreamingWindowState::LeadLagState::ComputeDefault(context, wexpr, dflt) &&
		       StreamingWindowState::LeadLagState::ComputeOffset(context, wexpr, offset);
	}
	default:
		return false;
	}
}

unique_ptr<GlobalOperatorState> PhysicalStreamingWindow::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<StreamingWindowGlobalState>();
}

unique_ptr<OperatorState> PhysicalStreamingWindow::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<StreamingWindowState>(context.client);
}

void StreamingWindowState::AggregateState::Execute(ExecutionContext &context, DataChunk &input, Vector &result) {
	//	Establish the aggregation environment
	const idx_t count = input.size();
	auto &aggregate = *wexpr.aggregate;
	auto &aggr_state = *this;
	auto &statev = aggr_state.statev;

	// Compute the FILTER mask (if any)
	ValidityMask filter_mask;
	auto filtered = count;
	auto &filter_sel = aggr_state.filter_sel;
	if (wexpr.filter_expr) {
		filtered = filter_executor.SelectExpression(input, filter_sel);
		if (filtered < count) {
			filter_mask.Initialize(count);
			filter_mask.SetAllInvalid(count);
			for (idx_t f = 0; f < filtered; ++f) {
				filter_mask.SetValid(filter_sel.get_index(f));
			}
		}
	}

	// Check for COUNT(*)
	if (wexpr.children.empty()) {
		D_ASSERT(GetTypeIdSize(result.GetType().InternalType()) == sizeof(int64_t));
		auto data = FlatVector::GetData<int64_t>(result);
		auto &unfiltered = aggr_state.unfiltered;
		for (idx_t i = 0; i < count; ++i) {
			unfiltered += int64_t(filter_mask.RowIsValid(i));
			data[i] = unfiltered;
		}
		return;
	}

	// Compute the arguments
	auto &arg_chunk = aggr_state.arg_chunk;
	executor.Execute(input, arg_chunk);
	arg_chunk.Flatten();

	// Update the distinct hash table
	ValidityMask distinct_mask;
	if (aggr_state.distinct) {
		auto &distinct_args = aggr_state.distinct_args;
		distinct_args.Reference(arg_chunk);
		if (wexpr.filter_expr) {
			distinct_args.Slice(filter_sel, filtered);
		}
		idx_t distinct = 0;
		auto &distinct_sel = aggr_state.distinct_sel;
		if (filtered) {
			// FindOrCreateGroups assumes non-empty input
			auto &hashes = aggr_state.hashes;
			distinct_args.Hash(hashes);

			auto &addresses = aggr_state.addresses;
			distinct = aggr_state.distinct->FindOrCreateGroups(distinct_args, hashes, addresses, distinct_sel);
		}

		//	Translate the distinct selection from filtered row numbers
		//	back to input row numbers. We need to produce output for all input rows,
		//	so we filter out
		if (distinct < filtered) {
			distinct_mask.Initialize(count);
			distinct_mask.SetAllInvalid(count);
			for (idx_t d = 0; d < distinct; ++d) {
				const auto f = distinct_sel.get_index(d);
				distinct_mask.SetValid(filter_sel.get_index(f));
			}
		}
	}

	// Iterate through them using a single SV
	sel_t s = 0;
	SelectionVector sel(&s);
	auto &arg_cursor = aggr_state.arg_cursor;
	arg_cursor.Reset();
	arg_cursor.Slice(sel, 1);
	// This doesn't work for STRUCTs because the SV
	// is not copied to the children when you slice
	vector<column_t> structs;
	for (column_t col_idx = 0; col_idx < arg_chunk.ColumnCount(); ++col_idx) {
		auto &col_vec = arg_cursor.data[col_idx];
		DictionaryVector::Child(col_vec).Reference(arg_chunk.data[col_idx]);
		if (col_vec.GetType().InternalType() == PhysicalType::STRUCT) {
			structs.emplace_back(col_idx);
		}
	}

	// Update the state and finalize it one row at a time.
	AggregateInputData aggr_input_data(wexpr.bind_info.get(), aggr_state.arena_allocator);
	for (idx_t i = 0; i < count; ++i) {
		sel.set_index(0, i);
		for (const auto struct_idx : structs) {
			arg_cursor.data[struct_idx].Slice(arg_chunk.data[struct_idx], sel, 1);
		}
		if (filter_mask.RowIsValid(i) && distinct_mask.RowIsValid(i)) {
			aggregate.update(arg_cursor.data.data(), aggr_input_data, arg_cursor.ColumnCount(), statev, 1);
		}
		aggregate.finalize(statev, aggr_input_data, result, 1, i);
	}
}

void PhysicalStreamingWindow::ExecuteFunctions(ExecutionContext &context, DataChunk &output, DataChunk &delayed,
                                               GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = gstate_p.Cast<StreamingWindowGlobalState>();
	auto &state = state_p.Cast<StreamingWindowState>();

	// Compute window functions
	const idx_t count = output.size();
	const column_t input_width = children[0].get().GetTypes().size();
	for (column_t expr_idx = 0; expr_idx < select_list.size(); expr_idx++) {
		column_t col_idx = input_width + expr_idx;
		auto &expr = *select_list[expr_idx];
		auto &result = output.data[col_idx];
		switch (expr.GetExpressionType()) {
		case ExpressionType::WINDOW_AGGREGATE:
			state.aggregate_states[expr_idx]->Execute(context, output, result);
			break;
		case ExpressionType::WINDOW_FIRST_VALUE:
		case ExpressionType::WINDOW_PERCENT_RANK:
		case ExpressionType::WINDOW_RANK:
		case ExpressionType::WINDOW_RANK_DENSE: {
			// Reference constant vector
			output.data[col_idx].Reference(*state.const_vectors[expr_idx]);
			break;
		}
		case ExpressionType::WINDOW_ROW_NUMBER: {
			// Set row numbers
			int64_t start_row = gstate.row_number;
			auto rdata = FlatVector::GetData<int64_t>(output.data[col_idx]);
			for (idx_t i = 0; i < count; i++) {
				rdata[i] = NumericCast<int64_t>(start_row + NumericCast<int64_t>(i));
			}
			break;
		}
		case ExpressionType::WINDOW_LAG:
		case ExpressionType::WINDOW_LEAD:
			state.lead_lag_states[expr_idx]->Execute(context, output, delayed, result);
			break;
		default:
			throw NotImplementedException("%s for StreamingWindow", ExpressionTypeToString(expr.GetExpressionType()));
		}
	}
	gstate.row_number += NumericCast<int64_t>(count);
}

void PhysicalStreamingWindow::ExecuteInput(ExecutionContext &context, DataChunk &delayed, DataChunk &input,
                                           DataChunk &output, GlobalOperatorState &gstate_p,
                                           OperatorState &state_p) const {
	auto &state = state_p.Cast<StreamingWindowState>();

	// Put payload columns in place
	for (idx_t col_idx = 0; col_idx < input.data.size(); col_idx++) {
		output.data[col_idx].Reference(input.data[col_idx]);
	}
	idx_t count = input.size();

	//	Handle LEAD
	if (state.lead_count > 0) {
		//	Nothing delayed yet, so just truncate and copy the delayed values
		count -= state.lead_count;
		input.Copy(delayed, count);
	}
	output.SetCardinality(count);

	ExecuteFunctions(context, output, state.delayed, gstate_p, state_p);
}

void PhysicalStreamingWindow::ExecuteShifted(ExecutionContext &context, DataChunk &delayed, DataChunk &input,
                                             DataChunk &output, GlobalOperatorState &gstate_p,
                                             OperatorState &state_p) const {
	auto &state = state_p.Cast<StreamingWindowState>();
	auto &shifted = state.shifted;

	idx_t out = output.size();
	idx_t in = input.size();
	idx_t delay = delayed.size();
	D_ASSERT(out <= delay);

	state.Reset(shifted);
	// shifted = delayed
	delayed.Copy(shifted);
	state.Reset(delayed);
	for (idx_t col_idx = 0; col_idx < delayed.data.size(); ++col_idx) {
		// output[0:out] = delayed[0:out]
		output.data[col_idx].Reference(shifted.data[col_idx]);
		// delayed[0:out] = delayed[out:delay-out]
		VectorOperations::Copy(shifted.data[col_idx], delayed.data[col_idx], delay, out, 0);
		// delayed[delay-out:delay-out+in] = input[0:in]
		VectorOperations::Copy(input.data[col_idx], delayed.data[col_idx], in, 0, delay - out);
	}
	delayed.SetCardinality(delay - out + in);

	ExecuteFunctions(context, output, delayed, gstate_p, state_p);
}

void PhysicalStreamingWindow::ExecuteDelayed(ExecutionContext &context, DataChunk &delayed, DataChunk &input,
                                             DataChunk &output, GlobalOperatorState &gstate_p,
                                             OperatorState &state_p) const {
	// Put payload columns in place
	for (idx_t col_idx = 0; col_idx < delayed.data.size(); col_idx++) {
		output.data[col_idx].Reference(delayed.data[col_idx]);
	}
	idx_t count = delayed.size();
	output.SetCardinality(count);

	ExecuteFunctions(context, output, input, gstate_p, state_p);
}

OperatorResultType PhysicalStreamingWindow::Execute(ExecutionContext &context, DataChunk &input, DataChunk &output,
                                                    GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &state = state_p.Cast<StreamingWindowState>();
	if (!state.initialized) {
		state.Initialize(context.client, input, select_list);
	}

	auto &delayed = state.delayed;
	// We can Reset delayed now that no one can be referencing it.
	if (!delayed.size()) {
		state.Reset(delayed);
	}
	if (delayed.size() < state.lead_count) {
		//	If we don't have enough to produce a single row,
		//	then just delay more rows, return nothing
		//	and ask for more data.
		delayed.Append(input);
		output.SetCardinality(0);
		return OperatorResultType::NEED_MORE_INPUT;
	} else if (input.size() < delayed.size()) {
		// If we can't consume all of the delayed values,
		// we need to split them instead of referencing them all
		output.SetCardinality(input.size());
		ExecuteShifted(context, delayed, input, output, gstate_p, state_p);
		// We delayed the unused input so ask for more
		return OperatorResultType::NEED_MORE_INPUT;
	} else if (delayed.size()) {
		//	We have enough delayed rows so flush them
		ExecuteDelayed(context, delayed, input, output, gstate_p, state_p);
		// Defer resetting delayed as it may be referenced.
		delayed.SetCardinality(0);
		// Come back to process the input
		return OperatorResultType::HAVE_MORE_OUTPUT;
	} else {
		//	No delayed rows, so emit what we can and delay the rest.
		ExecuteInput(context, delayed, input, output, gstate_p, state_p);
		return OperatorResultType::NEED_MORE_INPUT;
	}
}

OperatorFinalizeResultType PhysicalStreamingWindow::FinalExecute(ExecutionContext &context, DataChunk &output,
                                                                 GlobalOperatorState &gstate_p,
                                                                 OperatorState &state_p) const {
	auto &state = state_p.Cast<StreamingWindowState>();

	if (state.initialized && state.lead_count) {
		auto &delayed = state.delayed;
		//	There are no more input rows
		auto &input = state.shifted;
		state.Reset(input);

		if (output.GetCapacity() < delayed.size()) {
			//	More than one output buffer was delayed, so shift in what we can
			output.SetCardinality(output.GetCapacity());
			ExecuteShifted(context, delayed, input, output, gstate_p, state_p);
			return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
		}
		ExecuteDelayed(context, delayed, input, output, gstate_p, state_p);
	}

	return OperatorFinalizeResultType::FINISHED;
}

InsertionOrderPreservingMap<string> PhysicalStreamingWindow::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string projections;
	for (idx_t i = 0; i < select_list.size(); i++) {
		if (i > 0) {
			projections += "\n";
		}
		projections += select_list[i]->GetName();
	}
	result["Projections"] = projections;
	return result;
}

} // namespace duckdb
