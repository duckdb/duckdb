#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/nested_loop_join.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/operator/join/outer_join_marker.hpp"

namespace duckdb {

PhysicalNestedLoopJoin::PhysicalNestedLoopJoin(LogicalOperator &op, PhysicalOperator &left, PhysicalOperator &right,
                                               vector<JoinCondition> cond, JoinType join_type,
                                               idx_t estimated_cardinality)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::NESTED_LOOP_JOIN, std::move(cond), join_type,
                             estimated_cardinality) {
	children.push_back(left);
	children.push_back(right);
}

bool PhysicalJoin::HasNullValues(DataChunk &chunk) {
	for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
		UnifiedVectorFormat vdata;
		chunk.data[col_idx].ToUnifiedFormat(chunk.size(), vdata);

		if (vdata.validity.AllValid()) {
			continue;
		}
		for (idx_t i = 0; i < chunk.size(); i++) {
			auto idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(idx)) {
				return true;
			}
		}
	}
	return false;
}

template <bool MATCH>
static void ConstructSemiOrAntiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]) {
	D_ASSERT(left.ColumnCount() == result.ColumnCount());
	// create the selection vector from the matches that were found
	idx_t result_count = 0;
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < left.size(); i++) {
		if (found_match[i] == MATCH) {
			sel.set_index(result_count++, i);
		}
	}
	// construct the final result
	if (result_count > 0) {
		// we only return the columns on the left side
		// project them using the result selection vector
		// reference the columns of the left side from the result
		result.Slice(left, sel, result_count);
	} else {
		result.SetCardinality(0);
	}
}

void PhysicalJoin::ConstructSemiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]) {
	ConstructSemiOrAntiJoinResult<true>(left, result, found_match);
}

void PhysicalJoin::ConstructAntiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]) {
	ConstructSemiOrAntiJoinResult<false>(left, result, found_match);
}

void PhysicalJoin::ConstructMarkJoinResult(DataChunk &join_keys, DataChunk &left, DataChunk &result, bool found_match[],
                                           bool has_null) {
	// for the initial set of columns we just reference the left side
	result.SetCardinality(left);
	for (idx_t i = 0; i < left.ColumnCount(); i++) {
		result.data[i].Reference(left.data[i]);
	}
	auto &mark_vector = result.data.back();
	mark_vector.SetVectorType(VectorType::FLAT_VECTOR);
	// first we set the NULL values from the join keys
	// if there is any NULL in the keys, the result is NULL
	auto bool_result = FlatVector::GetData<bool>(mark_vector);
	auto &mask = FlatVector::Validity(mark_vector);
	for (idx_t col_idx = 0; col_idx < join_keys.ColumnCount(); col_idx++) {
		UnifiedVectorFormat jdata;
		join_keys.data[col_idx].ToUnifiedFormat(join_keys.size(), jdata);
		if (!jdata.validity.AllValid()) {
			for (idx_t i = 0; i < join_keys.size(); i++) {
				auto jidx = jdata.sel->get_index(i);
				mask.Set(i, jdata.validity.RowIsValid(jidx));
			}
		}
	}
	// now set the remaining entries to either true or false based on whether a match was found
	if (found_match) {
		for (idx_t i = 0; i < left.size(); i++) {
			bool_result[i] = found_match[i];
		}
	} else {
		memset(bool_result, 0, sizeof(bool) * left.size());
	}
	// if the right side contains NULL values, the result of any FALSE becomes NULL
	if (has_null) {
		for (idx_t i = 0; i < left.size(); i++) {
			if (!bool_result[i]) {
				mask.SetInvalid(i);
			}
		}
	}
}

bool PhysicalNestedLoopJoin::IsSupported(const vector<JoinCondition> &conditions, JoinType join_type) {
	if (join_type == JoinType::MARK) {
		return true;
	}
	for (auto &cond : conditions) {
		if (cond.left->return_type.InternalType() == PhysicalType::STRUCT ||
		    cond.left->return_type.InternalType() == PhysicalType::LIST ||
		    cond.left->return_type.InternalType() == PhysicalType::ARRAY) {
			return false;
		}
	}
	// To avoid situations like https://github.com/duckdb/duckdb/issues/10046
	// If there is an equality in the conditions, a hash join is planned
	// with one condition, we can use mark join logic, otherwise we should use physical blockwise nl join
	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
		return conditions.size() == 1;
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class NestedLoopJoinLocalState : public LocalSinkState {
public:
	explicit NestedLoopJoinLocalState(ClientContext &context, const vector<JoinCondition> &conditions)
	    : rhs_executor(context) {
		vector<LogicalType> condition_types;
		for (auto &cond : conditions) {
			rhs_executor.AddExpression(*cond.right);
			condition_types.push_back(cond.right->return_type);
		}
		right_condition.Initialize(Allocator::Get(context), condition_types);
	}

	//! The chunk holding the right condition
	DataChunk right_condition;
	//! The executor of the RHS condition
	ExpressionExecutor rhs_executor;
};

class NestedLoopJoinGlobalState : public GlobalSinkState {
public:
	explicit NestedLoopJoinGlobalState(ClientContext &context, const PhysicalNestedLoopJoin &op)
	    : right_payload_data(context, op.children[1].get().GetTypes()),
	      right_condition_data(context, op.GetJoinTypes()), has_null(false),
	      right_outer(PropagatesBuildSide(op.join_type)) {
	}

	mutex nj_lock;
	//! Materialized data of the RHS
	ColumnDataCollection right_payload_data;
	//! Materialized join condition of the RHS
	ColumnDataCollection right_condition_data;
	//! Whether or not the RHS of the nested loop join has NULL values
	atomic<bool> has_null;
	//! A bool indicating for each tuple in the RHS if they found a match (only used in FULL OUTER JOIN)
	OuterJoinMarker right_outer;
};

vector<LogicalType> PhysicalNestedLoopJoin::GetJoinTypes() const {
	vector<LogicalType> result;
	for (auto &op : conditions) {
		result.push_back(op.right->return_type);
	}
	return result;
}

SinkResultType PhysicalNestedLoopJoin::Sink(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<NestedLoopJoinGlobalState>();
	auto &nlj_state = input.local_state.Cast<NestedLoopJoinLocalState>();

	// resolve the join expression of the right side
	nlj_state.right_condition.Reset();
	nlj_state.rhs_executor.Execute(chunk, nlj_state.right_condition);

	// if we have not seen any NULL values yet, and we are performing a MARK join, check if there are NULL values in
	// this chunk
	if (join_type == JoinType::MARK && !gstate.has_null) {
		if (HasNullValues(nlj_state.right_condition)) {
			gstate.has_null = true;
		}
	}

	// append the payload data and the conditions
	lock_guard<mutex> nj_guard(gstate.nj_lock);
	gstate.right_payload_data.Append(chunk);
	gstate.right_condition_data.Append(nlj_state.right_condition);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalNestedLoopJoin::Combine(ExecutionContext &context,
                                                      OperatorSinkCombineInput &input) const {
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(*this);
	client_profiler.Flush(context.thread.profiler);
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalNestedLoopJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<NestedLoopJoinGlobalState>();
	gstate.right_outer.Initialize(gstate.right_payload_data.Count());
	if (gstate.right_payload_data.Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> PhysicalNestedLoopJoin::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<NestedLoopJoinGlobalState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalNestedLoopJoin::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<NestedLoopJoinLocalState>(context.client, conditions);
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class PhysicalNestedLoopJoinState : public CachingOperatorState {
public:
	PhysicalNestedLoopJoinState(ClientContext &context, const PhysicalNestedLoopJoin &op,
	                            const vector<JoinCondition> &conditions)
	    : fetch_next_left(true), fetch_next_right(false), lhs_executor(context), left_tuple(0), right_tuple(0),
	      left_outer(IsLeftOuterJoin(op.join_type)) {
		vector<LogicalType> condition_types;
		for (auto &cond : conditions) {
			lhs_executor.AddExpression(*cond.left);
			condition_types.push_back(cond.left->return_type);
		}
		auto &allocator = Allocator::Get(context);
		left_condition.Initialize(allocator, condition_types);
		right_condition.Initialize(allocator, condition_types);
		right_payload.Initialize(allocator, op.children[1].get().GetTypes());
		left_outer.Initialize(STANDARD_VECTOR_SIZE);
	}

	bool fetch_next_left;
	bool fetch_next_right;
	DataChunk left_condition;
	//! The executor of the LHS condition
	ExpressionExecutor lhs_executor;

	ColumnDataScanState condition_scan_state;
	ColumnDataScanState payload_scan_state;
	DataChunk right_condition;
	DataChunk right_payload;

	idx_t left_tuple;
	idx_t right_tuple;

	OuterJoinMarker left_outer;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op);
	}
};

unique_ptr<OperatorState> PhysicalNestedLoopJoin::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<PhysicalNestedLoopJoinState>(context.client, *this, conditions);
}

OperatorResultType PhysicalNestedLoopJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input,
                                                           DataChunk &chunk, GlobalOperatorState &gstate_p,
                                                           OperatorState &state_p) const {
	auto &gstate = sink_state->Cast<NestedLoopJoinGlobalState>();

	if (gstate.right_payload_data.Count() == 0) {
		// empty RHS
		if (!EmptyResultIfRHSIsEmpty()) {
			ConstructEmptyJoinResult(join_type, gstate.has_null, input, chunk);
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			return OperatorResultType::FINISHED;
		}
	}

	switch (join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
		// simple joins can have max STANDARD_VECTOR_SIZE matches per chunk
		ResolveSimpleJoin(context, input, chunk, state_p);
		return OperatorResultType::NEED_MORE_INPUT;
	case JoinType::LEFT:
	case JoinType::INNER:
	case JoinType::OUTER:
	case JoinType::RIGHT:
		return ResolveComplexJoin(context, input, chunk, state_p);
	default:
		throw NotImplementedException("Unimplemented type " + JoinTypeToString(join_type) + " for nested loop join!");
	}
}

void PhysicalNestedLoopJoin::ResolveSimpleJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                               OperatorState &state_p) const {
	auto &state = state_p.Cast<PhysicalNestedLoopJoinState>();
	auto &gstate = sink_state->Cast<NestedLoopJoinGlobalState>();

	// resolve the left join condition for the current chunk
	state.left_condition.Reset();
	state.lhs_executor.Execute(input, state.left_condition);

	bool found_match[STANDARD_VECTOR_SIZE] = {false};
	NestedLoopJoinMark::Perform(state.left_condition, gstate.right_condition_data, found_match, conditions);
	switch (join_type) {
	case JoinType::MARK:
		// now construct the mark join result from the found matches
		PhysicalJoin::ConstructMarkJoinResult(state.left_condition, input, chunk, found_match, gstate.has_null);
		break;
	case JoinType::SEMI:
		// construct the semi join result from the found matches
		PhysicalJoin::ConstructSemiJoinResult(input, chunk, found_match);
		break;
	case JoinType::ANTI:
		// construct the anti join result from the found matches
		PhysicalJoin::ConstructAntiJoinResult(input, chunk, found_match);
		break;
	default:
		throw NotImplementedException("Unimplemented type for simple nested loop join!");
	}
}

OperatorResultType PhysicalNestedLoopJoin::ResolveComplexJoin(ExecutionContext &context, DataChunk &input,
                                                              DataChunk &chunk, OperatorState &state_p) const {
	auto &state = state_p.Cast<PhysicalNestedLoopJoinState>();
	auto &gstate = sink_state->Cast<NestedLoopJoinGlobalState>();

	idx_t match_count;
	do {
		if (state.fetch_next_right) {
			// we exhausted the chunk on the right: move to the next chunk on the right
			state.left_tuple = 0;
			state.right_tuple = 0;
			state.fetch_next_right = false;
			// check if we exhausted all chunks on the RHS
			if (gstate.right_condition_data.Scan(state.condition_scan_state, state.right_condition)) {
				if (!gstate.right_payload_data.Scan(state.payload_scan_state, state.right_payload)) {
					throw InternalException("Nested loop join: payload and conditions are unaligned!?");
				}
				if (state.right_condition.size() != state.right_payload.size()) {
					throw InternalException("Nested loop join: payload and conditions are unaligned!?");
				}
			} else {
				// we exhausted all chunks on the right: move to the next chunk on the left
				state.fetch_next_left = true;
				if (state.left_outer.Enabled()) {
					// left join: before we move to the next chunk, see if we need to output any vectors that didn't
					// have a match found
					state.left_outer.ConstructLeftJoinResult(input, chunk);
					state.left_outer.Reset();
				}
				return OperatorResultType::NEED_MORE_INPUT;
			}
		}
		if (state.fetch_next_left) {
			// resolve the left join condition for the current chunk
			state.left_condition.Reset();
			state.lhs_executor.Execute(input, state.left_condition);

			state.left_tuple = 0;
			state.right_tuple = 0;
			gstate.right_condition_data.InitializeScan(state.condition_scan_state);
			gstate.right_condition_data.Scan(state.condition_scan_state, state.right_condition);

			gstate.right_payload_data.InitializeScan(state.payload_scan_state);
			gstate.right_payload_data.Scan(state.payload_scan_state, state.right_payload);
			state.fetch_next_left = false;
		}
		// now we have a left and a right chunk that we can join together
		// note that we only get here in the case of a LEFT, INNER or FULL join
		auto &left_chunk = input;
		auto &right_condition = state.right_condition;
		auto &right_payload = state.right_payload;

		// sanity check
		left_chunk.Verify();
		right_condition.Verify();
		right_payload.Verify();

		// now perform the join
		SelectionVector lvector(STANDARD_VECTOR_SIZE), rvector(STANDARD_VECTOR_SIZE);
		match_count = NestedLoopJoinInner::Perform(state.left_tuple, state.right_tuple, state.left_condition,
		                                           right_condition, lvector, rvector, conditions);
		// we have finished resolving the join conditions
		if (match_count > 0) {
			// we have matching tuples!
			// construct the result
			state.left_outer.SetMatches(lvector, match_count);
			gstate.right_outer.SetMatches(rvector, match_count, state.condition_scan_state.current_row_index);

			chunk.Slice(input, lvector, match_count);
			chunk.Slice(right_payload, rvector, match_count, input.ColumnCount());
		}

		// check if we exhausted the RHS, if we did we need to move to the next right chunk in the next iteration
		if (state.right_tuple >= right_condition.size()) {
			state.fetch_next_right = true;
		}
	} while (match_count == 0);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class NestedLoopJoinGlobalScanState : public GlobalSourceState {
public:
	explicit NestedLoopJoinGlobalScanState(const PhysicalNestedLoopJoin &op) : op(op) {
		D_ASSERT(op.sink_state);
		auto &sink = op.sink_state->Cast<NestedLoopJoinGlobalState>();
		sink.right_outer.InitializeScan(sink.right_payload_data, scan_state);
	}

	const PhysicalNestedLoopJoin &op;
	OuterJoinGlobalScanState scan_state;

public:
	idx_t MaxThreads() override {
		auto &sink = op.sink_state->Cast<NestedLoopJoinGlobalState>();
		return sink.right_outer.MaxThreads();
	}
};

class NestedLoopJoinLocalScanState : public LocalSourceState {
public:
	explicit NestedLoopJoinLocalScanState(const PhysicalNestedLoopJoin &op, NestedLoopJoinGlobalScanState &gstate) {
		D_ASSERT(op.sink_state);
		auto &sink = op.sink_state->Cast<NestedLoopJoinGlobalState>();
		sink.right_outer.InitializeScan(gstate.scan_state, scan_state);
	}

	OuterJoinLocalScanState scan_state;
};

unique_ptr<GlobalSourceState> PhysicalNestedLoopJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<NestedLoopJoinGlobalScanState>(*this);
}

unique_ptr<LocalSourceState> PhysicalNestedLoopJoin::GetLocalSourceState(ExecutionContext &context,
                                                                         GlobalSourceState &gstate) const {
	return make_uniq<NestedLoopJoinLocalScanState>(*this, gstate.Cast<NestedLoopJoinGlobalScanState>());
}

SourceResultType PhysicalNestedLoopJoin::GetData(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	D_ASSERT(PropagatesBuildSide(join_type));
	// check if we need to scan any unmatched tuples from the RHS for the full/right outer join
	auto &sink = sink_state->Cast<NestedLoopJoinGlobalState>();
	auto &gstate = input.global_state.Cast<NestedLoopJoinGlobalScanState>();
	auto &lstate = input.local_state.Cast<NestedLoopJoinLocalScanState>();

	// if the LHS is exhausted in a FULL/RIGHT OUTER JOIN, we scan chunks we still need to output
	sink.right_outer.Scan(gstate.scan_state, lstate.scan_state, chunk);

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
