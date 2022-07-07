#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

PhysicalHashJoin::PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   const vector<idx_t> &left_projection_map,
                                   const vector<idx_t> &right_projection_map_p, vector<LogicalType> delim_types,
                                   idx_t estimated_cardinality, PerfectHashJoinStats perfect_join_stats)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::HASH_JOIN, move(cond), join_type, estimated_cardinality),
      right_projection_map(right_projection_map_p), delim_types(move(delim_types)),
      perfect_join_statistics(move(perfect_join_stats)) {

	children.push_back(move(left));
	children.push_back(move(right));

	D_ASSERT(left_projection_map.empty());
	for (auto &condition : conditions) {
		condition_types.push_back(condition.left->return_type);
	}

	// for ANTI, SEMI and MARK join, we only need to store the keys, so for these the build types are empty
	if (join_type != JoinType::ANTI && join_type != JoinType::SEMI && join_type != JoinType::MARK) {
		build_types = LogicalOperator::MapTypes(children[1]->GetTypes(), right_projection_map);
	}
}

PhysicalHashJoin::PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   idx_t estimated_cardinality, PerfectHashJoinStats perfect_join_state)
    : PhysicalHashJoin(op, move(left), move(right), move(cond), join_type, {}, {}, {}, estimated_cardinality,
                       std::move(perfect_join_state)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashJoinLocalState : public LocalSinkState {
public:
	HashJoinLocalState(Allocator &allocator, const PhysicalHashJoin &hj) : build_executor(allocator) {
		if (!hj.right_projection_map.empty()) {
			build_chunk.Initialize(allocator, hj.build_types);
		}
		for (auto &cond : hj.conditions) {
			build_executor.AddExpression(*cond.right);
		}
		join_keys.Initialize(allocator, hj.condition_types);
	}

	DataChunk build_chunk;
	DataChunk join_keys;
	ExpressionExecutor build_executor;
};

class HashJoinGlobalState : public GlobalSinkState {
public:
	HashJoinGlobalState() {
	}

	//! The HT used by the join
	unique_ptr<JoinHashTable> hash_table;
	//! The perfect hash join executor (if any)
	unique_ptr<PerfectHashJoinExecutor> perfect_join_executor;
	//! Whether or not the hash table has been finalized
	bool finalized = false;
};

unique_ptr<GlobalSinkState> PhysicalHashJoin::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_unique<HashJoinGlobalState>();
	state->hash_table =
	    make_unique<JoinHashTable>(BufferManager::GetBufferManager(context), conditions, build_types, join_type);
	if (!delim_types.empty() && join_type == JoinType::MARK) {
		// correlated MARK join
		if (delim_types.size() + 1 == conditions.size()) {
			// the correlated MARK join has one more condition than the amount of correlated columns
			// this is the case in a correlated ANY() expression
			// in this case we need to keep track of additional entries, namely:
			// - (1) the total amount of elements per group
			// - (2) the amount of non-null elements per group
			// we need these to correctly deal with the cases of either:
			// - (1) the group being empty [in which case the result is always false, even if the comparison is NULL]
			// - (2) the group containing a NULL value [in which case FALSE becomes NULL]
			auto &info = state->hash_table->correlated_mark_join_info;

			vector<LogicalType> payload_types;
			vector<BoundAggregateExpression *> correlated_aggregates;
			unique_ptr<BoundAggregateExpression> aggr;

			// jury-rigging the GroupedAggregateHashTable
			// we need a count_star and a count to get counts with and without NULLs
			aggr = AggregateFunction::BindAggregateFunction(context, CountStarFun::GetFunction(), {}, nullptr, false);
			correlated_aggregates.push_back(&*aggr);
			payload_types.push_back(aggr->return_type);
			info.correlated_aggregates.push_back(move(aggr));

			auto count_fun = CountFun::GetFunction();
			vector<unique_ptr<Expression>> children;
			// this is a dummy but we need it to make the hash table understand whats going on
			children.push_back(make_unique_base<Expression, BoundReferenceExpression>(count_fun.return_type, 0));
			aggr = AggregateFunction::BindAggregateFunction(context, count_fun, move(children), nullptr, false);
			correlated_aggregates.push_back(&*aggr);
			payload_types.push_back(aggr->return_type);
			info.correlated_aggregates.push_back(move(aggr));

			auto &allocator = Allocator::Get(context);
			info.correlated_counts = make_unique<GroupedAggregateHashTable>(
			    allocator, BufferManager::GetBufferManager(context), delim_types, payload_types, correlated_aggregates);
			info.correlated_types = delim_types;
			info.group_chunk.Initialize(allocator, delim_types);
			info.result_chunk.Initialize(allocator, payload_types);
		}
	}
	// for perfect hash join
	state->perfect_join_executor =
	    make_unique<PerfectHashJoinExecutor>(*this, *state->hash_table, perfect_join_statistics);
	return move(state);
}

unique_ptr<LocalSinkState> PhysicalHashJoin::GetLocalSinkState(ExecutionContext &context) const {
	auto &allocator = Allocator::Get(context.client);
	auto state = make_unique<HashJoinLocalState>(allocator, *this);
	return move(state);
}

SinkResultType PhysicalHashJoin::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p,
                                      DataChunk &input) const {
	auto &sink = (HashJoinGlobalState &)state;
	auto &lstate = (HashJoinLocalState &)lstate_p;
	// resolve the join keys for the right chunk
	lstate.join_keys.Reset();
	lstate.build_executor.Execute(input, lstate.join_keys);
	// TODO: add statement to check for possible per
	// build the HT
	if (!right_projection_map.empty()) {
		// there is a projection map: fill the build chunk with the projected columns
		lstate.build_chunk.Reset();
		lstate.build_chunk.SetCardinality(input);
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			lstate.build_chunk.data[i].Reference(input.data[right_projection_map[i]]);
		}
		sink.hash_table->Build(lstate.join_keys, lstate.build_chunk);
	} else if (!build_types.empty()) {
		// there is not a projected map: place the entire right chunk in the HT
		sink.hash_table->Build(lstate.join_keys, input);
	} else {
		// there are only keys: place an empty chunk in the payload
		lstate.build_chunk.SetCardinality(input.size());
		sink.hash_table->Build(lstate.join_keys, lstate.build_chunk);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalHashJoin::Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const {
	auto &state = (HashJoinLocalState &)lstate;
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(this, &state.build_executor, "build_executor", 1);
	client_profiler.Flush(context.thread.profiler);
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalHashJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            GlobalSinkState &gstate) const {
	auto &sink = (HashJoinGlobalState &)gstate;
	// check for possible perfect hash table
	auto use_perfect_hash = sink.perfect_join_executor->CanDoPerfectHashJoin();
	if (use_perfect_hash) {
		D_ASSERT(sink.hash_table->equality_types.size() == 1);
		auto key_type = sink.hash_table->equality_types[0];
		use_perfect_hash = sink.perfect_join_executor->BuildPerfectHashTable(key_type);
	}
	// In case of a large build side or duplicates, use regular hash join
	if (!use_perfect_hash) {
		sink.perfect_join_executor.reset();
		sink.hash_table->Finalize();
	}
	sink.finalized = true;
	if (sink.hash_table->Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class PhysicalHashJoinState : public OperatorState {
public:
	explicit PhysicalHashJoinState(Allocator &allocator) : probe_executor(allocator) {
	}

	DataChunk join_keys;
	ExpressionExecutor probe_executor;
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;
	unique_ptr<OperatorState> perfect_hash_join_state;

public:
	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, &probe_executor, "probe_executor", 0);
	}
};

unique_ptr<OperatorState> PhysicalHashJoin::GetOperatorState(ExecutionContext &context) const {
	auto &allocator = Allocator::Get(context.client);
	auto &sink = (HashJoinGlobalState &)*sink_state;
	auto state = make_unique<PhysicalHashJoinState>(allocator);
	if (sink.perfect_join_executor) {
		state->perfect_hash_join_state = sink.perfect_join_executor->GetOperatorState(context);
	} else {
		state->join_keys.Initialize(allocator, condition_types);
		for (auto &cond : conditions) {
			state->probe_executor.AddExpression(*cond.left);
		}
	}
	return move(state);
}

OperatorResultType PhysicalHashJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                             GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = (PhysicalHashJoinState &)state_p;
	auto &sink = (HashJoinGlobalState &)*sink_state;
	D_ASSERT(sink.finalized);

	if (sink.hash_table->Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return OperatorResultType::FINISHED;
	}
	if (sink.perfect_join_executor) {
		return sink.perfect_join_executor->ProbePerfectHashTable(context, input, chunk, *state.perfect_hash_join_state);
	}

	if (state.scan_structure) {
		// still have elements remaining from the previous probe (i.e. we got
		// >1024 elements in the previous probe)
		state.scan_structure->Next(state.join_keys, input, chunk);
		if (chunk.size() > 0) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		state.scan_structure = nullptr;
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// probe the HT
	if (sink.hash_table->Count() == 0) {
		ConstructEmptyJoinResult(sink.hash_table->join_type, sink.hash_table->has_null, input, chunk);
		return OperatorResultType::NEED_MORE_INPUT;
	}
	// resolve the join keys for the left chunk
	state.join_keys.Reset();
	state.probe_executor.Execute(input, state.join_keys);

	// perform the actual probe
	state.scan_structure = sink.hash_table->Probe(state.join_keys);
	state.scan_structure->Next(state.join_keys, input, chunk);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class HashJoinScanState : public GlobalSourceState {
public:
	explicit HashJoinScanState(const PhysicalHashJoin &op) : op(op) {
	}

	const PhysicalHashJoin &op;
	//! Only used for FULL OUTER JOIN: scan state of the final scan to find unmatched tuples in the build-side
	JoinHTScanState ht_scan_state;

	idx_t MaxThreads() override {
		auto &sink = (HashJoinGlobalState &)*op.sink_state;
		return sink.hash_table->Count() / (STANDARD_VECTOR_SIZE * 10);
	}
};

unique_ptr<GlobalSourceState> PhysicalHashJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<HashJoinScanState>(*this);
}

void PhysicalHashJoin::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                               LocalSourceState &lstate) const {
	D_ASSERT(IsRightOuterJoin(join_type));
	// check if we need to scan any unmatched tuples from the RHS for the full/right outer join
	auto &sink = (HashJoinGlobalState &)*sink_state;
	auto &state = (HashJoinScanState &)gstate;
	sink.hash_table->ScanFullOuter(chunk, state.ht_scan_state);
}

} // namespace duckdb
