#include "duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp"

#include "duckdb/execution/operator/aggregate/grouped_aggregate_data.hpp"
#include "duckdb/execution/perfect_aggregate_hashtable.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/execution/aggregate_state_spilling.hpp"
#include "duckdb/execution/radix_partitioned_hashtable.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

PhysicalPerfectHashAggregate::PhysicalPerfectHashAggregate(PhysicalPlan &physical_plan, ClientContext &context,
                                                           vector<LogicalType> types_p,
                                                           vector<unique_ptr<Expression>> aggregates_p,
                                                           vector<unique_ptr<Expression>> groups_p,
                                                           const vector<unique_ptr<BaseStatistics>> &group_stats,
                                                           vector<idx_t> required_bits_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::PERFECT_HASH_GROUP_BY, std::move(types_p),
                       estimated_cardinality),
      groups(std::move(groups_p)), aggregates(std::move(aggregates_p)), required_bits(std::move(required_bits_p)) {
	D_ASSERT(groups.size() == group_stats.size());
	group_minima.reserve(group_stats.size());
	for (auto &stats : group_stats) {
		D_ASSERT(stats);
		auto &nstats = *stats;
		D_ASSERT(NumericStats::HasMin(nstats));
		group_minima.push_back(NumericStats::Min(nstats));
	}
	for (auto &expr : groups) {
		group_types.push_back(expr->GetReturnType());
	}

	vector<BoundAggregateExpression *> bindings;
	for (auto &expr : aggregates) {
		D_ASSERT(expr->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
		D_ASSERT(expr->IsAggregate());
		auto &aggr = expr->Cast<BoundAggregateExpression>();
		bindings.push_back(&aggr);

		D_ASSERT(!aggr.IsDistinct());
		D_ASSERT(aggr.Function().HasStateCombineCallback());
		for (auto &child : aggr.GetChildren()) {
			payload_types.push_back(child->GetReturnType());
		}
	}
	// filter_columns must be pre-built: everything that lives in this class is read-only at execution time
	filter_columns = GroupedAggregateData::AddFilterColumns(aggregates, payload_types);
	aggregate_objects = AggregateObject::CreateAggregateObjects(bindings);
}

unique_ptr<PerfectAggregateHashTable> PhysicalPerfectHashAggregate::CreateHT(Allocator &allocator,
                                                                             ClientContext &context) const {
	return make_uniq<PerfectAggregateHashTable>(context, allocator, group_types, payload_types, aggregate_objects,
	                                            group_minima, required_bits);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PerfectHashAggregateGlobalState : public GlobalSinkState {
public:
	PerfectHashAggregateGlobalState(const PhysicalPerfectHashAggregate &op, ClientContext &context)
	    : ht(op.CreateHT(BufferAllocator::Get(context), context)), capacity(ht->Capacity()),
	      spill_plan(AggregateStateSpilling::TryCreateSpillPlan(ht->GetLayout())) {
		if (!spill_plan) {
			return;
		}
		for (idx_t i = 0; i < op.aggregate_objects.size(); i++) {
			auto &aggregate = op.aggregate_objects[i];
			const auto width = spill_plan->FixedListElementWidth(i, aggregate);
			list_width += width;
			if (!width && spill_plan->HasArenaReferences(i)) {
				has_variable_width_state = true;
			}
		}
	}

	idx_t AvailableMemory(ClientContext &context) const {
		auto &manager = BufferManager::GetBufferManager(context);
		const auto limit = manager.GetOperatorMemoryLimit();
		const auto allocated = arena_capacity.load();
		// BufferAllocator reservations stay resident. Cached storage blocks can be evicted.
		const auto pool_limit = manager.GetMaxMemory();
		const auto reserved = manager.GetBufferPool().GetUsedMemory(MemoryTag::ALLOCATOR, false);
		return MinValue(limit - MinValue(limit, allocated), pool_limit - MinValue(pool_limit, reserved));
	}

	bool NeedsRadix(ExecutionContext &context, PerfectAggregateHashTable &table, idx_t rows) const {
		auto available = AvailableMemory(context.client);
		const auto allocated = arena_capacity.load();
		if (allocated <= available) {
			if (!list_width || !rows || allocated <= available / 2) {
				return false;
			}
			// One list must fit in a spill batch even after the arena grows again.
			const auto block = BufferManager::GetBufferManager(context.client).GetBlockAllocSize();
			const auto room = MaxValue(block, available - allocated);
			return table.SingleGroupExceeds(*spill_plan, room, list_width, rows);
		}
		if (has_variable_width_state) {
			// Dense combine can copy string states into another arena before they can be exported.
			const auto block = BufferManager::GetBufferManager(context.client).GetBlockAllocSize();
			if (!list_width || !table.ReserveStringResults(*spill_plan, block, available)) {
				return true;
			}
			// A small native allocation should not double the arena under pressure.
			table.GetArena().SetMaximumBlockSize(block);
		}
		// Each LIST accepts at most one element per input row. Inspect individual groups only near the limit.
		if (!list_width || !rows ||
		    (rows <= DConstants::MAX_VECTOR_SIZE && VectorBuffer::GetReserveSize(rows) <= available / list_width)) {
			return false;
		}
		return table.ListResultExceeds(*spill_plan, available);
	}

	unique_ptr<LocalSinkState> MoveToRadix(ExecutionContext &context, unique_ptr<PerfectAggregateHashTable> dense,
	                                       idx_t rows) {
		auto local = radix->GetLocalSinkState(context);
		if (rows) {
			radix->Sink(context, *dense, *radix_sink, *local, AvailableMemory(context.client));
		}
		return local;
	}

	//! Caller holds the combine lock, so completed states can be transferred exactly once.
	void SwitchToRadix(ExecutionContext &context, const PhysicalPerfectHashAggregate &op) {
		if (external) {
			return;
		}
		vector<unique_ptr<Expression>> groups, aggregates;
		for (auto &group : op.groups) {
			grouping.insert(ProjectionIndex(groups.size()));
			groups.push_back(group->Copy());
		}
		for (auto &aggregate : op.aggregates) {
			filter.push_back(aggregates.size());
			aggregates.push_back(aggregate->Copy());
		}
		grouped.InitializeGroupby(std::move(groups), std::move(aggregates), {});
		grouped.payload_types = op.payload_types;
		radix = make_uniq<RadixPartitionedHashTable>(grouping, grouped, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
		radix_sink = radix->GetGlobalSinkState(context.client);
		if (combined_rows) {
			auto local = MoveToRadix(context, std::move(ht), combined_rows);
			radix->Combine(context, *radix_sink, *local);
		}
		ht.reset();
		external = true;
	}

	mutex lock;
	unique_ptr<PerfectAggregateHashTable> ht;
	const idx_t capacity;
	unique_ptr<AggregateStateSpillPlan> spill_plan;
	atomic<idx_t> arena_capacity {0};
	atomic<bool> external {false};
	idx_t list_width = 0;
	bool has_variable_width_state = false;
	idx_t combined_rows = 0;
	idx_t scan_count = STANDARD_VECTOR_SIZE;
	atomic<bool> radix_finalized {false};
	GroupedAggregateData grouped;
	GroupingSet grouping;
	unsafe_vector<idx_t> filter;
	unique_ptr<RadixPartitionedHashTable> radix;
	unique_ptr<GlobalSinkState> radix_sink;
};

class PerfectHashAggregateLocalState : public LocalSinkState {
public:
	PerfectHashAggregateLocalState(const PhysicalPerfectHashAggregate &op, ExecutionContext &context)
	    : ht(op.CreateHT(BufferAllocator::Get(context.client), context.client)) {
		group_chunk.InitializeEmpty(op.group_types);
		if (!op.payload_types.empty()) {
			aggregate_input_chunk.InitializeEmpty(op.payload_types);
		}
	}

	void AccountArena(PerfectHashAggregateGlobalState &global) {
		const auto capacity = ht->GetArena().AllocationSize();
		if (capacity != accounted) {
			global.arena_capacity.fetch_add(capacity - accounted);
			accounted = capacity;
		}
	}

	idx_t rows = 0;
	idx_t accounted = 0;
	unique_ptr<LocalSinkState> radix;
	//! The local aggregate hash table
	unique_ptr<PerfectAggregateHashTable> ht;
	DataChunk group_chunk;
	DataChunk aggregate_input_chunk;
};

unique_ptr<GlobalSinkState> PhysicalPerfectHashAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<PerfectHashAggregateGlobalState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalPerfectHashAggregate::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<PerfectHashAggregateLocalState>(*this, context);
}

SinkResultType PhysicalPerfectHashAggregate::Sink(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<PerfectHashAggregateLocalState>();
	DataChunk &group_chunk = lstate.group_chunk;
	DataChunk &aggregate_input_chunk = lstate.aggregate_input_chunk;

	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &group = groups[group_idx];
		D_ASSERT(group->GetExpressionType() == ExpressionType::BOUND_REF);
		auto &bound_ref_expr = group->Cast<BoundReferenceExpression>();
		group_chunk.data[group_idx].Reference(chunk.data[bound_ref_expr.Index()]);
	}
	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		for (auto &child_expr : aggr.GetChildren()) {
			D_ASSERT(child_expr->GetExpressionType() == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = child_expr->Cast<BoundReferenceExpression>();
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(chunk.data[bound_ref_expr.Index()]);
		}
	}
	for (auto &entry : filter_columns) {
		D_ASSERT(entry.first >= aggregate_input_idx && entry.second < chunk.data.size());
		aggregate_input_chunk.data[entry.first].Reference(chunk.data[entry.second]);
	}

	group_chunk.Verify(context.client.db);
	aggregate_input_chunk.Verify(context.client.db);
	D_ASSERT(aggregate_input_chunk.ColumnCount() == 0 || group_chunk.size() == aggregate_input_chunk.size());

	auto &global = input.global_state.Cast<PerfectHashAggregateGlobalState>();
	if (global.spill_plan) {
		if (!global.external) {
			lstate.AccountArena(global);
			if (global.NeedsRadix(context, *lstate.ht, lstate.rows)) {
				lock_guard<mutex> guard(global.lock);
				global.SwitchToRadix(context, *this);
			}
		}
		if (global.external) {
			if (!lstate.radix) {
				lstate.radix = global.MoveToRadix(context, std::move(lstate.ht), lstate.rows);
			}
			OperatorSinkInput radix_input {*global.radix_sink, *lstate.radix, input.interrupt_state};
			global.radix->Sink(context, chunk, radix_input, aggregate_input_chunk, global.filter);
			return SinkResultType::NEED_MORE_INPUT;
		}
	}
	lstate.ht->AddChunk(group_chunk, aggregate_input_chunk);
	lstate.rows += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType PhysicalPerfectHashAggregate::Combine(ExecutionContext &context,
                                                            OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<PerfectHashAggregateLocalState>();
	auto &gstate = input.global_state.Cast<PerfectHashAggregateGlobalState>();

	if (!gstate.external) {
		lock_guard<mutex> guard(gstate.lock);
		if (!gstate.external) {
			if (!gstate.spill_plan) {
				gstate.ht->Combine(*lstate.ht);
				lstate.ht.reset();
				return SinkCombineResultType::FINISHED;
			}
			lstate.AccountArena(gstate);
			if (!gstate.combined_rows) {
				// The first local table already owns its states and their arena.
				gstate.ht = std::move(lstate.ht);
			} else if (gstate.has_variable_width_state && gstate.NeedsRadix(context, *lstate.ht, lstate.rows)) {
				// The last input chunk may have filled string states that dense combine would copy.
				gstate.SwitchToRadix(context, *this);
			} else {
				const auto before = gstate.ht->GetArena().AllocationSize();
				gstate.ht->Combine(*lstate.ht);
				lstate.ht.reset();
				gstate.arena_capacity.fetch_add(gstate.ht->GetArena().AllocationSize() - before);
			}
			if (!gstate.external) {
				gstate.combined_rows += lstate.rows;
				if (gstate.NeedsRadix(context, *gstate.ht, gstate.combined_rows)) {
					gstate.SwitchToRadix(context, *this);
				}
				return SinkCombineResultType::FINISHED;
			}
		}
	}
	if (!lstate.radix) {
		lstate.radix = gstate.MoveToRadix(context, std::move(lstate.ht), lstate.rows);
	}
	gstate.radix->Combine(context, *gstate.radix_sink, *lstate.radix);
	lstate.radix.reset();

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalPerfectHashAggregate::Finalize(Pipeline &, Event &, ClientContext &context,
                                                        OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<PerfectHashAggregateGlobalState>();
	if (state.external) {
		state.radix->Finalize(context, *state.radix_sink);
		state.radix_finalized = true;
	} else if (state.list_width) {
		const auto available = state.AvailableMemory(context);
		if (state.arena_capacity.load() > available) {
			// Leave room for the consumer to copy the materialized lists while their native states are live.
			while (state.scan_count > 1 &&
			       state.ht->ListResultExceeds(*state.spill_plan, available / 2, state.scan_count)) {
				state.scan_count /= 2;
			}
		}
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PerfectHashAggregateState : public GlobalSourceState {
public:
	explicit PerfectHashAggregateState(const PhysicalPerfectHashAggregate &op) : op(op) {
	}
	idx_t MaxThreads() override {
		if (!op.sink_state) {
			return 1;
		}
		auto &sink = op.sink_state->Cast<PerfectHashAggregateGlobalState>();
		return sink.radix_finalized ? MaxValue<idx_t>(1, sink.radix->MaxThreads(*sink.radix_sink)) : 1;
	}

	const PhysicalPerfectHashAggregate &op;
	idx_t ht_scan_position = 0;
	atomic<idx_t> scanned_slots {0};
	mutex source_lock;
	unique_ptr<GlobalSourceState> radix;
};

unique_ptr<GlobalSourceState> PhysicalPerfectHashAggregate::GetGlobalSourceState(ClientContext &) const {
	return make_uniq<PerfectHashAggregateState>(*this);
}

unique_ptr<LocalSourceState> PhysicalPerfectHashAggregate::GetLocalSourceState(ExecutionContext &context,
                                                                               GlobalSourceState &global) const {
	auto &sink = sink_state->Cast<PerfectHashAggregateGlobalState>();
	if (!sink.external) {
		return PhysicalOperator::GetLocalSourceState(context, global);
	}
	auto &state = global.Cast<PerfectHashAggregateState>();
	lock_guard<mutex> guard(state.source_lock);
	if (!state.radix) {
		state.radix = sink.radix->GetGlobalSourceState(context.client);
	}
	return sink.radix->GetLocalSourceState(context);
}

ProgressData PhysicalPerfectHashAggregate::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &state = gstate_p.Cast<PerfectHashAggregateState>();
	auto &gstate = sink_state->Cast<PerfectHashAggregateGlobalState>();
	if (gstate.external) {
		lock_guard<mutex> guard(state.source_lock);
		if (state.radix) {
			return gstate.radix->GetProgress(context, *gstate.radix_sink, *state.radix);
		}
	}
	return ProgressData {double(state.scanned_slots.load()), double(gstate.capacity), false};
}

void PhysicalPerfectHashAggregate::SourceFinished(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &state = gstate_p.Cast<PerfectHashAggregateState>();
	auto &gstate = sink_state->Cast<PerfectHashAggregateGlobalState>();
	if (!gstate.external) {
		state.scanned_slots = gstate.capacity;
	}
}

SourceResultType PhysicalPerfectHashAggregate::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                               OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<PerfectHashAggregateState>();
	auto &gstate = sink_state->Cast<PerfectHashAggregateGlobalState>();

	if (gstate.external) {
		OperatorSourceInput source {*state.radix, input.local_state, input.interrupt_state};
		return gstate.radix->GetData(context, chunk, *gstate.radix_sink, source);
	}
	gstate.ht->Scan(state.ht_scan_position, chunk, gstate.scan_count);
	state.scanned_slots.store(state.ht_scan_position, std::memory_order_relaxed);

	if (chunk.size() > 0) {
		return SourceResultType::HAVE_MORE_OUTPUT;
	} else {
		return SourceResultType::FINISHED;
	}
}

InsertionOrderPreservingMap<string> PhysicalPerfectHashAggregate::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string groups_info;
	for (idx_t i = 0; i < groups.size(); i++) {
		if (i > 0) {
			groups_info += "\n";
		}
		groups_info += groups[i]->GetName();
	}
	result["Groups"] = groups_info;

	string aggregate_info;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		if (i > 0) {
			aggregate_info += "\n";
		}
		aggregate_info += aggregates[i]->GetName();
		auto &aggregate = aggregates[i]->Cast<BoundAggregateExpression>();
		if (aggregate.GetFilter()) {
			aggregate_info += " Filter: " + aggregate.GetFilter()->GetName();
		}
	}
	result["Aggregates"] = aggregate_info;
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
