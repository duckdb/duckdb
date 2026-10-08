#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/execution/radix_ht_sink_state.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "test_helpers.hpp"

namespace duckdb {

class RadixSinkFixture {
public:
	RadixSinkFixture(const string &memory_limit, bool aggregate = false)
	    : db(nullptr), con(db), thread(*con.context), context(*con.context, thread, nullptr) {
		REQUIRE_NO_FAIL(con.Query("SET threads=4; SET memory_limit='" + memory_limit + "';"));
		REQUIRE_NO_FAIL(
		    con.Query("SET temp_directory = " + Value(TestCreatePath("radix_ht_sink_spill")).ToSQLString()));
		vector<unique_ptr<Expression>> groups;
		groups.push_back(make_uniq<BoundReferenceExpression>(LogicalType::VARCHAR, 0));
		vector<unique_ptr<Expression>> aggregates;
		if (aggregate) {
			auto plan = con.ExtractPlan("SELECT i::VARCHAR k, string_agg(i::VARCHAR) FROM range(10) t(i) GROUP BY k");
			optional_ptr<LogicalOperator> aggr = plan.get();
			while (aggr->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
				aggr = aggr->children[0].get();
			}
			aggregates.push_back(aggr->expressions[0]->Copy());
			filter.push_back(0);
			payload.Initialize(Allocator::Get(*con.context), {LogicalType::VARCHAR});
			SetPayload(Value(LogicalType::VARCHAR));
		}
		op.InitializeGroupby(std::move(groups), std::move(aggregates), {});
		grouping_set.insert(ProjectionIndex(0));
		radix = make_uniq<RadixPartitionedHashTable>(grouping_set, op, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
		global = radix->GetGlobalSinkState(*con.context);
		for (idx_t i = 0; i < 4; i++) {
			locals.push_back(radix->GetLocalSinkState(context));
		}
		chunk.Initialize(Allocator::Get(*con.context), {LogicalType::VARCHAR});
	}

	RadixHTGlobalSinkState &Global() {
		return global->Cast<RadixHTGlobalSinkState>();
	}

	RadixHTLocalSinkState &Local(idx_t index = 0) {
		return locals[index]->Cast<RadixHTLocalSinkState>();
	}

	void Keys(idx_t offset, idx_t distinct, idx_t width = 0, bool random = false) {
		chunk.Reset();
		{
			auto writer = FlatVector::Writer<string_t>(chunk.data[0], STANDARD_VECTOR_SIZE);
			for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
				const auto row = offset + i;
				const auto key = (random ? Hash(row) : row) % distinct;
				const auto text = string(width, 'x') + std::to_string(key);
				writer.WriteValue(string_t(text));
			}
		}
		chunk.CheckCardinality(STANDARD_VECTOR_SIZE);
	}

	void SetPayload(const Value &value) {
		payload.data[0].Reference(value, count_t(STANDARD_VECTOR_SIZE));
		payload.CheckCardinality(STANDARD_VECTOR_SIZE);
	}

	void Sink(idx_t index = 0) {
		OperatorSinkInput input {*global, *locals[index], interrupt};
		radix->Sink(context, chunk, input, payload, filter);
	}

	void Combine(idx_t index) {
		radix->Combine(context, *global, *locals[index]);
	}

	idx_t ScanGroupCount() {
		radix->Finalize(*con.context, *global);
		radix->MaxThreads(*global);
		auto source = radix->GetGlobalSourceState(*con.context);
		auto local_source = radix->GetLocalSourceState(context);
		OperatorSourceInput input {*source, *local_source, interrupt};
		DataChunk output;
		output.Initialize(Allocator::Get(*con.context), {LogicalType::VARCHAR});
		idx_t count = 0;
		while (true) {
			output.Reset();
			const auto result = radix->GetData(context, output, *global, input);
			REQUIRE(result != SourceResultType::BLOCKED);
			count += output.size();
			if (result == SourceResultType::FINISHED) {
				return count;
			}
		}
	}

public:
	DuckDB db;
	Connection con;
	ThreadContext thread;
	ExecutionContext context;
	InterruptState interrupt;
	GroupedAggregateData op;
	GroupingSet grouping_set;
	unique_ptr<RadixPartitionedHashTable> radix;
	unique_ptr<GlobalSinkState> global;
	vector<unique_ptr<LocalSinkState>> locals;
	DataChunk chunk;
	DataChunk payload;
	unsafe_vector<idx_t> filter;
};

TEST_CASE("Growing aggregate tables spill before their occupancy limit", "[radix_ht_sink]") {
	RadixSinkFixture fixture("128MB");
	for (idx_t offset = 0; offset < 400000; offset += STANDARD_VECTOR_SIZE) {
		fixture.Keys(offset, 65536);
		for (idx_t worker = 0; worker < 4; worker++) {
			fixture.Sink(worker);
		}
	}
	auto &global = fixture.Global();
	REQUIRE_FALSE(global.external);
	for (idx_t worker = 0; worker < 4; worker++) {
		REQUIRE(fixture.Local(worker).has_grown);
		REQUIRE(fixture.Local(worker).ht->Capacity() > global.config.sink_capacity);
	}
	bool spilled_below_threshold = false;
	for (idx_t offset = 0; offset < 262144; offset += STANDARD_VECTOR_SIZE) {
		fixture.Keys(offset, 262144, 800);
		for (idx_t worker = 0; worker < 4; worker++) {
			auto &local = fixture.Local(worker);
			const auto old_capacity = local.ht->Capacity();
			const auto old_count = local.ht->Count();
			fixture.Sink(worker);
			if (local.ht->Capacity() < old_capacity) {
				spilled_below_threshold |=
				    old_count + 2 * STANDARD_VECTOR_SIZE < GroupedAggregateHashTable::ResizeThreshold(old_capacity);
			}
		}
	}
	REQUIRE(global.external);
	REQUIRE(spilled_below_threshold);
	for (idx_t worker = 0; worker < 4; worker++) {
		auto &local = fixture.Local(worker);
		REQUIRE_FALSE(local.ht->HLLEnabled());
		REQUIRE(local.ht->Capacity() == global.config.sink_capacity);
		REQUIRE(local.local_sink_capacity == local.ht->Capacity());
		REQUIRE(local.abandoned_data);
		REQUIRE(local.abandoned_data->Count() > 0);
		fixture.Combine(worker);
	}
	REQUIRE(fixture.ScanGroupCount() == 327680);
}

TEST_CASE("Increasing the reservation preserves an underfull aggregate table", "[radix_ht_sink]") {
	RadixSinkFixture fixture("128MB");
	fixture.Keys(0, 65536);
	fixture.Sink();
	auto &local = fixture.Local();
	auto &global = fixture.Global();
	const auto old_count = local.ht->Count();
	// Simulate a reservation reduction while the pointer table still has spare capacity.
	global.temporary_memory_state->SetRemainingSizeAndUpdateReservation(*fixture.con.context,
	                                                                    local.ht->GetSizeInBytes());
	const auto old_limit = global.GetThreadLimit();
	REQUIRE(local.ht->GetSizeInBytes() > old_limit);
	REQUIRE(old_count + 2 * STANDARD_VECTOR_SIZE < local.ht->ResizeThreshold());
	fixture.Keys(STANDARD_VECTOR_SIZE, 65536);
	fixture.Sink();
	REQUIRE(global.GetThreadLimit() > old_limit);
	REQUIRE(local.ht->Count() == old_count + STANDARD_VECTOR_SIZE);
	REQUIRE_FALSE(local.spilling);
}

TEST_CASE("State export retires aggregate telemetry even after another worker combines", "[radix_ht_sink]") {
	for (const idx_t input_count : {idx_t(262144), idx_t(1114112)}) {
		CAPTURE(input_count);
		RadixSinkFixture fixture("512MB", true);
		for (idx_t offset = 0; offset < input_count; offset += STANDARD_VECTOR_SIZE) {
			fixture.Keys(offset, 65536);
			fixture.Sink();
		}
		auto &local = fixture.Local();
		REQUIRE(local.has_grown);
		REQUIRE(local.ht->HLLEnabled());
		fixture.Sink(1);
		fixture.Combine(1);
		REQUIRE(fixture.Global().any_combined);
		fixture.SetPayload(Value(string(2048, 'p')));
		for (idx_t offset = 0; offset < 65536 && local.abandoned_exported_data.empty();
		     offset += STANDARD_VECTOR_SIZE) {
			fixture.Keys(offset, 65536);
			fixture.Sink();
		}
		REQUIRE_FALSE(local.abandoned_exported_data.empty());
		REQUIRE_FALSE(fixture.Global().external);
		REQUIRE_FALSE(local.ht->HLLEnabled());
		REQUIRE(local.spilling);
		REQUIRE(local.adapted);
		REQUIRE_FALSE(local.has_grown);
		REQUIRE(local.materialized_count_at_growth == 0);
		REQUIRE(local.materialized_count_at_observation == 0);
		REQUIRE(local.stable_observation_count == 0);
		REQUIRE(local.ht->Capacity() == fixture.Global().config.sink_capacity);
		fixture.SetPayload(Value(LogicalType::VARCHAR));
		for (idx_t offset = 0; offset < 1048576; offset += STANDARD_VECTOR_SIZE) {
			fixture.Keys(offset, 65536);
			fixture.Sink();
		}
		fixture.radix->ResetLocalSinkState(fixture.context, *fixture.global, *fixture.locals[0]);
		REQUIRE(local.ht->HLLEnabled());
		REQUIRE_FALSE(local.spilling);
		REQUIRE_FALSE(local.adapted);
		REQUIRE_FALSE(local.has_grown);
		REQUIRE(local.ht->GetSinkCount() == 0);
		REQUIRE(local.materialized_count_at_observation == 0);
	}
}

TEST_CASE("Aggregate observation retires on hits and distinct convergence", "[radix_ht_sink]") {
	for (const bool convergence : {false, true}) {
		CAPTURE(convergence);
		RadixSinkFixture fixture("1GB");
		const idx_t distinct = convergence ? 524288 : 65536;
		bool retired = false;
		for (idx_t offset = 0; offset < 3 * 1048576; offset += STANDARD_VECTOR_SIZE) {
			fixture.Keys(offset, distinct, 0, convergence && offset >= distinct);
			auto &local = fixture.Local();
			const auto previous_materialized = local.materialized_count_at_observation;
			const auto previous_input = local.sink_count_at_observation;
			fixture.Sink();
			if (!local.ht->HLLEnabled()) {
				REQUIRE(local.has_grown);
				REQUIRE(local.stable_observation_count == 3);
				const auto new_groups = local.ht->GetMaterializedCount() - previous_materialized;
				const auto new_input = local.ht->GetSinkCount() - previous_input;
				if (convergence) {
					REQUIRE(new_groups > new_input / 100);
				} else {
					REQUIRE(new_groups <= new_input / 100);
				}
				retired = true;
				break;
			}
		}
		REQUIRE(retired);
	}
}

TEST_CASE("Spilling tolerates memory pressure while shrinking retained pointer allocations", "[radix_ht_sink]") {
	for (const bool memory_pressure : {false, true}) {
		CAPTURE(memory_pressure);
		RadixSinkFixture fixture("128MB");
		fixture.Keys(0, STANDARD_VECTOR_SIZE);
		fixture.Sink();
		auto &local = fixture.Local();
		local.ht->Resize(1048576);
		fixture.radix->ResetLocalSinkState(fixture.context, *fixture.global, *fixture.locals[0]);
		fixture.Sink();
		const auto old_capacity = local.ht->Capacity();
		REQUIRE(old_capacity < fixture.Global().config.sink_capacity);
		const auto retained_size = local.ht->GetSizeInBytes();
		const auto old_materialized = local.ht->GetMaterializedCount();
		REQUIRE(retained_size >= 1048576 * sizeof(ht_entry_t));
		{
			AllocatedData competing_allocation;
			if (memory_pressure) {
				auto &manager = BufferManager::GetBufferManager(*fixture.con.context);
				competing_allocation =
				    manager.GetBufferAllocator().Allocate(manager.GetMaxMemory() - manager.GetUsedMemory() - 65536);
			}
			REQUIRE(fixture.Global().config.SetRadixBitsToExternal());
			REQUIRE_NOTHROW(fixture.Sink());
			REQUIRE(local.spilling);
			REQUIRE_FALSE(local.ht->HLLEnabled());
			REQUIRE(local.ht->GetMaterializedCount() == old_materialized + STANDARD_VECTOR_SIZE);
			REQUIRE(local.local_sink_capacity == local.ht->Capacity());
			if (memory_pressure) {
				REQUIRE(local.ht->Capacity() == old_capacity);
				REQUIRE(local.ht->GetSizeInBytes() >= retained_size);
			} else {
				REQUIRE(local.ht->Capacity() == fixture.Global().config.sink_capacity);
				REQUIRE(local.ht->GetSizeInBytes() < retained_size);
			}
		}
		fixture.Sink();
		fixture.Combine(0);
		REQUIRE(fixture.ScanGroupCount() == STANDARD_VECTOR_SIZE);
	}
}

TEST_CASE("Aggregate memory accounting tracks hits and replacement tuple storage", "[radix_ht_sink]") {
	RadixSinkFixture fixture("128MB", true);
	fixture.Keys(0, STANDARD_VECTOR_SIZE, 800);
	fixture.Sink();
	auto &ht = *fixture.Local().ht;
	const auto wide_size = ht.GetSizeInBytes();
	const auto materialized = ht.GetMaterializedCount();
	const auto old_arena_size = ht.GetAggregateAllocator()->AllocationSize();
	fixture.SetPayload(Value(string(2048, 'p')));
	// Updating existing groups can grow aggregate states without allocating tuple blocks.
	ht.AddChunk(fixture.chunk, fixture.payload, fixture.filter);
	REQUIRE(ht.GetMaterializedCount() == materialized);
	const auto arena_growth = ht.GetAggregateAllocator()->AllocationSize() - old_arena_size;
	REQUIRE(arena_growth > 0);
	REQUIRE(ht.GetSizeInBytes() == wide_size + arena_growth);

	fixture.SetPayload(Value(LogicalType::VARCHAR));
	ht.ResetForNewIteration(ht.GetRadixBits());
	fixture.Keys(0, STANDARD_VECTOR_SIZE);
	ht.AddChunk(fixture.chunk, fixture.payload, fixture.filter);
	REQUIRE(ht.GetMaterializedCount() == materialized);
	const auto narrow_size = ht.GetSizeInBytes();
	REQUIRE(narrow_size < wide_size);

	ht.Abandon();
	ht.SetRadixBits(ht.GetRadixBits() + 1);
	ht.Repartition();
	REQUIRE(ht.GetMaterializedCount() == materialized);
	REQUIRE(ht.GetSizeInBytes() > narrow_size);
	const auto repartitioned_size = ht.GetSizeInBytes();
	auto old_data = ht.AcquirePartitionedData();
	REQUIRE(ht.GetSizeInBytes() < repartitioned_size);
	ht.AddChunk(fixture.chunk, fixture.payload, fixture.filter);
	REQUIRE(ht.GetMaterializedCount() == materialized);
	REQUIRE(ht.GetSizeInBytes() >= narrow_size);
}

TEST_CASE("Failed adaptive growth preserves rows and falls back to the existing table", "[radix_ht_sink]") {
	RadixSinkFixture fixture("128MB");
	fixture.Keys(0, 65536);
	fixture.Sink();
	auto &local = fixture.Local();
	auto &global = fixture.Global();
	// Materialize repeated groups without attempting the optional growth yet.
	for (idx_t offset = STANDARD_VECTOR_SIZE; offset < 3 * 65536; offset += STANDARD_VECTOR_SIZE) {
		fixture.Keys(offset, 65536);
		if (local.ht->Count() + STANDARD_VECTOR_SIZE >= local.ht->ResizeThreshold()) {
			local.ht->Abandon();
		}
		local.ht->AddChunk(fixture.chunk, fixture.payload, fixture.filter);
	}
	global.temporary_memory_state->SetMinimumReservation(64 * 1024 * 1024);
	global.temporary_memory_state->SetRemainingSizeAndUpdateReservation(*fixture.con.context, 64 * 1024 * 1024);
	REQUIRE_FALSE(local.has_grown);
	REQUIRE(local.ht->HLLEnabled());
	const auto old_capacity = local.ht->Capacity();
	const auto old_materialized = local.ht->GetMaterializedCount();
	auto &manager = BufferManager::GetBufferManager(*fixture.con.context);
	{
		auto competing_allocation =
		    manager.GetBufferAllocator().Allocate(manager.GetMaxMemory() - manager.GetUsedMemory() - 65536);
		REQUIRE_FALSE(TryGrowSinkHashTable(global, local));
		REQUIRE_FALSE(local.ht->HLLEnabled());
		REQUIRE(local.ht->Capacity() == old_capacity);
		REQUIRE(local.local_sink_capacity == old_capacity);
		REQUIRE(local.ht->GetMaterializedCount() == old_materialized);
		REQUIRE(local.ht->Count() == 0);
	}
	for (idx_t offset = 3 * 65536; offset < 262144; offset += STANDARD_VECTOR_SIZE) {
		fixture.Keys(offset, 65536);
		fixture.Sink();
	}
	fixture.Combine(0);
	REQUIRE(fixture.ScanGroupCount() == 65536);
}

} // namespace duckdb
