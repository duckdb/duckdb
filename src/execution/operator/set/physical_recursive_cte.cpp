#include "duckdb/execution/operator/set/physical_recursive_cte_state.hpp"

#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/main/settings.hpp"

namespace duckdb {

RecursiveCTEPartialKeySpec::RecursiveCTEPartialKeySpec(vector<idx_t> indices_p, idx_t full_key_count)
    : indices(std::move(indices_p)) {
	if (indices.empty() || indices.size() >= full_key_count || !std::is_sorted(indices.begin(), indices.end()) ||
	    std::adjacent_find(indices.begin(), indices.end()) != indices.end() || indices.back() >= full_key_count) {
		throw InternalException("Invalid USING KEY partial-key index specification");
	}
}

struct RecursiveCTEDistinctPartition {
	RecursiveCTEDistinctPartition(ClientContext &context, const vector<LogicalType> &types)
	    : ht(context, BufferAllocator::Get(context), types) {
	}

	mutex lock;
	GroupedAggregateHashTable ht;
};

PhysicalRecursiveCTE::PhysicalRecursiveCTE(PhysicalPlan &physical_plan, Identifier ctename, TableIndex table_index,
                                           vector<LogicalType> types, bool union_all, PhysicalOperator &top,
                                           PhysicalOperator &bottom, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::RECURSIVE_CTE, std::move(types), estimated_cardinality),
      ctename(std::move(ctename)), table_index(table_index), union_all(union_all),
      shared_executor_pool(make_shared_ptr<RecursiveExecutorPool>()) {
	children.push_back(top);
	children.push_back(bottom);
}

PhysicalRecursiveCTE::~PhysicalRecursiveCTE() {
}

idx_t PhysicalRecursiveCTE::NextMetricsInvocation() const {
	return metrics_invocations.fetch_add(1) + 1;
}

//===--------------------------------------------------------------------===//
// Sink State
//===--------------------------------------------------------------------===//
RecursiveCTEState::RecursiveCTEState(ClientContext &context, const PhysicalRecursiveCTE &op)
    : op(op), executor(context), new_group_addresses(LogicalType::POINTER), new_groups(STANDARD_VECTOR_SIZE),
      allow_executor_reuse(Settings::Get<EnableCachingOperatorsSetting>(context)), metrics(context, op),
      scheduler(op.shared_executor_pool, allow_executor_reuse),
      intermediate_table(context, op.using_key ? op.internal_types : op.GetTypes()) {
	if (metrics.Enabled()) {
		epoch_metrics = make_uniq<RecursiveCTEEpochMetrics>();
	}
	vector<LogicalType> aggr_input_types;
	vector<AggregateObject> payload_aggregates;
	for (idx_t i = 0; i < op.payload_aggregates.size(); i++) {
		D_ASSERT(op.payload_aggregates[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
		auto &bound_aggr_expr = op.payload_aggregates[i]->Cast<BoundAggregateExpression>();
		for (auto &child_expr : bound_aggr_expr.GetChildren()) {
			executor.AddExpression(*child_expr);
			aggr_input_types.push_back(child_expr->GetReturnType());
		}
		payload_aggregates.emplace_back(bound_aggr_expr);
	}

	payload_rows.Initialize(Allocator::Get(context), aggr_input_types);

	if (op.using_key) {
		ht = make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), op.distinct_types,
		                                          op.payload_types, std::move(payload_aggregates));
		for (auto &spec : op.partial_key_index_specs) {
			partial_key_indexes.push_back(
			    make_uniq<RecursiveCTEPartialKeyIndex>(Allocator::Get(context), op.distinct_types, spec.Indices()));
		}
	} else if (!op.union_all) {
		ht = make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), op.distinct_types);
	}
	if (op.using_key) {
		distinct_rows.Initialize(Allocator::DefaultAllocator(), op.distinct_types);
		update_rows.Initialize(Allocator::DefaultAllocator(), op.internal_types);
		source_distinct_rows.Initialize(Allocator::DefaultAllocator(), op.distinct_types);
		source_payload_rows.Initialize(Allocator::DefaultAllocator(), op.payload_types);
	}
	source_result.Initialize(Allocator::DefaultAllocator(), op.GetTypes());
	if (op.using_key) {
		InitializeIntermediateAppend();
		op.working_table->InitializeAppend(working_append_state);
	}
	if (op.recurring_table) {
		op.recurring_table->InitializeAppend(recurring_append_state);
	}
}

RecursiveCTEState::~RecursiveCTEState() {
	metrics.Log(partial_key_indexes);
	if (epoch_metrics) {
		metrics.LogEpochSummary(*epoch_metrics);
	}
}

const RecursiveCTEPartialKeyIndex &RecursiveCTEState::GetPartialKeyIndex(const vector<idx_t> &key_indices) const {
	for (auto &index : partial_key_indexes) {
		if (index->key_indices == key_indices) {
			return *index;
		}
	}
	throw InternalException("USING KEY partial-key index is missing");
}

void RecursiveCTEState::RecordSinkMetrics(idx_t wait_ns, idx_t work_ns, idx_t rows) {
	metrics.RecordSink(wait_ns, work_ns, rows);
}

void RecursiveCTEState::AppendOutput(DataChunk &chunk) {
	const auto collect_metrics = metrics.Enabled();
	const auto before_lock =
	    collect_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	lock_guard<mutex> guard(intermediate_table_lock);
	const auto after_lock =
	    collect_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	// USING KEY collects updates without mutating the hash state read by recurring.T in this epoch.
	CurrentOutputTable().Append(CurrentOutputAppendState(), chunk);
	if (collect_metrics) {
		const auto after_work = std::chrono::steady_clock::now();
		RecordSinkMetrics(
		    NumericCast<idx_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(after_lock - before_lock).count()),
		    NumericCast<idx_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(after_work - after_lock).count()),
		    chunk.size());
	}
}

void RecursiveCTEState::CombineOutput(ColumnDataCollection &output) {
	const auto collect_metrics = metrics.Enabled();
	const auto before_lock =
	    collect_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	lock_guard<mutex> guard(intermediate_table_lock);
	const auto after_lock =
	    collect_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	const auto row_count = output.Count();
	CurrentOutputTable().Combine(output);
	if (collect_metrics) {
		const auto after_work = std::chrono::steady_clock::now();
		RecordSinkMetrics(
		    NumericCast<idx_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(after_lock - before_lock).count()),
		    NumericCast<idx_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(after_work - after_lock).count()),
		    row_count);
	}
}

void RecursiveCTEState::FinalizePayload(RowOperationsState &row_state, Vector &addresses, DataChunk &payload,
                                        idx_t payload_idx) {
	lock_guard<mutex> guard(ht_finalize_lock);
	RowOperations::FinalizeStates(row_state, *GetHashTable().GetLayoutPtr(), addresses, payload, payload_idx);
}

void RecursiveCTEState::InitializeIntermediateAppend() {
	intermediate_table.InitializeAppend(intermediate_append_state);
}

void RecursiveCTEState::InitializeSharedOutputAppend() {
	CurrentOutputTable().InitializeAppend(CurrentOutputAppendState());
}

ColumnDataCollection &RecursiveCTEState::CurrentOutputTable() {
	if (op.using_key || !output_is_working) {
		return intermediate_table;
	}
	D_ASSERT(op.working_table);
	return *op.working_table;
}

ColumnDataCollection &RecursiveCTEState::CurrentInputTable() {
	if (op.using_key) {
		D_ASSERT(op.working_table);
		return *op.working_table;
	}
	if (output_is_working) {
		return intermediate_table;
	}
	D_ASSERT(op.working_table);
	return *op.working_table;
}

const ColumnDataCollection &RecursiveCTEState::CurrentInputTable() const {
	if (op.using_key) {
		D_ASSERT(op.working_table);
		return *op.working_table;
	}
	if (output_is_working) {
		return intermediate_table;
	}
	D_ASSERT(op.working_table);
	return *op.working_table;
}

ColumnDataAppendState &RecursiveCTEState::CurrentOutputAppendState() {
	if (op.using_key || !output_is_working) {
		return intermediate_append_state;
	}
	return working_append_state;
}

void RecursiveCTEState::AdvanceIterationBuffers() {
	if (!op.using_key) {
		output_is_working = !output_is_working;
	}
}

void RecursiveCTEState::ResetCurrentOutputTableForReuse() {
	auto &output = CurrentOutputTable();
	output.ResetForReuse();
	if (!op.using_key) {
		return;
	}
	InitializeIntermediateAppend();
}

void RecursiveCTEState::RebindRecursiveScans() {
	if (op.using_key) {
		return;
	}
	auto &input_table = CurrentInputTable();
	for (auto &scan_ref : op.recursive_scans) {
		auto &scan = scan_ref.get();
		scan.collection = input_table;
	}
}

unique_ptr<GlobalSinkState> PhysicalRecursiveCTE::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<RecursiveCTEState>(context, *this);
}

class RecursiveCTELocalState : public LocalSinkState {
public:
	RecursiveCTELocalState(ClientContext &context, const PhysicalRecursiveCTE &op)
	    : hashes(LogicalType::HASH), partition_hashes(LogicalType::HASH), dummy_addresses(LogicalType::POINTER),
	      new_groups(STANDARD_VECTOR_SIZE) {
		if (!op.using_key) {
			output = make_uniq<ColumnDataCollection>(context, op.GetTypes());
			output->InitializeAppend(append_state);
		}
		if (!op.using_key && !op.union_all) {
			partition_chunk.Initialize(Allocator::Get(context), op.GetTypes());
		}
	}

	unique_ptr<ColumnDataCollection> output;
	ColumnDataAppendState append_state;
	Vector hashes;
	Vector partition_hashes;
	Vector dummy_addresses;
	SelectionVector new_groups;
	DataChunk partition_chunk;
	vector<SelectionVector> partition_selections;
	vector<idx_t> partition_counts;

	void InitializePartitions(idx_t partition_count) {
		if (partition_selections.size() == partition_count) {
			return;
		}
		partition_selections.clear();
		partition_selections.reserve(partition_count);
		for (idx_t partition_idx = 0; partition_idx < partition_count; partition_idx++) {
			partition_selections.emplace_back(STANDARD_VECTOR_SIZE);
		}
		partition_counts.resize(partition_count);
	}

	bool SupportsReuse() const override {
		return true;
	}

	void Reset(ExecutionContext &context, GlobalSinkState &gstate) override {
		if (!output) {
			return;
		}
		auto &recursive_state = gstate.Cast<RecursiveCTEState>();
		if (recursive_state.GetOperator().union_all && !recursive_state.UsesLocalUnionAllOutput()) {
			return;
		}
		output->ResetForReuse();
		output->InitializeAppend(append_state);
	}
};

unique_ptr<LocalSinkState> PhysicalRecursiveCTE::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<RecursiveCTELocalState>(context.client, *this);
}

void RecursiveCTEState::SinkSerialDistinct(DataChunk &chunk, RecursiveCTELocalState &lstate) {
	D_ASSERT(ht);
	const auto collect_metrics = metrics.Enabled();
	const auto candidate_count = chunk.size();
	const auto before_lock =
	    collect_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	idx_t new_group_count;
	{
		lock_guard<mutex> guard(intermediate_table_lock);
		const auto after_lock =
		    collect_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		if (collect_metrics) {
			metrics.RecordHashRows(candidate_count);
		}
		new_group_count = ht->FindOrCreateGroups(chunk, lstate.dummy_addresses, lstate.new_groups);
		chunk.Slice(lstate.new_groups, new_group_count);
		if (collect_metrics) {
			const auto after_work = std::chrono::steady_clock::now();
			GetEpochMetrics().RecordDistinctGrouping(
			    candidate_count, new_group_count,
			    NumericCast<idx_t>(
			        std::chrono::duration_cast<std::chrono::nanoseconds>(after_work - after_lock).count()));
			RecordSinkMetrics(
			    NumericCast<idx_t>(
			        std::chrono::duration_cast<std::chrono::nanoseconds>(after_lock - before_lock).count()),
			    NumericCast<idx_t>(
			        std::chrono::duration_cast<std::chrono::nanoseconds>(after_work - after_lock).count()),
			    candidate_count);
		}
	}
	if (new_group_count > 0) {
		lstate.output->Append(lstate.append_state, chunk);
	}
}

void RecursiveCTEState::SinkDistinct(DataChunk &chunk, RecursiveCTELocalState &lstate, bool emit_rows,
                                     bool record_sink_metrics) {
	auto &partitions = distinct_partitions;
	D_ASSERT(!partitions.empty());
	D_ASSERT((partitions.size() & (partitions.size() - 1)) == 0);
	lstate.InitializePartitions(partitions.size());
	std::fill(lstate.partition_counts.begin(), lstate.partition_counts.end(), 0);

	chunk.Hash(lstate.hashes);
	auto hash_data = FlatVector::GetData<hash_t>(lstate.hashes);
	const auto partition_mask = partitions.size() - 1;
	for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
		const auto partition_idx = hash_data[row_idx] & partition_mask;
		auto &partition_count = lstate.partition_counts[partition_idx];
		lstate.partition_selections[partition_idx].set_index(partition_count++, row_idx);
	}

	for (idx_t partition_idx = 0; partition_idx < partitions.size(); partition_idx++) {
		const auto partition_count = lstate.partition_counts[partition_idx];
		if (partition_count == 0) {
			continue;
		}
		lstate.partition_chunk.Reset();
		lstate.partition_chunk.Slice(chunk, lstate.partition_selections[partition_idx], partition_count);
		lstate.partition_hashes.Slice(lstate.hashes, lstate.partition_selections[partition_idx], partition_count);
		auto &partition = *partitions[partition_idx];
		const auto collect_sink_metrics = metrics.Enabled() && record_sink_metrics;
		const auto before_lock =
		    collect_sink_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		idx_t new_group_count;
		{
			lock_guard<mutex> guard(partition.lock);
			const auto after_lock =
			    collect_sink_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
			if (metrics.Enabled()) {
				metrics.RecordHashRows(partition_count);
			}
			new_group_count = partition.ht.FindOrCreateGroups(lstate.partition_chunk, lstate.partition_hashes,
			                                                  lstate.dummy_addresses, lstate.new_groups);
			lstate.partition_chunk.Slice(lstate.new_groups, new_group_count);
			if (collect_sink_metrics) {
				const auto after_work = std::chrono::steady_clock::now();
				GetEpochMetrics().RecordDistinctGrouping(
				    partition_count, new_group_count,
				    NumericCast<idx_t>(
				        std::chrono::duration_cast<std::chrono::nanoseconds>(after_work - after_lock).count()));
				RecordSinkMetrics(
				    NumericCast<idx_t>(
				        std::chrono::duration_cast<std::chrono::nanoseconds>(after_lock - before_lock).count()),
				    NumericCast<idx_t>(
				        std::chrono::duration_cast<std::chrono::nanoseconds>(after_work - after_lock).count()),
				    partition_count);
			}
		}
		if (emit_rows && new_group_count > 0) {
			lstate.output->Append(lstate.append_state, lstate.partition_chunk);
		}
	}
}

void RecursiveCTEState::PromoteDistinctState(ClientContext &context, idx_t partition_count) {
	D_ASSERT(!op.using_key && !op.union_all);
	if (!distinct_partitions.empty() || partition_count <= 1) {
		return;
	}
	D_ASSERT(ht);
	const auto migrated_rows = ht->Count();
	const auto promotion_start =
	    metrics.Enabled() ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	distinct_partitions.reserve(partition_count);
	for (idx_t partition_idx = 0; partition_idx < partition_count; partition_idx++) {
		distinct_partitions.push_back(make_uniq<RecursiveCTEDistinctPartition>(context, op.distinct_types));
	}

	RecursiveCTELocalState migration_state(context, op);
	DataChunk groups;
	groups.Initialize(Allocator::Get(context), op.distinct_types);
	DataChunk payload;
	AggregateHTScanState scan_state;
	ht->InitializeScan(scan_state);
	while (ht->Scan(scan_state, groups, payload)) {
		context.InterruptCheck();
		if (groups.size() > 0) {
			SinkDistinct(groups, migration_state, false, false);
		}
	}
	ht.reset();
	if (metrics.Enabled()) {
		const auto promotion_end = std::chrono::steady_clock::now();
		const auto elapsed_us = NumericCast<idx_t>(
		    std::chrono::duration_cast<std::chrono::microseconds>(promotion_end - promotion_start).count());
		metrics.LogDistinctPromotion(partition_count, migrated_rows, elapsed_us);
	}
}

static void GatherChunk(DataChunk &output_chunk, DataChunk &input_chunk, const vector<idx_t> &idx_set) {
	idx_t chunk_index = 0;
	for (auto &group_idx : idx_set) {
		output_chunk.data[chunk_index++].Reference(input_chunk.data[group_idx]);
	}
}

static void ScatterChunk(DataChunk &output_chunk, DataChunk &input_chunk, const vector<idx_t> &idx_set) {
	idx_t chunk_index = 0;
	for (auto &group_idx : idx_set) {
		output_chunk.data[group_idx].Reference(input_chunk.data[chunk_index++]);
	}
}

template <bool COLLECT_METRICS>
void RecursiveCTEState::CommitUsingKeyUpdatesInternal() {
	D_ASSERT(op.using_key);
	ColumnDataScanState update_scan_state;
	intermediate_table.InitializeScan(update_scan_state);
	while (intermediate_table.Scan(update_scan_state, update_rows)) {
		if constexpr (COLLECT_METRICS) {
			metrics.RecordHashRows(update_rows.size());
		}
		const auto hash_start =
		    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		distinct_rows.Reset();
		GatherChunk(distinct_rows, update_rows, op.distinct_idx);
		if (!executor.expressions.empty()) {
			payload_rows.Reset();
			executor.Execute(update_rows, payload_rows);
		}
		if (partial_key_indexes.empty()) {
			ht->AddChunk(distinct_rows, payload_rows, AggregateType::NON_DISTINCT);
			if constexpr (COLLECT_METRICS) {
				const auto hash_end = std::chrono::steady_clock::now();
				GetEpochMetrics().RecordKeyedHashCommit(NumericCast<idx_t>(
				    std::chrono::duration_cast<std::chrono::nanoseconds>(hash_end - hash_start).count()));
			}
			continue;
		}
		const auto new_group_count = ht->AddChunkAndGetNewGroups(
		    distinct_rows, payload_rows, AggregateType::NON_DISTINCT, new_group_addresses, new_groups);
		if constexpr (COLLECT_METRICS) {
			const auto hash_end = std::chrono::steady_clock::now();
			GetEpochMetrics().RecordKeyedHashCommit(NumericCast<idx_t>(
			    std::chrono::duration_cast<std::chrono::nanoseconds>(hash_end - hash_start).count()));
		}
		const auto index_start =
		    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		for (auto &index : partial_key_indexes) {
			index->AddGroups(distinct_rows, new_groups, new_group_addresses, new_group_count);
		}
		if constexpr (COLLECT_METRICS) {
			const auto index_end = std::chrono::steady_clock::now();
			const auto elapsed_ns = NumericCast<idx_t>(
			    std::chrono::duration_cast<std::chrono::nanoseconds>(index_end - index_start).count());
			GetEpochMetrics().RecordPartialIndexMaintenance(elapsed_ns);
			metrics.RecordPartialIndexBuild(NumericCast<idx_t>(
			    std::chrono::duration_cast<std::chrono::microseconds>(index_end - index_start).count()));
		}
	}
}

void RecursiveCTEState::CommitUsingKeyUpdates() {
	if (metrics.Enabled()) {
		CommitUsingKeyUpdatesInternal<true>();
	} else {
		CommitUsingKeyUpdatesInternal<false>();
	}
}

class RecursiveCTEStateScanGlobalState : public GlobalSourceState {
public:
	mutex lock;
	AggregateHTScanState scan_state;
	bool initialized = false;
};

class RecursiveCTEStateScanLocalState : public LocalSourceState {
public:
	RecursiveCTEStateScanLocalState(ClientContext &context, const PhysicalRecursiveCTE &op)
	    : found_groups(STANDARD_VECTOR_SIZE), arena(Allocator::Get(context)), row_state(arena) {
		distinct_rows.Initialize(Allocator::Get(context), op.distinct_types);
		payload_rows.Initialize(Allocator::Get(context), op.payload_types);
	}

	DataChunk distinct_rows;
	DataChunk payload_rows;
	AggregateHTLookupState lookup_state;
	SelectionVector found_groups;
	ArenaAllocator arena;
	RowOperationsState row_state;
};

PhysicalRecursiveCTEStateScan::PhysicalRecursiveCTEStateScan(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                                             idx_t estimated_cardinality, TableIndex cte_index)
    : PhysicalColumnDataScan(physical_plan, std::move(types), PhysicalOperatorType::RECURSIVE_RECURRING_CTE_SCAN,
                             estimated_cardinality, cte_index) {
}

unique_ptr<GlobalSourceState> PhysicalRecursiveCTEStateScan::GetGlobalSourceState(ClientContext &context) const {
	return GetGlobalSourceState(context, OperatorPartitionInfo::NoPartitionInfo());
}

unique_ptr<GlobalSourceState>
PhysicalRecursiveCTEStateScan::GetGlobalSourceState(ClientContext &context,
                                                    const OperatorPartitionInfo &partition_info) const {
	(void)partition_info;
	return make_uniq<RecursiveCTEStateScanGlobalState>();
}

unique_ptr<LocalSourceState> PhysicalRecursiveCTEStateScan::GetLocalSourceState(ExecutionContext &context,
                                                                                GlobalSourceState &gstate) const {
	if (!recursive_cte) {
		throw InternalException("USING KEY state scan is not linked to its recursive CTE");
	}
	return make_uniq<RecursiveCTEStateScanLocalState>(context.client, *recursive_cte);
}

SourceResultType PhysicalRecursiveCTEStateScan::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                                OperatorSourceInput &input) const {
	if (!recursive_cte || !recursive_cte->sink_state) {
		throw InternalException("USING KEY state scan has no recursive state");
	}
	auto &recursive_state = recursive_cte->sink_state->Cast<RecursiveCTEState>();
	if (!recursive_state.GetMetrics().Enabled()) {
		return GetDataFromState(chunk, input, recursive_state);
	}
	const auto scan_start = std::chrono::steady_clock::now();
	const auto result = GetDataFromState(chunk, input, recursive_state);
	const auto scan_end = std::chrono::steady_clock::now();
	recursive_state.GetEpochMetrics().RecordRecurringScan(
	    NumericCast<idx_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(scan_end - scan_start).count()));
	return result;
}

SourceResultType PhysicalRecursiveCTEStateScan::GetDataFromState(DataChunk &chunk, OperatorSourceInput &input,
                                                                 RecursiveCTEState &recursive_state) const {
	auto &gstate = input.global_state.Cast<RecursiveCTEStateScanGlobalState>();
	auto &lstate = input.local_state.Cast<RecursiveCTEStateScanLocalState>();
	while (true) {
		{
			lock_guard<mutex> guard(gstate.lock);
			if (!gstate.initialized) {
				recursive_state.GetHashTable().InitializeScan(gstate.scan_state);
				gstate.initialized = true;
			}
			if (!recursive_state.GetHashTable().ScanGroups(gstate.scan_state, lstate.distinct_rows)) {
				return SourceResultType::FINISHED;
			}
		}
		if (lstate.distinct_rows.size() == 0) {
			continue;
		}
		const auto group_count = lstate.distinct_rows.size();
		const auto found_count =
		    recursive_state.GetHashTable().LookupGroups(lstate.distinct_rows, lstate.lookup_state, lstate.found_groups);
		if (found_count != group_count) {
			throw InternalException("USING KEY state scan could not find %d of %d frozen groups",
			                        group_count - found_count, group_count);
		}
		lstate.payload_rows.Reset();
		lstate.payload_rows.SetChildCardinality(group_count);
		if (lstate.payload_rows.ColumnCount() > 0) {
			recursive_state.FinalizePayload(lstate.row_state, lstate.lookup_state.addresses, lstate.payload_rows, 0);
		}
		ScatterChunk(chunk, lstate.distinct_rows, distinct_idx);
		ScatterChunk(chunk, lstate.payload_rows, payload_idx);
		chunk.CheckCardinality(lstate.distinct_rows.size());
		if (recursive_state.GetMetrics().Enabled()) {
			recursive_state.GetMetrics().RecordRecurringScanRows(chunk.size());
		}
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
}

InsertionOrderPreservingMap<string> PhysicalRecursiveCTEStateScan::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Index"] = StringUtil::Format("%llu", cte_index.index);
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

SinkResultType PhysicalRecursiveCTE::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<RecursiveCTEState>();

	if (!using_key && union_all) {
		if (!gstate.UsesLocalUnionAllOutput()) {
			gstate.AppendOutput(chunk);
			return SinkResultType::NEED_MORE_INPUT;
		}
		auto &lstate = input.local_state.Cast<RecursiveCTELocalState>();
		D_ASSERT(lstate.output);
		lstate.output->Append(lstate.append_state, chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}
	if (!using_key) {
		auto &lstate = input.local_state.Cast<RecursiveCTELocalState>();
		D_ASSERT(lstate.output);
		if (!gstate.HasDistinctPartitions()) {
			gstate.SinkSerialDistinct(chunk, lstate);
		} else {
			gstate.SinkDistinct(chunk, lstate);
		}
		return SinkResultType::NEED_MORE_INPUT;
	}

	gstate.AppendOutput(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalRecursiveCTE::PrepareFinalize(ClientContext &context, GlobalSinkState &sink_state) const {
	if (using_key) {
		sink_state.Cast<RecursiveCTEState>().CommitUsingKeyUpdates();
	}
}

SinkCombineResultType PhysicalRecursiveCTE::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	if (!using_key) {
		auto &gstate = input.global_state.Cast<RecursiveCTEState>();
		if (union_all && !gstate.UsesLocalUnionAllOutput()) {
			return SinkCombineResultType::FINISHED;
		}
		auto &lstate = input.local_state.Cast<RecursiveCTELocalState>();
		D_ASSERT(lstate.output);
		gstate.CombineOutput(*lstate.output);
	}
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalRecursiveCTE::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<RecursiveCTEState>();
	return gstate.GetData(context, chunk);
}

SourceResultType RecursiveCTEState::GetData(ExecutionContext &context, DataChunk &chunk) {
	if (source_phase == RecursiveCTESourcePhase::INITIAL) {
		if (op.using_key) {
			source_phase = RecursiveCTESourcePhase::RECURSING_KEY;
		} else {
			CurrentOutputTable().InitializeScan(scan_state);
			source_phase = RecursiveCTESourcePhase::SCANNING_UNION;
		}
	}
	return op.using_key ? GetUsingKeyData(context, chunk) : GetUnionData(context, chunk);
}

SourceResultType RecursiveCTEState::GetUsingKeyData(ExecutionContext &context, DataChunk &chunk) {
	if (metrics.Enabled()) {
		return GetUsingKeyDataInternal<true>(context, chunk);
	}
	return GetUsingKeyDataInternal<false>(context, chunk);
}

template <bool COLLECT_METRICS>
SourceResultType RecursiveCTEState::GetUsingKeyDataInternal(ExecutionContext &context, DataChunk &chunk) {
	D_ASSERT(op.using_key);
	while (true) {
		switch (source_phase) {
		case RecursiveCTESourcePhase::RECURSING_KEY: {
			const auto expected_new = intermediate_table.Count();
			op.working_table->Reset();
			op.working_table->Combine(intermediate_table);
			InitializeIntermediateAppend();

			if (expected_new > 0) {
				const auto desired_capacity =
				    GroupedAggregateHashTable::GetCapacityForCount(ht->Count() + expected_new);
				if (desired_capacity > ht->Capacity()) {
					ht->Resize(desired_capacity);
				}
			}

			op.ExecuteRecursivePipelines(context);
			if (intermediate_table.Count() == 0) {
				ht->InitializeScan(ht_scan_state);
				source_phase = RecursiveCTESourcePhase::DRAINING_FINAL_KEY_STATE;
			}
			break;
		}
		case RecursiveCTESourcePhase::DRAINING_FINAL_KEY_STATE: {
			const auto drain_start =
			    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
			while (ht->Scan(ht_scan_state, source_distinct_rows, source_payload_rows)) {
				if (source_distinct_rows.size() == 0) {
					continue;
				}
				ScatterChunk(chunk, source_distinct_rows, op.distinct_idx);
				ScatterChunk(chunk, source_payload_rows, op.payload_idx);
				chunk.CheckCardinality(source_distinct_rows.size());
				if constexpr (COLLECT_METRICS) {
					metrics.RecordFinalStateRows(chunk.size());
					const auto drain_end = std::chrono::steady_clock::now();
					GetEpochMetrics().RecordFinalStateDrain(NumericCast<idx_t>(
					    std::chrono::duration_cast<std::chrono::nanoseconds>(drain_end - drain_start).count()));
				}
				return SourceResultType::HAVE_MORE_OUTPUT;
			}
			if constexpr (COLLECT_METRICS) {
				const auto drain_end = std::chrono::steady_clock::now();
				GetEpochMetrics().RecordFinalStateDrain(NumericCast<idx_t>(
				    std::chrono::duration_cast<std::chrono::nanoseconds>(drain_end - drain_start).count()));
			}
			source_phase = RecursiveCTESourcePhase::FINISHED;
			break;
		}
		case RecursiveCTESourcePhase::FINISHED:
			return SourceResultType::FINISHED;
		default:
			throw InternalException("Unsupported recursive CTE key source phase");
		}
	}
}

SourceResultType RecursiveCTEState::GetUnionData(ExecutionContext &context, DataChunk &chunk) {
	D_ASSERT(!op.using_key);
	while (chunk.size() == 0) {
		if (source_phase == RecursiveCTESourcePhase::SCANNING_UNION) {
			// scan any chunks we have collected so far
			CurrentOutputTable().Scan(scan_state, chunk);
			if (chunk.size() != 0) {
				break;
			}
		} else if (source_phase == RecursiveCTESourcePhase::FINISHED) {
			break;
		} else {
			throw InternalException("Unsupported recursive CTE union source phase");
		}

		if (chunk.size() == 0) {
			// we have run out of chunks
			// now we need to recurse
			// we set up the working table as the data we gathered in this iteration of the recursion
			auto &current_output = CurrentOutputTable();

			// After an iteration, we reset the recurring table
			// and fill it up with the new hash table rows for the next iteration.
			if (op.ref_recurring && current_output.Count() != 0) {
				// we need to populate the recurring table from the intermediate table
				// careful: we can not just use Combine here, because this destroys the intermediate table
				// instead we need to scan and append to create a copy
				// Note: as we are in the "normal" recursion case here, not the USING KEY case,
				// we can just scan the intermediate table directly, instead of going through the HT
				ColumnDataScanState recurring_scan_state;
				current_output.InitializeScan(recurring_scan_state);
				while (current_output.Scan(recurring_scan_state, source_result)) {
					op.recurring_table->Append(recurring_append_state, source_result);
				}
			}

			AdvanceIterationBuffers();
			ResetCurrentOutputTableForReuse();
			RebindRecursiveScans();

			// Pre-grow the dedup HT to avoid costly Resize + ReinsertTuples during the next Sink phase.
			// current_output.Count() is the count of rows output in the previous iteration — an upper bound
			// on the number of new unique rows the next iteration can add (since the recursion is converging).
			if (!op.union_all) {
				const idx_t expected_new = current_output.Count();
				if (expected_new > 0) {
					if (distinct_partitions.empty()) {
						const idx_t desired_capacity =
						    GroupedAggregateHashTable::GetCapacityForCount(ht->Count() + expected_new);
						if (desired_capacity > ht->Capacity()) {
							ht->Resize(desired_capacity);
						}
					} else {
						const auto expected_per_partition =
						    (expected_new + distinct_partitions.size() - 1) / distinct_partitions.size();
						for (auto &partition : distinct_partitions) {
							const auto desired_capacity = GroupedAggregateHashTable::GetCapacityForCount(
							    partition->ht.Count() + expected_per_partition);
							if (desired_capacity > partition->ht.Capacity()) {
								partition->ht.Resize(desired_capacity);
							}
						}
					}
				}
			}

			// now we need to re-execute all of the pipelines that depend on the recursion
			op.ExecuteRecursivePipelines(context);

			// check if we obtained any results
			// if not, we are done
			if (CurrentOutputTable().Count() == 0) {
				source_phase = RecursiveCTESourcePhase::FINISHED;
				break;
			}
			// set up the scan again
			CurrentOutputTable().InitializeScan(scan_state);
		}
	}

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

vector<const_reference<PhysicalOperator>> PhysicalRecursiveCTE::GetSources() const {
	return {*this};
}

InsertionOrderPreservingMap<string> PhysicalRecursiveCTE::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Name"] = ctename.GetIdentifierName();
	result["Table Index"] = StringUtil::Format("%llu", table_index.index);
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
