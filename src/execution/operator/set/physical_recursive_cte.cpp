#include "duckdb/execution/operator/set/physical_recursive_cte_state.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte_delta.hpp"

#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
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
      intermediate_table(context, op.using_key ? op.internal_types : op.GetTypes()), context(context),
      preaggregation_hashes(LogicalType::HASH, nullptr, 0) {
	if (metrics.Enabled()) {
		epoch_metrics = make_uniq<RecursiveCTEEpochMetrics>();
	}
	vector<LogicalType> aggr_input_types;
	for (idx_t i = 0; i < op.payload_aggregates.size(); i++) {
		D_ASSERT(op.payload_aggregates[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
		auto &bound_aggr_expr = op.payload_aggregates[i]->Cast<BoundAggregateExpression>();
		for (auto &child_expr : bound_aggr_expr.GetChildren()) {
			executor.AddExpression(*child_expr);
			aggr_input_types.push_back(child_expr->GetReturnType());
		}
		payload_aggregate_objects.emplace_back(bound_aggr_expr);
	}
	if (!op.key_normalizers.empty()) {
		key_executor = make_uniq<ExpressionExecutor>(context);
		for (auto &normalizer : op.key_normalizers) {
			key_executor->AddExpression(*normalizer);
		}
	}

	payload_rows.Initialize(Allocator::Get(context), aggr_input_types);
	for (auto &comparison : op.payload_comparisons) {
		if (comparison) {
			payload_comparison_executors.push_back(make_uniq<ExpressionExecutor>(context, *comparison));
			has_payload_comparison_executors = true;
		} else {
			payload_comparison_executors.push_back(nullptr);
		}
	}

	if (op.using_key) {
		ht = CreateUsingKeyHashTable();
		for (auto &spec : op.partial_key_index_specs) {
			partial_key_indexes.push_back(
			    make_uniq<RecursiveCTEPartialKeyIndex>(Allocator::Get(context), op.hash_key_types, spec.Indices()));
		}
		if (!op.union_all) {
			can_preaggregate_using_key = true;
			for (idx_t payload_idx = 0; payload_idx < op.payload_types.size(); payload_idx++) {
				auto &aggregate = payload_aggregate_objects[payload_idx];
				if (!aggregate.function.HasStateCombineCallback() ||
				    aggregate.function.GetOrderDependent() == AggregateOrderDependent::ORDER_DEPENDENT) {
					can_preaggregate_using_key = false;
					break;
				}
			}
			if (can_preaggregate_using_key) {
				preaggregation_hashes.Initialize();
			}
			can_reuse_new_group_candidates = op.internal_types == op.GetTypes();
			for (idx_t payload_idx = 0; can_reuse_new_group_candidates && payload_idx < op.payload_types.size();
			     payload_idx++) {
				auto &aggregate = op.payload_aggregates[payload_idx]->Cast<BoundAggregateExpression>();
				auto &children = aggregate.GetChildren();
				can_reuse_new_group_candidates =
				    aggregate.Function().HasSingleValueIdentity() && !children.empty() &&
				    children[0]->GetExpressionClass() == ExpressionClass::BOUND_REF &&
				    children[0]->Cast<BoundReferenceExpression>().Index() == op.payload_idx[payload_idx];
			}
			can_reuse_changed_group_candidates =
			    can_reuse_new_group_candidates && op.key_normalizers.empty() && !has_payload_comparison_executors;
			for (auto &key_type : op.hash_key_types) {
				switch (key_type.InternalType()) {
				case PhysicalType::FLOAT:
				case PhysicalType::DOUBLE:
				case PhysicalType::LIST:
				case PhysicalType::STRUCT:
				case PhysicalType::ARRAY:
				case PhysicalType::UNKNOWN:
					can_reuse_changed_group_candidates = false;
					break;
				default:
					break;
				}
			}
			for (auto &payload_type : op.payload_types) {
				switch (payload_type.InternalType()) {
				case PhysicalType::LIST:
				case PhysicalType::STRUCT:
				case PhysicalType::ARRAY:
				case PhysicalType::UNKNOWN:
					can_reuse_changed_group_candidates = false;
					break;
				default:
					break;
				}
			}
			key_delta = make_uniq<RecursiveCTEKeyDeltaState>(context, op);
		}
	} else if (!op.union_all) {
		ht = make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), op.distinct_types);
	}
	if (op.using_key) {
		distinct_rows.Initialize(Allocator::DefaultAllocator(), op.hash_key_types);
		if (!op.key_normalizers.empty()) {
			raw_distinct_rows.Initialize(Allocator::DefaultAllocator(), op.distinct_types);
		}
		update_rows.Initialize(Allocator::DefaultAllocator(), op.internal_types);
		source_distinct_rows.Initialize(Allocator::DefaultAllocator(), op.hash_key_types);
		source_aggregate_rows.Initialize(Allocator::DefaultAllocator(), op.aggregate_types);
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

unique_ptr<GroupedAggregateHashTable> RecursiveCTEState::CreateUsingKeyHashTable() const {
	return make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), op.hash_key_types,
	                                            op.aggregate_types, payload_aggregate_objects);
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
	// Keyed workers can resume shared appends after publishing local output.
	if (op.using_key) {
		InitializeSharedOutputAppend();
	}
	if (collect_metrics) {
		const auto after_work = std::chrono::steady_clock::now();
		RecordSinkMetrics(
		    NumericCast<idx_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(after_lock - before_lock).count()),
		    NumericCast<idx_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(after_work - after_lock).count()),
		    row_count);
	}
}

void RecursiveCTEState::RegisterLocalPreaggregation(unique_ptr<GroupedAggregateHashTable> local_ht,
                                                    idx_t candidate_rows, idx_t classification_work_ns,
                                                    idx_t preaggregation_work_ns) {
	D_ASSERT(local_ht && candidate_rows > 0 && local_ht->Count() <= candidate_rows);
	const auto group_count = local_ht->Count();
	{
		lock_guard<mutex> guard(intermediate_table_lock);
		local_preaggregate_candidate_count += candidate_rows;
		local_preaggregates.push_back(std::move(local_ht));
	}
	if (metrics.Enabled()) {
		metrics.RecordHashRows(candidate_rows);
		GetEpochMetrics().RecordLocalKeyPreaggregationClassification(classification_work_ns);
		GetEpochMetrics().RecordLocalKeyPreaggregation(candidate_rows, group_count, preaggregation_work_ns);
	}
}

void RecursiveCTEState::AssembleStateRows(DataChunk &keys, DataChunk &aggregates, DataChunk &result) const {
	result.Reset();
	for (idx_t key_idx = 0; key_idx < op.distinct_idx.size(); key_idx++) {
		const auto representative_idx = op.key_representative_indices[key_idx];
		auto &source =
		    representative_idx == DConstants::INVALID_INDEX ? keys.data[key_idx] : aggregates.data[representative_idx];
		result.data[op.distinct_idx[key_idx]].Reference(source);
	}
	for (idx_t payload_idx = 0; payload_idx < op.payload_idx.size(); payload_idx++) {
		result.data[op.payload_idx[payload_idx]].Reference(aggregates.data[payload_idx]);
	}
	result.CheckCardinality(keys.size());
}

void RecursiveCTEState::FinalizeStateRows(RowOperationsState &row_state, Vector &addresses, DataChunk &keys,
                                          DataChunk &aggregates, DataChunk &result) {
	FinalizeAggregateRows(row_state, addresses, aggregates, keys.size());
	AssembleStateRows(keys, aggregates, result);
}

void RecursiveCTEState::FinalizeAggregateRows(RowOperationsState &row_state, Vector &addresses, DataChunk &aggregates,
                                              idx_t count) {
	aggregates.Reset();
	aggregates.SetChildCardinality(count);
	{
		lock_guard<mutex> guard(ht_finalize_lock);
		RowOperations::FinalizeStates(row_state, *GetHashTable().GetLayoutPtr(), addresses, aggregates, 0);
	}
}

void RecursiveCTEState::ExtractUsingKeyKeys(DataChunk &input) {
	distinct_rows.Reset();
	if (op.key_normalizers.empty()) {
		for (idx_t key_idx = 0; key_idx < op.distinct_idx.size(); key_idx++) {
			distinct_rows.data[key_idx].Reference(input.data[op.distinct_idx[key_idx]]);
		}
		distinct_rows.CheckCardinality(input.size());
		return;
	}
	raw_distinct_rows.Reset();
	for (idx_t key_idx = 0; key_idx < op.distinct_idx.size(); key_idx++) {
		raw_distinct_rows.data[key_idx].Reference(input.data[op.distinct_idx[key_idx]]);
	}
	raw_distinct_rows.CheckCardinality(input.size());
	D_ASSERT(key_executor);
	key_executor->Execute(raw_distinct_rows, distinct_rows);
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

enum class RecursiveCTELocalPreaggregationDecision : uint8_t { DEFER, PREAGGREGATE, DIRECT };

class RecursiveCTELocalPreaggregationState {
public:
	RecursiveCTELocalPreaggregationState(ClientContext &context_p, const PhysicalRecursiveCTE &op_p)
	    : context(context_p), op(op_p), payload_executor(context), hashes(LogicalType::HASH) {
		vector<LogicalType> aggregate_input_types;
		for (auto &payload_aggregate : op.payload_aggregates) {
			auto &bound_aggregate = payload_aggregate->Cast<BoundAggregateExpression>();
			for (auto &child : bound_aggregate.GetChildren()) {
				payload_executor.AddExpression(*child);
				aggregate_input_types.push_back(child->GetReturnType());
			}
			aggregates.emplace_back(bound_aggregate);
		}
		if (!op.key_normalizers.empty()) {
			key_executor = make_uniq<ExpressionExecutor>(context);
			for (auto &normalizer : op.key_normalizers) {
				key_executor->AddExpression(*normalizer);
			}
			raw_keys.Initialize(Allocator::Get(context), op.distinct_types);
		}
		keys.Initialize(Allocator::Get(context), op.hash_key_types);
		payload.Initialize(Allocator::Get(context), aggregate_input_types);
		input.Initialize(Allocator::Get(context), op.internal_types);
	}

	RecursiveCTELocalPreaggregationDecision Classify(ColumnDataCollection &candidates) {
		const auto candidate_count = candidates.Count();
		D_ASSERT(candidate_count >= STANDARD_VECTOR_SIZE);
		sampled_candidate_count += candidate_count;
		ColumnDataScanState scan_state;
		candidates.InitializeScan(scan_state);
		while (candidates.Scan(scan_state, input)) {
			ExtractKeys(input);
			keys.Hash(hashes);
			cardinality.Update(hashes);
		}
		const auto distinct_upper_bound =
		    LossyNumericCast<idx_t>((1 + HyperLogLog::GetErrorRate()) * static_cast<double>(cardinality.Count()));
		if (distinct_upper_bound < sampled_candidate_count / 4) {
			return RecursiveCTELocalPreaggregationDecision::PREAGGREGATE;
		}
		// Keep sampling across vector boundaries, but bound the extra hashing for unique streams.
		static constexpr idx_t MAX_SAMPLE_VECTORS = 8;
		const auto sample_size = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, 16);
		if (sampled_candidate_count >= sample_size * MAX_SAMPLE_VECTORS) {
			return RecursiveCTELocalPreaggregationDecision::DIRECT;
		}
		return RecursiveCTELocalPreaggregationDecision::DEFER;
	}

	void ResetClassification() {
		cardinality = HyperLogLog();
		sampled_candidate_count = 0;
	}

	unique_ptr<GroupedAggregateHashTable> Preaggregate(ColumnDataCollection &candidates) {
		auto result = make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), op.hash_key_types,
		                                                   op.aggregate_types, aggregates);
		ColumnDataScanState scan_state;
		candidates.InitializeScan(scan_state);
		while (candidates.Scan(scan_state, input)) {
			ExtractKeys(input);
			if (!payload_executor.expressions.empty()) {
				payload.Reset();
				payload_executor.Execute(input, payload);
			}
			result->AddChunk(keys, payload, AggregateType::NON_DISTINCT);
		}
		return result;
	}

private:
	void ExtractKeys(DataChunk &source) {
		keys.Reset();
		if (!key_executor) {
			for (idx_t key_idx = 0; key_idx < op.distinct_idx.size(); key_idx++) {
				keys.data[key_idx].Reference(source.data[op.distinct_idx[key_idx]]);
			}
			keys.CheckCardinality(source.size());
			return;
		}
		raw_keys.Reset();
		for (idx_t key_idx = 0; key_idx < op.distinct_idx.size(); key_idx++) {
			raw_keys.data[key_idx].Reference(source.data[op.distinct_idx[key_idx]]);
		}
		raw_keys.CheckCardinality(source.size());
		key_executor->Execute(raw_keys, keys);
	}

private:
	ClientContext &context;
	const PhysicalRecursiveCTE &op;
	ExpressionExecutor payload_executor;
	unique_ptr<ExpressionExecutor> key_executor;
	vector<AggregateObject> aggregates;
	DataChunk raw_keys;
	DataChunk keys;
	DataChunk payload;
	DataChunk input;
	Vector hashes;
	HyperLogLog cardinality;
	idx_t sampled_candidate_count = 0;
};

class RecursiveCTELocalState : public LocalSinkState {
public:
	RecursiveCTELocalState(ClientContext &context, const PhysicalRecursiveCTE &op)
	    : context(context), op(op), hashes(LogicalType::HASH), partition_hashes(LogicalType::HASH),
	      dummy_addresses(LogicalType::POINTER), new_groups(STANDARD_VECTOR_SIZE) {
		if (!op.using_key) {
			output = make_uniq<ColumnDataCollection>(context, op.GetTypes());
			output->InitializeAppend(append_state);
		}
		if (!op.using_key && !op.union_all) {
			partition_chunk.Initialize(Allocator::Get(context), op.GetTypes());
		}
	}

	ClientContext &context;
	const PhysicalRecursiveCTE &op;
	unique_ptr<ColumnDataCollection> output;
	ColumnDataAppendState append_state;
	Vector hashes;
	Vector partition_hashes;
	Vector dummy_addresses;
	SelectionVector new_groups;
	DataChunk partition_chunk;
	vector<SelectionVector> partition_selections;
	vector<idx_t> partition_counts;
	idx_t using_key_candidate_count = 0;
	idx_t using_key_classification_work_ns = 0;
	bool buffer_using_key_output = false;
	bool direct_using_key_output = false;
	unique_ptr<RecursiveCTELocalPreaggregationState> using_key_preaggregation;

	void SinkUsingKeyOutput(DataChunk &chunk, RecursiveCTEState &gstate) {
		D_ASSERT(op.using_key && !op.union_all);
		using_key_candidate_count += chunk.size();
		if (buffer_using_key_output) {
			BufferUsingKeyOutput(chunk);
			return;
		}
		const auto sample_size = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, 16);
		const auto coalesce_small_chunks = chunk.size() < sample_size && (using_key_candidate_count >= sample_size ||
		                                                                  gstate.CurrentInputCount() >= sample_size);
		if (direct_using_key_output || !gstate.CanPreaggregateUsingKey()) {
			if (coalesce_small_chunks) {
				BufferUsingKeyOutput(chunk);
			} else {
				gstate.AppendOutput(chunk);
			}
			return;
		}
		if (using_key_candidate_count <= gstate.CurrentInputCount() && !coalesce_small_chunks) {
			gstate.AppendOutput(chunk);
			return;
		}
		BufferUsingKeyOutput(chunk);
		if (output->Count() < sample_size) {
			return;
		}
		ClassifyBufferedUsingKeyOutput();
		if (!buffer_using_key_output) {
			gstate.CombineOutput(*output);
			output->ResetForReuse();
			output->InitializeAppend(append_state);
		}
	}

	void ClassifyBufferedUsingKeyOutput() {
		D_ASSERT(output && output->Count() >= STANDARD_VECTOR_SIZE && !buffer_using_key_output &&
		         !direct_using_key_output);
		if (!using_key_preaggregation) {
			using_key_preaggregation = make_uniq<RecursiveCTELocalPreaggregationState>(context, op);
		}
		const auto classification_start = std::chrono::steady_clock::now();
		const auto decision = using_key_preaggregation->Classify(*output);
		const auto classification_end = std::chrono::steady_clock::now();
		using_key_classification_work_ns += NumericCast<idx_t>(
		    std::chrono::duration_cast<std::chrono::nanoseconds>(classification_end - classification_start).count());
		buffer_using_key_output = decision == RecursiveCTELocalPreaggregationDecision::PREAGGREGATE;
		direct_using_key_output = decision == RecursiveCTELocalPreaggregationDecision::DIRECT;
	}

	void BufferUsingKeyOutput(DataChunk &chunk) {
		D_ASSERT(op.using_key && !op.union_all);
		if (!output) {
			output = make_uniq<ColumnDataCollection>(context, op.internal_types);
			output->InitializeAppend(append_state);
		}
		output->Append(append_state, chunk);
	}

	unique_ptr<GroupedAggregateHashTable> Preaggregate(idx_t &preaggregation_work_ns) {
		D_ASSERT(output && op.using_key && !op.union_all);
		D_ASSERT(buffer_using_key_output && using_key_preaggregation);
		const auto preaggregation_start = std::chrono::steady_clock::now();
		auto result = using_key_preaggregation->Preaggregate(*output);
		const auto preaggregation_end = std::chrono::steady_clock::now();
		preaggregation_work_ns = NumericCast<idx_t>(
		    std::chrono::duration_cast<std::chrono::nanoseconds>(preaggregation_end - preaggregation_start).count());
		return result;
	}

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
		using_key_candidate_count = 0;
		using_key_classification_work_ns = 0;
		buffer_using_key_output = false;
		direct_using_key_output = false;
		if (using_key_preaggregation) {
			using_key_preaggregation->ResetClassification();
		}
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

bool RecursiveCTEState::ShouldPreaggregateUsingKeyUpdates(idx_t candidate_count) {
	D_ASSERT(can_preaggregate_using_key && candidate_count >= STANDARD_VECTOR_SIZE);
	if (op.working_table->Count() >= candidate_count) {
		return false;
	}
	HyperLogLog key_cardinality;
	const auto group_limit = candidate_count / 4;
	ColumnDataScanState sample_scan_state;
	intermediate_table.InitializeScan(sample_scan_state);
	while (intermediate_table.Scan(sample_scan_state, update_rows)) {
		ExtractUsingKeyKeys(update_rows);
		distinct_rows.Hash(preaggregation_hashes);
		key_cardinality.Update(preaggregation_hashes);
		const auto distinct_upper_bound =
		    LossyNumericCast<idx_t>((1 + HyperLogLog::GetErrorRate()) * static_cast<double>(key_cardinality.Count()));
		if (distinct_upper_bound >= group_limit) {
			return false;
		}
	}
	// A second keyed hash table only pays off when the estimated fan-in is substantial.
	return true;
}

template <bool COLLECT_METRICS>
void RecursiveCTEState::CommitUsingKeyUpdatesInternal() {
	D_ASSERT(op.using_key);
	if (!local_preaggregates.empty()) {
		D_ASSERT(!op.union_all && local_preaggregate_candidate_count > 0);
		const auto combine_start =
		    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		auto epoch_ht = std::move(local_preaggregates[0]);
		for (idx_t local_idx = 1; local_idx < local_preaggregates.size(); local_idx++) {
			epoch_ht->Combine(*local_preaggregates[local_idx]);
		}
		local_preaggregates.clear();
		auto preaggregated_candidate_count = local_preaggregate_candidate_count;
		local_preaggregate_candidate_count = 0;
		if constexpr (COLLECT_METRICS) {
			const auto combine_end = std::chrono::steady_clock::now();
			GetEpochMetrics().RecordKeyPreaggregationCombine(NumericCast<idx_t>(
			    std::chrono::duration_cast<std::chrono::nanoseconds>(combine_end - combine_start).count()));
		}

		const auto raw_candidate_count = intermediate_table.Count();
		if constexpr (COLLECT_METRICS) {
			GetEpochMetrics().RecordLocalKeyPreaggregationResidual(raw_candidate_count);
		}
		bool preaggregate_raw_candidates = false;
		if (raw_candidate_count >= STANDARD_VECTOR_SIZE) {
			const auto classification_start =
			    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
			preaggregate_raw_candidates = ShouldPreaggregateUsingKeyUpdates(raw_candidate_count);
			if constexpr (COLLECT_METRICS) {
				const auto classification_end = std::chrono::steady_clock::now();
				GetEpochMetrics().RecordKeyPreaggregationClassification(NumericCast<idx_t>(
				    std::chrono::duration_cast<std::chrono::nanoseconds>(classification_end - classification_start)
				        .count()));
			}
		}
		if (preaggregate_raw_candidates) {
			auto raw_ht = CreateUsingKeyHashTable();
			const auto preaggregation_work_ns = PreaggregateUsingKeyUpdates<COLLECT_METRICS>(*raw_ht);
			if constexpr (COLLECT_METRICS) {
				GetEpochMetrics().RecordKeyPreaggregation(raw_candidate_count, raw_ht->Count(), preaggregation_work_ns);
			}
			const auto raw_combine_start =
			    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
			epoch_ht->Combine(*raw_ht);
			if constexpr (COLLECT_METRICS) {
				const auto raw_combine_end = std::chrono::steady_clock::now();
				GetEpochMetrics().RecordKeyPreaggregationCombine(NumericCast<idx_t>(
				    std::chrono::duration_cast<std::chrono::nanoseconds>(raw_combine_end - raw_combine_start).count()));
			}
			preaggregated_candidate_count += raw_candidate_count;
			intermediate_table.ResetForReuse();
			InitializeIntermediateAppend();
		}
		CommitMixedUsingKeyUpdatesInternal<COLLECT_METRICS>(std::move(epoch_ht), preaggregated_candidate_count);
		return;
	}
	const auto candidate_count = intermediate_table.Count();
	bool use_preaggregation = false;
	if (can_preaggregate_using_key && candidate_count >= STANDARD_VECTOR_SIZE) {
		const auto classification_start =
		    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		use_preaggregation = ShouldPreaggregateUsingKeyUpdates(candidate_count);
		if constexpr (COLLECT_METRICS) {
			const auto classification_end = std::chrono::steady_clock::now();
			GetEpochMetrics().RecordKeyPreaggregationClassification(NumericCast<idx_t>(
			    std::chrono::duration_cast<std::chrono::nanoseconds>(classification_end - classification_start)
			        .count()));
		}
	}
	if (use_preaggregation) {
		CommitPreaggregatedUsingKeyUpdatesInternal<COLLECT_METRICS>();
		return;
	}
	const auto delta_candidate_count = op.union_all ? idx_t(0) : candidate_count;
	idx_t delta_work_ns = 0;
	if (!op.union_all) {
		D_ASSERT(key_delta);
		const auto delta_start =
		    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		key_delta->Reset();
		if constexpr (COLLECT_METRICS) {
			const auto delta_end = std::chrono::steady_clock::now();
			delta_work_ns += NumericCast<idx_t>(
			    std::chrono::duration_cast<std::chrono::nanoseconds>(delta_end - delta_start).count());
		}
	}
	ColumnDataScanState update_scan_state;
	intermediate_table.InitializeScan(update_scan_state);
	while (intermediate_table.Scan(update_scan_state, update_rows)) {
		if constexpr (COLLECT_METRICS) {
			metrics.RecordHashRows(update_rows.size());
		}
		const auto hash_start =
		    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		idx_t snapshot_work_ns = 0;
		ExtractUsingKeyKeys(update_rows);
		if (!executor.expressions.empty()) {
			payload_rows.Reset();
			executor.Execute(update_rows, payload_rows);
		}
		if (!op.union_all) {
			const auto new_group_count = ht->AddChunk(
			    distinct_rows, payload_rows, AggregateType::NON_DISTINCT,
			    [&](const Vector &group_addresses, const SelectionVector &new_groups, idx_t new_group_count) {
				    const auto delta_start =
				        COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
				    SnapshotUsingKeyDelta(group_addresses, new_groups, new_group_count, update_rows.size());
				    if constexpr (COLLECT_METRICS) {
					    const auto delta_end = std::chrono::steady_clock::now();
					    snapshot_work_ns = NumericCast<idx_t>(
					        std::chrono::duration_cast<std::chrono::nanoseconds>(delta_end - delta_start).count());
					    delta_work_ns += snapshot_work_ns;
				    }
			    });
			if (key_delta->deferred_previous_rows) {
				const auto delta_start =
				    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
				ValidateDeferredUsingKeyCandidateReuse(update_rows);
				if constexpr (COLLECT_METRICS) {
					const auto delta_end = std::chrono::steady_clock::now();
					const auto elapsed_ns = NumericCast<idx_t>(
					    std::chrono::duration_cast<std::chrono::nanoseconds>(delta_end - delta_start).count());
					snapshot_work_ns += elapsed_ns;
					delta_work_ns += elapsed_ns;
				}
			}
			if constexpr (COLLECT_METRICS) {
				const auto hash_end = std::chrono::steady_clock::now();
				const auto hash_work_ns = NumericCast<idx_t>(
				    std::chrono::duration_cast<std::chrono::nanoseconds>(hash_end - hash_start).count());
				D_ASSERT(snapshot_work_ns <= hash_work_ns);
				GetEpochMetrics().RecordKeyedHashCommit(hash_work_ns - snapshot_work_ns);
			}
			const auto index_start =
			    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
			for (auto &index : partial_key_indexes) {
				index->AddGroups(distinct_rows, new_groups, new_group_addresses,
				                 *FlatVector::IncrementalSelectionVector(), new_group_count);
			}
			if constexpr (COLLECT_METRICS) {
				const auto index_end = std::chrono::steady_clock::now();
				const auto elapsed_ns = NumericCast<idx_t>(
				    std::chrono::duration_cast<std::chrono::nanoseconds>(index_end - index_start).count());
				GetEpochMetrics().RecordPartialIndexMaintenance(elapsed_ns);
				metrics.RecordPartialIndexBuild(NumericCast<idx_t>(elapsed_ns / 1000));
			}
			continue;
		}
		if (partial_key_indexes.empty()) {
			ht->AddChunk(distinct_rows, payload_rows, AggregateType::NON_DISTINCT);
			if constexpr (COLLECT_METRICS) {
				const auto hash_end = std::chrono::steady_clock::now();
				const auto hash_work_ns = NumericCast<idx_t>(
				    std::chrono::duration_cast<std::chrono::nanoseconds>(hash_end - hash_start).count());
				D_ASSERT(snapshot_work_ns <= hash_work_ns);
				GetEpochMetrics().RecordKeyedHashCommit(hash_work_ns - snapshot_work_ns);
			}
			continue;
		}
		const auto new_group_count = ht->AddChunkAndGetNewGroups(
		    distinct_rows, payload_rows, AggregateType::NON_DISTINCT, new_group_addresses, new_groups);
		if constexpr (COLLECT_METRICS) {
			const auto hash_end = std::chrono::steady_clock::now();
			const auto hash_work_ns =
			    NumericCast<idx_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(hash_end - hash_start).count());
			D_ASSERT(snapshot_work_ns <= hash_work_ns);
			GetEpochMetrics().RecordKeyedHashCommit(hash_work_ns - snapshot_work_ns);
		}
		const auto index_start =
		    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		for (auto &index : partial_key_indexes) {
			index->AddGroups(distinct_rows, new_groups, new_group_addresses, *FlatVector::IncrementalSelectionVector(),
			                 new_group_count);
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
	if (!op.union_all) {
		const auto delta_start =
		    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		if (can_reuse_new_group_candidates && key_delta->new_count == delta_candidate_count) {
			op.working_table->Reset();
			op.working_table->Combine(intermediate_table);
			InitializeIntermediateAppend();
		} else if (TryReuseChangedGroupCandidates(delta_candidate_count)) {
			op.working_table->Reset();
			op.working_table->Combine(intermediate_table);
			InitializeIntermediateAppend();
		} else {
			op.working_table->ResetForReuse();
			op.working_table->InitializeAppend(working_append_state);
			FinalizeUsingKeyDelta(false, COLLECT_METRICS);
			intermediate_table.ResetForReuse();
			InitializeIntermediateAppend();
		}
		if constexpr (COLLECT_METRICS) {
			const auto delta_end = std::chrono::steady_clock::now();
			delta_work_ns += NumericCast<idx_t>(
			    std::chrono::duration_cast<std::chrono::nanoseconds>(delta_end - delta_start).count());
			GetEpochMetrics().RecordKeyDelta(delta_candidate_count, key_delta->touched_count, key_delta->new_count,
			                                 key_delta->changed_count, delta_work_ns);
		}
	}
}

template <bool COLLECT_METRICS>
void RecursiveCTEState::ApplyPreaggregatedUsingKeyUpdates(GroupedAggregateHashTable &epoch_ht, idx_t &delta_work_ns) {
	AggregateHTScanState epoch_scan_state;
	epoch_ht.InitializeScan(epoch_scan_state);
	while (epoch_ht.ScanGroups(epoch_scan_state, distinct_rows)) {
		if (distinct_rows.size() == 0) {
			continue;
		}
		const auto snapshot_start =
		    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		SnapshotPreaggregatedUsingKeyDeltaGroups(distinct_rows);
		if constexpr (COLLECT_METRICS) {
			const auto snapshot_end = std::chrono::steady_clock::now();
			delta_work_ns += NumericCast<idx_t>(
			    std::chrono::duration_cast<std::chrono::nanoseconds>(snapshot_end - snapshot_start).count());
		}
	}

	const auto combine_start =
	    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	ht->Combine(epoch_ht);
	if constexpr (COLLECT_METRICS) {
		const auto combine_end = std::chrono::steady_clock::now();
		GetEpochMetrics().RecordKeyPreaggregationCombine(NumericCast<idx_t>(
		    std::chrono::duration_cast<std::chrono::nanoseconds>(combine_end - combine_start).count()));
	}
}

template <bool COLLECT_METRICS>
void RecursiveCTEState::CommitMixedUsingKeyUpdatesInternal(unique_ptr<GroupedAggregateHashTable> epoch_ht,
                                                           idx_t preaggregated_candidate_count) {
	D_ASSERT(op.using_key && !op.union_all && key_delta && epoch_ht && preaggregated_candidate_count > 0);
	auto &delta = *key_delta;
	const auto raw_candidate_count = intermediate_table.Count();
	const auto delta_candidate_count = raw_candidate_count + preaggregated_candidate_count;
	idx_t delta_work_ns = 0;
	const auto reset_start =
	    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	delta.Reset();
	if constexpr (COLLECT_METRICS) {
		const auto reset_end = std::chrono::steady_clock::now();
		delta_work_ns +=
		    NumericCast<idx_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(reset_end - reset_start).count());
	}

	ColumnDataScanState update_scan_state;
	intermediate_table.InitializeScan(update_scan_state);
	while (intermediate_table.Scan(update_scan_state, update_rows)) {
		if constexpr (COLLECT_METRICS) {
			metrics.RecordHashRows(update_rows.size());
		}
		const auto hash_start =
		    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		idx_t snapshot_work_ns = 0;
		ExtractUsingKeyKeys(update_rows);
		if (!executor.expressions.empty()) {
			payload_rows.Reset();
			executor.Execute(update_rows, payload_rows);
		}
		ht->AddChunk(
		    distinct_rows, payload_rows, AggregateType::NON_DISTINCT,
		    [&](const Vector &group_addresses, const SelectionVector &new_groups, idx_t new_group_count) {
			    const auto snapshot_start =
			        COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
			    SnapshotUsingKeyDelta(group_addresses, new_groups, new_group_count, update_rows.size(), false);
			    if constexpr (COLLECT_METRICS) {
				    const auto snapshot_end = std::chrono::steady_clock::now();
				    snapshot_work_ns = NumericCast<idx_t>(
				        std::chrono::duration_cast<std::chrono::nanoseconds>(snapshot_end - snapshot_start).count());
				    delta_work_ns += snapshot_work_ns;
			    }
		    });
		if constexpr (COLLECT_METRICS) {
			const auto hash_end = std::chrono::steady_clock::now();
			const auto hash_work_ns =
			    NumericCast<idx_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(hash_end - hash_start).count());
			D_ASSERT(snapshot_work_ns <= hash_work_ns);
			GetEpochMetrics().RecordKeyedHashCommit(hash_work_ns - snapshot_work_ns);
		}
	}

	ApplyPreaggregatedUsingKeyUpdates<COLLECT_METRICS>(*epoch_ht, delta_work_ns);

	const auto finalize_start =
	    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	op.working_table->ResetForReuse();
	op.working_table->InitializeAppend(working_append_state);
	const auto index_work_ns = FinalizeUsingKeyDelta(!partial_key_indexes.empty(), COLLECT_METRICS);
	intermediate_table.ResetForReuse();
	InitializeIntermediateAppend();
	if constexpr (COLLECT_METRICS) {
		const auto finalize_end = std::chrono::steady_clock::now();
		const auto finalize_work_ns = NumericCast<idx_t>(
		    std::chrono::duration_cast<std::chrono::nanoseconds>(finalize_end - finalize_start).count());
		D_ASSERT(index_work_ns <= finalize_work_ns);
		delta_work_ns += finalize_work_ns - index_work_ns;
		GetEpochMetrics().RecordKeyDelta(delta_candidate_count, delta.touched_count, delta.new_count,
		                                 delta.changed_count, delta_work_ns);
	}
}

template <bool COLLECT_METRICS>
idx_t RecursiveCTEState::PreaggregateUsingKeyUpdates(GroupedAggregateHashTable &epoch_ht) {
	idx_t preaggregation_work_ns = 0;
	ColumnDataScanState update_scan_state;
	intermediate_table.InitializeScan(update_scan_state);
	while (intermediate_table.Scan(update_scan_state, update_rows)) {
		if constexpr (COLLECT_METRICS) {
			metrics.RecordHashRows(update_rows.size());
		}
		const auto hash_start =
		    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
		ExtractUsingKeyKeys(update_rows);
		if (!executor.expressions.empty()) {
			payload_rows.Reset();
			executor.Execute(update_rows, payload_rows);
		}
		epoch_ht.AddChunk(distinct_rows, payload_rows, AggregateType::NON_DISTINCT);
		if constexpr (COLLECT_METRICS) {
			const auto hash_end = std::chrono::steady_clock::now();
			preaggregation_work_ns +=
			    NumericCast<idx_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(hash_end - hash_start).count());
		}
	}
	return preaggregation_work_ns;
}

template <bool COLLECT_METRICS>
void RecursiveCTEState::CommitPreaggregatedUsingKeyUpdatesInternal() {
	D_ASSERT(op.using_key && !op.union_all && key_delta);
	auto &delta = *key_delta;
	const auto delta_candidate_count = intermediate_table.Count();
	idx_t delta_work_ns = 0;
	const auto delta_start =
	    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	delta.Reset();
	if constexpr (COLLECT_METRICS) {
		const auto delta_end = std::chrono::steady_clock::now();
		delta_work_ns +=
		    NumericCast<idx_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(delta_end - delta_start).count());
	}

	auto epoch_ht = CreateUsingKeyHashTable();
	const auto preaggregation_work_ns = PreaggregateUsingKeyUpdates<COLLECT_METRICS>(*epoch_ht);
	if constexpr (COLLECT_METRICS) {
		GetEpochMetrics().RecordKeyPreaggregation(delta_candidate_count, epoch_ht->Count(), preaggregation_work_ns);
	}
	ApplyPreaggregatedUsingKeyUpdates<COLLECT_METRICS>(*epoch_ht, delta_work_ns);

	const auto finalize_start =
	    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
	op.working_table->ResetForReuse();
	op.working_table->InitializeAppend(working_append_state);
	const auto index_work_ns = FinalizeUsingKeyDelta(!partial_key_indexes.empty(), COLLECT_METRICS);
	intermediate_table.ResetForReuse();
	InitializeIntermediateAppend();
	if constexpr (COLLECT_METRICS) {
		const auto finalize_end = std::chrono::steady_clock::now();
		const auto finalize_work_ns = NumericCast<idx_t>(
		    std::chrono::duration_cast<std::chrono::nanoseconds>(finalize_end - finalize_start).count());
		D_ASSERT(index_work_ns <= finalize_work_ns);
		delta_work_ns += finalize_work_ns - index_work_ns;
		GetEpochMetrics().RecordKeyDelta(delta_candidate_count, delta.touched_count, delta.new_count,
		                                 delta.changed_count, delta_work_ns);
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
		distinct_rows.Initialize(Allocator::Get(context), op.hash_key_types);
		aggregate_rows.Initialize(Allocator::Get(context), op.aggregate_types);
	}

	DataChunk distinct_rows;
	DataChunk aggregate_rows;
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
		recursive_state.FinalizeStateRows(lstate.row_state, lstate.lookup_state.addresses, lstate.distinct_rows,
		                                  lstate.aggregate_rows, chunk);
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

	if (union_all) {
		gstate.AppendOutput(chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}
	auto &lstate = input.local_state.Cast<RecursiveCTELocalState>();
	lstate.SinkUsingKeyOutput(chunk, gstate);
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalRecursiveCTE::PrepareFinalize(ClientContext &context, GlobalSinkState &sink_state) const {
	if (using_key) {
		sink_state.Cast<RecursiveCTEState>().CommitUsingKeyUpdates();
	}
}

SinkCombineResultType PhysicalRecursiveCTE::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<RecursiveCTEState>();
	if (using_key) {
		if (!union_all) {
			auto &lstate = input.local_state.Cast<RecursiveCTELocalState>();
			if (!lstate.buffer_using_key_output && !lstate.direct_using_key_output && lstate.output &&
			    lstate.output->Count() >= STANDARD_VECTOR_SIZE &&
			    lstate.using_key_candidate_count > gstate.CurrentInputCount() && gstate.CanPreaggregateUsingKey()) {
				lstate.ClassifyBufferedUsingKeyOutput();
			}
			if (!lstate.buffer_using_key_output) {
				if (gstate.GetMetrics().Enabled() && lstate.using_key_classification_work_ns > 0) {
					gstate.GetEpochMetrics().RecordLocalKeyPreaggregationClassification(
					    lstate.using_key_classification_work_ns);
				}
				if (lstate.output && lstate.output->Count() > 0) {
					gstate.CombineOutput(*lstate.output);
				}
				return SinkCombineResultType::FINISHED;
			}
			D_ASSERT(lstate.output);
			const auto candidate_count = lstate.output->Count();
			D_ASSERT(candidate_count > 0 && lstate.buffer_using_key_output && gstate.CanPreaggregateUsingKey());
			idx_t preaggregation_work_ns = 0;
			auto local_ht = lstate.Preaggregate(preaggregation_work_ns);
			gstate.RegisterLocalPreaggregation(std::move(local_ht), candidate_count,
			                                   lstate.using_key_classification_work_ns, preaggregation_work_ns);
			return SinkCombineResultType::FINISHED;
		}
	} else {
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
			idx_t expected_new;
			if (op.union_all) {
				expected_new = intermediate_table.Count();
				op.working_table->Reset();
				op.working_table->Combine(intermediate_table);
				InitializeIntermediateAppend();
			} else {
				expected_new = op.working_table->Count();
				if (expected_new == 0) {
					ht->InitializeScan(ht_scan_state);
					source_phase = RecursiveCTESourcePhase::DRAINING_FINAL_KEY_STATE;
					break;
				}
			}

			if (expected_new > 0) {
				const auto desired_capacity =
				    GroupedAggregateHashTable::GetCapacityForCount(ht->Count() + expected_new);
				if (desired_capacity > ht->Capacity()) {
					ht->Resize(desired_capacity);
				}
			}

			op.ExecuteRecursivePipelines(context);
			const auto next_count = op.union_all ? intermediate_table.Count() : op.working_table->Count();
			if (next_count == 0) {
				ht->InitializeScan(ht_scan_state);
				source_phase = RecursiveCTESourcePhase::DRAINING_FINAL_KEY_STATE;
			}
			break;
		}
		case RecursiveCTESourcePhase::DRAINING_FINAL_KEY_STATE: {
			const auto drain_start =
			    COLLECT_METRICS ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
			while (ht->Scan(ht_scan_state, source_distinct_rows, source_aggregate_rows)) {
				if (source_distinct_rows.size() == 0) {
					continue;
				}
				AssembleStateRows(source_distinct_rows, source_aggregate_rows, chunk);
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
