#include "duckdb/execution/operator/join/join_filter_pushdown.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/execution/operator/aggregate/ungrouped_aggregate_state.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/storage/block_allocator.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <new>

namespace duckdb {

struct JoinFilterPushdownConfig {
	//! Building the bloom filter is costly on the build side to make probing faster,
	//! so only use it if there are less build tuples than probing tuples
	static constexpr double BUILD_TO_PROBE_RATIO_THRESHOLD = 1.0;
	//! If we have a build side without a filter, only build the bloom filter if the ratio/size is small enough
	static constexpr double NON_FILTERING_RATIO_THRESHOLD = 0.1;
	static constexpr idx_t NON_FILTERING_BUILD_SIDE_THRESHOLD = 4194304;
	//! Max build/probe ratio at which a multi-key join builds per-key-column filters
	static constexpr double MULTI_KEY_RATIO_THRESHOLD = 0.05;
	//! A join is expected to stay in memory while its estimated build side is below this share of the memory budget
	static constexpr idx_t LIKELY_IN_MEMORY_BUILD_MEMORY_FRACTION = 4;
	static constexpr idx_t MAX_MEMORY_FRACTION = 64;
	//! A build side with more rows than the threshold can still have few distinct values in one of its key columns,
	//! e.g. the partition key of a multi-column join. Counting them requires a scan of the key column, so only build
	//! sides up to this many rows are scanned.
	static constexpr idx_t IN_FILTER_MAX_BUILD_SCAN_COUNT = 65536;
	static constexpr idx_t SMALL_EXACT_PRF_BITS = 1ULL << 26;
};

idx_t JoinFilterPushdownInfo::GetMemoryBudget(ClientContext &context) {
	return BufferManager::GetBufferManager(context).GetMaxMemory() / JoinFilterPushdownConfig::MAX_MEMORY_FRACTION;
}

//! Whether the build side is expected to exceed the memory budget and spill
static bool JoinIsLikelyExternal(ClientContext &context, const PhysicalComparisonJoin &op) {
	if (Settings::Get<DebugForceExternalSetting>(context)) {
		return true;
	}
	idx_t row_width = sizeof(data_ptr_t) + sizeof(hash_t); // per-row pointer table and hash overhead
	for (auto &type : op.children[1].get().GetTypes()) {
		row_width += GetTypeIdSize(type.InternalType());
	}
	const auto build_size = op.children[1].get().estimated_cardinality * row_width;
	const auto memory_budget = BufferManager::GetBufferManager(context).GetMaxMemory();
	return build_size > memory_budget / JoinFilterPushdownConfig::LIKELY_IN_MEMORY_BUILD_MEMORY_FRACTION;
}

static bool IsEqualityComparison(ExpressionType cmp) {
	return cmp == ExpressionType::COMPARE_EQUAL || cmp == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
}

//! Whether to accumulate bloom filters during Sink. Only an external join needs that: its finalize only ever
//! materializes some partitions, so a filter built there would be incomplete, while every row passes through
//! Sink. An in-memory join builds its filters during finalize instead, where the actual build count and the
//! competing filter choices are known. The estimate-based checks mirror CanUseBloomFilter, which decides
//! publication once the actual build count is known.
static bool ShouldBuildBloomFilterInSink(ClientContext &context, const PhysicalComparisonJoin &op) {
	if (!op.filter_pushdown || op.filter_pushdown->probe_info.empty()) {
		return false;
	}
	idx_t equality_count = 0;
	for (auto &cond : op.conditions) {
		if (IsEqualityComparison(cond.GetComparisonType())) {
			equality_count++;
		}
	}
	if (equality_count == 0) {
		return false;
	}
	const auto build_estimate = static_cast<double>(op.children[1].get().estimated_cardinality);
	const auto probe_estimate = static_cast<double>(op.children[0].get().estimated_cardinality);
	if (build_estimate > JoinFilterPushdownConfig::BUILD_TO_PROBE_RATIO_THRESHOLD * probe_estimate) {
		return false;
	}
	if (equality_count > 1 && build_estimate > JoinFilterPushdownConfig::MULTI_KEY_RATIO_THRESHOLD * probe_estimate) {
		return false;
	}
	if (!op.filter_pushdown->build_side_has_filter &&
	    build_estimate > JoinFilterPushdownConfig::NON_FILTERING_RATIO_THRESHOLD * probe_estimate &&
	    build_estimate > JoinFilterPushdownConfig::NON_FILTERING_BUILD_SIDE_THRESHOLD) {
		return false;
	}
	return JoinIsLikelyExternal(context, op);
}

JoinFilterGlobalState::~JoinFilterGlobalState() {
}

JoinFilterLocalState::~JoinFilterLocalState() {
}

unique_ptr<JoinFilterGlobalState> JoinFilterPushdownInfo::GetGlobalState(ClientContext &context,
                                                                         const PhysicalOperator &op) const {
	// clear any previously set filters
	// we can have previous filters for this operator in case of e.g. recursive CTEs
	for (auto &info : probe_info) {
		info.dynamic_filters->ClearFilters(op);
	}
	auto result = make_uniq<JoinFilterGlobalState>();
	result->global_aggregate_state =
	    make_uniq<GlobalUngroupedAggregateState>(BufferAllocator::Get(context), min_max_aggregates);
	return result;
}

unique_ptr<JoinFilterLocalState> JoinFilterPushdownInfo::GetLocalState(JoinFilterGlobalState &gstate) const {
	auto result = make_uniq<JoinFilterLocalState>();
	result->local_aggregate_state = make_uniq<LocalUngroupedAggregateState>(*gstate.global_aggregate_state);
	return result;
}

void JoinFilterPushdownInfo::InitializeBloomFilters(ClientContext &context, const PhysicalComparisonJoin &op,
                                                    JoinHashTable &ht, shared_ptr<BloomFilter> recycled_filter) {
	// a published filter is shared with probe-side scans and immutable, so it cannot be reused
	if (recycled_filter && recycled_filter->IsPublished()) {
		recycled_filter.reset();
	}
	if (!ShouldBuildBloomFilterInSink(context, op)) {
		ht.SetBloomFilters({});
		// an in-memory join builds its filter during finalize, which can still reuse the allocation;
		// an iteration without a filter must not clear a slot that finalize has not consumed yet
		if (recycled_filter) {
			ht.SetRecycledBloomFilter(std::move(recycled_filter));
		}
		return;
	}
	const auto build_estimate = MaxValue<idx_t>(op.children[1].get().estimated_cardinality, 1);
	if (!ht.HasSingleEqualityKey()) {
		// the hash table hashes all keys together, so each pushed key column gets a filter of its own
		vector<idx_t> key_idxs;
		for (auto &info : op.filter_pushdown->probe_info) {
			for (auto &column : info.columns) {
				const auto key_idx = op.filter_pushdown->join_condition[column.join_filter_idx];
				if (IsEqualityComparison(op.conditions[key_idx].GetComparisonType()) &&
				    column.mode == JoinFilterPushdownMode::RECONSTRUCT_EXPRESSION &&
				    std::find(key_idxs.begin(), key_idxs.end(), key_idx) == key_idxs.end()) {
					key_idxs.push_back(key_idx);
				}
			}
		}
		ht.InitializeBloomFilters(key_idxs, build_estimate, GetMemoryBudget(context));
		return;
	}
	if (!recycled_filter) {
		recycled_filter = make_shared_ptr<BloomFilter>();
	}
	recycled_filter->Initialize(context, build_estimate);
	ht.SetBloomFilter(0, std::move(recycled_filter));
}

void JoinFilterPushdownInfo::Sink(DataChunk &chunk, JoinFilterLocalState &lstate) const {
	// if we are pushing any filters into a probe-side, compute the min/max over the columns that we are pushing
	for (idx_t pushdown_idx = 0; pushdown_idx < join_condition.size(); pushdown_idx++) {
		auto join_condition_idx = join_condition[pushdown_idx];
		for (idx_t i = 0; i < 2; i++) {
			idx_t aggr_idx = pushdown_idx * 2 + i;
			lstate.local_aggregate_state->Sink(chunk, join_condition_idx, aggr_idx, chunk.size());
		}
	}
}

void JoinFilterPushdownInfo::Combine(JoinFilterGlobalState &gstate, JoinFilterLocalState &lstate) const {
	gstate.global_aggregate_state->Combine(*lstate.local_aggregate_state);
}

static idx_t InFilterMaxBuildScanCount(idx_t dynamic_or_filter_threshold) {
	return MaxValue<idx_t>(dynamic_or_filter_threshold, JoinFilterPushdownConfig::IN_FILTER_MAX_BUILD_SCAN_COUNT);
}

bool JoinFilterPushdownInfo::CanUseInFilter(const ClientContext &context, optional_ptr<JoinHashTable> ht,
                                            const ExpressionType &cmp) const {
	auto dynamic_or_filter_threshold = Settings::Get<DynamicOrFilterThresholdSetting>(context);
	if (dynamic_or_filter_threshold <= 1) {
		return false;
	}
	return ht && ht->Count() > 1 && ht->Count() <= InFilterMaxBuildScanCount(dynamic_or_filter_threshold) &&
	       (cmp == ExpressionType::COMPARE_EQUAL || cmp == ExpressionType::COMPARE_NOT_DISTINCT_FROM);
}

bool JoinFilterPushdownInfo::CanUseBloomFilter(const JoinFilterPushdownFilter &info, const ClientContext &context,
                                               const PhysicalComparisonJoin &op, const ExpressionType &cmp,
                                               optional_ptr<JoinHashTable> ht) const {
	if (!ht) {
		return false;
	}

	// bf is only supported for equality conditions
	if (cmp != ExpressionType::COMPARE_EQUAL && cmp != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		return false;
	}

	auto build_count = ht->Count();
	if (build_count == 0) {
		build_count = ht->GetSinkCollection().Count();
	}
	auto probe_cardinality = op.children[0].get().estimated_cardinality;
	if (build_side_has_filter) {
		probe_cardinality = MaxValue(info.estimated_scan_cardinality, probe_cardinality);
	}
	const double build_to_probe_ratio = static_cast<double>(build_count) / static_cast<double>(probe_cardinality);
	if (build_to_probe_ratio > JoinFilterPushdownConfig::BUILD_TO_PROBE_RATIO_THRESHOLD) {
		return false;
	}

	// if we have a build side without a filter, only build the bloom filter if the ratio/size is small enough
	if (!build_side_has_filter && build_to_probe_ratio > JoinFilterPushdownConfig::NON_FILTERING_RATIO_THRESHOLD &&
	    build_count > JoinFilterPushdownConfig::NON_FILTERING_BUILD_SIDE_THRESHOLD) {
		return false;
	}

	// per-key filters only pay off when the build side is much smaller than the probe side
	if (!ht->HasSingleEqualityKey() && build_to_probe_ratio > JoinFilterPushdownConfig::MULTI_KEY_RATIO_THRESHOLD) {
		return false;
	}

	return true;
}

struct RuntimeFilterInput {
	unique_ptr<Expression> expression;
	bool preserves_cast_errors;
};

static RuntimeFilterInput CreateRuntimeFilterInputExpression(ClientContext &context,
                                                             const JoinFilterPushdownColumn &column,
                                                             const LogicalType &runtime_type) {
	bool preserves_cast_errors = false;
	auto expression = RuntimeFilterCastUtil::CreateRuntimeFilterInputExpression(context, column, preserves_cast_errors);
	D_ASSERT(expression->GetReturnType() == RuntimeFilterCastUtil::GetRuntimeFilterInputType(column, runtime_type));
	return {std::move(expression), preserves_cast_errors};
}

static unique_ptr<Expression> RetainRuntimeFilterNulls(unique_ptr<Expression> filter_expr,
                                                       unique_ptr<Expression> null_check_input) {
	if (!null_check_input) {
		return filter_expr;
	}
	// Retain NULL join keys and inputs whose original cast can fail.
	auto null_check =
	    ExpressionFilter::CreateNullCheckExpression(std::move(null_check_input), ExpressionType::OPERATOR_IS_NULL);
	return make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(null_check),
	                                             std::move(filter_expr));
}

void JoinFilterPushdownInfo::DeferRuntimeFilter(DeferredRuntimeFilterType type, const PhysicalOperator &op,
                                                const JoinFilterPushdownFilter &info,
                                                const JoinFilterPushdownColumn &column, ProjectionIndex filter_col_idx,
                                                idx_t key_idx, JoinFilterGlobalState &gstate) const {
	gstate.deferred_runtime_filters.push_back({type, &op, info.dynamic_filters, column, filter_col_idx, key_idx});
}

bool JoinFilterPushdownInfo::TryRegisterPrefixRangeFilter(const JoinFilterPushdownFilter &info, ClientContext &context,
                                                          JoinHashTable &ht, const PhysicalOperator &op,
                                                          const JoinFilterPushdownColumn &column,
                                                          ProjectionIndex filter_col_idx, idx_t key_idx,
                                                          const Value &min_val, const Value &max_val, idx_t max_bits,
                                                          JoinFilterGlobalState &gstate) const {
	if (!ht.GetPrefixRangeFilter(key_idx)) {
		const auto key_type = ht.conditions[key_idx].GetLHS().GetReturnType();
		auto prefix_filter = PrefixRangeFilter::CreatePrefixRangeFilter(key_type);
		prefix_filter->Initialize(context, ht.Count(), min_val, max_val, max_bits);
		ht.SetPrefixRangeFilter(key_idx, std::move(prefix_filter));
		ht.SetBuildPrefixRangeFilter();
	}

	DeferRuntimeFilter(DeferredRuntimeFilterType::PREFIX_RANGE, op, info, column, filter_col_idx, key_idx, gstate);
	return true;
}

unique_ptr<DataChunk> JoinFilterPushdownInfo::FinalizeMinMax(JoinFilterGlobalState &gstate) const {
	// finalize the min/max aggregates
	vector<LogicalType> min_max_types;
	for (auto &aggr_expr : min_max_aggregates) {
		min_max_types.push_back(aggr_expr->GetReturnType());
	}
	auto final_min_max = make_uniq<DataChunk>();
	final_min_max->Initialize(Allocator::DefaultAllocator(), min_max_types);

	gstate.global_aggregate_state->Finalize(*final_min_max);
	return final_min_max;
}

static unique_ptr<ExpressionFilter> CreateSelectivityOptionalExpressionFilter(unique_ptr<Expression> child_expr,
                                                                              const LogicalType &column_type,
                                                                              SelectivityOptionalFilterType type) {
	float selectivity_threshold;
	idx_t n_vectors_to_check;
	GetThresholdAndVectorsToCheck(type, selectivity_threshold, n_vectors_to_check);
	return make_uniq<ExpressionFilter>(CreateSelectivityOptionalFilterExpression(
	    std::move(child_expr), column_type, selectivity_threshold, n_vectors_to_check));
}

static SelectivityOptionalFilterType GetSelectivityOptionalFilterType(DeferredRuntimeFilterType type) {
	switch (type) {
	case DeferredRuntimeFilterType::BLOOM_FILTER:
		return SelectivityOptionalFilterType::BF;
	case DeferredRuntimeFilterType::PREFIX_RANGE:
		return SelectivityOptionalFilterType::PRF;
	default:
		throw InternalException("Unknown deferred runtime filter type");
	}
}

static const char *GetRuntimeFilterTypeName(DeferredRuntimeFilterType type) {
	switch (type) {
	case DeferredRuntimeFilterType::BLOOM_FILTER:
		return "BLOOM_FILTER";
	case DeferredRuntimeFilterType::PREFIX_RANGE:
		return "PREFIX_RANGE";
	default:
		throw InternalException("Unknown deferred runtime filter type");
	}
}

static const char *GetRuntimeFilterReconstructionModeName(JoinFilterPushdownMode mode) {
	switch (mode) {
	case JoinFilterPushdownMode::RECONSTRUCT_EXPRESSION:
		return "RECONSTRUCT_EXPRESSION";
	case JoinFilterPushdownMode::STORAGE_ONLY:
		return "STORAGE_ONLY";
	default:
		throw InternalException("Unknown join filter pushdown mode");
	}
}

static unique_ptr<Expression> CreateRuntimeFilterExpression(ClientContext &context, JoinHashTable &ht,
                                                            const DeferredRuntimeFilterPushdown &deferred,
                                                            float selectivity_threshold, idx_t n_vectors_to_check,
                                                            const BloomFilterReadLocality &locality) {
	const auto key_idx = deferred.key_idx;
	const auto key_name = ht.conditions[key_idx].GetRHS().ToString();
	const auto key_type = ht.conditions[key_idx].GetLHS().GetReturnType();
	auto filter_input_type = RuntimeFilterCastUtil::GetRuntimeFilterInputType(deferred.column, key_type);
	auto input = CreateRuntimeFilterInputExpression(context, deferred.column, key_type);
	const auto filters_null_values = !input.preserves_cast_errors && !ht.NullValuesAreEqual(key_idx);
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(input.expression));

	unique_ptr<Expression> filter_expr;
	switch (deferred.type) {
	case DeferredRuntimeFilterType::BLOOM_FILTER: {
		// a filter that is not sealed does not describe the complete build side - publishing it would drop
		// rows - and a filter that saturated is not worth the hashing it costs the scan
		if (!ht.HasPublishableBloomFilter(key_idx)) {
			return nullptr;
		}
		auto bloom_filter = ht.GetBloomFilter(key_idx);
		// the filter escapes to probe-side scans here, so a later recursive iteration must not reuse it
		bloom_filter->MarkPublished();
		filter_expr = make_uniq<BoundFunctionExpression>(
		    BoundScalarFunction(BloomFilterScalarFun::GetFunction(filter_input_type)), std::move(children),
		    make_uniq<BloomFilterFunctionData>(std::move(bloom_filter), filters_null_values, key_name, key_type,
		                                       selectivity_threshold, n_vectors_to_check, locality));
		break;
	}
	case DeferredRuntimeFilterType::PREFIX_RANGE: {
		auto prefix_range_filter = ht.GetPrefixRangeFilter(key_idx);
		D_ASSERT(prefix_range_filter && prefix_range_filter->IsInitialized());
		if (!prefix_range_filter || !prefix_range_filter->IsInitialized()) {
			return nullptr;
		}
		prefix_range_filter->MarkPublished();
		filter_expr = make_uniq<BoundFunctionExpression>(
		    BoundScalarFunction(PrefixRangeScalarFun::GetFunction(filter_input_type)), std::move(children),
		    make_uniq<PrefixRangeFunctionData>(prefix_range_filter, filters_null_values, key_name, key_type,
		                                       selectivity_threshold, n_vectors_to_check));
		break;
	}
	default:
		throw InternalException("Unknown deferred runtime filter type");
	}
	return filter_expr;
}

static void PublishDeferredRuntimeFilters(ClientContext &context, JoinHashTable &ht, JoinFilterGlobalState &gstate,
                                          const vector<BloomFilterReadLocality> &localities = {}) {
	if (gstate.deferred_runtime_filters.empty()) {
		return;
	}
	for (auto &deferred : gstate.deferred_runtime_filters) {
		D_ASSERT(deferred.op);
		if (!deferred.op || !deferred.dynamic_filters) {
			continue;
		}
		const auto filter_type = GetSelectivityOptionalFilterType(deferred.type);
		float selectivity_threshold;
		idx_t n_vectors_to_check;
		GetThresholdAndVectorsToCheck(filter_type, selectivity_threshold, n_vectors_to_check);
		const auto locality =
		    deferred.key_idx < localities.size() ? localities[deferred.key_idx] : BloomFilterReadLocality();
		auto filter_expr =
		    CreateRuntimeFilterExpression(context, ht, deferred, selectivity_threshold, n_vectors_to_check, locality);
		if (!filter_expr) {
			continue;
		}
		deferred.dynamic_filters->PushFilter(*deferred.op, deferred.filter_col_idx,
		                                     CreateSelectivityOptionalExpressionFilter(
		                                         std::move(filter_expr), deferred.column.storage_type, filter_type));
		DUCKDB_LOG(context, PhysicalOperatorLogType, *deferred.op, "PhysicalHashJoin", "PublishRuntimeFilter",
		           {{"kind", GetRuntimeFilterTypeName(deferred.type)},
		            {"storage_type", deferred.column.storage_type.ToString()},
		            {"reconstruction_mode", GetRuntimeFilterReconstructionModeName(deferred.column.mode)},
		            {"uses_try_cast", to_string(RuntimeFilterCastUtil::RuntimeFilterUsesTryCast(deferred.column))}});
	}
	gstate.deferred_runtime_filters.clear();
}

static void CreateDynamicMinMaxFilter(const PhysicalComparisonJoin &op, const JoinFilterPushdownFilter &info,
                                      const ProjectionIndex &filter_col_idx, unique_ptr<Expression> filter_expr,
                                      const LogicalType &column_type, bool selectivity_optional) {
	if (!filter_expr) {
		return;
	}
	if (selectivity_optional) {
		info.dynamic_filters->PushFilter(
		    op, filter_col_idx,
		    CreateSelectivityOptionalExpressionFilter(std::move(filter_expr), column_type,
		                                              SelectivityOptionalFilterType::MIN_MAX));
	} else {
		info.dynamic_filters->PushFilter(
		    op, filter_col_idx,
		    make_uniq<ExpressionFilter>(CreateOptionalFilterExpression(std::move(filter_expr), column_type)));
	}
}

static unique_ptr<Expression> CreateComparisonExpressionFilter(ExpressionType comparison_type,
                                                               unique_ptr<Expression> input, const Value &constant,
                                                               const LogicalType &comparison_logical_type) {
	auto constant_value = constant.DefaultTryCastAs(comparison_logical_type);
	if (!constant_value) {
		return nullptr;
	}
	return BoundComparisonExpression::Create(comparison_type, std::move(input),
	                                         make_uniq<BoundConstantExpression>(std::move(*constant_value)));
}

static unique_ptr<Expression> CreateComparisonExpressionFilter(ExpressionType comparison_type, const Value &constant,
                                                               const LogicalType &column_type) {
	auto column = make_uniq<BoundReferenceExpression>(column_type, 0ULL);
	return CreateComparisonExpressionFilter(comparison_type, std::move(column), constant, column_type);
}

static unique_ptr<Expression> CreateJoinFilterComparisonExpression(
    ClientContext &context, const JoinFilterPushdownColumn &column, ExpressionType comparison_type,
    const Value &constant, const LogicalType &comparison_logical_type, bool reconstruct_expression, bool retain_nulls) {
	if (!reconstruct_expression) {
		D_ASSERT(!retain_nulls);
		return CreateComparisonExpressionFilter(comparison_type, constant, comparison_logical_type);
	}
	auto input = CreateRuntimeFilterInputExpression(context, column, comparison_logical_type);
	auto input_type = input.expression->GetReturnType();
	unique_ptr<Expression> null_check_input;
	if (input.preserves_cast_errors || retain_nulls) {
		null_check_input = input.expression->Copy();
	}
	auto filter_expr =
	    CreateComparisonExpressionFilter(comparison_type, std::move(input.expression), constant, input_type);
	if (!filter_expr) {
		return nullptr;
	}
	return RetainRuntimeFilterNulls(std::move(filter_expr), std::move(null_check_input));
}

static bool CollectInFilterValues(ClientContext &context, Vector &build_vector, const LogicalType &filter_input_type,
                                  idx_t threshold, bool retain_nulls, vector<Value> &in_list) {
	GroupedAggregateHashTable distinct_keys(context, Allocator::Get(context), {filter_input_type});
	DataChunk keys;
	keys.Initialize(context, {filter_input_type});
	Vector input(build_vector.GetType());
	Vector addresses(LogicalType::POINTER);
	SelectionVector new_groups(STANDARD_VECTOR_SIZE);
	CastFunctionSet cast_functions;
	auto cast_input = GetCastFunctionInput::Default();
	for (idx_t offset = 0; offset < build_vector.size(); offset += STANDARD_VECTOR_SIZE) {
		const auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, build_vector.size() - offset);
		keys.Reset();
		input.Slice(build_vector, offset, offset + count);
		if (build_vector.GetType() == filter_input_type) {
			keys.data[0].Reference(input);
		} else {
			string error;
			if (!VectorOperations::TryCast(cast_functions, cast_input, input, keys.data[0], count, &error)) {
				return false;
			}
		}
		keys.CheckCardinality(count);
		const auto new_count = distinct_keys.FindOrCreateGroups(keys, addresses, new_groups);
		for (idx_t i = 0; i < new_count; i++) {
			auto value = keys.data[0].GetValue(new_groups.get_index(i));
			if (value.IsNull()) {
				if (retain_nulls) {
					continue;
				}
				return false;
			}
			in_list.push_back(std::move(value));
			if (in_list.size() > threshold) {
				return false;
			}
		}
	}
	return true;
}

bool JoinFilterPushdownInfo::PushInFilter(ClientContext &context, const JoinFilterPushdownFilter &info,
                                          const JoinFilterPushdownColumn &column, JoinHashTable &ht,
                                          const PhysicalOperator &op, idx_t filter_idx, ProjectionIndex filter_col_idx,
                                          InFilterMode mode) const {
	// Generate an IN filter by scanning the build-side key column.
	auto build_idx = join_condition[filter_idx];
	const auto &runtime_type = ht.conditions[build_idx].GetLHS().GetReturnType();
	Vector tuples_addresses(LogicalType::POINTER, ht.Count());
	Vector build_vector(ht.layout_ptr->GetTypes()[build_idx], ht.Count());
	auto key_count = ht.ScanKeyColumn(tuples_addresses, build_vector, build_idx);
	if (key_count == 0 || key_count != ht.Count()) {
		return false;
	}

	const auto retain_nulls = ht.conditions[build_idx].GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	const auto reconstruct_expression =
	    retain_nulls || RuntimeFilterCastUtil::RequiresRuntimeFilterExpressionReconstruction(column, runtime_type);
	auto filter_input_type = column.storage_type;
	unique_ptr<Expression> filter_input;
	unique_ptr<Expression> null_check_input;
	if (reconstruct_expression) {
		auto input = CreateRuntimeFilterInputExpression(context, column, runtime_type);
		filter_input_type = input.expression->GetReturnType();
		if (input.preserves_cast_errors || retain_nulls) {
			null_check_input = input.expression->Copy();
		}
		filter_input = std::move(input.expression);
	} else {
		filter_input = make_uniq<BoundReferenceExpression>(column.storage_type, static_cast<idx_t>(0));
	}

	const auto dynamic_or_filter_threshold = Settings::Get<DynamicOrFilterThresholdSetting>(context);
	vector<Value> in_list;
	if (!CollectInFilterValues(context, build_vector, filter_input_type, dynamic_or_filter_threshold, retain_nulls,
	                           in_list)) {
		return false;
	}

	// An IN filter is only useful when the values are not dense.
	if (in_list.empty() || FilterCombiner::IsDenseRange(in_list)) {
		return false;
	}

	auto in_expr = ExpressionFilter::CreateInExpression(std::move(filter_input), std::move(in_list));
	in_expr = RetainRuntimeFilterNulls(std::move(in_expr), std::move(null_check_input));
	if (mode == InFilterMode::ADAPTIVE_ROW_FILTERING && column.mode == JoinFilterPushdownMode::RECONSTRUCT_EXPRESSION &&
	    ExpressionFilterExecutor::TryCreate(*in_expr)) {
		auto filter = CreateSelectivityOptionalExpressionFilter(std::move(in_expr), column.storage_type,
		                                                        SelectivityOptionalFilterType::IN_FILTER);
		info.dynamic_filters->PushFilter(op, filter_col_idx, std::move(filter));
		return true;
	}
	auto filter = make_uniq<ExpressionFilter>(CreateOptionalFilterExpression(std::move(in_expr), column.storage_type));
	info.dynamic_filters->PushFilter(op, filter_col_idx, std::move(filter));
	return true;
}

static void CreateDynamicMinMaxFilters(const PhysicalComparisonJoin &op, const JoinFilterPushdownFilter &info,
                                       ClientContext &context, const JoinFilterPushdownColumn &column,
                                       ProjectionIndex filter_col_idx, ExpressionType cmp, const Value &min_val,
                                       const Value &max_val, const LogicalType &condition_type,
                                       bool reconstruct_expression, bool selectivity_optional) {
	auto filter_column_type = reconstruct_expression ? column.storage_type : condition_type;
	const auto retain_nulls = cmp == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	switch (cmp) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
		CreateDynamicMinMaxFilter(
		    op, info, filter_col_idx,
		    CreateJoinFilterComparisonExpression(context, column, ExpressionType::COMPARE_GREATERTHANOREQUALTO, min_val,
		                                         condition_type, reconstruct_expression, retain_nulls),
		    filter_column_type, selectivity_optional);
		break;
	}
	default:
		break;
	}
	switch (cmp) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
		CreateDynamicMinMaxFilter(
		    op, info, filter_col_idx,
		    CreateJoinFilterComparisonExpression(context, column, ExpressionType::COMPARE_LESSTHANOREQUALTO, max_val,
		                                         condition_type, reconstruct_expression, retain_nulls),
		    filter_column_type, selectivity_optional);
		break;
	}
	default:
		break;
	}
}

static idx_t BloomFilterBitBudget(idx_t ht_count) {
	return BloomFilter::GetNumberOfSectors(ht_count) * 64;
}

struct JoinFilterPushdownCandidate {
	const JoinFilterPushdownFilter &info;
	const JoinFilterPushdownColumn &column;
	Value min_val;
	Value max_val;
	bool reconstruct_expression;
	bool can_emit_runtime_filter;
};

struct JoinFilterPushdownInfo::FilterFinalizer {
	const JoinFilterPushdownInfo &pushdown;
	ClientContext &context;
	const PhysicalComparisonJoin &op;
	DataChunk &final_min_max;
	optional_ptr<JoinHashTable> ht;
	optional_ptr<JoinFilterGlobalState> gstate;
	bool allow_bloom_filters;
	bool allow_prefix_range_filters;

public:
	vector<JoinFilterPushdownCandidate> PrepareCandidates() const;
	//! A zero bit budget disables runtime filters when the per-key share is too small.
	idx_t SetRuntimeFilterBudget(const vector<JoinFilterPushdownCandidate> &candidates);
	void FinalizeCandidate(const JoinFilterPushdownCandidate &candidate, idx_t prefix_range_bit_budget);
};

vector<JoinFilterPushdownCandidate> JoinFilterPushdownInfo::FilterFinalizer::PrepareCandidates() const {
	vector<JoinFilterPushdownCandidate> candidates;
	// Prepare each destination once before dividing the budget among distinct keys.
	for (auto &info : pushdown.probe_info) {
		for (auto &pushdown_column : info.columns) {
			auto filter_idx = pushdown_column.join_filter_idx;
			D_ASSERT(filter_idx != DConstants::INVALID_INDEX);
			D_ASSERT(filter_idx < pushdown.join_condition.size());
			const auto key_idx = pushdown.join_condition[filter_idx];
			const auto cmp = op.conditions[key_idx].GetComparisonType();
			auto min_idx = filter_idx * 2;
			auto max_idx = min_idx + 1;

			auto min_val_before_cast = final_min_max.data[min_idx].GetValue(0);
			auto max_val_before_cast = final_min_max.data[max_idx].GetValue(0);

			auto min_val = min_val_before_cast;
			auto max_val = max_val_before_cast;
			const bool retain_nulls = cmp == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
			const bool reconstruct_filter_expression =
			    retain_nulls || RuntimeFilterCastUtil::RequiresRuntimeFilterExpressionReconstruction(
			                        pushdown_column, min_val_before_cast.type());

			// Cast to storage type, skip if fails
			if (pushdown_column.storage_type.IsValid() && !reconstruct_filter_expression) {
				auto cast_min = min_val.DefaultTryCastAs(pushdown_column.storage_type);
				auto cast_max = max_val.DefaultTryCastAs(pushdown_column.storage_type);
				if (!cast_min || !cast_max) {
					continue;
				}
				min_val = std::move(*cast_min);
				max_val = std::move(*cast_max);
			}

			if (min_val.IsNull() || max_val.IsNull()) {
				// min/max is NULL
				// this can happen in case all values in the RHS column are NULL, but they are still pushed into the
				// hash table e.g. because they are part of a RIGHT join
				continue;
			}

			auto condition_type = min_val.type();
			auto runtime_filter_input_type =
			    RuntimeFilterCastUtil::GetRuntimeFilterInputType(pushdown_column, condition_type);
			const bool can_emit_runtime_filter =
			    ht && gstate && (allow_bloom_filters || allow_prefix_range_filters) &&
			    pushdown_column.mode == JoinFilterPushdownMode::RECONSTRUCT_EXPRESSION &&
			    runtime_filter_input_type == ht->conditions[key_idx].GetLHS().GetReturnType() &&
			    !Value::NotDistinctFrom(min_val, max_val) && pushdown.CanUseBloomFilter(info, context, op, cmp, ht);
			candidates.push_back({info, pushdown_column, std::move(min_val), std::move(max_val),
			                      reconstruct_filter_expression, can_emit_runtime_filter});
		}
	}
	return candidates;
}

idx_t JoinFilterPushdownInfo::FilterFinalizer::SetRuntimeFilterBudget(
    const vector<JoinFilterPushdownCandidate> &candidates) {
	if (!ht || ht->HasSingleEqualityKey()) {
		return NumericLimits<idx_t>::Maximum();
	}

	vector<bool> runtime_filter_keys(op.conditions.size(), false);
	idx_t runtime_filter_count = 0;
	for (auto &candidate : candidates) {
		const auto key_idx = pushdown.join_condition[candidate.column.join_filter_idx];
		if (candidate.can_emit_runtime_filter && !runtime_filter_keys[key_idx]) {
			runtime_filter_keys[key_idx] = true;
			runtime_filter_count++;
		}
	}
	const auto budget_share = MaxValue<idx_t>(runtime_filter_count, 1);
	const auto memory_budget = GetMemoryBudget(context);
	ht->SetBloomFilterBudget(memory_budget / budget_share);
	// Leave room for at least one complete build lane as well as the published bitmaps.
	const auto prefix_range_budget = MinValue(memory_budget, MAX_PREFIX_RANGE_BUILD_MEMORY);
	const auto column_budget = prefix_range_budget / budget_share;
	return column_budget >= 128 ? ((column_budget - 64) / 8) * 64 : 0;
}

void JoinFilterPushdownInfo::FilterFinalizer::FinalizeCandidate(const JoinFilterPushdownCandidate &candidate,
                                                                idx_t prefix_range_bit_budget) {
	auto &info = candidate.info;
	auto &pushdown_column = candidate.column;
	const auto filter_idx = pushdown_column.join_filter_idx;
	const auto key_idx = pushdown.join_condition[filter_idx];
	const auto cmp = op.conditions[key_idx].GetComparisonType();
	auto &filter_col_idx = pushdown_column.probe_column_index.column_index;
	auto &min_val = candidate.min_val;
	auto &max_val = candidate.max_val;
	const auto &condition_type = min_val.type();
	const bool retain_nulls = cmp == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	const auto reconstruct_filter_expression = candidate.reconstruct_expression;
	const auto can_emit_runtime_filters = prefix_range_bit_budget > 0 && candidate.can_emit_runtime_filter;
	if (Value::NotDistinctFrom(min_val, max_val)) {
		// A single build value produces one comparison, including for non-equality joins.
		auto filter_expr = CreateJoinFilterComparisonExpression(context, pushdown_column, cmp, min_val, condition_type,
		                                                        reconstruct_filter_expression, retain_nulls);
		if (filter_expr) {
			info.dynamic_filters->PushFilter(op, filter_col_idx, make_uniq<ExpressionFilter>(std::move(filter_expr)));
		}
		return;
	}
	if (cmp != ExpressionType::COMPARE_EQUAL && !retain_nulls) {
		// Non-equality comparisons need a half-open range.
		CreateDynamicMinMaxFilters(op, info, context, pushdown_column, filter_col_idx, cmp, min_val, max_val,
		                           condition_type, reconstruct_filter_expression, true);
		return;
	}

	auto min_val_before_cast = final_min_max.data[filter_idx * 2].GetValue(0);
	auto max_val_before_cast = final_min_max.data[filter_idx * 2 + 1].GetValue(0);
	uhugeint_t span;
	const auto can_compute_span = PrefixRangeFilter::TryComputeSpan(min_val_before_cast, max_val_before_cast, span);
	const auto can_emit_prf = allow_prefix_range_filters && can_emit_runtime_filters &&
	                          PrefixRangeFilter::SupportedType(ht->conditions[key_idx].GetLHS().GetReturnType()) &&
	                          can_compute_span;

	const auto exact_prf_bits =
	    MinValue<idx_t>(JoinFilterPushdownConfig::SMALL_EXACT_PRF_BITS, prefix_range_bit_budget);
	bool registered_prf =
	    can_emit_prf && span < exact_prf_bits &&
	    pushdown.TryRegisterPrefixRangeFilter(info, context, *ht, op, pushdown_column, filter_col_idx, key_idx,
	                                          min_val_before_cast, max_val_before_cast, exact_prf_bits, *gstate);

	if (can_emit_prf && !registered_prf) {
		auto build_count = ht->Count();
		if (build_count == 0) {
			build_count = ht->GetSinkCollection().Count();
		}
		const auto bloom_filter_bits = MinValue(BloomFilterBitBudget(build_count), prefix_range_bit_budget);
		registered_prf =
		    span <= bloom_filter_bits &&
		    pushdown.TryRegisterPrefixRangeFilter(info, context, *ht, op, pushdown_column, filter_col_idx, key_idx,
		                                          min_val_before_cast, max_val_before_cast, bloom_filter_bits, *gstate);
	}
	const auto can_use_bloom = !registered_prf && allow_bloom_filters && can_emit_runtime_filters &&
	                           (ht->HasPublishableBloomFilter(key_idx) || allow_prefix_range_filters);
	bool pushed_in_filter = false;
	if (pushdown.CanUseInFilter(context, ht, cmp)) {
		// String prefixes can leave false positives that a small IN filter removes.
		const auto prefer_prf =
		    registered_prf && ht->conditions[key_idx].GetLHS().GetReturnType().InternalType() != PhysicalType::VARCHAR;
		const auto mode =
		    prefer_prf || can_use_bloom ? InFilterMode::PRUNING_ONLY : InFilterMode::ADAPTIVE_ROW_FILTERING;
		pushed_in_filter =
		    pushdown.PushInFilter(context, info, pushdown_column, *ht, op, filter_idx, filter_col_idx, mode);
	}
	// Keep multi-key min/max filters for their complementary statistics pruning.
	if (registered_prf && ht->HasSingleEqualityKey()) {
		return;
	}

	if (!pushed_in_filter) {
		CreateDynamicMinMaxFilters(op, info, context, pushdown_column, filter_col_idx, cmp, min_val, max_val,
		                           condition_type, reconstruct_filter_expression, true);
	}
	if (can_use_bloom) {
		if (ht->HasPublishableBloomFilter(key_idx)) {
			// A complete filter was accumulated during Sink (external joins build it there).
			pushdown.DeferRuntimeFilter(DeferredRuntimeFilterType::BLOOM_FILTER, op, info, pushdown_column,
			                            filter_col_idx, key_idx, *gstate);
		} else if (allow_prefix_range_filters) {
			// Build during finalize only when it materializes the whole build side; external finalize does not.
			ht->SetBuildBloomFilter(key_idx);
			pushdown.DeferRuntimeFilter(DeferredRuntimeFilterType::BLOOM_FILTER, op, info, pushdown_column,
			                            filter_col_idx, key_idx, *gstate);
		}
	}
}

unique_ptr<DataChunk> JoinFilterPushdownInfo::FinalizeFilters(ClientContext &context, const PhysicalComparisonJoin &op,
                                                              unique_ptr<DataChunk> final_min_max,
                                                              optional_ptr<JoinHashTable> ht, bool allow_bloom_filters,
                                                              bool allow_prefix_range_filters,
                                                              optional_ptr<JoinFilterGlobalState> gstate) const {
	if (probe_info.empty()) {
		return final_min_max;
	}

	FilterFinalizer finalizer {
	    *this, context, op, *final_min_max, ht, gstate, allow_bloom_filters, allow_prefix_range_filters};
	auto candidates = finalizer.PrepareCandidates();
	const auto prefix_range_bit_budget = finalizer.SetRuntimeFilterBudget(candidates);
	for (auto &candidate : candidates) {
		finalizer.FinalizeCandidate(candidate, prefix_range_bit_budget);
	}
	return final_min_max;
}

unique_ptr<DataChunk> JoinFilterPushdownInfo::Finalize(ClientContext &context, JoinFilterGlobalState &gstate,
                                                       const PhysicalComparisonJoin &op,
                                                       optional_ptr<JoinHashTable> ht) const {
	auto final_min_max = FinalizeMinMax(gstate);
	return FinalizeFilters(context, op, std::move(final_min_max), ht, true, true, &gstate);
}

enum class HashJoinFilterCopyPhase { PRIMARY, REPLICA };

struct HashJoinFilterCopy {
	HashJoinFilterCopy(idx_t key_idx, shared_ptr<BloomFilter> filter) : key_idx(key_idx), filter(std::move(filter)) {
	}

	idx_t key_idx;
	shared_ptr<BloomFilter> filter;
	HugeAllocation buffer;
	optional_idx replica_group;
	atomic<bool> claimed {false};
};

struct HashJoinFilterCopies {
	explicit HashJoinFilterCopies(idx_t shard_count) : shard_count(shard_count) {
	}

	idx_t shard_count;
	vector<unique_ptr<HashJoinFilterCopy>> filters;
};

class HashJoinFilterCopyTask : public ExecutorTask {
public:
	HashJoinFilterCopyTask(ClientContext &context, const PhysicalOperator &op, shared_ptr<Event> event,
	                       HashJoinFilterCopy &copy, idx_t shard_count, HashJoinFilterCopyPhase phase)
	    : ExecutorTask(context, std::move(event), op), context(context), copy(copy), shard_count(shard_count),
	      phase(phase) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		const auto group = BlockAllocator::GetLocalShardGroup(shard_count);
		const auto force = Settings::Get<DebugForceBloomFilterReplicationSetting>(context);
		if (phase == HashJoinFilterCopyPhase::PRIMARY || ((force || group == copy.replica_group.GetIndex()) &&
		                                                  !copy.claimed.exchange(true, std::memory_order_relaxed))) {
			try {
				auto buffer = copy.filter->CopyBuffer(context);
				const auto home = buffer.GetAllocationShard();
				if (force || home.IsValid()) {
					const auto buffer_group =
					    force ? group : BlockAllocator::GetShardGroup(home.GetIndex(), shard_count);
					if (phase == HashJoinFilterCopyPhase::PRIMARY) {
						copy.replica_group = optional_idx(force ? group : 1 - buffer_group);
						copy.buffer = std::move(buffer);
					} else if (force || buffer_group == copy.replica_group.GetIndex()) {
						copy.buffer = std::move(buffer);
					}
				}
			} catch (const OutOfMemoryException &) {
				// Optional copies may fail; the completed primary remains readable.
			} catch (const std::bad_alloc &) {
			}
		}
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "HashJoinFilterCopy";
	}

private:
	ClientContext &context;
	HashJoinFilterCopy &copy;
	idx_t shard_count;
	HashJoinFilterCopyPhase phase;
};

class HashJoinFilterCopyEvent : public BasePipelineEvent {
public:
	HashJoinFilterCopyEvent(Pipeline &pipeline, const PhysicalOperator &op, JoinHashTable &ht,
	                        JoinFilterGlobalState &filters, shared_ptr<HashJoinFilterCopies> copies,
	                        HashJoinFilterCopyPhase phase = HashJoinFilterCopyPhase::PRIMARY)
	    : BasePipelineEvent(pipeline), op(op), ht(ht), filters(filters), copies(std::move(copies)), phase(phase) {
	}

	void Schedule() override {
		vector<shared_ptr<Task>> tasks;
		for (auto &copy : copies->filters) {
			if (phase == HashJoinFilterCopyPhase::REPLICA && !copy->replica_group.IsValid()) {
				continue;
			}
			// Bounded opportunities on the other group; never wait for a particular worker.
			const idx_t task_count = phase == HashJoinFilterCopyPhase::PRIMARY ? 1 : 16;
			for (idx_t i = 0; i < task_count; i++) {
				tasks.push_back(make_uniq<HashJoinFilterCopyTask>(pipeline->GetClientContext(), op, shared_from_this(),
				                                                  *copy, copies->shard_count, phase));
			}
		}
		SetTasks(std::move(tasks));
	}

	void FinishEvent() override {
		vector<BloomFilterReadLocality> localities(ht.GetBloomFilters().size());
		bool replaced_primary = false;
		for (auto &copy : copies->filters) {
			if (!copy->buffer.IsSet()) {
				continue;
			}
			if (phase == HashJoinFilterCopyPhase::PRIMARY) {
				copy->filter->ReplaceBuffer(std::move(copy->buffer));
				replaced_primary = true;
			} else {
				copy->filter->InstallReadCopy(std::move(copy->buffer));
				localities[copy->key_idx] = {copies->shard_count, copy->replica_group};
			}
		}
		if (replaced_primary) {
			// All old primaries have been released before a second buffer can be allocated.
			InsertEvent(make_shared_ptr<HashJoinFilterCopyEvent>(*pipeline, op, ht, filters, copies,
			                                                     HashJoinFilterCopyPhase::REPLICA));
			return;
		}
		PublishDeferredRuntimeFilters(pipeline->GetClientContext(), ht, filters, localities);
		ht.finalized = true;
	}

private:
	const PhysicalOperator &op;
	JoinHashTable &ht;
	JoinFilterGlobalState &filters;
	shared_ptr<HashJoinFilterCopies> copies;
	HashJoinFilterCopyPhase phase;
};

void JoinFilterPushdownInfo::FinalizeRuntimeFilters(Pipeline &pipeline, Event &event, const PhysicalOperator &op,
                                                    JoinHashTable &ht, optional_ptr<JoinFilterGlobalState> filter_state,
                                                    idx_t num_threads) {
	auto &context = pipeline.GetClientContext();
	if (filter_state) {
		ht.SealBloomFilters();
		auto &gstate = *filter_state;
		const auto shard_count = BlockAllocator::Get(*context.db).GetFallbackShardCount();
		const auto force = Settings::Get<DebugForceBloomFilterReplicationSetting>(context);
		if ((force || (num_threads > 128 && shard_count > 32)) && !gstate.deferred_runtime_filters.empty()) {
			auto copies = make_shared_ptr<HashJoinFilterCopies>(shard_count);
			vector<bool> considered(ht.GetBloomFilters().size(), false);
			auto remaining = GetMemoryBudget(context);
			for (auto &filter : ht.GetBloomFilters()) {
				if (filter) {
					remaining -= MinValue(remaining, filter->GetAllocationSize());
				}
			}
			for (auto &deferred : gstate.deferred_runtime_filters) {
				if (deferred.type != DeferredRuntimeFilterType::BLOOM_FILTER || considered[deferred.key_idx]) {
					continue;
				}
				considered[deferred.key_idx] = true;
				auto filter = ht.GetBloomFilter(deferred.key_idx);
				if (!filter || filter->IsPublished() || !ht.HasPublishableBloomFilter(deferred.key_idx)) {
					continue;
				}
				const auto bytes = filter->GetBitmapSize();
				const auto budget = filter->GetMemoryBudget();
				if ((!force && bytes < (1ULL << 20)) || bytes > (256ULL << 20) || bytes > budget / 2 ||
				    filter->GetAllocationSize() > budget - bytes || bytes > remaining) {
					continue;
				}
				remaining -= bytes;
				copies->filters.push_back(make_uniq<HashJoinFilterCopy>(deferred.key_idx, std::move(filter)));
			}
			if (!copies->filters.empty()) {
				event.InsertEvent(
				    make_shared_ptr<HashJoinFilterCopyEvent>(pipeline, op, ht, gstate, std::move(copies)));
				return;
			}
		}
		PublishDeferredRuntimeFilters(context, ht, gstate);
	}
	ht.finalized = true;
}

} // namespace duckdb
