#include "duckdb/execution/operator/join/physical_hash_join.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/projection_index.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/execution/operator/aggregate/ungrouped_aggregate_state.hpp"
#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

PhysicalHashJoin::PhysicalHashJoin(PhysicalPlan &physical_plan, LogicalOperator &op, PhysicalOperator &left,
                                   PhysicalOperator &right, vector<JoinCondition> conds, JoinType join_type,
                                   const vector<ProjectionIndex> &left_projection_map,
                                   const vector<ProjectionIndex> &right_projection_map, vector<LogicalType> delim_types,
                                   idx_t estimated_cardinality, unique_ptr<JoinFilterPushdownInfo> pushdown_info_p)
    : PhysicalComparisonJoin(physical_plan, op, PhysicalOperatorType::HASH_JOIN, std::move(conds), join_type,
                             estimated_cardinality),
      delim_types(std::move(delim_types)) {
	filter_pushdown = std::move(pushdown_info_p);

	children.push_back(left);
	children.push_back(right);

	auto &lhs_input_types = children[0].get().GetTypes();
	auto &rhs_input_types = children[1].get().GetTypes();

	for (auto &condition : conditions) {
		D_ASSERT(condition.IsComparison());
		condition_types.push_back(condition.GetLHS().GetReturnType());
	}

	vector<idx_t> probe_cols;
	vector<idx_t> build_cols;

	if (predicate) {
		ExtractResidualPredicateColumns(predicate, lhs_input_types.size(), probe_cols, build_cols);
		residual_info = make_uniq<ResidualPredicateInfo>();
	}

	// build lhs_output_columns
	lhs_output_columns.col_idxs = FillProjectionMap(left, left_projection_map);
	for (auto &lhs_col : lhs_output_columns.col_idxs) {
		lhs_output_columns.col_types.push_back(lhs_input_types[lhs_col]);
	}

	// initialize residual predicate structures if present
	if (residual_info) {
		InitializeResidualPredicate(lhs_input_types, probe_cols);
	} else {
		// lhs_probe_columns = lhs_output_columns
		lhs_probe_columns = lhs_output_columns;
		lhs_output_in_probe.reserve(lhs_output_columns.col_idxs.size());
		for (idx_t i = 0; i < lhs_output_columns.col_idxs.size(); i++) {
			lhs_output_in_probe.push_back(i);
		}
	}

	// store probe types
	if (residual_info) {
		residual_info->probe_types = lhs_probe_columns.col_types;
	}

	// handle build side (RHS)
	InitializeBuildSide(lhs_input_types, rhs_input_types, right_projection_map, build_cols);
}

PhysicalHashJoin::PhysicalHashJoin(PhysicalPlan &physical_plan, LogicalOperator &op, PhysicalOperator &left,
                                   PhysicalOperator &right, vector<JoinCondition> cond, JoinType join_type,
                                   idx_t estimated_cardinality)
    : PhysicalHashJoin(physical_plan, op, left, right, std::move(cond), join_type, {}, {}, {}, estimated_cardinality,
                       nullptr) {
}

void PhysicalHashJoin::ExtractResidualPredicateColumns(unique_ptr<Expression> &predicate, idx_t probe_column_count,
                                                       vector<idx_t> &probe_column_ids,
                                                       vector<idx_t> &build_column_ids) {
	unordered_set<idx_t> probe_cols;
	unordered_set<idx_t> build_cols;

	ExpressionIterator::EnumerateExpression(predicate, [&](unique_ptr<Expression> &expr) {
		if (expr->GetExpressionClass() == ExpressionClass::BOUND_REF) {
			auto &ref = expr->Cast<BoundReferenceExpression>();
			idx_t col_idx = ref.Index();

			if (col_idx < probe_column_count) {
				// Probe (LHS) column
				probe_cols.insert(col_idx);
			} else {
				// Build (RHS) column
				build_cols.insert(col_idx);
			}
		}
	});

	probe_column_ids.assign(probe_cols.begin(), probe_cols.end());
	build_column_ids.assign(build_cols.begin(), build_cols.end());
}

void PhysicalHashJoin::InitializeResidualPredicate(const vector<LogicalType> &lhs_input_types,
                                                   const vector<idx_t> &probe_cols) {
	D_ASSERT(residual_info);
	// build lhs_probe_columns (output + predicate columns)
	unordered_set<idx_t> required_probe_cols;
	for (auto col : lhs_output_columns.col_idxs) {
		required_probe_cols.insert(col);
	}
	for (auto col : probe_cols) {
		required_probe_cols.insert(col);
	}

	lhs_probe_columns.col_idxs.assign(required_probe_cols.begin(), required_probe_cols.end());
	std::sort(lhs_probe_columns.col_idxs.begin(), lhs_probe_columns.col_idxs.end());

	for (auto col_idx : lhs_probe_columns.col_idxs) {
		lhs_probe_columns.col_types.push_back(lhs_input_types[col_idx]);
	}

	// build mapping for predicate probe columns
	for (auto predicate_col_idx : probe_cols) {
		for (idx_t i = 0; i < lhs_probe_columns.col_idxs.size(); i++) {
			if (lhs_probe_columns.col_idxs[i] == predicate_col_idx) {
				residual_info->probe_input_to_probe_map[predicate_col_idx] = i;
				break;
			}
		}
	}

	// build lhs_output_in_probe mapping
	lhs_output_in_probe.reserve(lhs_output_columns.col_idxs.size());
	for (auto output_col_idx : lhs_output_columns.col_idxs) {
		for (idx_t i = 0; i < lhs_probe_columns.col_idxs.size(); i++) {
			if (lhs_probe_columns.col_idxs[i] == output_col_idx) {
				lhs_output_in_probe.push_back(i);
				break;
			}
		}
	}
}

void PhysicalHashJoin::InitializeBuildSide(const vector<LogicalType> &lhs_input_types,
                                           const vector<LogicalType> &rhs_input_types,
                                           const vector<ProjectionIndex> &right_projection_map,
                                           const vector<idx_t> &build_cols) {
	unordered_map<idx_t, idx_t> build_columns_in_conditions;
	unordered_map<idx_t, idx_t> build_input_to_layout;

	// Only consider comparison conditions for the hash join conditions
	idx_t cond_idx = 0;
	for (auto &condition : conditions) {
		if (condition.GetRHS().GetExpressionClass() == ExpressionClass::BOUND_REF) {
			auto build_input_idx = condition.GetRHS().Cast<BoundReferenceExpression>().Index();
			build_columns_in_conditions.emplace(build_input_idx, cond_idx);
		}
		cond_idx++;
	}

	// handle SEMI/ANTI/MARK joins
	if (join_type == JoinType::ANTI || join_type == JoinType::SEMI || join_type == JoinType::MARK) {
		MapResidualBuildColumns(lhs_input_types, rhs_input_types, build_cols, build_columns_in_conditions,
		                        build_input_to_layout);

		if (residual_info) {
			residual_info->build_input_to_layout_map = std::move(build_input_to_layout);
		}
		return;
	}

	// for other join types
	auto right_projection_map_copy = FillProjectionMap(children[1].get(), right_projection_map);

	// map ALL predicate columns (both in conditions and not)
	MapResidualBuildColumns(lhs_input_types, rhs_input_types, build_cols, build_columns_in_conditions,
	                        build_input_to_layout);

	// build rhs_output_columns
	for (auto &rhs_col : right_projection_map_copy) {
		auto &rhs_col_type = rhs_input_types[rhs_col];
		idx_t rhs_col_with_offset = lhs_input_types.size() + rhs_col;

		auto it = build_columns_in_conditions.find(rhs_col);
		if (it != build_columns_in_conditions.end()) {
			// it's in join conditions
			rhs_output_columns.col_idxs.push_back(it->second);
		} else {
			// check if already in payload (from predicate)
			auto layout_it = build_input_to_layout.find(rhs_col_with_offset);
			if (layout_it != build_input_to_layout.end()) {
				rhs_output_columns.col_idxs.push_back(layout_it->second);
			} else {
				// new column - add to payload
				idx_t layout_pos = condition_types.size() + payload_columns.col_idxs.size();
				payload_columns.col_idxs.push_back(rhs_col);
				payload_columns.col_types.push_back(rhs_col_type);
				rhs_output_columns.col_idxs.push_back(layout_pos);
			}
		}
		rhs_output_columns.col_types.push_back(rhs_col_type);
	}

	if (residual_info) {
		residual_info->build_input_to_layout_map = std::move(build_input_to_layout);
	}
}

void PhysicalHashJoin::MapResidualBuildColumns(const vector<LogicalType> &lhs_input_types,
                                               const vector<LogicalType> &rhs_input_types,
                                               const vector<idx_t> &build_cols,
                                               const unordered_map<idx_t, idx_t> &build_columns_in_conditions,
                                               unordered_map<idx_t, idx_t> &build_input_to_layout) {
	if (!residual_info) {
		return;
	}

	for (auto rhs_idx_with_offset : build_cols) {
		idx_t rhs_idx = rhs_idx_with_offset - lhs_input_types.size();
		auto it = build_columns_in_conditions.find(rhs_idx);

		if (it != build_columns_in_conditions.end()) {
			// column IS in conditions
			build_input_to_layout[rhs_idx_with_offset] = it->second;
		} else {
			// column NOT in conditions - add to payload
			idx_t layout_pos = condition_types.size() + payload_columns.col_idxs.size();
			build_input_to_layout[rhs_idx_with_offset] = layout_pos;
			payload_columns.col_idxs.push_back(rhs_idx);
			payload_columns.col_types.push_back(rhs_input_types[rhs_idx]);
		}
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
JoinFilterGlobalState::~JoinFilterGlobalState() {
}

JoinFilterLocalState::~JoinFilterLocalState() {
}

static bool CanUsePerfectHashJoin(const PhysicalHashJoin &op, PerfectHashJoinExecutor &perfect_join_executor) {
	if (op.conditions.size() != 1 || !op.conditions[0].GetRightStats()) {
		return false;
	}
	const auto &right_stats = *op.conditions[0].GetRightStats();
	if (!TypeIsIntegral(right_stats.GetType().InternalType()) || !NumericStats::HasMinMax(right_stats)) {
		return false;
	}
	return perfect_join_executor.CanDoPerfectHashJoin(op, NumericStats::Min(right_stats),
	                                                  NumericStats::Max(right_stats));
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

//! True iff the build subtree funnels multiple producer pipelines into one sink (UNION ALL, recursive CTE),
//! breaking the "decide layout once on the first chunk" contract. Conservative: may over-exclude, never misses one.
static bool BuildSideHasMultipleSources(const PhysicalOperator &op) {
	if (op.type == PhysicalOperatorType::UNION || op.type == PhysicalOperatorType::RECURSIVE_CTE ||
	    op.type == PhysicalOperatorType::RECURSIVE_KEY_CTE) {
		return true;
	}
	for (const auto &child : op.children) {
		if (BuildSideHasMultipleSources(child.get())) {
			return true;
		}
	}
	return false;
}

//! Synchronisation state for the first-chunk publication of the TupleDataLayout: the canonical layout
//! shared by all per-thread JHTs, plus the per-column decision on whether to narrow the row-store slot.
struct LayoutGate {
	mutex publish_mutex;
	atomic<bool> published {false};
	shared_ptr<TupleDataLayout> layout_ptr;
	vector<uint8_t> dict_index_width;

	void Reset() {
		published.store(false, std::memory_order_release);
		layout_ptr.reset();
		dict_index_width.clear();
	}
};

class HashJoinGlobalSinkState : public GlobalSinkState {
public:
	HashJoinGlobalSinkState(const PhysicalHashJoin &op_p, ClientContext &context_p)
	    : context(context_p), op(op_p), num_threads(TaskScheduler::GetScheduler(context).NumberOfThreads()),
	      temporary_memory_state(TemporaryMemoryManager::Get(context).Register(context)),
	      initial_radix_bits(num_threads < 100 ? 4 : 5), finalized(false), active_local_states(0), total_size(0),
	      max_partition_size(0), max_partition_count(0), probe_side_requirement(0), scanned_data(false) {
		hash_table = op.InitializeHashTable(context, initial_radix_bits);

		// For perfect hash join
		perfect_join_executor = make_uniq<PerfectHashJoinExecutor>(op, *hash_table);
		auto use_perfect_hash = CanUsePerfectHashJoin(op, *perfect_join_executor);
		can_use_perfect_hash = use_perfect_hash;
		// A multi-source build side (UNION ALL / recursive CTE) feeds the sink from several producers,
		// disqualifying dict-surviving. Computed once from the static plan; cannot change at runtime.
		build_side_multi_source = BuildSideHasMultipleSources(op.children[1].get());
		// For external hash join
		external = Settings::Get<DebugForceExternalSetting>(context);
		// Set probe types
		probe_types = op.children[0].get().GetTypes();
		probe_types.emplace_back(LogicalType::HASH);

		if (op.filter_pushdown) {
			if (op.filter_pushdown->probe_info.empty() && use_perfect_hash) {
				// Only computing min/max to check for perfect HJ, but we already can
				skip_filter_pushdown = true;
			}
			global_filter_state = op.filter_pushdown->GetGlobalState(context, op);
		}
	}

	~HashJoinGlobalSinkState() override {
		DUCKDB_LOG(context, PhysicalOperatorLogType, op, "PhysicalHashJoin", "GetData",
		           {{"total_probe_matches", to_string(hash_table->total_probe_matches)}});
	}

	void ScheduleFinalize(Pipeline &pipeline, Event &event);
	void InitializeProbeSpill();
	//! First-chunk election: build the canonical layout (with per-column slot narrowing) and publish it.
	//! Idempotent and safe to call concurrently; only the first thread runs the slow path.
	void PublishLayoutIfFirst(class HashJoinLocalSinkState &lstate, DataChunk &payload_chunk);
	//! True iff at least one column on the global JHT carries a pinned upstream dictionary entry.
	bool DictSurvivingActive() const;

	bool SupportsReuse() const override {
		return true;
	}

	void Reset(ClientContext &context) override {
		// Use single-partition mode on subsequent iterations to avoid 16-partition overhead for small joins.
		hash_table->ResetForNewIterationSinglePartition();
		perfect_join_executor = make_uniq<PerfectHashJoinExecutor>(op, *hash_table);
		auto use_perfect_hash = CanUsePerfectHashJoin(op, *perfect_join_executor);
		can_use_perfect_hash = use_perfect_hash;
		finalized = false;
		active_local_states = 0;
		external = Settings::Get<DebugForceExternalSetting>(context);
		total_size = 0;
		max_partition_size = 0;
		max_partition_count = 0;
		probe_side_requirement = 0;
		local_hash_tables.clear();
		owned_local_hash_tables.clear();
		probe_spill.reset();
		scanned_data = false;
		preserve_build_for_reuse = false;
		skip_filter_pushdown = false;
		global_filter_state.reset();
		temporary_memory_state->SetZero();
		keep_local_hash_tables = true;
		if (op.filter_pushdown) {
			if (op.filter_pushdown->probe_info.empty() && use_perfect_hash) {
				skip_filter_pushdown = true;
			}
			global_filter_state = op.filter_pushdown->GetGlobalState(context, op);
		}
		// Keep the published layout across CTE iterations (same upstream operator, same arrival types).
		// ResetForNewIterationSinglePartition already cleared the row data and dict_registry.
		GlobalSinkState::Reset(context);
	}

public:
	ClientContext &context;
	const PhysicalHashJoin &op;

	const idx_t num_threads;
	//! Temporary memory state for managing this operator's memory usage
	unique_ptr<TemporaryMemoryState> temporary_memory_state;

	//! Initial radix bits
	const idx_t initial_radix_bits;
	//! Global HT used by the join
	unique_ptr<JoinHashTable> hash_table;
	//! The perfect hash join executor (if any)
	unique_ptr<PerfectHashJoinExecutor> perfect_join_executor;
	//! Whether or not the hash table has been finalized
	bool finalized;
	//! The number of active local states
	atomic<idx_t> active_local_states;

	//! Whether we are doing an external + some sizes
	bool external;
	idx_t total_size;
	idx_t max_partition_size;
	idx_t max_partition_count;
	idx_t probe_side_requirement;

	//! Hash tables built by each thread (owned by the corresponding local sink states)
	vector<reference<JoinHashTable>> local_hash_tables;
	//! Local hash tables whose ownership has been transferred during the normal one-shot finalize path
	vector<unique_ptr<JoinHashTable>> owned_local_hash_tables;

	//! Excess probe data gathered during Sink
	vector<LogicalType> probe_types;
	unique_ptr<JoinHashTable::ProbeSpill> probe_spill;

	//! Whether or not we have started scanning data using GetData
	atomic<bool> scanned_data;
	//! Preserve the finalized build-side state across recursive CTE iterations
	bool preserve_build_for_reuse = false;

	bool skip_filter_pushdown = false;
	unique_ptr<JoinFilterGlobalState> global_filter_state;
	bool keep_local_hash_tables = false;

	//! Coordinates first-chunk publication of the TupleDataLayout across parallel sinks.
	LayoutGate layout_gate;
	//! True iff this join may use perfect-hash-join at Finalize. PHJ's FullScanHashTable reads payload at native
	//! width, so it disables dict-surviving slot narrowing.
	bool can_use_perfect_hash = false;
	//! True iff the build subtree funnels multiple producer pipelines into this sink (UNION ALL /
	//! recursive CTE); disables dict-surviving because the first-chunk layout election is unsound there.
	bool build_side_multi_source = false;
};

unique_ptr<JoinFilterLocalState> JoinFilterPushdownInfo::GetLocalState(JoinFilterGlobalState &gstate) const {
	auto result = make_uniq<JoinFilterLocalState>();
	result->local_aggregate_state = make_uniq<LocalUngroupedAggregateState>(*gstate.global_aggregate_state);
	return result;
}

class HashJoinLocalSinkState : public LocalSinkState {
public:
	HashJoinLocalSinkState(const PhysicalHashJoin &op, ClientContext &context, HashJoinGlobalSinkState &gstate)
	    : op(op), join_key_executor(context) {
		auto &allocator = BufferAllocator::Get(context);

		for (auto &cond : op.conditions) {
			join_key_executor.AddExpression(cond.GetRHS());
		}
		join_keys.Initialize(allocator, op.condition_types);

		if (!op.payload_columns.col_types.empty()) {
			payload_chunk.Initialize(allocator, op.payload_columns.col_types);
		}

		hash_table = op.InitializeHashTable(context, gstate.hash_table->GetRadixBits());
		// sink_collection exists only after the layout is published on the first build chunk, so
		// InitializeAppendState runs lazily inside Sink.
		keep_hash_table = gstate.keep_local_hash_tables;

		gstate.active_local_states++;

		if (op.filter_pushdown) {
			local_filter_state = op.filter_pushdown->GetLocalState(*gstate.global_filter_state);
		}
	}

public:
	const PhysicalHashJoin &op;
	PartitionedTupleDataAppendState append_state;
	//! True once InitializeAppendState has been called against the published sink_collection
	bool append_state_initialised = false;

	ExpressionExecutor join_key_executor;
	DataChunk join_keys;

	DataChunk payload_chunk;

	//! Thread-local HT
	unique_ptr<JoinHashTable> hash_table;
	bool keep_hash_table = false;

	unique_ptr<JoinFilterLocalState> local_filter_state;

	bool SupportsReuse() const override {
		return true;
	}

	void Reset(ExecutionContext &context, GlobalSinkState &gstate_p) override {
		auto &gstate = gstate_p.Cast<HashJoinGlobalSinkState>();
		join_keys.Reset();
		payload_chunk.Reset();
		if (hash_table && append_state_initialised) {
			// the layout survives the iteration; only the row data is dropped
			hash_table->ResetForNewIterationSinglePartition();
			hash_table->GetSinkCollection().ResetAppendState(append_state);
		} else {
			// HT was moved into gstate during Combine, or never had a layout published. Rebuild against the global
			// radix_bits so partition counts stay consistent in PrepareFinalize.
			hash_table = op.InitializeHashTable(context.client, gstate.hash_table->GetRadixBits());
			append_state_initialised = false;
		}
		keep_hash_table = gstate.keep_local_hash_tables;
		gstate.active_local_states++;
		if (op.filter_pushdown) {
			local_filter_state = op.filter_pushdown->GetLocalState(*gstate.global_filter_state);
		} else {
			local_filter_state.reset();
		}
	}
};

//! Map a dict-index byte width to its row-store slot type (the width is decided by GetDictSurvivingIndexWidth)
static LogicalType DictIndexType(uint8_t index_width) {
	switch (index_width) {
	case sizeof(uint8_t):
		return LogicalType::UTINYINT;
	case sizeof(uint16_t):
		return LogicalType::USMALLINT;
	default:
		return LogicalType::UINTEGER;
	}
}

//! Build the row layout [conditions, build payload, (found flag), hash], narrowing a payload column to its
//! dict-index slot when dict_index_width[col] != 0. Shared by publisher and empty-input fallback to avoid drift.
static shared_ptr<TupleDataLayout> BuildJoinLayout(const vector<LogicalType> &cond_types,
                                                   const vector<LogicalType> &build_types, JoinType join_type,
                                                   const vector<uint8_t> &dict_index_width) {
	vector<LogicalType> layout_types(cond_types);
	for (idx_t col = 0; col < build_types.size(); col++) {
		if (col < dict_index_width.size() && dict_index_width[col] != 0) {
			layout_types.emplace_back(DictIndexType(dict_index_width[col]));
		} else {
			layout_types.emplace_back(build_types[col]);
		}
	}
	if (PropagatesBuildSide(join_type)) {
		layout_types.emplace_back(LogicalType::BOOLEAN);
	}
	layout_types.emplace_back(LogicalType::HASH);

	auto layout = make_shared_ptr<TupleDataLayout>();
	layout->Initialize(layout_types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	return layout;
}

//! Join-level gate: shape-only eligibility checks, mirroring the dict-emission path plus a PHJ exclusion
static bool CanUseDictSurvivingJoin(const PhysicalHashJoin &op, const JoinHashTable &ht, bool can_use_perfect_hash,
                                    bool build_side_multi_source) {
	// external is safe here: the dictionary is an in-memory self-owned copy and the index is a plain row-store
	// column, so a spill/repartition preserves both (unlike the pointer-embedding dict-emission/compressed-probe paths)
	// a multi-source build can deliver a later chunk flat or as a different dictionary under the
	// already-narrowed slot, so disqualify the whole join (see BuildSideHasMultipleSources)
	if (build_side_multi_source) {
		return false;
	}
	// SINGLE joins need FlatVector::SetNull on unmatched rows; dictionary vectors cannot supply it
	if (ht.join_type == JoinType::SINGLE) {
		return false;
	}
	// LEFT may dispatch into NextUniqueLeftJoin, which gathers payload via ScanStructure::GatherResult and
	// bypasses GatherRHS' dict branch; the narrowed slot would be read as native type and trip the gather type check.
	if (ht.join_type == JoinType::LEFT) {
		return false;
	}
	// OUTER fills unmatched-probe rows with CONSTANT_NULL (NextLeftJoin), mixing dict chunks with flat fill chunks;
	// admitting it would re-emit a falsely global dictionary a downstream consumer cannot trust.
	if (ht.join_type == JoinType::OUTER) {
		return false;
	}
	if (op.rhs_output_columns.col_types.empty()) {
		return false;
	}
	// PHJ's FullScanHashTable reads payload from the row store at native width; a narrowed slot would corrupt it.
	if (can_use_perfect_hash) {
		return false;
	}
	return true;
}

bool HashJoinGlobalSinkState::DictSurvivingActive() const {
	if (!hash_table) {
		return false;
	}
	for (const auto &entry : hash_table->dict_registry) {
		if (entry) {
			return true;
		}
	}
	return false;
}

void HashJoinGlobalSinkState::PublishLayoutIfFirst(HashJoinLocalSinkState &lstate, DataChunk &payload_chunk) {
	if (layout_gate.published.load(std::memory_order_acquire)) {
		return;
	}
	unique_lock<mutex> guard(layout_gate.publish_mutex);
	if (layout_gate.published.load(std::memory_order_relaxed)) {
		return;
	}

	const auto &cond_types = lstate.hash_table->condition_types;
	const auto &build_types = lstate.hash_table->build_types;
	layout_gate.dict_index_width.assign(build_types.size(), 0);

	if (CanUseDictSurvivingJoin(op, *lstate.hash_table, can_use_perfect_hash, build_side_multi_source)) {
		// Per-column width decision lives on the JHT (GetDictSurvivingIndexWidth); feed it each arriving vector.
		for (idx_t col = 0; col < build_types.size(); col++) {
			if (col >= payload_chunk.ColumnCount()) {
				continue;
			}
			layout_gate.dict_index_width[col] =
			    lstate.hash_table->GetDictSurvivingIndexWidth(col, payload_chunk.data[col]);
		}
	}

	auto layout = BuildJoinLayout(cond_types, build_types, lstate.hash_table->join_type, layout_gate.dict_index_width);
	layout_gate.layout_ptr = layout;

	// global HT receives the same layout so Merge/Combine and finalize-time scans operate against it
	if (hash_table && !hash_table->IsLayoutFinalized()) {
		hash_table->FinishInitWithLayout(layout, layout_gate.dict_index_width);
	}

	layout_gate.published.store(true, std::memory_order_release);
}

static bool ShouldPrepareBloomFilterBuild(const PhysicalHashJoin &op) {
	if (!op.filter_pushdown || op.filter_pushdown->probe_info.empty()) {
		return false;
	}
	idx_t equality_column_count = 0;
	for (auto &cond : op.conditions) {
		auto cmp = cond.GetComparisonType();
		if (cmp == ExpressionType::COMPARE_EQUAL || cmp == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			equality_column_count++;
		}
	}
	if (equality_column_count != 1) {
		return false;
	}
	auto probe_estimated_cardinality = op.children[0].get().estimated_cardinality;
	auto build_estimated_cardinality = op.children[1].get().estimated_cardinality;
	if (probe_estimated_cardinality == 0 || build_estimated_cardinality == 0) {
		return false;
	}
	static constexpr double BUILD_TO_PROBE_RATIO_THRESHOLD = 1.0;
	const double build_to_probe_ratio =
	    static_cast<double>(build_estimated_cardinality) / static_cast<double>(probe_estimated_cardinality);
	if (build_to_probe_ratio > BUILD_TO_PROBE_RATIO_THRESHOLD) {
		return false;
	}
	static constexpr double NON_FILTERING_RATIO_THRESHOLD = 0.1;
	static constexpr idx_t NON_FILTERING_BUILD_SIDE_THRESHOLD = 4194304;
	if (!op.filter_pushdown->build_side_has_filter && build_to_probe_ratio > NON_FILTERING_RATIO_THRESHOLD &&
	    build_estimated_cardinality > NON_FILTERING_BUILD_SIDE_THRESHOLD) {
		return false;
	}
	return true;
}

unique_ptr<JoinHashTable> PhysicalHashJoin::InitializeHashTable(ClientContext &context,
                                                                const idx_t initial_radix_bits) const {
	auto result =
	    make_uniq<JoinHashTable>(context, *this, conditions, payload_columns.col_types, join_type, initial_radix_bits,
	                             rhs_output_columns.col_idxs, residual_info ? residual_info->Copy() : nullptr,
	                             predicate ? predicate.get() : nullptr, lhs_output_in_probe);
	if (ShouldPrepareBloomFilterBuild(*this)) {
		result->PrepareBuildBloomFilter(children[1].get().estimated_cardinality);
	}

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
			auto &info = result->correlated_mark_join_info;

			vector<LogicalType> delim_payload_types;
			vector<AggregateObject> correlated_aggregates;
			unique_ptr<BoundAggregateExpression> aggr;

			// jury-rigging the GroupedAggregateHashTable
			// we need a count_star and a count to get counts with and without NULLs

			FunctionBinder function_binder(context);
			aggr = function_binder.BindAggregateFunction(CountStarFun::GetFunction(), {}, nullptr,
			                                             AggregateType::NON_DISTINCT);
			correlated_aggregates.emplace_back(*aggr);
			delim_payload_types.push_back(aggr->GetReturnType());
			info.correlated_aggregates.push_back(std::move(aggr));

			auto count_fun = CountFunctionBase::GetFunction();
			vector<unique_ptr<Expression>> children;
			// this is a dummy but we need it to make the hash table understand whats going on
			children.push_back(make_uniq_base<Expression, BoundReferenceExpression>(count_fun.GetReturnType(), 0U));
			aggr = function_binder.BindAggregateFunction(count_fun, std::move(children), nullptr,
			                                             AggregateType::NON_DISTINCT);
			correlated_aggregates.emplace_back(*aggr);
			delim_payload_types.push_back(aggr->GetReturnType());
			info.correlated_aggregates.push_back(std::move(aggr));

			auto &allocator = BufferAllocator::Get(context);
			info.correlated_counts = make_uniq<GroupedAggregateHashTable>(
			    context, allocator, delim_types, delim_payload_types, std::move(correlated_aggregates));
			info.correlated_types = delim_types;
			info.group_chunk.Initialize(allocator, delim_types);
			info.result_chunk.Initialize(allocator, delim_payload_types);
		}
	}
	return result;
}

unique_ptr<GlobalSinkState> PhysicalHashJoin::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<HashJoinGlobalSinkState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalHashJoin::GetLocalSinkState(ExecutionContext &context) const {
	auto &gstate = sink_state->Cast<HashJoinGlobalSinkState>();
	return make_uniq<HashJoinLocalSinkState>(*this, context.client, gstate);
}

void PhysicalHashJoin::SetPreserveBuildForRecursiveReuse(bool preserve) const {
	if (!sink_state) {
		return;
	}
	auto &state = sink_state->Cast<HashJoinGlobalSinkState>();
	state.preserve_build_for_reuse = preserve;
	if (preserve) {
		state.scanned_data = false;
	}
}

bool PhysicalHashJoin::CanPreserveBuildForRecursiveReuse() const {
	if (!sink_state) {
		return false;
	}
	auto &state = sink_state->Cast<HashJoinGlobalSinkState>();
	return state.preserve_build_for_reuse && !state.external;
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

SinkResultType PhysicalHashJoin::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<HashJoinGlobalSinkState>();
	auto &lstate = input.local_state.Cast<HashJoinLocalSinkState>();

	// resolve the join keys for the right chunk
	lstate.join_keys.Reset();
	lstate.join_key_executor.Execute(chunk, lstate.join_keys);

	if (filter_pushdown && !gstate.skip_filter_pushdown) {
		filter_pushdown->Sink(lstate.join_keys, *lstate.local_filter_state);
	}

	if (payload_columns.col_types.empty()) { // there are only keys: place an empty chunk in the payload
		lstate.payload_chunk.SetChildCardinality(chunk.size());
	} else { // there are payload columns
		lstate.payload_chunk.ReferenceColumns(chunk, payload_columns.col_idxs);
	}

	// first-chunk: publish the canonical layout against the actually-arriving vector types
	gstate.PublishLayoutIfFirst(lstate, lstate.payload_chunk);

	// lazy per-thread setup against the published layout
	if (!lstate.append_state_initialised) {
		lstate.hash_table->FinishInitWithLayout(gstate.layout_gate.layout_ptr, gstate.layout_gate.dict_index_width);
		lstate.hash_table->GetSinkCollection().InitializeAppendState(lstate.append_state);
		lstate.append_state_initialised = true;
	}

	// build the HT
	lstate.hash_table->Build(lstate.append_state, lstate.join_keys, lstate.payload_chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

void JoinFilterPushdownInfo::Combine(JoinFilterGlobalState &gstate, JoinFilterLocalState &lstate) const {
	gstate.global_aggregate_state->Combine(*lstate.local_aggregate_state);
}

SinkCombineResultType PhysicalHashJoin::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<HashJoinGlobalSinkState>();
	auto &lstate = input.local_state.Cast<HashJoinLocalSinkState>();

	// Under deferred layout, a thread that never received a Sink chunk has no sink_collection to flush
	const bool has_layout = lstate.append_state_initialised;
	if (has_layout) {
		lstate.hash_table->GetSinkCollection().FlushAppendState(lstate.append_state);
	}
	annotated_lock_guard<annotated_mutex> guard(gstate.lock);
	if (!has_layout) {
		// nothing to merge — drop the empty thread-local hash table
		gstate.active_local_states--;
	} else if (lstate.keep_hash_table) {
		gstate.local_hash_tables.push_back(*lstate.hash_table);
	} else {
		gstate.owned_local_hash_tables.push_back(std::move(lstate.hash_table));
		gstate.local_hash_tables.push_back(*gstate.owned_local_hash_tables.back());
	}
	if (gstate.local_hash_tables.size() == gstate.active_local_states) {
		// Set to 0 until PrepareFinalize
		gstate.temporary_memory_state->SetZero();
	}

	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(*this);
	client_profiler.Flush(context.thread.profiler);
	if (filter_pushdown && !gstate.skip_filter_pushdown) {
		filter_pushdown->Combine(*gstate.global_filter_state, *lstate.local_filter_state);
	}

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//

static constexpr idx_t PARALLEL_CONSTRUCT_THRESHOLD = 1048576;
static constexpr double SKEW_SINGLE_THREADED_THRESHOLD = 0.33;

//! If the data is very skewed (many of the exact same key), our finalize will become slow,
//! due to completely slamming the same atomic using compare-and-swaps.
//! We can detect this because we partition the data, and go for a single-threaded finalize instead.
static bool KeysAreSkewed(const HashJoinGlobalSinkState &sink) {
	const auto max_partition_ht_size =
	    sink.max_partition_size + sink.hash_table->PointerTableSize(sink.max_partition_count);
	const auto skew = static_cast<double>(max_partition_ht_size) / static_cast<double>(sink.total_size);
	return skew > SKEW_SINGLE_THREADED_THRESHOLD;
}

//! If we have only one thread, always finalize single-threaded. Otherwise, we finalize in parallel if we
//! have more than 1M rows or if we want to verify parallelism.
static bool FinalizeSingleThreaded(const HashJoinGlobalSinkState &sink, const bool consider_skew) {
	// if only one thread, finalize single-threaded
	const auto num_threads = NumericCast<idx_t>(sink.num_threads);
	if (num_threads == 1) {
		return true;
	}

	// if we want to verify parallelism, finalize parallel
	if (sink.context.config.verify_parallelism) {
		return false;
	}

	// finalize single-threaded if we have less than 1M rows
	const auto &ht = *sink.hash_table;
	const bool ht_is_small = ht.Count() < PARALLEL_CONSTRUCT_THRESHOLD;

	if (consider_skew) {
		return ht_is_small || KeysAreSkewed(sink);
	}
	return ht_is_small;
}

static idx_t GetTupleWidth(const vector<LogicalType> &types, bool &all_constant) {
	TupleDataLayout layout;
	layout.Initialize(types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	all_constant = layout.AllConstant();
	return layout.GetRowWidth();
}

static idx_t GetPartitioningSpaceRequirement(ClientContext &context, const vector<LogicalType> &types,
                                             const idx_t radix_bits, const idx_t num_threads) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	bool all_constant;
	idx_t tuple_width = GetTupleWidth(types, all_constant);

	if (tuple_width == 0) {
		throw InternalException("GetPartitioningSpaceRequirement: tuple width should not be 0");
	}

	auto tuples_per_block = MaxValue<idx_t>(buffer_manager.GetBlockSize() / tuple_width, 1);
	auto blocks_per_chunk = (STANDARD_VECTOR_SIZE + tuples_per_block) / tuples_per_block + 1;
	if (!all_constant) {
		blocks_per_chunk += 2;
	}
	auto size_per_partition = blocks_per_chunk * buffer_manager.GetBlockAllocSize();
	auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);

	return num_threads * num_partitions * size_per_partition;
}

void PhysicalHashJoin::PrepareFinalize(ClientContext &context, GlobalSinkState &global_state) const {
	auto &gstate = global_state.Cast<HashJoinGlobalSinkState>();
	// If no Sink chunk ever arrived, the layout was never published. Fall back to a default layout
	// (all columns at their native width) so finalize-time scans can dereference data_collection.
	if (!gstate.layout_gate.published.load(std::memory_order_acquire)) {
		unique_lock<mutex> guard(gstate.layout_gate.publish_mutex);
		if (!gstate.layout_gate.published.load(std::memory_order_relaxed)) {
			const auto &cond_types = gstate.hash_table->condition_types;
			const auto &build_types = gstate.hash_table->build_types;
			gstate.layout_gate.dict_index_width.assign(build_types.size(), 0);
			// all-zero dict_index_width => BuildJoinLayout keeps every build column at its native width
			auto layout = BuildJoinLayout(cond_types, build_types, gstate.hash_table->join_type,
			                              gstate.layout_gate.dict_index_width);
			gstate.layout_gate.layout_ptr = layout;
			if (!gstate.hash_table->IsLayoutFinalized()) {
				gstate.hash_table->FinishInitWithLayout(layout);
			}
			gstate.layout_gate.published.store(true, std::memory_order_release);
		}
	}
	const auto &ht = *gstate.hash_table;

	gstate.total_size =
	    ht.GetTotalSize(gstate.local_hash_tables, gstate.max_partition_size, gstate.max_partition_count);
	gstate.probe_side_requirement =
	    GetPartitioningSpaceRequirement(context, children[0].get().GetTypes(), ht.GetRadixBits(), gstate.num_threads);
	const auto max_partition_ht_size =
	    gstate.max_partition_size + gstate.hash_table->PointerTableSize(gstate.max_partition_count);
	gstate.temporary_memory_state->SetMinimumReservation(max_partition_ht_size + gstate.probe_side_requirement);

	bool all_constant;
	gstate.temporary_memory_state->SetMaterializationPenalty(GetTupleWidth(children[0].get().GetTypes(), all_constant));
	gstate.temporary_memory_state->SetRemainingSize(gstate.total_size);
}

static void ExecuteHashJoinTableInitTask(HashJoinGlobalSinkState &sink, idx_t entry_idx_from, idx_t entry_idx_to) {
	sink.hash_table->InitializePointerTable(entry_idx_from, entry_idx_to);
}

static void ExecuteHashJoinFinalizeTask(HashJoinGlobalSinkState &sink, optional_idx partition_idx,
                                        optional_ptr<PrefixRangeFilter::BuildState> prefix_range_state) {
	const auto &data_collection = sink.hash_table->GetDataCollection();
	if (!partition_idx.IsValid() || sink.hash_table->GetRadixBits() == 0) {
		// Unpartitioned builds still finalize over the full chunk range even if the scheduler created a
		// single "partition 0" task, because tuple-data segments are not tagged with partition ids there.
		sink.hash_table->Finalize(0U, data_collection.ChunkCount(), false, prefix_range_state);
	} else {
		// Parallel finalize - each thread processes one partition
		const auto chunk_ranges = data_collection.GetChunkRangesForPartition(partition_idx.GetIndex());
		for (auto &chunk_range : chunk_ranges) {
			sink.hash_table->Finalize(chunk_range.first, chunk_range.second, true, prefix_range_state);
		}
	}
}

class HashJoinTableInitTask : public ExecutorTask {
public:
	HashJoinTableInitTask(shared_ptr<Event> event_p, ClientContext &context, HashJoinGlobalSinkState &sink_p,
	                      idx_t entry_idx_from_p, idx_t entry_idx_to_p, const PhysicalOperator &op_p)
	    : ExecutorTask(context, std::move(event_p), op_p), sink(sink_p), entry_idx_from(entry_idx_from_p),
	      entry_idx_to(entry_idx_to_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		ExecuteHashJoinTableInitTask(sink, entry_idx_from, entry_idx_to);
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "HashJoinTableInitTask";
	}

private:
	HashJoinGlobalSinkState &sink;
	idx_t entry_idx_from;
	idx_t entry_idx_to;
};

class HashJoinTableInitEvent : public BasePipelineEvent {
public:
	HashJoinTableInitEvent(Pipeline &pipeline_p, HashJoinGlobalSinkState &sink)
	    : BasePipelineEvent(pipeline_p), sink(sink) {
	}

	HashJoinGlobalSinkState &sink;

public:
	vector<shared_ptr<Task>> GetTasks() {
		auto &ht = *sink.hash_table;
		const auto entry_count = ht.capacity;
		auto &context = pipeline->GetClientContext();
		vector<shared_ptr<Task>> finalize_tasks;
		if (FinalizeSingleThreaded(sink, false)) {
			finalize_tasks.push_back(
			    make_uniq<HashJoinTableInitTask>(shared_from_this(), context, sink, 0U, entry_count, sink.op));
			return finalize_tasks;
		}

		auto num_threads = NumericCast<idx_t>(sink.num_threads);
		// have 4 times more tasks than threads, but bound the to a minimum
		const idx_t entries_per_task = MaxValue(entry_count / num_threads / 4, MINIMUM_ENTRIES_PER_TASK);
		// Parallel memset
		for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx += entries_per_task) {
			auto entry_idx_to = MinValue<idx_t>(entry_idx + entries_per_task, entry_count);
			finalize_tasks.push_back(
			    make_uniq<HashJoinTableInitTask>(shared_from_this(), context, sink, entry_idx, entry_idx_to, sink.op));
		}
		return finalize_tasks;
	}

	void ExecuteDirectly() {
		ExecuteHashJoinTableInitTask(sink, 0U, sink.hash_table->capacity);
	}

	void Schedule() override {
		SetTasks(GetTasks());
	}
	static constexpr const idx_t MINIMUM_ENTRIES_PER_TASK = 131072;
};

class HashJoinFinalizeTask : public ExecutorTask {
public:
	HashJoinFinalizeTask(HashJoinGlobalSinkState &sink_p, shared_ptr<Event> event, optional_idx partition_idx_p,
	                     optional_ptr<PrefixRangeFilter::BuildState> prefix_range_state_p = nullptr)
	    : ExecutorTask(sink_p.context, std::move(event), sink_p.op), sink(sink_p), partition_idx(partition_idx_p),
	      prefix_range_state(prefix_range_state_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		ExecuteHashJoinFinalizeTask(sink, partition_idx, prefix_range_state);
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}
	string TaskType() const override {
		return "HashJoinFinalizeTask";
	}

private:
	HashJoinGlobalSinkState &sink;
	optional_idx partition_idx;
	optional_ptr<PrefixRangeFilter::BuildState> prefix_range_state;
};

class HashJoinFinalizeEvent : public BasePipelineEvent {
public:
	HashJoinFinalizeEvent(Pipeline &pipeline_p, HashJoinGlobalSinkState &sink)
	    : BasePipelineEvent(pipeline_p), sink(sink) {
	}

	HashJoinGlobalSinkState &sink;

public:
	vector<shared_ptr<Task>> GetTasks() {
		auto &ht = *sink.hash_table;
		const auto build_prefix_range_filter = ht.ShouldBuildPrefixRangeFilter();
		vector<shared_ptr<Task>> finalize_tasks;
		if (FinalizeSingleThreaded(sink, false)) {
			auto prefix_range_state = build_prefix_range_filter ? RegisterPrefixRangeState(ht) : nullptr;
			finalize_tasks.push_back(
			    make_uniq<HashJoinFinalizeTask>(sink, shared_from_this(), optional_idx(), prefix_range_state));
			return finalize_tasks;
		}

		// Parallel finalize
		const auto num_partitions = RadixPartitioning::NumberOfPartitions(ht.GetRadixBits());
		const auto &current_partitions = ht.GetCurrentPartitions();
		for (idx_t partition_idx = 0; partition_idx < num_partitions; partition_idx++) {
			if (sink.external && !current_partitions.RowIsValidUnsafe(partition_idx)) {
				continue; // Partition is not being built on
			}
			auto prefix_range_state = build_prefix_range_filter ? RegisterPrefixRangeState(ht) : nullptr;
			finalize_tasks.push_back(
			    make_uniq<HashJoinFinalizeTask>(sink, shared_from_this(), partition_idx, prefix_range_state));
		}
		return finalize_tasks;
	}

	void ExecuteDirectly() {
		auto &ht = *sink.hash_table;
		auto prefix_range_state = ht.ShouldBuildPrefixRangeFilter() ? RegisterPrefixRangeState(ht) : nullptr;
		ExecuteHashJoinFinalizeTask(sink, optional_idx(), prefix_range_state);
		FinishTasks();
	}

	void Schedule() override {
		SetTasks(GetTasks());
	}

	void FinishEvent() override {
		FinishTasks();
	}

	static constexpr idx_t CHUNKS_PER_TASK = 64;

private:
	void FinishTasks() {
		for (auto &prefix_range_state : prefix_range_states) {
			sink.hash_table->MergePrefixRangeBuildState(*prefix_range_state);
		}
		sink.hash_table->GetDataCollection().VerifyEverythingPinned();

		// Both finalize paths finish writing the chains before reaching here,
		// so dictionary emission is safe on either path
		if (sink.hash_table->CanUseDictionaryEmission(sink.op, sink.external,
		                                              sink.op.children[0].get().estimated_cardinality)) {
			sink.hash_table->BuildDictionaryArrays(sink.op);
		}

		sink.hash_table->finalized = true;
	}

	optional_ptr<PrefixRangeFilter::BuildState> RegisterPrefixRangeState(JoinHashTable &ht) {
		prefix_range_states.push_back(ht.InitializePrefixRangeBuildState());
		return *prefix_range_states.back();
	}

	vector<unique_ptr<PrefixRangeFilter::BuildState>> prefix_range_states;
};

void HashJoinGlobalSinkState::ScheduleFinalize(Pipeline &pipeline, Event &event) {
	if (hash_table->Count() == 0) {
		hash_table->finalized = true;
		return;
	}
	hash_table->AllocatePointerTable();

	auto new_init_event = make_shared_ptr<HashJoinTableInitEvent>(pipeline, *this);
	if (FinalizeSingleThreaded(*this, false)) {
		new_init_event->ExecuteDirectly();
		auto new_finalize_event = make_shared_ptr<HashJoinFinalizeEvent>(pipeline, *this);
		new_finalize_event->ExecuteDirectly();
		return;
	}
	event.InsertEvent(new_init_event);

	auto new_finalize_event = make_shared_ptr<HashJoinFinalizeEvent>(pipeline, *this);
	new_init_event->InsertEvent(std::move(new_finalize_event));
}

void HashJoinGlobalSinkState::InitializeProbeSpill() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (!probe_spill) {
		probe_spill = make_uniq<JoinHashTable::ProbeSpill>(*hash_table, context, probe_types);
	}
}

class HashJoinRepartitionTask : public ExecutorTask {
public:
	HashJoinRepartitionTask(shared_ptr<Event> event_p, ClientContext &context, JoinHashTable &global_ht,
	                        JoinHashTable &local_ht, const PhysicalOperator &op_p)
	    : ExecutorTask(context, std::move(event_p), op_p), global_ht(global_ht), local_ht(local_ht) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		local_ht.Repartition(global_ht);
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "HashJoinRepartitionTask";
	}

private:
	JoinHashTable &global_ht;
	JoinHashTable &local_ht;
};

class HashJoinRepartitionEvent : public BasePipelineEvent {
public:
	HashJoinRepartitionEvent(Pipeline &pipeline_p, const PhysicalHashJoin &op_p, HashJoinGlobalSinkState &sink,
	                         vector<reference<JoinHashTable>> &local_hts)
	    : BasePipelineEvent(pipeline_p), op(op_p), sink(sink), local_hts(local_hts) {
	}

	const PhysicalHashJoin &op;
	HashJoinGlobalSinkState &sink;
	vector<reference<JoinHashTable>> &local_hts;

public:
	void Schedule() override {
		D_ASSERT(sink.hash_table->GetRadixBits() > sink.initial_radix_bits);
		auto block_size = sink.hash_table->buffer_manager.GetBlockSize();

		idx_t total_size = 0;
		idx_t total_count = 0;
		for (auto &local_ht : local_hts) {
			auto &sink_collection = local_ht.get().GetSinkCollection();
			total_size += sink_collection.SizeInBytes();
			total_count += sink_collection.Count();
		}
		auto total_blocks = (total_size + block_size - 1) / block_size;
		auto count_per_block = total_count / total_blocks;
		auto blocks_per_vector = MaxValue<idx_t>(STANDARD_VECTOR_SIZE / count_per_block, 2);

		// Assume 8 blocks per partition per thread (4 input, 4 output)
		auto partition_multiplier =
		    RadixPartitioning::NumberOfPartitions(sink.hash_table->GetRadixBits() - sink.initial_radix_bits);
		auto thread_memory = 2 * blocks_per_vector * partition_multiplier * block_size;
		auto repartition_threads = MaxValue<idx_t>(sink.temporary_memory_state->GetReservation() / thread_memory, 1);

		if (repartition_threads < local_hts.size()) {
			// Limit the number of threads working on repartitioning based on our memory reservation
			for (idx_t thread_idx = repartition_threads; thread_idx < local_hts.size(); thread_idx++) {
				local_hts[thread_idx % repartition_threads].get().Merge(local_hts[thread_idx].get());
			}
			auto repartition_end =
			    local_hts.begin() + static_cast<vector<reference<JoinHashTable>>::difference_type>(repartition_threads);
			local_hts.erase(repartition_end, local_hts.end());
		}

		auto &context = pipeline->GetClientContext();

		vector<shared_ptr<Task>> partition_tasks;
		partition_tasks.reserve(local_hts.size());
		for (auto &local_ht : local_hts) {
			partition_tasks.push_back(
			    make_uniq<HashJoinRepartitionTask>(shared_from_this(), context, *sink.hash_table, local_ht.get(), op));
		}
		SetTasks(std::move(partition_tasks));
	}

	void FinishEvent() override {
		local_hts.clear();
		sink.owned_local_hash_tables.clear();

		// Minimum reservation is now the new smallest partition size
		const auto num_partitions = RadixPartitioning::NumberOfPartitions(sink.hash_table->GetRadixBits());
		vector<idx_t> partition_sizes(num_partitions, 0);
		vector<idx_t> partition_counts(num_partitions, 0);
		sink.total_size = sink.hash_table->GetTotalSize(partition_sizes, partition_counts, sink.max_partition_size,
		                                                sink.max_partition_count);
		sink.probe_side_requirement =
		    GetPartitioningSpaceRequirement(sink.context, op.types, sink.hash_table->GetRadixBits(), sink.num_threads);

		sink.temporary_memory_state->SetMinimumReservation(sink.max_partition_size +
		                                                   sink.hash_table->PointerTableSize(sink.max_partition_count) +
		                                                   sink.probe_side_requirement);
		sink.temporary_memory_state->UpdateReservation(executor.context);

		D_ASSERT(sink.temporary_memory_state->GetReservation() >= sink.probe_side_requirement);
		sink.hash_table->PrepareExternalFinalize(sink.temporary_memory_state->GetReservation() -
		                                         sink.probe_side_requirement);
		sink.ScheduleFinalize(*pipeline, *this);
	}
};

bool JoinFilterPushdownInfo::CanUseInFilter(const ClientContext &context, optional_ptr<JoinHashTable> ht,
                                            const ExpressionType &cmp) const {
	auto dynamic_or_filter_threshold = Settings::Get<DynamicOrFilterThresholdSetting>(context);
	return ht && ht->Count() > 1 && ht->Count() <= dynamic_or_filter_threshold && cmp == ExpressionType::COMPARE_EQUAL;
}

bool JoinFilterPushdownInfo::PushInFilter(const JoinFilterPushdownFilter &info, const JoinFilterPushdownColumn &column,
                                          JoinHashTable &ht, const PhysicalOperator &op, idx_t filter_idx,
                                          ProjectionIndex filter_col_idx) const {
	// generate a "OR" filter (i.e. x=1 OR x=535 OR x=997)
	// first scan the entire vector at the probe side
	auto build_idx = join_condition[filter_idx];
	Vector tuples_addresses(LogicalType::POINTER, ht.Count()); // allocate space for all the tuples
	Vector build_vector(ht.layout_ptr->GetTypes()[build_idx], ht.Count());
	auto key_count = ht.ScanKeyColumn(tuples_addresses, build_vector, build_idx);
	if (key_count == 0) {
		return false;
	}

	// generate the OR-clause - note that we only need to consider unique values here (so we use a seT)
	value_set_t unique_ht_values;
	for (idx_t k = 0; k < key_count; k++) {
		// Cast to storage type, only insert if it succeeds
		auto value = build_vector.GetValue(k);
		if (column.storage_type.IsValid() && !value.DefaultTryCastAs(column.storage_type)) {
			return false; // it's all or nothing sadly
		}
		unique_ht_values.insert(value);
	}
	vector<Value> in_list(unique_ht_values.begin(), unique_ht_values.end());

	// generating the OR filter only makes sense if the range is
	// not dense and that the range does not contain NULL
	// i.e. if we have the values [0, 1, 2, 3, 4] - the min/max is fully equivalent to the OR filter
	if (FilterCombiner::ContainsNull(in_list) || FilterCombiner::IsDenseRange(in_list)) {
		return false;
	}

	// we push the OR filter as an OptionalFilter so that we can use it for zonemap pruning only
	// the IN-list is expensive to execute otherwise
	auto in_expr = ExpressionFilter::CreateInExpression(
	    make_uniq<BoundReferenceExpression>(column.storage_type, idx_t(0)), std::move(in_list));
	auto filter = make_uniq<ExpressionFilter>(CreateOptionalFilterExpression(std::move(in_expr), column.storage_type));
	info.dynamic_filters->PushFilter(op, filter_col_idx, std::move(filter));
	return true;
}

bool JoinFilterPushdownInfo::CanUseBloomFilter(const ClientContext &context, const PhysicalComparisonJoin &op,
                                               const ExpressionType &cmp, optional_ptr<JoinHashTable> ht) const {
	if (!ht) {
		return false;
	}

	// bf is only supported for equality conditions
	if (cmp != ExpressionType::COMPARE_EQUAL && cmp != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		return false;
	}

	// and only if there is exactly one equality condition
	// as the Filter API only allows single-column filters so far
	idx_t equality_column_count = 0;
	for (auto &cond : ht->conditions) {
		const auto cond_cmp = cond.GetComparisonType();
		if (cond_cmp == ExpressionType::COMPARE_EQUAL || cond_cmp == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			equality_column_count++;
		}
	}
	if (equality_column_count != 1) {
		return false;
	}

	// building the bloom filter is costly on the build to make probing faster,
	// so only use it if there are less build tuples than probing tuples
	static constexpr double BUILD_TO_PROBE_RATIO_THRESHOLD = 1.0;
	auto build_count = ht->Count();
	if (build_count == 0) {
		build_count = ht->GetSinkCollection().Count();
	}
	const double build_to_probe_ratio =
	    static_cast<double>(build_count) / static_cast<double>(op.children[0].get().estimated_cardinality);
	if (build_to_probe_ratio > BUILD_TO_PROBE_RATIO_THRESHOLD) {
		return false;
	}

	// if we have a build side without a filter, only build the bloom filter if the ratio/size is small enough
	static constexpr double NON_FILTERING_RATIO_THRESHOLD = 0.1;
	static constexpr idx_t NON_FILTERING_BUILD_SIDE_THRESHOLD = 4194304;
	if (!build_side_has_filter && build_to_probe_ratio > NON_FILTERING_RATIO_THRESHOLD &&
	    build_count > NON_FILTERING_BUILD_SIDE_THRESHOLD) {
		return false;
	}

	return true;
}

bool JoinFilterPushdownInfo::CanUsePrefixRangeFilter(const ClientContext &context, const PhysicalComparisonJoin &op,
                                                     optional_ptr<JoinHashTable> ht, const ExpressionType &cmp) const {
	if (!CanUseBloomFilter(context, op, cmp, ht)) {
		return false;
	}
	if (cmp != ExpressionType::COMPARE_EQUAL) {
		return false;
	}
	if (ht->NullValuesAreEqual(0)) {
		// TODO: Support "A is B" type joins
		return false;
	}

	const auto &key_type = ht->conditions[0].GetLHS().GetReturnType();
	return PrefixRangeFilter::SupportedType(key_type);
}

static unique_ptr<ExpressionFilter> CreateSelectivityOptionalExpressionFilter(unique_ptr<Expression> child_expr,
                                                                              const LogicalType &column_type,
                                                                              SelectivityOptionalFilterType type);

static LogicalType GetRuntimeFilterInputType(const JoinFilterPushdownColumn &column, const LogicalType &runtime_type) {
	return column.runtime_filter_type.IsValid() ? column.runtime_filter_type : runtime_type;
}

static unique_ptr<Expression> CreateRuntimeFilterInputExpression(ClientContext &context,
                                                                 const JoinFilterPushdownColumn &column,
                                                                 const LogicalType &runtime_type) {
	D_ASSERT(column.storage_type.IsValid());
	auto input_type = GetRuntimeFilterInputType(column, runtime_type);
	unique_ptr<Expression> input = make_uniq<BoundReferenceExpression>(column.storage_type, idx_t(0));
	if (column.storage_type != input_type) {
		input = BoundCastExpression::AddCastToType(context, std::move(input), input_type);
	}
	return input;
}

void JoinFilterPushdownInfo::PushBloomFilter(ClientContext &context, const PhysicalOperator &op, JoinHashTable &ht,
                                             const JoinFilterPushdownFilter &info,
                                             const JoinFilterPushdownColumn &column,
                                             ProjectionIndex filter_col_idx) const {
	// If the nulls are equal, we let nulls pass. If not, we filter them
	auto filters_null_values = !ht.NullValuesAreEqual(0);
	const auto key_name = ht.conditions[0].GetRHS().ToString();
	const auto key_type = ht.conditions[0].GetLHS().GetReturnType();
	auto filter_input_type = GetRuntimeFilterInputType(column, key_type);
	ht.SetBuildBloomFilter(true);
	float selectivity_threshold;
	idx_t n_vectors_to_check;
	GetThresholdAndVectorsToCheck(SelectivityOptionalFilterType::BF, selectivity_threshold, n_vectors_to_check);
	vector<unique_ptr<Expression>> children;
	children.push_back(CreateRuntimeFilterInputExpression(context, column, key_type));
	auto filter_expr = make_uniq<BoundFunctionExpression>(
	    BoundScalarFunction(BloomFilterScalarFun::GetFunction(filter_input_type)), std::move(children),
	    make_uniq<BloomFilterFunctionData>(ht.GetBloomFilter(), filters_null_values, key_name, key_type,
	                                       selectivity_threshold, n_vectors_to_check));
	info.dynamic_filters->PushFilter(op, filter_col_idx,
	                                 CreateSelectivityOptionalExpressionFilter(std::move(filter_expr),
	                                                                           column.storage_type,
	                                                                           SelectivityOptionalFilterType::BF));
}

bool JoinFilterPushdownInfo::TryRegisterPrefixRangeFilter(const JoinFilterPushdownFilter &info, ClientContext &context,
                                                          JoinHashTable &ht, const PhysicalOperator &op,
                                                          const JoinFilterPushdownColumn &column,
                                                          ProjectionIndex filter_col_idx, const Value &min_val,
                                                          const Value &max_val, idx_t max_bits) const {
	const auto key_type = ht.conditions[0].GetLHS().GetReturnType();
	auto filter_input_type = GetRuntimeFilterInputType(column, key_type);
	if (!ht.GetPrefixRangeFilter()) {
		auto prefix_filter = PrefixRangeFilter::CreatePrefixRangeFilter(key_type);
		prefix_filter->Initialize(context, ht.Count(), min_val, max_val, max_bits);
		ht.SetPrefixRangeFilter(std::move(prefix_filter));
		ht.SetBuildPrefixRangeFilter();
	}

	const auto key_name = ht.conditions[0].GetRHS().ToString();
	float selectivity_threshold;
	idx_t n_vectors_to_check;
	GetThresholdAndVectorsToCheck(SelectivityOptionalFilterType::PRF, selectivity_threshold, n_vectors_to_check);
	vector<unique_ptr<Expression>> children;
	children.push_back(CreateRuntimeFilterInputExpression(context, column, key_type));
	auto filter_expr = make_uniq<BoundFunctionExpression>(
	    BoundScalarFunction(PrefixRangeScalarFun::GetFunction(filter_input_type)), std::move(children),
	    make_uniq<PrefixRangeFunctionData>(ht.GetPrefixRangeFilter(), key_name, key_type, selectivity_threshold,
	                                       n_vectors_to_check));
	info.dynamic_filters->PushFilter(op, filter_col_idx,
	                                 CreateSelectivityOptionalExpressionFilter(std::move(filter_expr),
	                                                                           column.storage_type,
	                                                                           SelectivityOptionalFilterType::PRF));
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

static void CreateDynamicMinMaxFilter(const PhysicalComparisonJoin &op, const JoinFilterPushdownFilter &info,
                                      const ProjectionIndex &filter_col_idx, unique_ptr<Expression> filter_expr,
                                      const LogicalType &column_type, bool selectivity_optional) {
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
	auto constant_value = constant;
	if (!constant_value.IsNull()) {
		constant_value.DefaultTryCastAs(comparison_logical_type);
	}
	return BoundComparisonExpression::Create(comparison_type, std::move(input),
	                                         make_uniq<BoundConstantExpression>(std::move(constant_value)));
}

static unique_ptr<Expression> CreateComparisonExpressionFilter(ExpressionType comparison_type, const Value &constant,
                                                               const LogicalType &column_type) {
	auto column = make_uniq<BoundReferenceExpression>(column_type, 0ULL);
	return CreateComparisonExpressionFilter(comparison_type, std::move(column), constant, column_type);
}

static unique_ptr<Expression>
CreateJoinFilterComparisonExpression(ClientContext &context, const JoinFilterPushdownColumn &column,
                                     ExpressionType comparison_type, const Value &constant,
                                     const LogicalType &comparison_logical_type, bool reconstruct_expression) {
	if (!reconstruct_expression) {
		return CreateComparisonExpressionFilter(comparison_type, constant, comparison_logical_type);
	}
	auto input = CreateRuntimeFilterInputExpression(context, column, comparison_logical_type);
	return CreateComparisonExpressionFilter(comparison_type, std::move(input), constant, comparison_logical_type);
}

static void CreateDynamicMinMaxFilters(const PhysicalComparisonJoin &op, const JoinFilterPushdownFilter &info,
                                       ClientContext &context, const JoinFilterPushdownColumn &column,
                                       ProjectionIndex filter_col_idx, ExpressionType cmp, const Value &min_val,
                                       const Value &max_val, const LogicalType &condition_type,
                                       bool reconstruct_expression, bool selectivity_optional) {
	auto filter_column_type = reconstruct_expression ? column.storage_type : condition_type;
	switch (cmp) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
		CreateDynamicMinMaxFilter(op, info, filter_col_idx,
		                          CreateJoinFilterComparisonExpression(context, column,
		                                                               ExpressionType::COMPARE_GREATERTHANOREQUALTO,
		                                                               min_val, condition_type, reconstruct_expression),
		                          filter_column_type, selectivity_optional);
		break;
	}
	default:
		break;
	}
	switch (cmp) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
		CreateDynamicMinMaxFilter(op, info, filter_col_idx,
		                          CreateJoinFilterComparisonExpression(context, column,
		                                                               ExpressionType::COMPARE_LESSTHANOREQUALTO,
		                                                               max_val, condition_type, reconstruct_expression),
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

unique_ptr<DataChunk> JoinFilterPushdownInfo::FinalizeFilters(ClientContext &context, const PhysicalComparisonJoin &op,
                                                              unique_ptr<DataChunk> final_min_max,
                                                              optional_ptr<JoinHashTable> ht, bool allow_bloom_filters,
                                                              bool allow_prefix_range_filters) const {
	if (probe_info.empty()) {
		return final_min_max; // There are no table sources in which we can push down filters
	}

	// create a filter for each column that reached a table scan
	for (auto &info : probe_info) {
		for (auto &pushdown_column : info.columns) {
			auto filter_idx = pushdown_column.join_filter_idx;
			D_ASSERT(filter_idx != DConstants::INVALID_INDEX);
			D_ASSERT(filter_idx < join_condition.size());
			const auto cmp = op.conditions[join_condition[filter_idx]].GetComparisonType();
			auto &filter_col_idx = pushdown_column.probe_column_index.column_index;
			auto min_idx = filter_idx * 2;
			auto max_idx = min_idx + 1;

			auto min_val_before_cast = final_min_max->data[min_idx].GetValue(0);
			auto max_val_before_cast = final_min_max->data[max_idx].GetValue(0);

			auto min_val = min_val_before_cast;
			auto max_val = max_val_before_cast;
			auto runtime_filter_input_type = GetRuntimeFilterInputType(pushdown_column, min_val_before_cast.type());
			const bool reconstruct_filter_expression =
			    pushdown_column.mode == JoinFilterPushdownMode::RECONSTRUCT_EXPRESSION &&
			    pushdown_column.storage_type.id() == LogicalTypeId::VARIANT &&
			    runtime_filter_input_type != pushdown_column.storage_type;

			// Cast to storage type, skip if fails
			if (pushdown_column.storage_type.IsValid() && !reconstruct_filter_expression) {
				if (!min_val.DefaultTryCastAs(pushdown_column.storage_type)) {
					continue;
				}
				if (!max_val.DefaultTryCastAs(pushdown_column.storage_type)) {
					continue;
				}
			}

			if (min_val.IsNull() || max_val.IsNull()) {
				// min/max is NULL
				// this can happen in case all values in the RHS column are NULL, but they are still pushed into the
				// hash table e.g. because they are part of a RIGHT join
				continue;
			}

			auto condition_type = min_val.type();
			runtime_filter_input_type = GetRuntimeFilterInputType(pushdown_column, condition_type);
			bool can_emit_runtime_filters = pushdown_column.mode == JoinFilterPushdownMode::RECONSTRUCT_EXPRESSION;
			if (can_emit_runtime_filters && ht) {
				can_emit_runtime_filters = runtime_filter_input_type == ht->conditions[0].GetLHS().GetReturnType();
			}

			if (Value::NotDistinctFrom(min_val, max_val)) {
				// min = max - single value
				// generate a "one-sided" comparison filter for the LHS
				// Note that this also works for equalities.
				info.dynamic_filters->PushFilter(
				    op, filter_col_idx,
				    make_uniq<ExpressionFilter>(CreateJoinFilterComparisonExpression(
				        context, pushdown_column, cmp, min_val, condition_type, reconstruct_filter_expression)));
			} else {
				if (cmp != ExpressionType::COMPARE_EQUAL) {
					// min != max - generate range filters for non-equality comparisons.
					// For non-equalities, the range must be half-open.
					CreateDynamicMinMaxFilters(op, info, context, pushdown_column, filter_col_idx, cmp, min_val,
					                           max_val, condition_type, reconstruct_filter_expression, true);
					continue;
				}

				uhugeint_t span;
				const auto can_compute_span =
				    PrefixRangeFilter::TryComputeSpan(min_val_before_cast, max_val_before_cast, span);
				const auto can_emit_prf = allow_prefix_range_filters && can_emit_runtime_filters &&
				                          CanUsePrefixRangeFilter(context, op, ht, cmp) && can_compute_span;

				bool pushed_in_filter = false;
				if (CanUseInFilter(context, ht, cmp)) {
					pushed_in_filter = PushInFilter(info, pushdown_column, *ht, op, filter_idx, filter_col_idx);
				}

				static constexpr idx_t SMALL_EXACT_PRF_BITS = 1ULL << 26;
				if (can_emit_prf && span < SMALL_EXACT_PRF_BITS &&
				    TryRegisterPrefixRangeFilter(info, context, *ht, op, pushdown_column, filter_col_idx,
				                                 min_val_before_cast, max_val_before_cast, SMALL_EXACT_PRF_BITS)) {
					continue;
				}

				if (can_emit_prf) {
					auto build_count = ht->Count();
					if (build_count == 0) {
						build_count = ht->GetSinkCollection().Count();
					}
					const auto bloom_filter_bits = BloomFilterBitBudget(build_count);
					if (span <= bloom_filter_bits &&
					    TryRegisterPrefixRangeFilter(info, context, *ht, op, pushdown_column, filter_col_idx,
					                                 min_val_before_cast, max_val_before_cast, bloom_filter_bits)) {
						continue;
					}
				}

				if (!pushed_in_filter) {
					CreateDynamicMinMaxFilters(op, info, context, pushdown_column, filter_col_idx, cmp, min_val,
					                           max_val, condition_type, reconstruct_filter_expression, false);
				}
				if (allow_bloom_filters && can_emit_runtime_filters && ht && CanUseBloomFilter(context, op, cmp, ht)) {
					PushBloomFilter(context, op, *ht, info, pushdown_column, filter_col_idx);
				}
			}
		}
	}
	return final_min_max;
}

unique_ptr<DataChunk> JoinFilterPushdownInfo::Finalize(ClientContext &context, JoinFilterGlobalState &gstate,
                                                       const PhysicalComparisonJoin &op,
                                                       optional_ptr<JoinHashTable> ht) const {
	auto final_min_max = FinalizeMinMax(gstate);
	return FinalizeFilters(context, op, std::move(final_min_max), ht, true);
}

SinkFinalizeType PhysicalHashJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	auto &sink = input.global_state.Cast<HashJoinGlobalSinkState>();
	auto &ht = *sink.hash_table;

	sink.temporary_memory_state->UpdateReservation(context);
	sink.external = sink.temporary_memory_state->GetReservation() < sink.total_size;
	if (sink.external) {
		// For external join we reduce the load factor, this may even prevent the external join altogether
		ht.load_factor = JoinHashTable::EXTERNAL_LOAD_FACTOR;

		idx_t temp_max_partition_size;
		idx_t temp_max_partition_count;
		idx_t temp_total_size =
		    ht.GetTotalSize(sink.local_hash_tables, temp_max_partition_size, temp_max_partition_count);

		if (temp_total_size < sink.temporary_memory_state->GetReservation()) {
			// We prevented the external join by reducing the load factor. Update the state accordingly
			sink.temporary_memory_state->SetMinimumReservation(temp_total_size);
			sink.temporary_memory_state->SetRemainingSizeAndUpdateReservation(context, temp_total_size);

			sink.total_size = temp_total_size;
			sink.max_partition_size = temp_max_partition_size;
			sink.max_partition_count = temp_max_partition_count;

			sink.external = false;
		}
	}
	DUCKDB_LOG(context, PhysicalOperatorLogType, *this, "PhysicalHashJoin", "Finalize",
	           {{"external", to_string(sink.external)}});
	if (sink.external) {
		// External Hash Join
		// Recursive preserved-build reuse only applies to the in-memory finalized HT. External hash join
		// runs through repeated build/probe rounds with partition/probe-spill state, so it cannot be
		// treated as a stable materialized build that later recursive iterations may skip entirely.
		sink.preserve_build_for_reuse = false;
		sink.perfect_join_executor.reset();

		const auto max_partition_ht_size = sink.max_partition_size + ht.PointerTableSize(sink.max_partition_count);
		const auto very_very_skewed = // No point in repartitioning if it's this skewed
		    static_cast<double>(max_partition_ht_size) >= 0.8 * static_cast<double>(sink.total_size);
		if (!very_very_skewed &&
		    (max_partition_ht_size + sink.probe_side_requirement) > sink.temporary_memory_state->GetReservation()) {
			// We have to repartition
			const auto radix_bits_before = ht.GetRadixBits();
			ht.SetRepartitionRadixBits(sink.temporary_memory_state->GetReservation(), sink.max_partition_size,
			                           sink.max_partition_count);
			DUCKDB_LOG(context, PhysicalOperatorLogType, *this, "PhysicalHashJoin", "Repartition",
			           {{"partitions_before", to_string(RadixPartitioning::NumberOfPartitions(radix_bits_before))},
			            {"partitions_after", to_string(RadixPartitioning::NumberOfPartitions(ht.GetRadixBits()))}});
			auto new_event = make_shared_ptr<HashJoinRepartitionEvent>(pipeline, *this, sink, sink.local_hash_tables);
			event.InsertEvent(std::move(new_event));
		} else {
			// No repartitioning!
			for (auto &local_ht : sink.local_hash_tables) {
				ht.Merge(local_ht.get());
			}
			sink.local_hash_tables.clear();
			sink.owned_local_hash_tables.clear();
			if (filter_pushdown && !sink.skip_filter_pushdown && ht.GetSinkCollection().Count() > 0) {
				auto filter_min_max = filter_pushdown->FinalizeMinMax(*sink.global_filter_state);
				filter_pushdown->FinalizeFilters(context, *this, std::move(filter_min_max), &ht, true, false);
			}
			ht.PrepareBloomFilterForFinalize();
			D_ASSERT(sink.temporary_memory_state->GetReservation() >= sink.probe_side_requirement);
			sink.hash_table->PrepareExternalFinalize(sink.temporary_memory_state->GetReservation() -
			                                         sink.probe_side_requirement);
			sink.ScheduleFinalize(pipeline, event);
		}
		sink.finalized = true;
		return SinkFinalizeType::READY;
	}

	// In-memory Hash Join
	for (auto &local_ht : sink.local_hash_tables) {
		ht.Merge(local_ht.get());
	}
	sink.local_hash_tables.clear();
	sink.owned_local_hash_tables.clear();
	ht.Unpartition();

	Value min;
	Value max;
	unique_ptr<DataChunk> filter_min_max = nullptr;

	if (filter_pushdown && !sink.skip_filter_pushdown && ht.Count() > 0) {
		filter_min_max = filter_pushdown->FinalizeMinMax(*sink.global_filter_state);
		min = filter_min_max->data[0].GetValue(0);
		max = filter_min_max->data[1].GetValue(0);
	} else if (TypeIsIntegral(conditions[0].GetRHS().GetReturnType().InternalType())) {
		min = Value::MinimumValue(conditions[0].GetRHS().GetReturnType());
		max = Value::MaximumValue(conditions[0].GetRHS().GetReturnType());
	}

	// check for possible perfect hash table
	auto use_perfect_hash = sink.perfect_join_executor->CanDoPerfectHashJoin(*this, min, max);
	// PHJ's FullScanHashTable reads payload at native width; if any slot was narrowed it would crash. Runtime
	// min/max from filter pushdown can re-enable PHJ here, so re-check after dict-surviving may have narrowed.
	if (use_perfect_hash && sink.DictSurvivingActive()) {
		use_perfect_hash = false;
	}
	if (use_perfect_hash) {
		use_perfect_hash = sink.perfect_join_executor->BuildPerfectHashTable();
	}

	if (!use_perfect_hash) {
		sink.perfect_join_executor.reset();
	}

	if (filter_min_max) {
		filter_pushdown->FinalizeFilters(context, *this, std::move(filter_min_max), &ht, !use_perfect_hash);
		if (use_perfect_hash) {
			ht.BuildPrefixRangeFilter();
		}
	}

	// In case of a large build side or duplicates, use regular hash join
	if (!use_perfect_hash) {
		ht.PrepareBloomFilterForFinalize();
		sink.ScheduleFinalize(pipeline, event);
	}
	sink.finalized = true;
	if (ht.Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
//! Structural gate for the compressed-vector probe paths; evaluated once per operator state.
//! Decides whether to allocate ProbeState::dict_state; Probe() dispatches into the fast paths
//! whenever that allocation is present.
static bool CanUseCompressedProbe(const HashJoinGlobalSinkState &sink, const vector<JoinCondition> &conditions) {
	if (sink.external) {
		// external joins re-finalize the HT mid-probe, invalidating the per-id pointer cache
		return false;
	}
	if (sink.perfect_join_executor) {
		return false;
	}
	if (conditions.size() != 1) {
		return false;
	}
	const auto cmp = conditions[0].GetComparisonType();
	if (cmp != ExpressionType::COMPARE_EQUAL && cmp != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		return false;
	}
	if (conditions[0].GetLHS().GetExpressionClass() != ExpressionClass::BOUND_REF) {
		return false;
	}
	if (conditions[0].GetLHS().GetReturnType().InternalType() == PhysicalType::STRUCT) {
		return false;
	}
	return true;
}

class HashJoinOperatorState : public CachingOperatorState {
public:
	HashJoinOperatorState(ClientContext &context, const PhysicalHashJoin &op_p, HashJoinGlobalSinkState &sink)
	    : op(op_p), probe_executor(context), scan_structure(*sink.hash_table, join_key_state) {
	}

	const PhysicalHashJoin &op;
	DataChunk lhs_join_keys;
	TupleDataChunkState join_key_state;
	DataChunk lhs_probe_data;

	ExpressionExecutor probe_executor;
	JoinHashTable::ScanStructure scan_structure;
	unique_ptr<OperatorState> perfect_hash_join_state;

	JoinHashTable::ProbeSpillLocalAppendState spill_state;
	JoinHashTable::ProbeState probe_state;
	//! Chunk to sink data into for external join
	DataChunk spill_chunk;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op);
	}

	bool SupportsReuse() const override {
		return true;
	}

	void Reset() override {
		auto &sink = op.sink_state->Cast<HashJoinGlobalSinkState>();
		ResetCachingState();
		lhs_join_keys.Reset();
		lhs_probe_data.Reset();
		scan_structure.Reset();
		perfect_hash_join_state.reset();
		spill_state = JoinHashTable::ProbeSpillLocalAppendState();
		TupleDataCollection::InitializeChunkState(join_key_state, op.condition_types);
		if (spill_chunk.ColumnCount() == 0) {
			spill_chunk.Initialize(BufferAllocator::Get(sink.context), sink.probe_types);
		} else {
			spill_chunk.Reset();
		}
		// discard cached build-side pointers; the HT may have been reset between iterations (e.g. recursive CTE)
		if (probe_state.dict_state) {
			probe_state.dict_state = make_uniq<JoinHashTable::ProbeDictionaryState>();
		}
		// perfect_hash_join_state will be lazily initialized on first Execute when we have a real ExecutionContext
	}
};

unique_ptr<OperatorState> PhysicalHashJoin::GetOperatorState(ExecutionContext &context) const {
	auto &allocator = BufferAllocator::Get(context.client);
	auto &sink = sink_state->Cast<HashJoinGlobalSinkState>();
	auto state = make_uniq<HashJoinOperatorState>(context.client, *this, sink);
	state->lhs_join_keys.Initialize(allocator, condition_types);

	// initialize probe data with ALL probe columns (output + predicate)
	if (!lhs_probe_columns.col_types.empty()) {
		state->lhs_probe_data.Initialize(allocator, lhs_probe_columns.col_types);
	}

	for (auto &cond : conditions) {
		state->probe_executor.AddExpression(cond.GetLHS());
	}
	TupleDataCollection::InitializeChunkState(state->join_key_state, condition_types);

	if (sink.perfect_join_executor) {
		state->perfect_hash_join_state = sink.perfect_join_executor->GetOperatorState(context);
	}

	if (sink.external) {
		state->spill_chunk.Initialize(allocator, sink.probe_types);
		sink.InitializeProbeSpill();
	}

	if (CanUseCompressedProbe(sink, conditions)) {
		// non-null dict_state is the enabling signal for the compressed-vector probe paths in Probe()
		state->probe_state.dict_state = make_uniq<JoinHashTable::ProbeDictionaryState>();
	}

	return std::move(state);
}

OperatorResultType PhysicalHashJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                     GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<HashJoinOperatorState>();
	auto &sink = sink_state->Cast<HashJoinGlobalSinkState>();
	D_ASSERT(sink.finalized);
	D_ASSERT(!sink.scanned_data);

	if (sink.hash_table->Count() == 0) {
		if (EmptyResultIfRHSIsEmpty()) {
			return OperatorResultType::FINISHED;
		}
		// for empty result, only need output columns (no predicate evaluation)
		state.lhs_probe_data.ReferenceColumns(input, lhs_output_columns.col_idxs);
		ConstructEmptyJoinResult(sink.hash_table->join_type, sink.hash_table->has_null, state.lhs_probe_data, chunk);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	if (sink.perfect_join_executor) {
		D_ASSERT(!sink.external);
		// Lazily initialize perfect_hash_join_state on first use if needed
		if (!state.perfect_hash_join_state) {
			state.perfect_hash_join_state = sink.perfect_join_executor->GetOperatorState(context);
		}
		// for perfect hash join, when predicate is NULL, only output columns are needed
		state.lhs_probe_data.ReferenceColumns(input, lhs_output_columns.col_idxs);
		return sink.perfect_join_executor->ProbePerfectHashTable(context, input, state.lhs_probe_data, chunk,
		                                                         *state.perfect_hash_join_state);
	}

	if (sink.external && !state.initialized) {
		// some initialization for external hash join
		if (!sink.probe_spill) {
			sink.InitializeProbeSpill();
		}
		state.spill_state = sink.probe_spill->RegisterThread();
		state.initialized = true;
	}

	if (state.scan_structure.is_null) {
		// probe the HT, start by resolving the join keys for the left chunk
		state.lhs_join_keys.Reset();
		state.probe_executor.Execute(input, state.lhs_join_keys);

		// perform the actual probe
		if (sink.external) {
			sink.hash_table->ProbeAndSpill(state.scan_structure, state.lhs_join_keys, state.join_key_state,
			                               state.probe_state, input, *sink.probe_spill, state.spill_state,
			                               state.spill_chunk);
		} else {
			sink.hash_table->Probe(state.scan_structure, state.lhs_join_keys, state.join_key_state, state.probe_state);
		}
	}

	// pass probe data and mapping to Next
	state.lhs_probe_data.ReferenceColumns(input, lhs_probe_columns.col_idxs);
	state.scan_structure.Next(state.lhs_join_keys, state.lhs_probe_data, chunk);

	if (state.scan_structure.PointersExhausted() && chunk.size() == 0) {
		state.scan_structure.is_null = true;
		return OperatorResultType::NEED_MORE_INPUT;
	}
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
enum class HashJoinSourceStage : uint8_t { INIT, BUILD, PROBE, SCAN_HT, DONE };

class HashJoinLocalSourceState;

class HashJoinGlobalSourceState : public GlobalSourceState {
public:
	HashJoinGlobalSourceState(const PhysicalHashJoin &op, ClientContext &context);

	//! Initialize this source state using the info in the sink
	void Initialize(HashJoinGlobalSinkState &sink);
	//! Try to prepare the next stage
	bool TryPrepareNextStage(HashJoinGlobalSinkState &sink);
	//! Prepare the next build/probe/scan_ht stage for external hash join (must hold lock)
	void PrepareBuild(HashJoinGlobalSinkState &sink);
	void PrepareProbe(HashJoinGlobalSinkState &sink);
	void PrepareScanHT(HashJoinGlobalSinkState &sink);
	//! Assigns a task to a local source state
	bool AssignTask(HashJoinGlobalSinkState &sink, HashJoinLocalSourceState &lstate);

	idx_t MaxThreads() override {
		D_ASSERT(op.sink_state);
		auto &gstate = op.sink_state->Cast<HashJoinGlobalSinkState>();

		idx_t count;
		if (gstate.probe_spill) {
			count = probe_count;
		} else if (PropagatesBuildSide(op.join_type)) {
			count = gstate.hash_table->Count();
		} else {
			return 0;
		}
		return count / ((idx_t)STANDARD_VECTOR_SIZE * parallel_scan_chunk_count);
	}

	bool SupportsReuse() const override {
		return true;
	}

public:
	const PhysicalHashJoin &op;

	//! For synchronizing the external hash join
	atomic<HashJoinSourceStage> global_stage;

	//! For HT build synchronization
	idx_t build_chunk_idx = DConstants::INVALID_INDEX;
	idx_t build_chunk_count;
	idx_t build_chunk_done;
	idx_t build_chunks_per_thread = DConstants::INVALID_INDEX;

	//! For probe synchronization
	atomic<idx_t> probe_chunk_count;
	atomic<idx_t> probe_chunk_done;

	//! To determine the number of threads
	idx_t probe_count;
	idx_t parallel_scan_chunk_count;

	//! For full/outer synchronization
	idx_t full_outer_chunk_idx = DConstants::INVALID_INDEX;
	atomic<idx_t> full_outer_chunk_count;
	atomic<idx_t> full_outer_chunk_done;
	idx_t full_outer_chunks_per_thread = DConstants::INVALID_INDEX;

	vector<InterruptState> blocked_tasks;

private:
	void ResetState(ClientContext &context) {
		global_stage = HashJoinSourceStage::INIT;
		build_chunk_idx = DConstants::INVALID_INDEX;
		build_chunk_count = 0;
		build_chunk_done = 0;
		build_chunks_per_thread = DConstants::INVALID_INDEX;
		probe_chunk_count = 0;
		probe_chunk_done = 0;
		probe_count = op.children[0].get().estimated_cardinality;
		parallel_scan_chunk_count = context.config.verify_parallelism ? 1 : 120;
		full_outer_chunk_idx = DConstants::INVALID_INDEX;
		full_outer_chunk_count = 0;
		full_outer_chunk_done = 0;
		full_outer_chunks_per_thread = DConstants::INVALID_INDEX;
		blocked_tasks.clear();
		GlobalSourceState::Reset(context);
	}

public:
	void Reset(ClientContext &context) override {
		ResetState(context);
	}
};

class HashJoinLocalSourceState : public LocalSourceState {
public:
	HashJoinLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate, const PhysicalHashJoin &op,
	                         const HashJoinGlobalSinkState &sink, Allocator &allocator);

	//! Do the work this thread has been assigned
	void ExecuteTask(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate, DataChunk &chunk);
	//! Whether this thread has finished the work it has been assigned
	bool TaskFinished() const;
	//! Build, probe and scan for external hash join
	void ExternalBuild(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate);
	void ExternalProbe(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate, DataChunk &chunk);
	void ExternalScanHT(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate, DataChunk &chunk);

public:
	const PhysicalHashJoin &op;
	//! The stage that this thread was assigned work for
	HashJoinSourceStage local_stage;
	//! Vector with pointers here so we don't have to re-initialize
	Vector addresses;

	//! Chunks assigned to this thread for building the pointer table
	idx_t build_chunk_idx_from = DConstants::INVALID_INDEX;
	idx_t build_chunk_idx_to = DConstants::INVALID_INDEX;

	//! Local scan state for probe spill
	ColumnDataConsumerScanState probe_local_scan;
	//! Chunks for holding the scanned probe collection
	DataChunk lhs_probe_chunk;
	DataChunk lhs_join_keys;
	DataChunk lhs_probe_data;
	TupleDataChunkState join_key_state;
	ExpressionExecutor lhs_join_key_executor;

	//! Scan structure for the external probe
	JoinHashTable::ScanStructure scan_structure;
	JoinHashTable::ProbeState probe_state;
	bool empty_ht_probe_in_progress = false;

	//! Chunks assigned to this thread for a full/outer scan
	idx_t full_outer_chunk_idx_from = DConstants::INVALID_INDEX;
	idx_t full_outer_chunk_idx_to = DConstants::INVALID_INDEX;
	unique_ptr<JoinHTScanState> full_outer_scan_state;

private:
	void ResetState() {
		local_stage = HashJoinSourceStage::INIT;
		build_chunk_idx_from = DConstants::INVALID_INDEX;
		build_chunk_idx_to = DConstants::INVALID_INDEX;
		probe_local_scan.allocator = nullptr;
		probe_local_scan.current_chunk_state.handles.clear();
		probe_local_scan.current_chunk_state.properties = ColumnDataScanProperties::ALLOW_ZERO_COPY;
		probe_local_scan.chunk_index = DConstants::INVALID_INDEX;
		lhs_probe_chunk.Reset();
		lhs_join_keys.Reset();
		lhs_probe_data.Reset();
		TupleDataCollection::InitializeChunkState(join_key_state, op.condition_types);
		scan_structure.Reset();
		empty_ht_probe_in_progress = false;
		full_outer_chunk_idx_from = DConstants::INVALID_INDEX;
		full_outer_chunk_idx_to = DConstants::INVALID_INDEX;
		full_outer_scan_state.reset();
	}

public:
	bool SupportsReuse() const override {
		return true;
	}

	void Reset(ExecutionContext &context, GlobalSourceState &gstate_p) override {
		ResetState();
	}
};

unique_ptr<GlobalSourceState> PhysicalHashJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<HashJoinGlobalSourceState>(*this, context);
}

unique_ptr<LocalSourceState> PhysicalHashJoin::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	return make_uniq<HashJoinLocalSourceState>(context, gstate, *this, sink_state->Cast<HashJoinGlobalSinkState>(),
	                                           BufferAllocator::Get(context.client));
}

HashJoinGlobalSourceState::HashJoinGlobalSourceState(const PhysicalHashJoin &op, ClientContext &context) : op(op) {
	ResetState(context);
}

void HashJoinGlobalSourceState::Initialize(HashJoinGlobalSinkState &sink) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (global_stage != HashJoinSourceStage::INIT) {
		// Another thread initialized
		return;
	}

	// Finalize the probe spill
	if (sink.probe_spill) {
		sink.probe_spill->Finalize();
	}

	global_stage = HashJoinSourceStage::PROBE;
	TryPrepareNextStage(sink);
}

bool HashJoinGlobalSourceState::TryPrepareNextStage(HashJoinGlobalSinkState &sink) {
	switch (global_stage.load()) {
	case HashJoinSourceStage::BUILD:
		if (build_chunk_done == build_chunk_count) {
			sink.hash_table->GetDataCollection().VerifyEverythingPinned();
			sink.hash_table->finalized = true;
			PrepareProbe(sink);
			return true;
		}
		break;
	case HashJoinSourceStage::PROBE:
		if (probe_chunk_done == probe_chunk_count) {
			if (PropagatesBuildSide(op.join_type)) {
				PrepareScanHT(sink);
			} else {
				PrepareBuild(sink);
			}
			return true;
		}
		break;
	case HashJoinSourceStage::SCAN_HT:
		if (full_outer_chunk_done == full_outer_chunk_count) {
			PrepareBuild(sink);
			return true;
		}
		break;
	default:
		break;
	}
	return false;
}

void HashJoinGlobalSourceState::PrepareBuild(HashJoinGlobalSinkState &sink) {
	D_ASSERT(global_stage != HashJoinSourceStage::BUILD);
	auto &ht = *sink.hash_table;

	// Update remaining size
	sink.temporary_memory_state->SetRemainingSizeAndUpdateReservation(sink.context, ht.GetRemainingSize() +
	                                                                                    sink.probe_side_requirement);

	// Try to put the next partitions in the block collection of the HT
	D_ASSERT(!sink.external || sink.temporary_memory_state->GetReservation() >= sink.probe_side_requirement);
	if (!sink.external ||
	    !ht.PrepareExternalFinalize(sink.temporary_memory_state->GetReservation() - sink.probe_side_requirement)) {
		global_stage = HashJoinSourceStage::DONE;
		sink.temporary_memory_state->SetZero();
		return;
	}

	auto &data_collection = ht.GetDataCollection();
	if (data_collection.Count() == 0 && op.EmptyResultIfRHSIsEmpty()) {
		PrepareBuild(sink);
		return;
	}

	build_chunk_idx = 0;
	build_chunk_count = data_collection.ChunkCount();
	build_chunk_done = 0;

	if (sink.context.config.verify_parallelism) {
		build_chunks_per_thread = 1;
	} else {
		if (KeysAreSkewed(sink)) {
			build_chunks_per_thread = build_chunk_count; // This forces single-threaded building
		} else {
			build_chunks_per_thread = // Same task size as in HashJoinFinalizeEvent
			    MaxValue<idx_t>(MinValue(build_chunk_count, HashJoinFinalizeEvent::CHUNKS_PER_TASK), 1);
		}
	}

	ht.AllocatePointerTable();
	ht.InitializePointerTable(0, ht.capacity);

	global_stage = HashJoinSourceStage::BUILD;
}

void HashJoinGlobalSourceState::PrepareProbe(HashJoinGlobalSinkState &sink) {
	sink.probe_spill->PrepareNextProbe();
	const auto &consumer = *sink.probe_spill->consumer;

	probe_chunk_count = consumer.Count() == 0 ? 0 : consumer.ChunkCount();
	probe_chunk_done = 0;

	global_stage = HashJoinSourceStage::PROBE;
	if (probe_chunk_count == 0) {
		TryPrepareNextStage(sink);
		return;
	}
}

void HashJoinGlobalSourceState::PrepareScanHT(HashJoinGlobalSinkState &sink) {
	D_ASSERT(global_stage != HashJoinSourceStage::SCAN_HT);
	auto &ht = *sink.hash_table;

	auto &data_collection = ht.GetDataCollection();
	full_outer_chunk_idx = 0;
	full_outer_chunk_count = data_collection.ChunkCount();
	full_outer_chunk_done = 0;

	full_outer_chunks_per_thread =
	    MaxValue<idx_t>((full_outer_chunk_count + sink.num_threads - 1) / sink.num_threads, 1);

	global_stage = HashJoinSourceStage::SCAN_HT;
}

bool HashJoinGlobalSourceState::AssignTask(HashJoinGlobalSinkState &sink, HashJoinLocalSourceState &lstate) {
	D_ASSERT(lstate.TaskFinished());

	annotated_lock_guard<annotated_mutex> guard(lock);
	switch (global_stage.load()) {
	case HashJoinSourceStage::BUILD:
		if (build_chunk_idx != build_chunk_count) {
			lstate.local_stage = global_stage;
			lstate.build_chunk_idx_from = build_chunk_idx;
			build_chunk_idx = MinValue<idx_t>(build_chunk_count, build_chunk_idx + build_chunks_per_thread);
			lstate.build_chunk_idx_to = build_chunk_idx;
			return true;
		}
		break;
	case HashJoinSourceStage::PROBE:
		if (sink.probe_spill->consumer && sink.probe_spill->consumer->AssignChunk(lstate.probe_local_scan)) {
			lstate.local_stage = global_stage;
			lstate.empty_ht_probe_in_progress = false;
			return true;
		}
		break;
	case HashJoinSourceStage::SCAN_HT:
		if (full_outer_chunk_idx != full_outer_chunk_count) {
			lstate.local_stage = global_stage;
			lstate.full_outer_chunk_idx_from = full_outer_chunk_idx;
			full_outer_chunk_idx =
			    MinValue<idx_t>(full_outer_chunk_count, full_outer_chunk_idx + full_outer_chunks_per_thread);
			lstate.full_outer_chunk_idx_to = full_outer_chunk_idx;
			return true;
		}
		break;
	case HashJoinSourceStage::DONE:
		break;
	default:
		throw InternalException("Unexpected HashJoinSourceStage in AssignTask!");
	}
	return false;
}

HashJoinLocalSourceState::HashJoinLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate,
                                                   const PhysicalHashJoin &op, const HashJoinGlobalSinkState &sink,
                                                   Allocator &allocator)
    : op(op), addresses(LogicalType::POINTER), lhs_join_key_executor(sink.context),
      scan_structure(*sink.hash_table, join_key_state) {
	lhs_probe_chunk.Initialize(allocator, sink.probe_types);
	lhs_join_keys.Initialize(allocator, op.condition_types);

	// initialize with PROBE columns (not just output)
	lhs_probe_data.Initialize(allocator, op.lhs_probe_columns.col_types);

	TupleDataCollection::InitializeChunkState(join_key_state, op.condition_types);

	for (auto &cond : op.conditions) {
		lhs_join_key_executor.AddExpression(cond.GetLHS());
	}
	ResetState();
}

void HashJoinLocalSourceState::ExecuteTask(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate,
                                           DataChunk &chunk) {
	switch (local_stage) {
	case HashJoinSourceStage::BUILD:
		ExternalBuild(sink, gstate);
		break;
	case HashJoinSourceStage::PROBE:
		ExternalProbe(sink, gstate, chunk);
		break;
	case HashJoinSourceStage::SCAN_HT:
		ExternalScanHT(sink, gstate, chunk);
		break;
	default:
		throw InternalException("Unexpected HashJoinSourceStage in ExecuteTask!");
	}
}

bool HashJoinLocalSourceState::TaskFinished() const {
	switch (local_stage) {
	case HashJoinSourceStage::INIT:
	case HashJoinSourceStage::BUILD:
		return true;
	case HashJoinSourceStage::PROBE:
		return scan_structure.is_null && !empty_ht_probe_in_progress;
	case HashJoinSourceStage::SCAN_HT:
		return full_outer_scan_state == nullptr;
	default:
		throw InternalException("Unexpected HashJoinSourceStage in TaskFinished!");
	}
}

void HashJoinLocalSourceState::ExternalBuild(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate) {
	D_ASSERT(local_stage == HashJoinSourceStage::BUILD);

	auto &ht = *sink.hash_table;
	ht.Finalize(build_chunk_idx_from, build_chunk_idx_to, true);

	annotated_lock_guard<annotated_mutex> guard(gstate.lock);
	gstate.build_chunk_done += build_chunk_idx_to - build_chunk_idx_from;
}

void HashJoinLocalSourceState::ExternalProbe(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate,
                                             DataChunk &chunk) {
	D_ASSERT(local_stage == HashJoinSourceStage::PROBE && sink.hash_table->finalized);

	if (!scan_structure.is_null) {
		// still have elements remaining
		scan_structure.Next(lhs_join_keys, lhs_probe_data, chunk);
		if (chunk.size() != 0 || !scan_structure.PointersExhausted()) {
			return;
		}
	}

	if (!scan_structure.is_null || empty_ht_probe_in_progress) {
		scan_structure.is_null = true;
		empty_ht_probe_in_progress = false;
		sink.probe_spill->consumer->FinishChunk(probe_local_scan);
		annotated_lock_guard<annotated_mutex> guard(gstate.lock);
		gstate.probe_chunk_done++;
		return;
	}

	// Scan input chunk for next probe
	sink.probe_spill->consumer->ScanChunk(probe_local_scan, lhs_probe_chunk);

	// Get the probe chunk columns/hashes
	lhs_join_keys.Reset();
	lhs_join_key_executor.Execute(lhs_probe_chunk, lhs_join_keys);

	// reference ALL probe columns
	lhs_probe_data.ReferenceColumns(lhs_probe_chunk, gstate.op.lhs_probe_columns.col_idxs);

	if (sink.hash_table->Count() == 0 && !gstate.op.EmptyResultIfRHSIsEmpty()) {
		// for empty result, only need output columns (no predicate evaluation)
		lhs_probe_data.ReferenceColumns(lhs_probe_chunk, gstate.op.lhs_output_columns.col_idxs);
		gstate.op.ConstructEmptyJoinResult(sink.hash_table->join_type, sink.hash_table->has_null, lhs_probe_data,
		                                   chunk);
		empty_ht_probe_in_progress = true;
		return;
	}

	// Perform the probe
	auto precomputed_hashes = &lhs_probe_chunk.data.back();
	sink.hash_table->Probe(scan_structure, lhs_join_keys, join_key_state, probe_state, precomputed_hashes);
	scan_structure.Next(lhs_join_keys, lhs_probe_data, chunk);
}

void HashJoinLocalSourceState::ExternalScanHT(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate,
                                              DataChunk &chunk) {
	D_ASSERT(local_stage == HashJoinSourceStage::SCAN_HT);

	if (!full_outer_scan_state) {
		full_outer_scan_state = make_uniq<JoinHTScanState>(sink.hash_table->GetDataCollection(),
		                                                   full_outer_chunk_idx_from, full_outer_chunk_idx_to);
	}
	sink.hash_table->ScanFullOuter(*full_outer_scan_state, addresses, chunk);

	if (chunk.size() == 0) {
		full_outer_scan_state = nullptr;
		annotated_lock_guard<annotated_mutex> guard(gstate.lock);
		gstate.full_outer_chunk_done += full_outer_chunk_idx_to - full_outer_chunk_idx_from;
	}
}

SourceResultType PhysicalHashJoin::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	auto &sink = sink_state->Cast<HashJoinGlobalSinkState>();
	auto &gstate = input.global_state.Cast<HashJoinGlobalSourceState>();
	auto &lstate = input.local_state.Cast<HashJoinLocalSourceState>();
	sink.scanned_data = true;

	if (!sink.external && !PropagatesBuildSide(join_type)) {
		annotated_lock_guard<annotated_mutex> guard(gstate.lock);
		if (gstate.global_stage != HashJoinSourceStage::DONE) {
			gstate.global_stage = HashJoinSourceStage::DONE;
			if (sink.preserve_build_for_reuse) {
				sink.scanned_data = false;
			} else {
				sink.hash_table->Reset();
				sink.temporary_memory_state->SetZero();
			}
		}
		return SourceResultType::FINISHED;
	}

	if (gstate.global_stage == HashJoinSourceStage::INIT) {
		gstate.Initialize(sink);
	}

	// Any call to GetData must produce tuples, otherwise the pipeline executor thinks that we're done
	// Therefore, we loop until we've produced tuples, or until the operator is actually done
	while (gstate.global_stage != HashJoinSourceStage::DONE && chunk.size() == 0) {
		if (!lstate.TaskFinished() || gstate.AssignTask(sink, lstate)) {
			lstate.ExecuteTask(sink, gstate, chunk);
		} else {
			annotated_lock_guard<annotated_mutex> guard(gstate.lock);
			if (gstate.TryPrepareNextStage(sink) || gstate.global_stage == HashJoinSourceStage::DONE) {
				gstate.UnblockTasks();
			} else {
				return gstate.BlockSource(input.interrupt_state);
			}
		}
	}

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

ProgressData PhysicalHashJoin::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &sink = sink_state->Cast<HashJoinGlobalSinkState>();
	auto &gstate = gstate_p.Cast<HashJoinGlobalSourceState>();

	ProgressData res;

	if (!sink.external) {
		if (PropagatesBuildSide(join_type)) {
			res.done = static_cast<double>(gstate.full_outer_chunk_done);
			res.total = static_cast<double>(gstate.full_outer_chunk_count);
			return res;
		}
		res.done = 0.0;
		res.total = 1.0;
		return res;
	}

	const auto &ht = *sink.hash_table;
	const auto num_partitions = static_cast<double>(RadixPartitioning::NumberOfPartitions(ht.GetRadixBits()));

	res.done = static_cast<double>(ht.FinishedPartitionCount());
	res.total = num_partitions;

	const auto probe_chunk_done = static_cast<double>(gstate.probe_chunk_done);
	const auto probe_chunk_count = static_cast<double>(gstate.probe_chunk_count);
	if (probe_chunk_count != 0) {
		// Progress of the current round of probing
		auto probe_progress = probe_chunk_done / probe_chunk_count;
		// Weighed by the number of partitions
		probe_progress *= static_cast<double>(ht.CurrentPartitionCount());
		// Add it to the progress
		res.done += probe_progress;
	}

	return res;
}

InsertionOrderPreservingMap<string> PhysicalHashJoin::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Join Type"] = EnumUtil::ToString(join_type);

	string condition_info;
	for (idx_t i = 0; i < conditions.size(); i++) {
		auto &join_condition = conditions[i];
		if (i > 0) {
			condition_info += "\n";
		}
		condition_info += StringUtil::Format("%s %s %s", join_condition.GetLHS().GetName().GetIdentifierName(),
		                                     ExpressionTypeToOperator(join_condition.GetComparisonType()),
		                                     join_condition.GetRHS().GetName().GetIdentifierName());
	}

	if (predicate) {
		if (!condition_info.empty()) {
			condition_info += "\n";
		}
		condition_info += predicate->ToString();
	}

	result["Conditions"] = condition_info;

	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
