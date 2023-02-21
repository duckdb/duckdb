#include "duckdb/execution/operator/aggregate/physical_window.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/column/column_data_consumer.hpp"
#include "duckdb/common/types/row/row_data_collection_scanner.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/windows_undefs.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/partitionable_hashtable.hpp"
#include "duckdb/execution/window_segment_tree.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/common/radix_partitioning.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>

namespace duckdb {

class WindowGlobalHashGroup {
public:
	using GlobalSortStatePtr = unique_ptr<GlobalSortState>;
	using LocalSortStatePtr = unique_ptr<LocalSortState>;
	using Orders = vector<BoundOrderByNode>;
	using Types = vector<LogicalType>;

	WindowGlobalHashGroup(BufferManager &buffer_manager, const Orders &partitions, const Orders &orders,
	                      const Types &payload_types, bool external)
	    : count(0) {

		RowLayout payload_layout;
		payload_layout.Initialize(payload_types);
		global_sort = make_unique<GlobalSortState>(buffer_manager, orders, payload_layout);
		global_sort->external = external;

		partition_layout = global_sort->sort_layout.GetPrefixComparisonLayout(partitions.size());
	}

	void ComputeMasks(ValidityMask &partition_mask, ValidityMask &order_mask);

	GlobalSortStatePtr global_sort;
	atomic<idx_t> count;

	// Mask computation
	SortLayout partition_layout;
};

void WindowGlobalHashGroup::ComputeMasks(ValidityMask &partition_mask, ValidityMask &order_mask) {
	D_ASSERT(count > 0);

	//	Set up a comparator for the partition subset
	const auto partition_size = partition_layout.comparison_size;

	SBIterator prev(*global_sort, ExpressionType::COMPARE_LESSTHAN);
	SBIterator curr(*global_sort, ExpressionType::COMPARE_LESSTHAN);

	partition_mask.SetValidUnsafe(0);
	order_mask.SetValidUnsafe(0);
	for (++curr; curr.GetIndex() < count; ++curr) {
		//	Compare the partition subset first because if that differs, then so does the full ordering
		int part_cmp = 0;
		if (partition_layout.all_constant) {
			part_cmp = FastMemcmp(prev.entry_ptr, curr.entry_ptr, partition_size);
		} else {
			part_cmp = Comparators::CompareTuple(prev.scan, curr.scan, prev.entry_ptr, curr.entry_ptr, partition_layout,
			                                     prev.external);
		}

		if (part_cmp) {
			partition_mask.SetValidUnsafe(curr.GetIndex());
			order_mask.SetValidUnsafe(curr.GetIndex());
		} else if (prev.Compare(curr)) {
			order_mask.SetValidUnsafe(curr.GetIndex());
		}
		++prev;
	}
}

//	Global sink state
class WindowGlobalSinkState : public GlobalSinkState {
public:
	using HashGroupPtr = unique_ptr<WindowGlobalHashGroup>;
	using Orders = vector<BoundOrderByNode>;
	using Types = vector<LogicalType>;

	using GroupingPartition = unique_ptr<PartitionedColumnData>;
	using GroupingAppend = unique_ptr<PartitionedColumnDataAppendState>;

	WindowGlobalSinkState(const PhysicalWindow &op_p, ClientContext &context)
	    : op(op_p), context(context), buffer_manager(BufferManager::GetBufferManager(context)),
	      allocator(Allocator::Get(context)), payload_types(op.children[0]->types), memory_per_thread(0), count(0),
	      mode(DBConfig::GetConfig(context).options.window_mode) {

		D_ASSERT(op.select_list[0]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto wexpr = reinterpret_cast<BoundWindowExpression *>(op.select_list[0].get());

		// we sort by both 1) partition by expression list and 2) order by expressions
		const auto partition_cols = wexpr->partitions.size();
		for (idx_t prt_idx = 0; prt_idx < partition_cols; prt_idx++) {
			auto &pexpr = wexpr->partitions[prt_idx];

			if (wexpr->partitions_stats.empty() || !wexpr->partitions_stats[prt_idx]) {
				orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, pexpr->Copy(), nullptr);
			} else {
				orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, pexpr->Copy(),
				                    wexpr->partitions_stats[prt_idx]->Copy());
			}
			partitions.emplace_back(orders.back().Copy());
		}

		for (const auto &order : wexpr->orders) {
			orders.emplace_back(order.Copy());
		}

		memory_per_thread = op.GetMaxThreadMemory(context);
		external = ClientConfig::GetConfig(context).force_external;

		if (!orders.empty()) {
			grouping_types = payload_types;
			grouping_types.push_back(LogicalType::HASH);

			ResizeGroupingData(op.estimated_cardinality);
		}
	}

	void UpdateLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append);
	void CombineLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append);

	void BuildSortState(ColumnDataCollection &group_data, WindowGlobalHashGroup &global_sort);

	const PhysicalWindow &op;
	ClientContext &context;
	BufferManager &buffer_manager;
	Allocator &allocator;
	mutex lock;

	// OVER(PARTITION BY...) (hash grouping)
	unique_ptr<RadixPartitionedColumnData> grouping_data;
	//! Payload plus hash column
	Types grouping_types;

	// OVER(...) (sorting)
	Orders partitions;
	Orders orders;
	const Types payload_types;
	vector<HashGroupPtr> hash_groups;
	bool external;

	// OVER() (no sorting)
	unique_ptr<RowDataCollection> rows;
	unique_ptr<RowDataCollection> strings;

	// Threading
	idx_t memory_per_thread;
	atomic<idx_t> count;
	WindowAggregationMode mode;

private:
	void ResizeGroupingData(idx_t cardinality);
	void SyncLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append);
};

void WindowGlobalSinkState::ResizeGroupingData(idx_t cardinality) {
	//	Is the average partition size too large?
	const idx_t partition_size = STANDARD_ROW_GROUPS_SIZE;
	const auto bits = grouping_data ? grouping_data->GetRadixBits() : 0;
	auto new_bits = bits ? bits : 4;
	while (new_bits < 10 && (cardinality / RadixPartitioning::NumberOfPartitions(new_bits)) > partition_size) {
		++new_bits;
	}

	// Repartition the grouping data
	if (new_bits != bits) {
		const auto hash_col_idx = payload_types.size();
		auto new_grouping_data =
		    make_unique<RadixPartitionedColumnData>(context, grouping_types, new_bits, hash_col_idx);

		// We have to append to a shared copy for some reason
		if (grouping_data) {
			auto new_shared = new_grouping_data->CreateShared();
			PartitionedColumnDataAppendState shared_append;
			new_shared->InitializeAppendState(shared_append);

			auto &partitions = grouping_data->GetPartitions();
			for (auto &partition : partitions) {
				ColumnDataScanState scanner;
				partition->InitializeScan(scanner);

				DataChunk scan_chunk;
				partition->InitializeScanChunk(scan_chunk);
				for (scan_chunk.Reset(); partition->Scan(scanner, scan_chunk); scan_chunk.Reset()) {
					new_shared->Append(shared_append, scan_chunk);
				}
			}
			new_shared->FlushAppendState(shared_append);
			new_grouping_data->Combine(*new_shared);
		}

		grouping_data = std::move(new_grouping_data);
	}
}

void WindowGlobalSinkState::SyncLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append) {
	// We are done if the local_partition is right sized.
	auto local_radix = (RadixPartitionedColumnData *)local_partition.get();
	if (local_radix->GetRadixBits() == grouping_data->GetRadixBits()) {
		return;
	}

	// If the local partition is now too small, flush it and reallocate
	auto new_partition = grouping_data->CreateShared();
	auto new_append = make_unique<PartitionedColumnDataAppendState>();
	new_partition->InitializeAppendState(*new_append);

	local_partition->FlushAppendState(*local_append);
	auto &local_groups = local_partition->GetPartitions();
	for (auto &local_group : local_groups) {
		ColumnDataScanState scanner;
		local_group->InitializeScan(scanner);

		DataChunk scan_chunk;
		local_group->InitializeScanChunk(scan_chunk);
		for (scan_chunk.Reset(); local_group->Scan(scanner, scan_chunk); scan_chunk.Reset()) {
			new_partition->Append(*new_append, scan_chunk);
		}
	}

	// The append state has stale pointers to the old local partition, so nuke it from orbit.
	new_partition->FlushAppendState(*new_append);

	local_partition = std::move(new_partition);
	local_append = make_unique<PartitionedColumnDataAppendState>();
	local_partition->InitializeAppendState(*local_append);
}

void WindowGlobalSinkState::UpdateLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append) {
	// Make sure grouping_data doesn't change under us.
	lock_guard<mutex> guard(lock);

	if (!local_partition) {
		local_partition = grouping_data->CreateShared();
		local_append = make_unique<PartitionedColumnDataAppendState>();
		local_partition->InitializeAppendState(*local_append);
		return;
	}

	// 	Grow the groups if they are too big
	ResizeGroupingData(count);

	//	Sync local partition to have the same bit count
	SyncLocalPartition(local_partition, local_append);
}

void WindowGlobalSinkState::CombineLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append) {
	if (!local_partition) {
		return;
	}
	local_partition->FlushAppendState(*local_append);

	// Make sure grouping_data doesn't change under us.
	// Combine has an internal mutex, so this is single-threaded anyway.
	lock_guard<mutex> guard(lock);
	SyncLocalPartition(local_partition, local_append);
	grouping_data->Combine(*local_partition);
}

void WindowGlobalSinkState::BuildSortState(ColumnDataCollection &group_data, WindowGlobalHashGroup &hash_group) {
	auto &global_sort = *hash_group.global_sort;

	//	 Set up the sort expression computation.
	vector<LogicalType> sort_types;
	ExpressionExecutor executor(context);
	for (auto &order : orders) {
		auto &oexpr = order.expression;
		sort_types.emplace_back(oexpr->return_type);
		executor.AddExpression(*oexpr);
	}
	DataChunk sort_chunk;
	sort_chunk.Initialize(allocator, sort_types);

	// Copy the data from the group into the sort code.
	LocalSortState local_sort;
	local_sort.Initialize(global_sort, global_sort.buffer_manager);

	//	Strip hash column
	DataChunk payload_chunk;
	payload_chunk.Initialize(allocator, payload_types);

	vector<column_t> column_ids;
	column_ids.reserve(payload_types.size());
	for (column_t i = 0; i < payload_types.size(); ++i) {
		column_ids.emplace_back(i);
	}
	ColumnDataConsumer scanner(group_data, column_ids);
	ColumnDataConsumerScanState chunk_state;
	chunk_state.current_chunk_state.properties = ColumnDataScanProperties::ALLOW_ZERO_COPY;
	scanner.InitializeScan();
	for (auto chunk_idx = scanner.ChunkCount(); chunk_idx-- > 0;) {
		if (!scanner.AssignChunk(chunk_state)) {
			break;
		}
		scanner.ScanChunk(chunk_state, payload_chunk);

		sort_chunk.Reset();
		executor.Execute(payload_chunk, sort_chunk);

		local_sort.SinkChunk(sort_chunk, payload_chunk);
		if (local_sort.SizeInBytes() > memory_per_thread) {
			local_sort.Sort(global_sort, true);
		}
		scanner.FinishChunk(chunk_state);
	}

	global_sort.AddLocalState(local_sort);

	hash_group.count += group_data.Count();
}

//	Per-thread sink state
class WindowLocalSinkState : public LocalSinkState {
public:
	WindowLocalSinkState(ClientContext &context, const PhysicalWindow &op_p)
	    : op(op_p), allocator(Allocator::Get(context)), executor(context) {
		D_ASSERT(op.select_list[0]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto wexpr = reinterpret_cast<BoundWindowExpression *>(op.select_list[0].get());

		vector<LogicalType> group_types;
		for (idx_t prt_idx = 0; prt_idx < wexpr->partitions.size(); prt_idx++) {
			auto &pexpr = wexpr->partitions[prt_idx];
			group_types.push_back(pexpr->return_type);
			executor.AddExpression(*pexpr);
		}
		sort_cols = wexpr->orders.size() + group_types.size();

		if (sort_cols) {
			if (!group_types.empty()) {
				// OVER(PARTITION BY...)
				group_chunk.Initialize(allocator, group_types);
			}
			// OVER(...)
			auto payload_types = op.children[0]->types;
			payload_types.emplace_back(LogicalType::HASH);
			payload_chunk.Initialize(allocator, payload_types);
		} else {
			// OVER()
			payload_layout.Initialize(op.children[0]->types);
		}
	}

	// Global state
	const PhysicalWindow &op;
	Allocator &allocator;

	// OVER(PARTITION BY...) (hash grouping)
	ExpressionExecutor executor;
	DataChunk group_chunk;
	DataChunk payload_chunk;
	unique_ptr<PartitionedColumnData> local_partition;
	unique_ptr<PartitionedColumnDataAppendState> local_append;

	// OVER(...) (sorting)
	size_t sort_cols;

	// OVER() (no sorting)
	RowLayout payload_layout;
	unique_ptr<RowDataCollection> rows;
	unique_ptr<RowDataCollection> strings;

	//! Compute the hash values
	void Hash(DataChunk &input_chunk, Vector &hash_vector);
	//! Sink an input chunk
	void Sink(DataChunk &input_chunk, WindowGlobalSinkState &gstate);
	//! Merge the state into the global state.
	void Combine(WindowGlobalSinkState &gstate);
};

void WindowLocalSinkState::Hash(DataChunk &input_chunk, Vector &hash_vector) {
	const auto count = input_chunk.size();
	if (group_chunk.ColumnCount() > 0) {
		// OVER(PARTITION BY...) (hash grouping)
		group_chunk.Reset();
		executor.Execute(input_chunk, group_chunk);
		VectorOperations::Hash(group_chunk.data[0], hash_vector, count);
		for (idx_t prt_idx = 1; prt_idx < group_chunk.ColumnCount(); ++prt_idx) {
			VectorOperations::CombineHash(hash_vector, group_chunk.data[prt_idx], count);
		}
	} else {
		// OVER(...) (sorting)
		// Single partition => single hash value
		hash_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto hashes = ConstantVector::GetData<hash_t>(hash_vector);
		hashes[0] = 0;
	}
}

void WindowLocalSinkState::Sink(DataChunk &input_chunk, WindowGlobalSinkState &gstate) {
	gstate.count += input_chunk.size();

	// OVER()
	if (sort_cols == 0) {
		//	No sorts, so build paged row chunks
		if (!rows) {
			const auto entry_size = payload_layout.GetRowWidth();
			const auto capacity = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, (Storage::BLOCK_SIZE / entry_size) + 1);
			rows = make_unique<RowDataCollection>(gstate.buffer_manager, capacity, entry_size);
			strings = make_unique<RowDataCollection>(gstate.buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1, true);
		}
		const auto row_count = input_chunk.size();
		const auto row_sel = FlatVector::IncrementalSelectionVector();
		Vector addresses(LogicalType::POINTER);
		auto key_locations = FlatVector::GetData<data_ptr_t>(addresses);
		const auto prev_rows_blocks = rows->blocks.size();
		auto handles = rows->Build(row_count, key_locations, nullptr, row_sel);
		auto input_data = input_chunk.ToUnifiedFormat();
		RowOperations::Scatter(input_chunk, input_data.get(), payload_layout, addresses, *strings, *row_sel, row_count);
		// Mark that row blocks contain pointers (heap blocks are pinned)
		if (!payload_layout.AllConstant()) {
			D_ASSERT(strings->keep_pinned);
			for (size_t i = prev_rows_blocks; i < rows->blocks.size(); ++i) {
				rows->blocks[i]->block->SetSwizzling("WindowLocalSinkState::Sink");
			}
		}
		return;
	}

	// OVER(...)
	gstate.UpdateLocalPartition(local_partition, local_append);

	payload_chunk.Reset();
	auto &hash_vector = payload_chunk.data.back();
	Hash(input_chunk, hash_vector);
	for (idx_t col_idx = 0; col_idx < input_chunk.ColumnCount(); ++col_idx) {
		payload_chunk.data[col_idx].Reference(input_chunk.data[col_idx]);
	}
	payload_chunk.SetCardinality(input_chunk);

	local_partition->Append(*local_append, payload_chunk);
}

void WindowLocalSinkState::Combine(WindowGlobalSinkState &gstate) {
	// OVER()
	if (sort_cols == 0) {
		// Only one partition again, so need a global lock.
		lock_guard<mutex> glock(gstate.lock);
		if (gstate.rows) {
			if (rows) {
				gstate.rows->Merge(*rows);
				gstate.strings->Merge(*strings);
				rows.reset();
				strings.reset();
			}
		} else {
			gstate.rows = std::move(rows);
			gstate.strings = std::move(strings);
		}
		return;
	}

	// OVER(...)
	gstate.CombineLocalPartition(local_partition, local_append);
}

// this implements a sorted window functions variant
PhysicalWindow::PhysicalWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list_p,
                               idx_t estimated_cardinality, PhysicalOperatorType type)
    : PhysicalOperator(type, std::move(types), estimated_cardinality), select_list(std::move(select_list_p)) {
	is_order_dependent = false;
	for (auto &expr : select_list) {
		D_ASSERT(expr->expression_class == ExpressionClass::BOUND_WINDOW);
		auto &bound_window = (BoundWindowExpression &)*expr;
		if (bound_window.partitions.empty() && bound_window.orders.empty()) {
			is_order_dependent = true;
		}
	}
}

static idx_t FindNextStart(const ValidityMask &mask, idx_t l, const idx_t r, idx_t &n) {
	if (mask.AllValid()) {
		auto start = MinValue(l + n - 1, r);
		n -= MinValue(n, r - l);
		return start;
	}

	while (l < r) {
		//	If l is aligned with the start of a block, and the block is blank, then skip forward one block.
		idx_t entry_idx;
		idx_t shift;
		mask.GetEntryIndex(l, entry_idx, shift);

		const auto block = mask.GetValidityEntry(entry_idx);
		if (mask.NoneValid(block) && !shift) {
			l += ValidityMask::BITS_PER_VALUE;
			continue;
		}

		// Loop over the block
		for (; shift < ValidityMask::BITS_PER_VALUE && l < r; ++shift, ++l) {
			if (mask.RowIsValid(block, shift) && --n == 0) {
				return MinValue(l, r);
			}
		}
	}

	//	Didn't find a start so return the end of the range
	return r;
}

static idx_t FindPrevStart(const ValidityMask &mask, const idx_t l, idx_t r, idx_t &n) {
	if (mask.AllValid()) {
		auto start = (r <= l + n) ? l : r - n;
		n -= r - start;
		return start;
	}

	while (l < r) {
		// If r is aligned with the start of a block, and the previous block is blank,
		// then skip backwards one block.
		idx_t entry_idx;
		idx_t shift;
		mask.GetEntryIndex(r - 1, entry_idx, shift);

		const auto block = mask.GetValidityEntry(entry_idx);
		if (mask.NoneValid(block) && (shift + 1 == ValidityMask::BITS_PER_VALUE)) {
			// r is nonzero (> l) and word aligned, so this will not underflow.
			r -= ValidityMask::BITS_PER_VALUE;
			continue;
		}

		// Loop backwards over the block
		// shift is probing r-1 >= l >= 0
		for (++shift; shift-- > 0; --r) {
			if (mask.RowIsValid(block, shift) && --n == 0) {
				return MaxValue(l, r - 1);
			}
		}
	}

	//	Didn't find a start so return the start of the range
	return l;
}

static void PrepareInputExpressions(Expression **exprs, idx_t expr_count, ExpressionExecutor &executor,
                                    DataChunk &chunk) {
	if (expr_count == 0) {
		return;
	}

	vector<LogicalType> types;
	for (idx_t expr_idx = 0; expr_idx < expr_count; ++expr_idx) {
		types.push_back(exprs[expr_idx]->return_type);
		executor.AddExpression(*exprs[expr_idx]);
	}

	if (!types.empty()) {
		auto &allocator = executor.GetAllocator();
		chunk.Initialize(allocator, types);
	}
}

static void PrepareInputExpression(Expression *expr, ExpressionExecutor &executor, DataChunk &chunk) {
	PrepareInputExpressions(&expr, 1, executor, chunk);
}

struct WindowInputExpression {
	WindowInputExpression(Expression *expr_p, ClientContext &context)
	    : expr(expr_p), ptype(PhysicalType::INVALID), scalar(true), executor(context) {
		if (expr) {
			PrepareInputExpression(expr, executor, chunk);
			ptype = expr->return_type.InternalType();
			scalar = expr->IsScalar();
		}
	}

	void Execute(DataChunk &input_chunk) {
		if (expr) {
			chunk.Reset();
			executor.Execute(input_chunk, chunk);
			chunk.Verify();
		}
	}

	template <typename T>
	inline T GetCell(idx_t i) const {
		D_ASSERT(!chunk.data.empty());
		const auto data = FlatVector::GetData<T>(chunk.data[0]);
		return data[scalar ? 0 : i];
	}

	inline bool CellIsNull(idx_t i) const {
		D_ASSERT(!chunk.data.empty());
		if (chunk.data[0].GetVectorType() == VectorType::CONSTANT_VECTOR) {
			return ConstantVector::IsNull(chunk.data[0]);
		}
		return FlatVector::IsNull(chunk.data[0], i);
	}

	inline void CopyCell(Vector &target, idx_t target_offset) const {
		D_ASSERT(!chunk.data.empty());
		auto &source = chunk.data[0];
		auto source_offset = scalar ? 0 : target_offset;
		VectorOperations::Copy(source, target, source_offset + 1, source_offset, target_offset);
	}

	Expression *expr;
	PhysicalType ptype;
	bool scalar;
	ExpressionExecutor executor;
	DataChunk chunk;
};

struct WindowInputColumn {
	WindowInputColumn(Expression *expr_p, ClientContext &context, idx_t capacity_p)
	    : input_expr(expr_p, context), count(0), capacity(capacity_p) {
		if (input_expr.expr) {
			target = make_unique<Vector>(input_expr.chunk.data[0].GetType(), capacity);
		}
	}

	void Append(DataChunk &input_chunk) {
		if (input_expr.expr && (!input_expr.scalar || !count)) {
			input_expr.Execute(input_chunk);
			auto &source = input_expr.chunk.data[0];
			const auto source_count = input_expr.chunk.size();
			D_ASSERT(count + source_count <= capacity);
			VectorOperations::Copy(source, *target, source_count, 0, count);
			count += source_count;
		}
	}

	inline bool CellIsNull(idx_t i) {
		D_ASSERT(target);
		D_ASSERT(i < count);
		return FlatVector::IsNull(*target, input_expr.scalar ? 0 : i);
	}

	template <typename T>
	inline T GetCell(idx_t i) {
		D_ASSERT(target);
		D_ASSERT(i < count);
		const auto data = FlatVector::GetData<T>(*target);
		return data[input_expr.scalar ? 0 : i];
	}

	WindowInputExpression input_expr;

private:
	unique_ptr<Vector> target;
	idx_t count;
	idx_t capacity;
};

static inline bool BoundaryNeedsPeer(const WindowBoundary &boundary) {
	switch (boundary) {
	case WindowBoundary::CURRENT_ROW_RANGE:
	case WindowBoundary::EXPR_PRECEDING_RANGE:
	case WindowBoundary::EXPR_FOLLOWING_RANGE:
		return true;
	default:
		return false;
	}
}

struct WindowBoundariesState {
	static inline bool IsScalar(const unique_ptr<Expression> &expr) {
		return expr ? expr->IsScalar() : true;
	}

	WindowBoundariesState(BoundWindowExpression *wexpr, const idx_t input_size)
	    : type(wexpr->type), input_size(input_size), start_boundary(wexpr->start), end_boundary(wexpr->end),
	      partition_count(wexpr->partitions.size()), order_count(wexpr->orders.size()),
	      range_sense(wexpr->orders.empty() ? OrderType::INVALID : wexpr->orders[0].type),
	      has_preceding_range(wexpr->start == WindowBoundary::EXPR_PRECEDING_RANGE ||
	                          wexpr->end == WindowBoundary::EXPR_PRECEDING_RANGE),
	      has_following_range(wexpr->start == WindowBoundary::EXPR_FOLLOWING_RANGE ||
	                          wexpr->end == WindowBoundary::EXPR_FOLLOWING_RANGE),
	      needs_peer(BoundaryNeedsPeer(wexpr->end) || wexpr->type == ExpressionType::WINDOW_CUME_DIST) {
	}

	void Update(const idx_t row_idx, WindowInputColumn &range_collection, const idx_t source_offset,
	            WindowInputExpression &boundary_start, WindowInputExpression &boundary_end,
	            const ValidityMask &partition_mask, const ValidityMask &order_mask);

	// Cached lookups
	const ExpressionType type;
	const idx_t input_size;
	const WindowBoundary start_boundary;
	const WindowBoundary end_boundary;
	const size_t partition_count;
	const size_t order_count;
	const OrderType range_sense;
	const bool has_preceding_range;
	const bool has_following_range;
	const bool needs_peer;

	idx_t partition_start = 0;
	idx_t partition_end = 0;
	idx_t peer_start = 0;
	idx_t peer_end = 0;
	idx_t valid_start = 0;
	idx_t valid_end = 0;
	int64_t window_start = -1;
	int64_t window_end = -1;
	bool is_same_partition = false;
	bool is_peer = false;
};

static bool WindowNeedsRank(BoundWindowExpression *wexpr) {
	return wexpr->type == ExpressionType::WINDOW_PERCENT_RANK || wexpr->type == ExpressionType::WINDOW_RANK ||
	       wexpr->type == ExpressionType::WINDOW_RANK_DENSE || wexpr->type == ExpressionType::WINDOW_CUME_DIST;
}

template <typename T>
static T GetCell(DataChunk &chunk, idx_t column, idx_t index) {
	D_ASSERT(chunk.ColumnCount() > column);
	auto &source = chunk.data[column];
	const auto data = FlatVector::GetData<T>(source);
	return data[index];
}

static bool CellIsNull(DataChunk &chunk, idx_t column, idx_t index) {
	D_ASSERT(chunk.ColumnCount() > column);
	auto &source = chunk.data[column];
	return FlatVector::IsNull(source, index);
}

static void CopyCell(DataChunk &chunk, idx_t column, idx_t index, Vector &target, idx_t target_offset) {
	D_ASSERT(chunk.ColumnCount() > column);
	auto &source = chunk.data[column];
	VectorOperations::Copy(source, target, index + 1, index, target_offset);
}

template <typename T>
struct WindowColumnIterator {
	using iterator = WindowColumnIterator<T>;
	using iterator_category = std::forward_iterator_tag;
	using difference_type = std::ptrdiff_t;
	using value_type = T;
	using reference = T;
	using pointer = idx_t;

	explicit WindowColumnIterator(WindowInputColumn &coll_p, pointer pos_p = 0) : coll(&coll_p), pos(pos_p) {
	}

	inline reference operator*() const {
		return coll->GetCell<T>(pos);
	}
	inline explicit operator pointer() const {
		return pos;
	}

	inline iterator &operator++() {
		++pos;
		return *this;
	}
	inline iterator operator++(int) {
		auto result = *this;
		++(*this);
		return result;
	}

	friend inline bool operator==(const iterator &a, const iterator &b) {
		return a.pos == b.pos;
	}
	friend inline bool operator!=(const iterator &a, const iterator &b) {
		return a.pos != b.pos;
	}

private:
	WindowInputColumn *coll;
	pointer pos;
};

template <typename T, typename OP>
struct OperationCompare : public std::function<bool(T, T)> {
	inline bool operator()(const T &lhs, const T &val) const {
		return OP::template Operation(lhs, val);
	}
};

template <typename T, typename OP, bool FROM>
static idx_t FindTypedRangeBound(WindowInputColumn &over, const idx_t order_begin, const idx_t order_end,
                                 WindowInputExpression &boundary, const idx_t boundary_row) {
	D_ASSERT(!boundary.CellIsNull(boundary_row));
	const auto val = boundary.GetCell<T>(boundary_row);

	OperationCompare<T, OP> comp;
	WindowColumnIterator<T> begin(over, order_begin);
	WindowColumnIterator<T> end(over, order_end);
	if (FROM) {
		return idx_t(std::lower_bound(begin, end, val, comp));
	} else {
		return idx_t(std::upper_bound(begin, end, val, comp));
	}
}

template <typename OP, bool FROM>
static idx_t FindRangeBound(WindowInputColumn &over, const idx_t order_begin, const idx_t order_end,
                            WindowInputExpression &boundary, const idx_t expr_idx) {
	D_ASSERT(boundary.chunk.ColumnCount() == 1);
	D_ASSERT(boundary.chunk.data[0].GetType().InternalType() == over.input_expr.ptype);

	switch (over.input_expr.ptype) {
	case PhysicalType::INT8:
		return FindTypedRangeBound<int8_t, OP, FROM>(over, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INT16:
		return FindTypedRangeBound<int16_t, OP, FROM>(over, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INT32:
		return FindTypedRangeBound<int32_t, OP, FROM>(over, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INT64:
		return FindTypedRangeBound<int64_t, OP, FROM>(over, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::UINT8:
		return FindTypedRangeBound<uint8_t, OP, FROM>(over, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::UINT16:
		return FindTypedRangeBound<uint16_t, OP, FROM>(over, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::UINT32:
		return FindTypedRangeBound<uint32_t, OP, FROM>(over, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::UINT64:
		return FindTypedRangeBound<uint64_t, OP, FROM>(over, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INT128:
		return FindTypedRangeBound<hugeint_t, OP, FROM>(over, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::FLOAT:
		return FindTypedRangeBound<float, OP, FROM>(over, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::DOUBLE:
		return FindTypedRangeBound<double, OP, FROM>(over, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INTERVAL:
		return FindTypedRangeBound<interval_t, OP, FROM>(over, order_begin, order_end, boundary, expr_idx);
	default:
		throw InternalException("Unsupported column type for RANGE");
	}
}

template <bool FROM>
static idx_t FindOrderedRangeBound(WindowInputColumn &over, const OrderType range_sense, const idx_t order_begin,
                                   const idx_t order_end, WindowInputExpression &boundary, const idx_t expr_idx) {
	switch (range_sense) {
	case OrderType::ASCENDING:
		return FindRangeBound<LessThan, FROM>(over, order_begin, order_end, boundary, expr_idx);
	case OrderType::DESCENDING:
		return FindRangeBound<GreaterThan, FROM>(over, order_begin, order_end, boundary, expr_idx);
	default:
		throw InternalException("Unsupported ORDER BY sense for RANGE");
	}
}

void WindowBoundariesState::Update(const idx_t row_idx, WindowInputColumn &range_collection, const idx_t expr_idx,
                                   WindowInputExpression &boundary_start, WindowInputExpression &boundary_end,
                                   const ValidityMask &partition_mask, const ValidityMask &order_mask) {

	auto &bounds = *this;
	if (bounds.partition_count + bounds.order_count > 0) {

		// determine partition and peer group boundaries to ultimately figure out window size
		bounds.is_same_partition = !partition_mask.RowIsValidUnsafe(row_idx);
		bounds.is_peer = !order_mask.RowIsValidUnsafe(row_idx);

		// when the partition changes, recompute the boundaries
		if (!bounds.is_same_partition) {
			bounds.partition_start = row_idx;
			bounds.peer_start = row_idx;

			// find end of partition
			bounds.partition_end = bounds.input_size;
			if (bounds.partition_count) {
				idx_t n = 1;
				bounds.partition_end = FindNextStart(partition_mask, bounds.partition_start + 1, bounds.input_size, n);
			}

			// Find valid ordering values for the new partition
			// so we can exclude NULLs from RANGE expression computations
			bounds.valid_start = bounds.partition_start;
			bounds.valid_end = bounds.partition_end;

			if ((bounds.valid_start < bounds.valid_end) && bounds.has_preceding_range) {
				// Exclude any leading NULLs
				if (range_collection.CellIsNull(bounds.valid_start)) {
					idx_t n = 1;
					bounds.valid_start = FindNextStart(order_mask, bounds.valid_start + 1, bounds.valid_end, n);
				}
			}

			if ((bounds.valid_start < bounds.valid_end) && bounds.has_following_range) {
				// Exclude any trailing NULLs
				if (range_collection.CellIsNull(bounds.valid_end - 1)) {
					idx_t n = 1;
					bounds.valid_end = FindPrevStart(order_mask, bounds.valid_start, bounds.valid_end, n);
				}
			}

		} else if (!bounds.is_peer) {
			bounds.peer_start = row_idx;
		}

		if (bounds.needs_peer) {
			bounds.peer_end = bounds.partition_end;
			if (bounds.order_count) {
				idx_t n = 1;
				bounds.peer_end = FindNextStart(order_mask, bounds.peer_start + 1, bounds.partition_end, n);
			}
		}

	} else {
		bounds.is_same_partition = false;
		bounds.is_peer = true;
		bounds.partition_end = bounds.input_size;
		bounds.peer_end = bounds.partition_end;
	}

	// determine window boundaries depending on the type of expression
	bounds.window_start = -1;
	bounds.window_end = -1;

	switch (bounds.start_boundary) {
	case WindowBoundary::UNBOUNDED_PRECEDING:
		bounds.window_start = bounds.partition_start;
		break;
	case WindowBoundary::CURRENT_ROW_ROWS:
		bounds.window_start = row_idx;
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		bounds.window_start = bounds.peer_start;
		break;
	case WindowBoundary::EXPR_PRECEDING_ROWS: {
		bounds.window_start = (int64_t)row_idx - boundary_start.GetCell<int64_t>(expr_idx);
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_ROWS: {
		bounds.window_start = row_idx + boundary_start.GetCell<int64_t>(expr_idx);
		break;
	}
	case WindowBoundary::EXPR_PRECEDING_RANGE: {
		if (boundary_start.CellIsNull(expr_idx)) {
			bounds.window_start = bounds.peer_start;
		} else {
			bounds.window_start = FindOrderedRangeBound<true>(range_collection, bounds.range_sense, bounds.valid_start,
			                                                  row_idx, boundary_start, expr_idx);
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_RANGE: {
		if (boundary_start.CellIsNull(expr_idx)) {
			bounds.window_start = bounds.peer_start;
		} else {
			bounds.window_start = FindOrderedRangeBound<true>(range_collection, bounds.range_sense, row_idx,
			                                                  bounds.valid_end, boundary_start, expr_idx);
		}
		break;
	}
	default:
		throw InternalException("Unsupported window start boundary");
	}

	switch (bounds.end_boundary) {
	case WindowBoundary::CURRENT_ROW_ROWS:
		bounds.window_end = row_idx + 1;
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		bounds.window_end = bounds.peer_end;
		break;
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		bounds.window_end = bounds.partition_end;
		break;
	case WindowBoundary::EXPR_PRECEDING_ROWS:
		bounds.window_end = (int64_t)row_idx - boundary_end.GetCell<int64_t>(expr_idx) + 1;
		break;
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
		bounds.window_end = row_idx + boundary_end.GetCell<int64_t>(expr_idx) + 1;
		break;
	case WindowBoundary::EXPR_PRECEDING_RANGE: {
		if (boundary_end.CellIsNull(expr_idx)) {
			bounds.window_end = bounds.peer_end;
		} else {
			bounds.window_end = FindOrderedRangeBound<false>(range_collection, bounds.range_sense, bounds.valid_start,
			                                                 row_idx, boundary_end, expr_idx);
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_RANGE: {
		if (boundary_end.CellIsNull(expr_idx)) {
			bounds.window_end = bounds.peer_end;
		} else {
			bounds.window_end = FindOrderedRangeBound<false>(range_collection, bounds.range_sense, row_idx,
			                                                 bounds.valid_end, boundary_end, expr_idx);
		}
		break;
	}
	default:
		throw InternalException("Unsupported window end boundary");
	}

	// clamp windows to partitions if they should exceed
	if (bounds.window_start < (int64_t)bounds.partition_start) {
		bounds.window_start = bounds.partition_start;
	}
	if (bounds.window_start > (int64_t)bounds.partition_end) {
		bounds.window_start = bounds.partition_end;
	}
	if (bounds.window_end < (int64_t)bounds.partition_start) {
		bounds.window_end = bounds.partition_start;
	}
	if (bounds.window_end > (int64_t)bounds.partition_end) {
		bounds.window_end = bounds.partition_end;
	}

	if (bounds.window_start < 0 || bounds.window_end < 0) {
		throw InternalException("Failed to compute window boundaries");
	}
}

struct WindowExecutor {
	WindowExecutor(BoundWindowExpression *wexpr, ClientContext &context, const idx_t count);

	void Sink(DataChunk &input_chunk, const idx_t input_idx, const idx_t total_count);
	void Finalize(WindowAggregationMode mode);

	void Evaluate(idx_t row_idx, DataChunk &input_chunk, Vector &result, const ValidityMask &partition_mask,
	              const ValidityMask &order_mask);

	// The function
	BoundWindowExpression *wexpr;

	// Frame management
	WindowBoundariesState bounds;
	uint64_t dense_rank = 1;
	uint64_t rank_equal = 0;
	uint64_t rank = 1;

	// Expression collections
	DataChunk payload_collection;
	ExpressionExecutor payload_executor;
	DataChunk payload_chunk;

	ExpressionExecutor filter_executor;
	ValidityMask filter_mask;
	vector<validity_t> filter_bits;
	SelectionVector filter_sel;

	// LEAD/LAG Evaluation
	WindowInputExpression leadlag_offset;
	WindowInputExpression leadlag_default;

	// evaluate boundaries if present. Parser has checked boundary types.
	WindowInputExpression boundary_start;
	WindowInputExpression boundary_end;

	// evaluate RANGE expressions, if needed
	WindowInputColumn range;

	// IGNORE NULLS
	ValidityMask ignore_nulls;

	// build a segment tree for frame-adhering aggregates
	// see http://www.vldb.org/pvldb/vol8/p1058-leis.pdf
	unique_ptr<WindowSegmentTree> segment_tree = nullptr;
};

WindowExecutor::WindowExecutor(BoundWindowExpression *wexpr, ClientContext &context, const idx_t count)
    : wexpr(wexpr), bounds(wexpr, count), payload_collection(), payload_executor(context), filter_executor(context),
      leadlag_offset(wexpr->offset_expr.get(), context), leadlag_default(wexpr->default_expr.get(), context),
      boundary_start(wexpr->start_expr.get(), context), boundary_end(wexpr->end_expr.get(), context),
      range((bounds.has_preceding_range || bounds.has_following_range) ? wexpr->orders[0].expression.get() : nullptr,
            context, count)

{
	// TODO we could evaluate those expressions in parallel

	// evaluate the FILTER clause and stuff it into a large mask for compactness and reuse
	if (wexpr->filter_expr) {
		// 	Start with all invalid and set the ones that pass
		filter_bits.resize(ValidityMask::ValidityMaskSize(count), 0);
		filter_mask.Initialize(filter_bits.data());
		filter_executor.AddExpression(*wexpr->filter_expr);
		filter_sel.Initialize(STANDARD_VECTOR_SIZE);
	}

	// TODO: child may be a scalar, don't need to materialize the whole collection then

	// evaluate inner expressions of window functions, could be more complex
	vector<Expression *> exprs;
	exprs.reserve(wexpr->children.size());
	for (auto &child : wexpr->children) {
		exprs.push_back(child.get());
	}
	PrepareInputExpressions(exprs.data(), exprs.size(), payload_executor, payload_chunk);

	auto types = payload_chunk.GetTypes();
	if (!types.empty()) {
		payload_collection.Initialize(Allocator::Get(context), types);
	}
}

void WindowExecutor::Sink(DataChunk &input_chunk, const idx_t input_idx, const idx_t total_count) {
	// Single pass over the input to produce the global data.
	// Vectorisation for the win...

	// Set up a validity mask for IGNORE NULLS
	bool check_nulls = false;
	if (wexpr->ignore_nulls) {
		switch (wexpr->type) {
		case ExpressionType::WINDOW_LEAD:
		case ExpressionType::WINDOW_LAG:
		case ExpressionType::WINDOW_FIRST_VALUE:
		case ExpressionType::WINDOW_LAST_VALUE:
		case ExpressionType::WINDOW_NTH_VALUE:
			check_nulls = true;
			break;
		default:
			break;
		}
	}

	const auto count = input_chunk.size();

	if (!wexpr->children.empty()) {
		payload_chunk.Reset();
		payload_executor.Execute(input_chunk, payload_chunk);
		payload_chunk.Verify();
		payload_collection.Append(payload_chunk, true);

		// process payload chunks while they are still piping hot
		if (check_nulls) {
			UnifiedVectorFormat vdata;
			payload_chunk.data[0].ToUnifiedFormat(count, vdata);
			if (!vdata.validity.AllValid()) {
				//	Lazily materialise the contents when we find the first NULL
				if (ignore_nulls.AllValid()) {
					ignore_nulls.Initialize(total_count);
				}
				// Write to the current position
				if (input_idx % ValidityMask::BITS_PER_VALUE == 0) {
					// If we are at the edge of an output entry, just copy the entries
					auto dst = ignore_nulls.GetData() + ignore_nulls.EntryCount(input_idx);
					auto src = vdata.validity.GetData();
					for (auto entry_count = vdata.validity.EntryCount(count); entry_count-- > 0;) {
						*dst++ = *src++;
					}
				} else {
					// If not, we have ragged data and need to copy one bit at a time.
					for (idx_t i = 0; i < count; ++i) {
						ignore_nulls.Set(input_idx + i, vdata.validity.RowIsValid(i));
					}
				}
			}
		}
	}

	if (wexpr->filter_expr) {
		const auto filtered = filter_executor.SelectExpression(input_chunk, filter_sel);
		for (idx_t f = 0; f < filtered; ++f) {
			filter_mask.SetValid(input_idx + filter_sel[f]);
		}
	}

	range.Append(input_chunk);
}

void WindowExecutor::Finalize(WindowAggregationMode mode) {
	// build a segment tree for frame-adhering aggregates
	// see http://www.vldb.org/pvldb/vol8/p1058-leis.pdf

	if (wexpr->aggregate) {
		segment_tree = make_unique<WindowSegmentTree>(*(wexpr->aggregate), wexpr->bind_info.get(), wexpr->return_type,
		                                              &payload_collection, filter_mask, mode);
	}
}

void WindowExecutor::Evaluate(idx_t row_idx, DataChunk &input_chunk, Vector &result, const ValidityMask &partition_mask,
                              const ValidityMask &order_mask) {
	// Evaluate the row-level arguments
	boundary_start.Execute(input_chunk);
	boundary_end.Execute(input_chunk);

	leadlag_offset.Execute(input_chunk);
	leadlag_default.Execute(input_chunk);

	// this is the main loop, go through all sorted rows and compute window function result
	for (idx_t output_offset = 0; output_offset < input_chunk.size(); ++output_offset, ++row_idx) {
		// special case, OVER (), aggregate over everything
		bounds.Update(row_idx, range, output_offset, boundary_start, boundary_end, partition_mask, order_mask);
		if (WindowNeedsRank(wexpr)) {
			if (!bounds.is_same_partition || row_idx == 0) { // special case for first row, need to init
				dense_rank = 1;
				rank = 1;
				rank_equal = 0;
			} else if (!bounds.is_peer) {
				dense_rank++;
				rank += rank_equal;
				rank_equal = 0;
			}
			rank_equal++;
		}

		// if no values are read for window, result is NULL
		if (bounds.window_start >= bounds.window_end) {
			FlatVector::SetNull(result, output_offset, true);
			continue;
		}

		switch (wexpr->type) {
		case ExpressionType::WINDOW_AGGREGATE: {
			segment_tree->Compute(result, output_offset, bounds.window_start, bounds.window_end);
			break;
		}
		case ExpressionType::WINDOW_ROW_NUMBER: {
			auto rdata = FlatVector::GetData<int64_t>(result);
			rdata[output_offset] = row_idx - bounds.partition_start + 1;
			break;
		}
		case ExpressionType::WINDOW_RANK_DENSE: {
			auto rdata = FlatVector::GetData<int64_t>(result);
			rdata[output_offset] = dense_rank;
			break;
		}
		case ExpressionType::WINDOW_RANK: {
			auto rdata = FlatVector::GetData<int64_t>(result);
			rdata[output_offset] = rank;
			break;
		}
		case ExpressionType::WINDOW_PERCENT_RANK: {
			int64_t denom = (int64_t)bounds.partition_end - bounds.partition_start - 1;
			double percent_rank = denom > 0 ? ((double)rank - 1) / denom : 0;
			auto rdata = FlatVector::GetData<double>(result);
			rdata[output_offset] = percent_rank;
			break;
		}
		case ExpressionType::WINDOW_CUME_DIST: {
			int64_t denom = (int64_t)bounds.partition_end - bounds.partition_start;
			double cume_dist = denom > 0 ? ((double)(bounds.peer_end - bounds.partition_start)) / denom : 0;
			auto rdata = FlatVector::GetData<double>(result);
			rdata[output_offset] = cume_dist;
			break;
		}
		case ExpressionType::WINDOW_NTILE: {
			D_ASSERT(payload_collection.ColumnCount() == 1);
			if (CellIsNull(payload_collection, 0, row_idx)) {
				FlatVector::SetNull(result, output_offset, true);
			} else {
				auto n_param = GetCell<int64_t>(payload_collection, 0, row_idx);
				if (n_param < 1) {
					throw InvalidInputException("Argument for ntile must be greater than zero");
				}
				// With thanks from SQLite's ntileValueFunc()
				int64_t n_total = bounds.partition_end - bounds.partition_start;
				if (n_param > n_total) {
					// more groups allowed than we have values
					// map every entry to a unique group
					n_param = n_total;
				}
				int64_t n_size = (n_total / n_param);
				// find the row idx within the group
				D_ASSERT(row_idx >= bounds.partition_start);
				int64_t adjusted_row_idx = row_idx - bounds.partition_start;
				// now compute the ntile
				int64_t n_large = n_total - n_param * n_size;
				int64_t i_small = n_large * (n_size + 1);
				int64_t result_ntile;

				D_ASSERT((n_large * (n_size + 1) + (n_param - n_large) * n_size) == n_total);

				if (adjusted_row_idx < i_small) {
					result_ntile = 1 + adjusted_row_idx / (n_size + 1);
				} else {
					result_ntile = 1 + n_large + (adjusted_row_idx - i_small) / n_size;
				}
				// result has to be between [1, NTILE]
				D_ASSERT(result_ntile >= 1 && result_ntile <= n_param);
				auto rdata = FlatVector::GetData<int64_t>(result);
				rdata[output_offset] = result_ntile;
			}
			break;
		}
		case ExpressionType::WINDOW_LEAD:
		case ExpressionType::WINDOW_LAG: {
			int64_t offset = 1;
			if (wexpr->offset_expr) {
				offset = leadlag_offset.GetCell<int64_t>(output_offset);
			}
			int64_t val_idx = (int64_t)row_idx;
			if (wexpr->type == ExpressionType::WINDOW_LEAD) {
				val_idx += offset;
			} else {
				val_idx -= offset;
			}

			idx_t delta = 0;
			if (val_idx < (int64_t)row_idx) {
				// Count backwards
				delta = idx_t(row_idx - val_idx);
				val_idx = FindPrevStart(ignore_nulls, bounds.partition_start, row_idx, delta);
			} else if (val_idx > (int64_t)row_idx) {
				delta = idx_t(val_idx - row_idx);
				val_idx = FindNextStart(ignore_nulls, row_idx + 1, bounds.partition_end, delta);
			}
			// else offset is zero, so don't move.

			if (!delta) {
				CopyCell(payload_collection, 0, val_idx, result, output_offset);
			} else if (wexpr->default_expr) {
				leadlag_default.CopyCell(result, output_offset);
			} else {
				FlatVector::SetNull(result, output_offset, true);
			}
			break;
		}
		case ExpressionType::WINDOW_FIRST_VALUE: {
			idx_t n = 1;
			const auto first_idx = FindNextStart(ignore_nulls, bounds.window_start, bounds.window_end, n);
			CopyCell(payload_collection, 0, first_idx, result, output_offset);
			break;
		}
		case ExpressionType::WINDOW_LAST_VALUE: {
			idx_t n = 1;
			CopyCell(payload_collection, 0, FindPrevStart(ignore_nulls, bounds.window_start, bounds.window_end, n),
			         result, output_offset);
			break;
		}
		case ExpressionType::WINDOW_NTH_VALUE: {
			D_ASSERT(payload_collection.ColumnCount() == 2);
			// Returns value evaluated at the row that is the n'th row of the window frame (counting from 1);
			// returns NULL if there is no such row.
			if (CellIsNull(payload_collection, 1, row_idx)) {
				FlatVector::SetNull(result, output_offset, true);
			} else {
				auto n_param = GetCell<int64_t>(payload_collection, 1, row_idx);
				if (n_param < 1) {
					FlatVector::SetNull(result, output_offset, true);
				} else {
					auto n = idx_t(n_param);
					const auto nth_index = FindNextStart(ignore_nulls, bounds.window_start, bounds.window_end, n);
					if (!n) {
						CopyCell(payload_collection, 0, nth_index, result, output_offset);
					} else {
						FlatVector::SetNull(result, output_offset, true);
					}
				}
			}
			break;
		}
		default:
			throw InternalException("Window aggregate type %s", ExpressionTypeToString(wexpr->type));
		}
	}

	result.Verify(input_chunk.size());
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalWindow::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                    DataChunk &input) const {
	auto &gstate = (WindowGlobalSinkState &)gstate_p;
	auto &lstate = (WindowLocalSinkState &)lstate_p;

	lstate.Sink(input, gstate);

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalWindow::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &gstate = (WindowGlobalSinkState &)gstate_p;
	auto &lstate = (WindowLocalSinkState &)lstate_p;
	lstate.Combine(gstate);
}

unique_ptr<LocalSinkState> PhysicalWindow::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<WindowLocalSinkState>(context.client, *this);
}

unique_ptr<GlobalSinkState> PhysicalWindow::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<WindowGlobalSinkState>(*this, context);
}

enum class WindowSortStage : uint8_t { INIT, PREPARE, MERGE, SORTED };

class WindowGlobalMergeState;

class WindowLocalMergeState {
public:
	WindowLocalMergeState() : merge_state(nullptr), stage(WindowSortStage::INIT) {
		finished = true;
	}

	bool TaskFinished() {
		return finished;
	}

	void Prepare();
	void Merge();

	void ExecuteTask();

	WindowGlobalMergeState *merge_state;
	WindowSortStage stage;
	atomic<bool> finished;
};

class WindowGlobalMergeState {
public:
	using GroupDataPtr = unique_ptr<ColumnDataCollection>;

	explicit WindowGlobalMergeState(WindowGlobalSinkState &sink, GroupDataPtr group_data)
	    : sink(sink), group_data(std::move(group_data)), stage(WindowSortStage::INIT), total_tasks(0),
	      tasks_assigned(0), tasks_completed(0) {

		const auto group_idx = sink.hash_groups.size();
		auto new_group = make_unique<WindowGlobalHashGroup>(sink.buffer_manager, sink.partitions, sink.orders,
		                                                    sink.payload_types, sink.external);
		sink.hash_groups.emplace_back(std::move(new_group));

		hash_group = sink.hash_groups[group_idx].get();
		global_sort = sink.hash_groups[group_idx]->global_sort.get();
	}

	bool IsSorted() const {
		lock_guard<mutex> guard(lock);
		return stage == WindowSortStage::SORTED;
	}

	bool AssignTask(WindowLocalMergeState &local_state);
	bool TryPrepareNextStage();
	void CompleteTask();

	WindowGlobalSinkState &sink;
	GroupDataPtr group_data;
	WindowGlobalHashGroup *hash_group;
	GlobalSortState *global_sort;

private:
	mutable mutex lock;
	WindowSortStage stage;
	idx_t total_tasks;
	idx_t tasks_assigned;
	idx_t tasks_completed;
};

void WindowLocalMergeState::Prepare() {
	auto &global_sort = *merge_state->global_sort;
	merge_state->sink.BuildSortState(*merge_state->group_data, *merge_state->hash_group);
	merge_state->group_data.reset();

	global_sort.PrepareMergePhase();
}

void WindowLocalMergeState::Merge() {
	auto &global_sort = *merge_state->global_sort;
	MergeSorter merge_sorter(global_sort, global_sort.buffer_manager);
	merge_sorter.PerformInMergeRound();
}

void WindowLocalMergeState::ExecuteTask() {
	switch (stage) {
	case WindowSortStage::PREPARE:
		Prepare();
		break;
	case WindowSortStage::MERGE:
		Merge();
		break;
	default:
		throw InternalException("Unexpected WindowGlobalMergeState in ExecuteTask!");
	}

	merge_state->CompleteTask();
	finished = true;
}

bool WindowGlobalMergeState::AssignTask(WindowLocalMergeState &local_state) {
	lock_guard<mutex> guard(lock);

	if (tasks_assigned >= total_tasks) {
		return false;
	}

	local_state.merge_state = this;
	local_state.stage = stage;
	local_state.finished = false;
	tasks_assigned++;

	return true;
}

void WindowGlobalMergeState::CompleteTask() {
	lock_guard<mutex> guard(lock);

	++tasks_completed;
}

bool WindowGlobalMergeState::TryPrepareNextStage() {
	lock_guard<mutex> guard(lock);

	if (tasks_completed < total_tasks) {
		return false;
	}

	tasks_assigned = tasks_completed = 0;

	switch (stage) {
	case WindowSortStage::INIT:
		total_tasks = 1;
		stage = WindowSortStage::PREPARE;
		return true;

	case WindowSortStage::PREPARE:
		total_tasks = global_sort->sorted_blocks.size() / 2;
		if (!total_tasks) {
			break;
		}
		stage = WindowSortStage::MERGE;
		global_sort->InitializeMergeRound();
		return true;

	case WindowSortStage::MERGE:
		global_sort->CompleteMergeRound(true);
		total_tasks = global_sort->sorted_blocks.size() / 2;
		if (!total_tasks) {
			break;
		}
		global_sort->InitializeMergeRound();
		return true;

	case WindowSortStage::SORTED:
		break;
	}

	stage = WindowSortStage::SORTED;

	return false;
}

class WindowGlobalMergeStates {
public:
	using WindowGlobalMergeStatePtr = unique_ptr<WindowGlobalMergeState>;

	explicit WindowGlobalMergeStates(WindowGlobalSinkState &sink) {
		// Schedule all the sorts for maximum thread utilisation
		for (auto &group_data : sink.grouping_data->GetPartitions()) {
			// Prepare for merge sort phase
			if (group_data->Count()) {
				auto state = make_unique<WindowGlobalMergeState>(sink, std::move(group_data));
				states.emplace_back(std::move(state));
			}
		}
	}

	vector<WindowGlobalMergeStatePtr> states;
};

class WindowMergeTask : public ExecutorTask {
public:
	WindowMergeTask(shared_ptr<Event> event_p, ClientContext &context_p, WindowGlobalMergeStates &hash_groups_p)
	    : ExecutorTask(context_p), event(std::move(event_p)), hash_groups(hash_groups_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

private:
	shared_ptr<Event> event;
	WindowLocalMergeState local_state;
	WindowGlobalMergeStates &hash_groups;
};

TaskExecutionResult WindowMergeTask::ExecuteTask(TaskExecutionMode mode) {
	// Loop until all hash groups are done
	size_t sorted = 0;
	while (sorted < hash_groups.states.size()) {
		// First check if there is an unfinished task for this thread
		if (!local_state.TaskFinished()) {
			local_state.ExecuteTask();
			continue;
		}

		// Thread is done with its assigned task, try to fetch new work
		for (auto group = sorted; group < hash_groups.states.size(); ++group) {
			auto &global_state = hash_groups.states[group];
			if (global_state->IsSorted()) {
				// This hash group is done
				// Update the high water mark of densely completed groups
				if (sorted == group) {
					++sorted;
				}
				continue;
			}

			// Try to assign work for this hash group to this thread
			if (global_state->AssignTask(local_state)) {
				// We assigned a task to this thread!
				// Break out of this loop to re-enter the top-level loop and execute the task
				break;
			}

			// Hash group global state couldn't assign a task to this thread
			// Try to prepare the next stage
			if (!global_state->TryPrepareNextStage()) {
				// This current hash group is not yet done
				// But we were not able to assign a task for it to this thread
				// See if the next hash group is better
				continue;
			}

			// We were able to prepare the next stage for this hash group!
			// Try to assign a task once more
			if (global_state->AssignTask(local_state)) {
				// We assigned a task to this thread!
				// Break out of this loop to re-enter the top-level loop and execute the task
				break;
			}

			// We were able to prepare the next merge round,
			// but we were not able to assign a task for it to this thread
			// The tasks were assigned to other threads while this thread waited for the lock
			// Go to the next iteration to see if another hash group has a task
		}
	}

	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

class WindowMergeEvent : public BasePipelineEvent {
public:
	WindowMergeEvent(WindowGlobalSinkState &gstate_p, Pipeline &pipeline_p)
	    : BasePipelineEvent(pipeline_p), gstate(gstate_p), merge_states(gstate_p) {
	}

	WindowGlobalSinkState &gstate;
	WindowGlobalMergeStates merge_states;

public:
	void Schedule() override {
		auto &context = pipeline->GetClientContext();

		// Schedule tasks equal to the number of threads, which will each merge multiple partitions
		auto &ts = TaskScheduler::GetScheduler(context);
		idx_t num_threads = ts.NumberOfThreads();

		vector<unique_ptr<Task>> merge_tasks;
		for (idx_t tnum = 0; tnum < num_threads; tnum++) {
			merge_tasks.push_back(make_unique<WindowMergeTask>(shared_from_this(), context, merge_states));
		}
		SetTasks(std::move(merge_tasks));
	}
};

SinkFinalizeType PhysicalWindow::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          GlobalSinkState &gstate_p) const {
	auto &state = (WindowGlobalSinkState &)gstate_p;

	//	Did we get any data?
	if (!state.count) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Do we have any sorting to schedule?
	if (state.rows) {
		D_ASSERT(!state.grouping_data);
		return state.rows->count ? SinkFinalizeType::READY : SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Find the first group to sort
	auto &groups = state.grouping_data->GetPartitions();
	if (groups.empty()) {
		// Empty input!
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Schedule all the sorts for maximum thread utilisation
	auto new_event = make_shared<WindowMergeEvent>(state, pipeline);
	event.InsertEvent(std::move(new_event));

	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class WindowGlobalSourceState : public GlobalSourceState {
public:
	explicit WindowGlobalSourceState(const PhysicalWindow &op) : op(op), next_bin(0) {
	}

	const PhysicalWindow &op;
	//! The output read position.
	atomic<idx_t> next_bin;

public:
	idx_t MaxThreads() override {
		auto &state = (WindowGlobalSinkState &)*op.sink_state;

		// If there is only one partition, we have to process it on one thread.
		if (!state.grouping_data) {
			return 1;
		}

		// If there is not a lot of data, process serially.
		if (state.count < STANDARD_ROW_GROUPS_SIZE) {
			return 1;
		}

		return state.hash_groups.size();
	}
};

// Per-thread read state
class WindowLocalSourceState : public LocalSourceState {
public:
	using HashGroupPtr = unique_ptr<WindowGlobalHashGroup>;
	using WindowExecutorPtr = unique_ptr<WindowExecutor>;
	using WindowExecutors = vector<WindowExecutorPtr>;

	WindowLocalSourceState(const PhysicalWindow &op, ExecutionContext &context, WindowGlobalSourceState &gstate)
	    : context(context.client), allocator(Allocator::Get(context.client)) {
		vector<LogicalType> output_types;
		for (idx_t expr_idx = 0; expr_idx < op.select_list.size(); ++expr_idx) {
			D_ASSERT(op.select_list[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
			auto wexpr = reinterpret_cast<BoundWindowExpression *>(op.select_list[expr_idx].get());
			output_types.emplace_back(wexpr->return_type);
		}
		output_chunk.Initialize(allocator, output_types);

		const auto &input_types = op.children[0]->types;
		layout.Initialize(input_types);
		input_chunk.Initialize(allocator, input_types);
	}

	void MaterializeSortedData();
	void GeneratePartition(WindowGlobalSinkState &gstate, const idx_t hash_bin);
	void Scan(DataChunk &chunk);

	HashGroupPtr hash_group;
	ClientContext &context;
	Allocator &allocator;

	//! The generated input chunks
	unique_ptr<RowDataCollection> rows;
	unique_ptr<RowDataCollection> heap;
	RowLayout layout;
	//! The partition boundary mask
	vector<validity_t> partition_bits;
	ValidityMask partition_mask;
	//! The order boundary mask
	vector<validity_t> order_bits;
	ValidityMask order_mask;
	//! The current execution functions
	WindowExecutors window_execs;

	//! The read partition
	idx_t hash_bin;
	//! The read cursor
	unique_ptr<RowDataCollectionScanner> scanner;
	//! Buffer for the inputs
	DataChunk input_chunk;
	//! Buffer for window results
	DataChunk output_chunk;
};

void WindowLocalSourceState::MaterializeSortedData() {
	auto &global_sort_state = *hash_group->global_sort;
	if (global_sort_state.sorted_blocks.empty()) {
		return;
	}

	// scan the sorted row data
	D_ASSERT(global_sort_state.sorted_blocks.size() == 1);
	auto &sb = *global_sort_state.sorted_blocks[0];

	// Free up some memory before allocating more
	sb.radix_sorting_data.clear();
	sb.blob_sorting_data = nullptr;

	// Move the sorting row blocks into our RDCs
	auto &buffer_manager = global_sort_state.buffer_manager;
	auto &sd = *sb.payload_data;

	// Data blocks are required
	D_ASSERT(!sd.data_blocks.empty());
	auto &block = sd.data_blocks[0];
	rows = make_unique<RowDataCollection>(buffer_manager, block->capacity, block->entry_size);
	rows->blocks = std::move(sd.data_blocks);
	rows->count = std::accumulate(rows->blocks.begin(), rows->blocks.end(), idx_t(0),
	                              [&](idx_t c, const unique_ptr<RowDataBlock> &b) { return c + b->count; });

	// Heap blocks are optional, but we want both for iteration.
	if (!sd.heap_blocks.empty()) {
		auto &block = sd.heap_blocks[0];
		heap = make_unique<RowDataCollection>(buffer_manager, block->capacity, block->entry_size);
		heap->blocks = std::move(sd.heap_blocks);
		hash_group.reset();
	} else {
		heap = make_unique<RowDataCollection>(buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1, true);
	}
	heap->count = std::accumulate(heap->blocks.begin(), heap->blocks.end(), idx_t(0),
	                              [&](idx_t c, const unique_ptr<RowDataBlock> &b) { return c + b->count; });
}

void WindowLocalSourceState::GeneratePartition(WindowGlobalSinkState &gstate, const idx_t hash_bin_p) {
	auto &op = (PhysicalWindow &)gstate.op;

	//	Get rid of any stale data
	hash_bin = hash_bin_p;

	// There are three types of partitions:
	// 1. No partition (no sorting)
	// 2. One partition (sorting, but no hashing)
	// 3. Multiple partitions (sorting and hashing)

	//	How big is the partition?
	idx_t count = 0;
	if (hash_bin < gstate.hash_groups.size() && gstate.hash_groups[hash_bin]) {
		count = gstate.hash_groups[hash_bin]->count;
	} else if (gstate.rows && !hash_bin) {
		count = gstate.count;
	} else {
		return;
	}

	// Create the executors for each function
	window_execs.clear();
	for (idx_t expr_idx = 0; expr_idx < op.select_list.size(); ++expr_idx) {
		D_ASSERT(op.select_list[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto wexpr = reinterpret_cast<BoundWindowExpression *>(op.select_list[expr_idx].get());
		auto wexec = make_unique<WindowExecutor>(wexpr, context, count);
		window_execs.emplace_back(std::move(wexec));
	}

	//	Initialise masks to false
	const auto bit_count = ValidityMask::ValidityMaskSize(count);
	partition_bits.clear();
	partition_bits.resize(bit_count, 0);
	partition_mask.Initialize(partition_bits.data());

	order_bits.clear();
	order_bits.resize(bit_count, 0);
	order_mask.Initialize(order_bits.data());

	// Scan the sorted data into new Collections
	auto external = gstate.external;
	if (gstate.rows && !hash_bin) {
		// Simple mask
		partition_mask.SetValidUnsafe(0);
		order_mask.SetValidUnsafe(0);
		//	No partition - align the heap blocks with the row blocks
		rows = gstate.rows->CloneEmpty(gstate.rows->keep_pinned);
		heap = gstate.strings->CloneEmpty(gstate.strings->keep_pinned);
		RowDataCollectionScanner::AlignHeapBlocks(*rows, *heap, *gstate.rows, *gstate.strings, layout);
		external = true;
	} else if (hash_bin < gstate.hash_groups.size() && gstate.hash_groups[hash_bin]) {
		// Overwrite the collections with the sorted data
		hash_group = std::move(gstate.hash_groups[hash_bin]);
		hash_group->ComputeMasks(partition_mask, order_mask);
		MaterializeSortedData();
	} else {
		return;
	}

	//	First pass over the input without flushing
	//	TODO: Factor out the constructor data as global state
	scanner = make_unique<RowDataCollectionScanner>(*rows, *heap, layout, external, false);
	idx_t input_idx = 0;
	while (true) {
		input_chunk.Reset();
		scanner->Scan(input_chunk);
		if (input_chunk.size() == 0) {
			break;
		}

		//	TODO: Parallelization opportunity
		for (auto &wexec : window_execs) {
			wexec->Sink(input_chunk, input_idx, scanner->Count());
		}
		input_idx += input_chunk.size();
	}

	//	TODO: Parallelization opportunity
	for (auto &wexec : window_execs) {
		wexec->Finalize(gstate.mode);
	}

	// External scanning assumes all blocks are swizzled.
	scanner->ReSwizzle();

	//	Second pass can flush
	scanner->Reset(true);
}

void WindowLocalSourceState::Scan(DataChunk &result) {
	D_ASSERT(scanner);
	if (!scanner->Remaining()) {
		return;
	}

	const auto position = scanner->Scanned();
	input_chunk.Reset();
	scanner->Scan(input_chunk);

	output_chunk.Reset();
	for (idx_t expr_idx = 0; expr_idx < window_execs.size(); ++expr_idx) {
		auto &executor = *window_execs[expr_idx];
		executor.Evaluate(position, input_chunk, output_chunk.data[expr_idx], partition_mask, order_mask);
	}
	output_chunk.SetCardinality(input_chunk);
	output_chunk.Verify();

	idx_t out_idx = 0;
	result.SetCardinality(input_chunk);
	for (idx_t col_idx = 0; col_idx < input_chunk.ColumnCount(); col_idx++) {
		result.data[out_idx++].Reference(input_chunk.data[col_idx]);
	}
	for (idx_t col_idx = 0; col_idx < output_chunk.ColumnCount(); col_idx++) {
		result.data[out_idx++].Reference(output_chunk.data[col_idx]);
	}
	result.Verify();
}

unique_ptr<LocalSourceState> PhysicalWindow::GetLocalSourceState(ExecutionContext &context,
                                                                 GlobalSourceState &gstate_p) const {
	auto &gstate = (WindowGlobalSourceState &)gstate_p;
	return make_unique<WindowLocalSourceState>(*this, context, gstate);
}

unique_ptr<GlobalSourceState> PhysicalWindow::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<WindowGlobalSourceState>(*this);
}

void PhysicalWindow::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                             LocalSourceState &lstate_p) const {
	auto &state = (WindowLocalSourceState &)lstate_p;
	auto &global_source = (WindowGlobalSourceState &)gstate_p;
	auto &gstate = (WindowGlobalSinkState &)*sink_state;

	const auto bin_count = gstate.hash_groups.empty() ? 1 : gstate.hash_groups.size();

	while (chunk.size() == 0) {
		//	Move to the next bin if we are done.
		while (!state.scanner || !state.scanner->Remaining()) {
			state.scanner.reset();
			state.rows.reset();
			state.heap.reset();
			state.hash_group.reset();
			auto hash_bin = global_source.next_bin++;
			if (hash_bin >= bin_count) {
				return;
			}

			for (; hash_bin < gstate.hash_groups.size(); hash_bin = global_source.next_bin++) {
				if (gstate.hash_groups[hash_bin]) {
					break;
				}
			}
			state.GeneratePartition(gstate, hash_bin);
		}

		state.Scan(chunk);
	}
}

string PhysicalWindow::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < select_list.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += select_list[i]->GetName();
	}
	return result;
}

} // namespace duckdb
