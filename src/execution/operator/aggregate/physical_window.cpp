#include "duckdb/execution/operator/aggregate/physical_window.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/windows_undefs.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/window_segment_tree.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/main/config.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>

namespace duckdb {

using counts_t = std::vector<size_t>;

//	Global sink state
class WindowGlobalState : public GlobalSinkState {
public:
	WindowGlobalState(const PhysicalWindow &op_p, ClientContext &context)
	    : op(op_p), buffer_manager(BufferManager::GetBufferManager(context)),
	      mode(DBConfig::GetConfig(context).window_mode) {
	}
	const PhysicalWindow &op;
	BufferManager &buffer_manager;
	mutex lock;
	ChunkCollection chunks;
	ChunkCollection over_collection;
	ChunkCollection hash_collection;
	counts_t counts;
	WindowAggregationMode mode;
};

//	Per-thread sink state
class WindowLocalState : public LocalSinkState {
public:
	explicit WindowLocalState(const PhysicalWindow &op_p, const unsigned partition_bits = 10)
	    : op(op_p), partition_count(size_t(1) << partition_bits) {
	}

	const PhysicalWindow &op;
	ChunkCollection chunks;
	ChunkCollection over_collection;
	ChunkCollection hash_collection;
	const size_t partition_count;
	counts_t counts;
};

// Per-thread read state
class WindowOperatorState : public LocalSourceState {
public:
	WindowOperatorState(const PhysicalWindow &op, ExecutionContext &context)
	    : buffer_manager(BufferManager::GetBufferManager(context.client)) {
		auto &gstate = (WindowGlobalState &)*op.sink_state;
		// initialize thread-local operator state
		partitions = gstate.counts.size();
		next_part = 0;
		position = 0;
	}

	//! The number of partitions to process (0 if there is no partitioning)
	size_t partitions;
	//! The output read position.
	size_t next_part;
	//! The generated input chunks
	ChunkCollection chunks;
	//! The generated output chunks
	ChunkCollection window_results;
	//! The read cursor
	idx_t position;

	BufferManager &buffer_manager;
	unique_ptr<GlobalSortState> global_sort_state;
};

// this implements a sorted window functions variant
PhysicalWindow::PhysicalWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                               idx_t estimated_cardinality, PhysicalOperatorType type)
    : PhysicalOperator(type, move(types), estimated_cardinality), select_list(move(select_list)) {
}

template <typename INPUT_TYPE>
struct ChunkIterator {

	ChunkIterator(ChunkCollection &collection, const idx_t col_idx)
	    : collection(collection), col_idx(col_idx), chunk_begin(0), chunk_end(0), ch_idx(0), data(nullptr),
	      validity(nullptr) {
		Update(0);
	}

	inline void Update(idx_t r) {
		if (r >= chunk_end) {
			ch_idx = collection.LocateChunk(r);
			auto &ch = collection.GetChunk(ch_idx);
			chunk_begin = ch_idx * STANDARD_VECTOR_SIZE;
			chunk_end = chunk_begin + ch.size();
			auto &vector = ch.data[col_idx];
			data = FlatVector::GetData<INPUT_TYPE>(vector);
			validity = &FlatVector::Validity(vector);
		}
	}

	inline bool IsValid(idx_t r) {
		return validity->RowIsValid(r - chunk_begin);
	}

	inline INPUT_TYPE GetValue(idx_t r) {
		return data[r - chunk_begin];
	}

private:
	ChunkCollection &collection;
	idx_t col_idx;
	idx_t chunk_begin;
	idx_t chunk_end;
	idx_t ch_idx;
	const INPUT_TYPE *data;
	ValidityMask *validity;
};

template <typename INPUT_TYPE>
static void MaskTypedColumn(ValidityMask &mask, ChunkCollection &over_collection, const idx_t c) {
	ChunkIterator<INPUT_TYPE> ci(over_collection, c);

	//	Record the first value
	idx_t r = 0;
	auto prev_valid = ci.IsValid(r);
	auto prev = ci.GetValue(r);

	//	Process complete blocks
	const auto count = over_collection.Count();
	const auto entry_count = mask.EntryCount(count);
	for (idx_t entry_idx = 0; entry_idx < entry_count; ++entry_idx) {
		auto validity_entry = mask.GetValidityEntry(entry_idx);

		//	Skip the block if it is all boundaries.
		idx_t next = MinValue<idx_t>(r + ValidityMask::BITS_PER_VALUE, count);
		if (ValidityMask::AllValid(validity_entry)) {
			r = next;
			continue;
		}

		//	Scan the rows in the complete block
		idx_t start = r;
		for (; r < next; ++r) {
			//	Update the chunk for this row
			ci.Update(r);

			auto curr_valid = ci.IsValid(r);
			auto curr = ci.GetValue(r);
			if (!ValidityMask::RowIsValid(validity_entry, r - start)) {
				if (curr_valid != prev_valid || (curr_valid && !Equals::Operation(curr, prev))) {
					mask.SetValidUnsafe(r);
				}
			}
			prev_valid = curr_valid;
			prev = curr;
		}
	}
}

static void MaskColumn(ValidityMask &mask, ChunkCollection &over_collection, const idx_t c) {
	auto &vector = over_collection.GetChunk(0).data[c];
	switch (vector.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		MaskTypedColumn<int8_t>(mask, over_collection, c);
		break;
	case PhysicalType::INT16:
		MaskTypedColumn<int16_t>(mask, over_collection, c);
		break;
	case PhysicalType::INT32:
		MaskTypedColumn<int32_t>(mask, over_collection, c);
		break;
	case PhysicalType::INT64:
		MaskTypedColumn<int64_t>(mask, over_collection, c);
		break;
	case PhysicalType::UINT8:
		MaskTypedColumn<uint8_t>(mask, over_collection, c);
		break;
	case PhysicalType::UINT16:
		MaskTypedColumn<uint16_t>(mask, over_collection, c);
		break;
	case PhysicalType::UINT32:
		MaskTypedColumn<uint32_t>(mask, over_collection, c);
		break;
	case PhysicalType::UINT64:
		MaskTypedColumn<uint64_t>(mask, over_collection, c);
		break;
	case PhysicalType::INT128:
		MaskTypedColumn<hugeint_t>(mask, over_collection, c);
		break;
	case PhysicalType::FLOAT:
		MaskTypedColumn<float>(mask, over_collection, c);
		break;
	case PhysicalType::DOUBLE:
		MaskTypedColumn<double>(mask, over_collection, c);
		break;
	case PhysicalType::VARCHAR:
		MaskTypedColumn<string_t>(mask, over_collection, c);
		break;
	case PhysicalType::INTERVAL:
		MaskTypedColumn<interval_t>(mask, over_collection, c);
		break;
	default:
		throw NotImplementedException("Type for comparison");
		break;
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

static void MaterializeExpressions(Expression **exprs, idx_t expr_count, ChunkCollection &input,
                                   ChunkCollection &output, bool scalar = false) {
	if (expr_count == 0) {
		return;
	}

	vector<LogicalType> types;
	ExpressionExecutor executor;
	for (idx_t expr_idx = 0; expr_idx < expr_count; ++expr_idx) {
		types.push_back(exprs[expr_idx]->return_type);
		executor.AddExpression(*exprs[expr_idx]);
	}

	for (idx_t i = 0; i < input.ChunkCount(); i++) {
		DataChunk chunk;
		chunk.Initialize(types);

		executor.Execute(input.GetChunk(i), chunk);

		chunk.Verify();
		output.Append(chunk);

		if (scalar) {
			break;
		}
	}
}

static void MaterializeExpression(Expression *expr, ChunkCollection &input, ChunkCollection &output,
                                  bool scalar = false) {
	MaterializeExpressions(&expr, 1, input, output, scalar);
}

static void SortCollectionForPartition(WindowOperatorState &state, BoundWindowExpression *wexpr, ChunkCollection &input,
                                       ChunkCollection &over, ChunkCollection *hashes, const hash_t hash_bin,
                                       const hash_t hash_mask) {
	if (input.Count() == 0) {
		return;
	}

	vector<BoundOrderByNode> orders;
	// we sort by both 1) partition by expression list and 2) order by expressions
	for (idx_t prt_idx = 0; prt_idx < wexpr->partitions.size(); prt_idx++) {
		if (wexpr->partitions_stats.empty() || !wexpr->partitions_stats[prt_idx]) {
			orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, wexpr->partitions[prt_idx]->Copy(),
			                    nullptr);
		} else {
			orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, wexpr->partitions[prt_idx]->Copy(),
			                    wexpr->partitions_stats[prt_idx]->Copy());
		}
	}
	for (const auto &order : wexpr->orders) {
		orders.push_back(order.Copy());
	}

	// fuse input and sort collection into one
	// (sorting columns are not decoded, and we need them later)
	auto payload_types = input.Types();
	payload_types.insert(payload_types.end(), over.Types().begin(), over.Types().end());
	DataChunk payload_chunk;
	payload_chunk.InitializeEmpty(payload_types);

	// initialise partitioning memory
	// to minimise copying, we fill up a chunk and then sink it.
	SelectionVector sel;
	DataChunk over_partition;
	DataChunk payload_partition;
	if (hashes) {
		sel.Initialize(STANDARD_VECTOR_SIZE);
		over_partition.Initialize(over.Types());
		payload_partition.Initialize(payload_types);
	}

	// initialize row layout for sorting
	RowLayout payload_layout;
	payload_layout.Initialize(payload_types);

	// initialize sorting states
	state.global_sort_state = make_unique<GlobalSortState>(state.buffer_manager, orders, payload_layout);
	auto &global_sort_state = *state.global_sort_state;
	LocalSortState local_sort_state;
	local_sort_state.Initialize(global_sort_state, state.buffer_manager);

	// sink collection chunks into row format
	const idx_t chunk_count = over.ChunkCount();
	for (idx_t i = 0; i < chunk_count; i++) {
		auto &input_chunk = *input.Chunks()[i];
		for (idx_t col_idx = 0; col_idx < input_chunk.ColumnCount(); ++col_idx) {
			payload_chunk.data[col_idx].Reference(input_chunk.data[col_idx]);
		}
		auto &over_chunk = *over.Chunks()[i];
		for (idx_t col_idx = 0; col_idx < over_chunk.ColumnCount(); ++col_idx) {
			payload_chunk.data[input_chunk.ColumnCount() + col_idx].Reference(over_chunk.data[col_idx]);
		}
		payload_chunk.SetCardinality(input_chunk);

		// Extract the hash partition, if any
		if (hashes) {
			auto &hash_chunk = *hashes->Chunks()[i];
			auto hash_size = hash_chunk.size();
			auto hash_data = FlatVector::GetData<hash_t>(hash_chunk.data[0]);
			idx_t bin_size = 0;
			for (idx_t i = 0; i < hash_size; ++i) {
				if ((hash_data[i] & hash_mask) == hash_bin) {
					sel.set_index(bin_size++, i);
				}
			}

			// Flush the partition chunks if we would overflow
			if (over_partition.size() + bin_size > STANDARD_VECTOR_SIZE) {
				local_sort_state.SinkChunk(over_partition, payload_partition);
				over_partition.Reset();
				payload_partition.Reset();
			}

			// Copy the data for each collection.
			if (bin_size) {
				over_partition.Append(over_chunk, false, &sel, bin_size);
				payload_partition.Append(payload_chunk, false, &sel, bin_size);
			}
		} else {
			local_sort_state.SinkChunk(over_chunk, payload_chunk);
		}
	}

	// Flush any ragged partition chunks
	if (over_partition.size() > 0) {
		local_sort_state.SinkChunk(over_partition, payload_partition);
		over_partition.Reset();
		payload_partition.Reset();
	}

	// If there are no hashes, release the input to save memory.
	if (!hashes) {
		over.Reset();
		input.Reset();
	}

	// add local state to global state, which sorts the data
	global_sort_state.AddLocalState(local_sort_state);
	// Prepare for merge phase (in this case we never have a merge phase, but this call is still needed)
	global_sort_state.PrepareMergePhase();
}

static void ScanSortedPartition(WindowOperatorState &state, ChunkCollection &input,
                                const vector<LogicalType> &input_types, ChunkCollection &over,
                                const vector<LogicalType> &over_types) {
	auto &global_sort_state = *state.global_sort_state;

	auto payload_types = input_types;
	payload_types.insert(payload_types.end(), over_types.begin(), over_types.end());

	// scan the sorted row data
	PayloadScanner scanner(*global_sort_state.sorted_blocks[0]->payload_data, global_sort_state);
	for (;;) {
		DataChunk payload_chunk;
		payload_chunk.Initialize(payload_types);
		payload_chunk.SetCardinality(0);
		scanner.Scan(payload_chunk);
		if (payload_chunk.size() == 0) {
			break;
		}

		// split into two
		DataChunk over_chunk;
		payload_chunk.Split(over_chunk, input_types.size());

		// append back to collection
		input.Append(payload_chunk);
		over.Append(over_chunk);
	}
}

static void HashChunk(counts_t &counts, DataChunk &hash_chunk, DataChunk &sort_chunk, const idx_t partition_cols) {
	const vector<LogicalType> hash_types(1, LogicalTypeId::HASH);
	hash_chunk.Initialize(hash_types);
	hash_chunk.SetCardinality(sort_chunk);
	auto &hash_vector = hash_chunk.data[0];

	const auto count = sort_chunk.size();
	VectorOperations::Hash(sort_chunk.data[0], hash_vector, count);
	for (idx_t prt_idx = 1; prt_idx < partition_cols; ++prt_idx) {
		VectorOperations::CombineHash(hash_vector, sort_chunk.data[prt_idx], count);
	}

	const auto partition_mask = hash_t(counts.size() - 1);
	auto hashes = FlatVector::GetData<hash_t>(hash_vector);
	if (hash_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		const auto bin = (hashes[0] & partition_mask);
		counts[bin] += count;
	} else {
		for (idx_t i = 0; i < count; ++i) {
			const auto bin = (hashes[i] & partition_mask);
			++counts[bin];
		}
	}
}

static void MaterializeOverForWindow(BoundWindowExpression *wexpr, DataChunk &input_chunk, DataChunk &over_chunk) {
	vector<LogicalType> over_types;
	ExpressionExecutor executor;

	// we sort by both 1) partition by expression list and 2) order by expressions
	for (idx_t prt_idx = 0; prt_idx < wexpr->partitions.size(); prt_idx++) {
		auto &pexpr = wexpr->partitions[prt_idx];
		over_types.push_back(pexpr->return_type);
		executor.AddExpression(*pexpr);
	}

	for (idx_t ord_idx = 0; ord_idx < wexpr->orders.size(); ord_idx++) {
		auto &oexpr = wexpr->orders[ord_idx].expression;
		over_types.push_back(oexpr->return_type);
		executor.AddExpression(*oexpr);
	}

	D_ASSERT(!over_types.empty());

	over_chunk.Initialize(over_types);
	executor.Execute(input_chunk, over_chunk);

	over_chunk.Verify();
}

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

	explicit WindowBoundariesState(BoundWindowExpression *wexpr)
	    : type(wexpr->type), start_boundary(wexpr->start), end_boundary(wexpr->end),
	      partition_count(wexpr->partitions.size()), order_count(wexpr->orders.size()),
	      range_sense(wexpr->orders.empty() ? OrderType::INVALID : wexpr->orders[0].type),
	      scalar_start(IsScalar(wexpr->start_expr)), scalar_end(IsScalar(wexpr->end_expr)),
	      has_preceding_range(wexpr->start == WindowBoundary::EXPR_PRECEDING_RANGE ||
	                          wexpr->end == WindowBoundary::EXPR_PRECEDING_RANGE),
	      has_following_range(wexpr->start == WindowBoundary::EXPR_FOLLOWING_RANGE ||
	                          wexpr->end == WindowBoundary::EXPR_FOLLOWING_RANGE),
	      needs_peer(BoundaryNeedsPeer(wexpr->end) || wexpr->type == ExpressionType::WINDOW_CUME_DIST) {
	}

	// Cached lookups
	const ExpressionType type;
	const WindowBoundary start_boundary;
	const WindowBoundary end_boundary;
	const idx_t partition_count;
	const idx_t order_count;
	const OrderType range_sense;
	const bool scalar_start;
	const bool scalar_end;
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
static T GetCell(ChunkCollection &collection, idx_t column, idx_t index) {
	D_ASSERT(collection.ColumnCount() > column);
	auto &chunk = collection.GetChunkForRow(index);
	auto &source = chunk.data[column];
	const auto source_offset = index % STANDARD_VECTOR_SIZE;
	const auto data = FlatVector::GetData<T>(source);
	return data[source_offset];
}

static bool CellIsNull(ChunkCollection &collection, idx_t column, idx_t index) {
	D_ASSERT(collection.ColumnCount() > column);
	auto &chunk = collection.GetChunkForRow(index);
	auto &source = chunk.data[column];
	const auto source_offset = index % STANDARD_VECTOR_SIZE;
	return FlatVector::IsNull(source, source_offset);
}

template <typename T>
struct ChunkCollectionIterator {
	using iterator = ChunkCollectionIterator<T>;
	using iterator_category = std::forward_iterator_tag;
	using difference_type = std::ptrdiff_t;
	using value_type = T;
	using reference = T;
	using pointer = idx_t;

	ChunkCollectionIterator(ChunkCollection &coll_p, idx_t col_no_p, pointer pos_p = 0)
	    : coll(&coll_p), col_no(col_no_p), pos(pos_p) {
	}

	inline reference operator*() const {
		return GetCell<T>(*coll, col_no, pos);
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
	ChunkCollection *coll;
	idx_t col_no;
	pointer pos;
};

template <typename T, typename OP>
struct OperationCompare : public std::function<bool(T, T)> {
	inline bool operator()(const T &lhs, const T &val) const {
		return OP::template Operation(lhs, val);
	}
};

template <typename T, typename OP, bool FROM>
static idx_t FindTypedRangeBound(ChunkCollection &over, const idx_t order_col, const idx_t order_begin,
                                 const idx_t order_end, ChunkCollection &boundary, const idx_t boundary_row) {
	D_ASSERT(!CellIsNull(boundary, 0, boundary_row));
	const auto val = GetCell<T>(boundary, 0, boundary_row);

	OperationCompare<T, OP> comp;
	ChunkCollectionIterator<T> begin(over, order_col, order_begin);
	ChunkCollectionIterator<T> end(over, order_col, order_end);
	if (FROM) {
		return idx_t(std::lower_bound(begin, end, val, comp));
	} else {
		return idx_t(std::upper_bound(begin, end, val, comp));
	}
}

template <typename OP, bool FROM>
static idx_t FindRangeBound(ChunkCollection &over, const idx_t order_col, const idx_t order_begin,
                            const idx_t order_end, ChunkCollection &boundary, const idx_t expr_idx) {
	const auto &over_types = over.Types();
	D_ASSERT(over_types.size() > order_col);
	D_ASSERT(boundary.Types().size() == 1);
	D_ASSERT(boundary.Types()[0] == over_types[order_col]);

	switch (over_types[order_col].InternalType()) {
	case PhysicalType::INT8:
		return FindTypedRangeBound<int8_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INT16:
		return FindTypedRangeBound<int16_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INT32:
		return FindTypedRangeBound<int32_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INT64:
		return FindTypedRangeBound<int64_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::UINT8:
		return FindTypedRangeBound<uint8_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::UINT16:
		return FindTypedRangeBound<uint16_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::UINT32:
		return FindTypedRangeBound<uint32_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::UINT64:
		return FindTypedRangeBound<uint64_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INT128:
		return FindTypedRangeBound<hugeint_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::FLOAT:
		return FindTypedRangeBound<float, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::DOUBLE:
		return FindTypedRangeBound<double, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INTERVAL:
		return FindTypedRangeBound<interval_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	default:
		throw InternalException("Unsupported column type for RANGE");
	}
}

template <bool FROM>
static idx_t FindOrderedRangeBound(ChunkCollection &over, const idx_t order_col, const OrderType range_sense,
                                   const idx_t order_begin, const idx_t order_end, ChunkCollection &boundary,
                                   const idx_t expr_idx) {
	switch (range_sense) {
	case OrderType::ASCENDING:
		return FindRangeBound<LessThan, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case OrderType::DESCENDING:
		return FindRangeBound<GreaterThan, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	default:
		throw InternalException("Unsupported ORDER BY sense for RANGE");
	}
}

static void UpdateWindowBoundaries(WindowBoundariesState &bounds, const idx_t input_size, const idx_t row_idx,
                                   ChunkCollection &over_collection, ChunkCollection &boundary_start_collection,
                                   ChunkCollection &boundary_end_collection, const ValidityMask &partition_mask,
                                   const ValidityMask &order_mask) {

	// RANGE sorting parameters
	const auto order_col = bounds.partition_count;

	if (bounds.partition_count + bounds.order_count > 0) {

		// determine partition and peer group boundaries to ultimately figure out window size
		bounds.is_same_partition = !partition_mask.RowIsValidUnsafe(row_idx);
		bounds.is_peer = !order_mask.RowIsValidUnsafe(row_idx);

		// when the partition changes, recompute the boundaries
		if (!bounds.is_same_partition) {
			bounds.partition_start = row_idx;
			bounds.peer_start = row_idx;

			// find end of partition
			bounds.partition_end = input_size;
			if (bounds.partition_count) {
				idx_t n = 1;
				bounds.partition_end = FindNextStart(partition_mask, bounds.partition_start + 1, input_size, n);
			}

			// Find valid ordering values for the new partition
			// so we can exclude NULLs from RANGE expression computations
			bounds.valid_start = bounds.partition_start;
			bounds.valid_end = bounds.partition_end;

			if ((bounds.valid_start < bounds.valid_end) && bounds.has_preceding_range) {
				// Exclude any leading NULLs
				if (CellIsNull(over_collection, order_col, bounds.valid_start)) {
					idx_t n = 1;
					bounds.valid_start = FindNextStart(order_mask, bounds.valid_start + 1, bounds.valid_end, n);
				}
			}

			if ((bounds.valid_start < bounds.valid_end) && bounds.has_following_range) {
				// Exclude any trailing NULLs
				if (CellIsNull(over_collection, order_col, bounds.valid_end - 1)) {
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
		bounds.partition_end = input_size;
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
		bounds.window_start =
		    (int64_t)row_idx - GetCell<int64_t>(boundary_start_collection, 0, bounds.scalar_start ? 0 : row_idx);
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_ROWS: {
		bounds.window_start =
		    row_idx + GetCell<int64_t>(boundary_start_collection, 0, bounds.scalar_start ? 0 : row_idx);
		break;
	}
	case WindowBoundary::EXPR_PRECEDING_RANGE: {
		const auto expr_idx = bounds.scalar_start ? 0 : row_idx;
		if (CellIsNull(boundary_start_collection, 0, expr_idx)) {
			bounds.window_start = bounds.peer_start;
		} else {
			bounds.window_start =
			    FindOrderedRangeBound<true>(over_collection, order_col, bounds.range_sense, bounds.valid_start, row_idx,
			                                boundary_start_collection, expr_idx);
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_RANGE: {
		const auto expr_idx = bounds.scalar_start ? 0 : row_idx;
		if (CellIsNull(boundary_start_collection, 0, expr_idx)) {
			bounds.window_start = bounds.peer_start;
		} else {
			bounds.window_start = FindOrderedRangeBound<true>(over_collection, order_col, bounds.range_sense, row_idx,
			                                                  bounds.valid_end, boundary_start_collection, expr_idx);
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
		bounds.window_end =
		    (int64_t)row_idx - GetCell<int64_t>(boundary_end_collection, 0, bounds.scalar_end ? 0 : row_idx) + 1;
		break;
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
		bounds.window_end = row_idx + GetCell<int64_t>(boundary_end_collection, 0, bounds.scalar_end ? 0 : row_idx) + 1;
		break;
	case WindowBoundary::EXPR_PRECEDING_RANGE: {
		const auto expr_idx = bounds.scalar_end ? 0 : row_idx;
		if (CellIsNull(boundary_end_collection, 0, expr_idx)) {
			bounds.window_end = bounds.peer_end;
		} else {
			bounds.window_end =
			    FindOrderedRangeBound<false>(over_collection, order_col, bounds.range_sense, bounds.valid_start,
			                                 row_idx, boundary_end_collection, expr_idx);
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_RANGE: {
		const auto expr_idx = bounds.scalar_end ? 0 : row_idx;
		if (CellIsNull(boundary_end_collection, 0, expr_idx)) {
			bounds.window_end = bounds.peer_end;
		} else {
			bounds.window_end = FindOrderedRangeBound<false>(over_collection, order_col, bounds.range_sense, row_idx,
			                                                 bounds.valid_end, boundary_end_collection, expr_idx);
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

static void ComputeWindowExpression(BoundWindowExpression *wexpr, ChunkCollection &input, ChunkCollection &output,
                                    ChunkCollection &over, const ValidityMask &partition_mask,
                                    const ValidityMask &order_mask, WindowAggregationMode mode) {

	// TODO we could evaluate those expressions in parallel

	// evaluate inner expressions of window functions, could be more complex
	ChunkCollection payload_collection;
	vector<Expression *> exprs;
	for (auto &child : wexpr->children) {
		exprs.push_back(child.get());
	}
	// TODO: child may be a scalar, don't need to materialize the whole collection then
	MaterializeExpressions(exprs.data(), exprs.size(), input, payload_collection);

	ChunkCollection leadlag_offset_collection;
	ChunkCollection leadlag_default_collection;
	if (wexpr->type == ExpressionType::WINDOW_LEAD || wexpr->type == ExpressionType::WINDOW_LAG) {
		if (wexpr->offset_expr) {
			MaterializeExpression(wexpr->offset_expr.get(), input, leadlag_offset_collection,
			                      wexpr->offset_expr->IsScalar());
		}
		if (wexpr->default_expr) {
			MaterializeExpression(wexpr->default_expr.get(), input, leadlag_default_collection,
			                      wexpr->default_expr->IsScalar());
		}
	}

	// evaluate the FILTER clause and stuff it into a large mask for compactness and reuse
	ValidityMask filter_mask;
	vector<validity_t> filter_bits;
	if (wexpr->filter_expr) {
		// 	Start with all invalid and set the ones that pass
		filter_bits.resize(ValidityMask::ValidityMaskSize(input.Count()), 0);
		filter_mask.Initialize(filter_bits.data());
		ExpressionExecutor filter_execution(*wexpr->filter_expr);
		SelectionVector true_sel(STANDARD_VECTOR_SIZE);
		idx_t base_idx = 0;
		for (auto &chunk : input.Chunks()) {
			const auto filtered = filter_execution.SelectExpression(*chunk, true_sel);
			for (idx_t f = 0; f < filtered; ++f) {
				filter_mask.SetValid(base_idx + true_sel[f]);
			}
			base_idx += chunk->size();
		}
	}

	// evaluate boundaries if present. Parser has checked boundary types.
	ChunkCollection boundary_start_collection;
	if (wexpr->start_expr) {
		MaterializeExpression(wexpr->start_expr.get(), input, boundary_start_collection, wexpr->start_expr->IsScalar());
	}

	ChunkCollection boundary_end_collection;
	if (wexpr->end_expr) {
		MaterializeExpression(wexpr->end_expr.get(), input, boundary_end_collection, wexpr->end_expr->IsScalar());
	}

	// Set up a validity mask for IGNORE NULLS
	ValidityMask ignore_nulls;
	if (wexpr->ignore_nulls) {
		switch (wexpr->type) {
		case ExpressionType::WINDOW_LEAD:
		case ExpressionType::WINDOW_LAG:
		case ExpressionType::WINDOW_FIRST_VALUE:
		case ExpressionType::WINDOW_LAST_VALUE:
		case ExpressionType::WINDOW_NTH_VALUE: {
			idx_t pos = 0;
			for (auto &chunk : payload_collection.Chunks()) {
				const auto count = chunk->size();
				VectorData vdata;
				chunk->data[0].Orrify(count, vdata);
				if (!vdata.validity.AllValid()) {
					//	Lazily materialise the contents when we find the first NULL
					if (ignore_nulls.AllValid()) {
						ignore_nulls.Initialize(payload_collection.Count());
					}
					// Write to the current position
					// Chunks in a collection are full, so we don't have to worry about raggedness
					auto dst = ignore_nulls.GetData() + ignore_nulls.EntryCount(pos);
					auto src = vdata.validity.GetData();
					for (auto entry_count = vdata.validity.EntryCount(count); entry_count-- > 0;) {
						*dst++ = *src++;
					}
				}
				pos += count;
			}
			break;
		}
		default:
			break;
		}
	}

	// build a segment tree for frame-adhering aggregates
	// see http://www.vldb.org/pvldb/vol8/p1058-leis.pdf
	unique_ptr<WindowSegmentTree> segment_tree = nullptr;

	if (wexpr->aggregate) {
		segment_tree = make_unique<WindowSegmentTree>(*(wexpr->aggregate), wexpr->bind_info.get(), wexpr->return_type,
		                                              &payload_collection, filter_mask, mode);
	}

	WindowBoundariesState bounds(wexpr);
	uint64_t dense_rank = 1, rank_equal = 0, rank = 1;

	// this is the main loop, go through all sorted rows and compute window function result
	const vector<LogicalType> output_types(1, wexpr->return_type);
	DataChunk output_chunk;
	output_chunk.Initialize(output_types);
	for (idx_t row_idx = 0; row_idx < input.Count(); row_idx++) {
		// Grow the chunk if necessary.
		const auto output_offset = row_idx % STANDARD_VECTOR_SIZE;
		if (output_offset == 0) {
			output.Append(output_chunk);
			output_chunk.Reset();
			output_chunk.SetCardinality(MinValue(idx_t(STANDARD_VECTOR_SIZE), input.Count() - row_idx));
		}
		auto &result = output_chunk.data[0];

		// special case, OVER (), aggregate over everything
		UpdateWindowBoundaries(bounds, input.Count(), row_idx, over, boundary_start_collection, boundary_end_collection,
		                       partition_mask, order_mask);
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
			auto n_param = GetCell<int64_t>(payload_collection, 0, row_idx);
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
			break;
		}
		case ExpressionType::WINDOW_LEAD:
		case ExpressionType::WINDOW_LAG: {
			int64_t offset = 1;
			if (wexpr->offset_expr) {
				offset = GetCell<int64_t>(leadlag_offset_collection, 0, wexpr->offset_expr->IsScalar() ? 0 : row_idx);
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
				payload_collection.CopyCell(0, val_idx, result, output_offset);
			} else if (wexpr->default_expr) {
				const auto source_row = wexpr->default_expr->IsScalar() ? 0 : row_idx;
				leadlag_default_collection.CopyCell(0, source_row, result, output_offset);
			} else {
				FlatVector::SetNull(result, output_offset, true);
			}
			break;
		}
		case ExpressionType::WINDOW_FIRST_VALUE: {
			idx_t n = 1;
			const auto first_idx = FindNextStart(ignore_nulls, bounds.window_start, bounds.window_end, n);
			payload_collection.CopyCell(0, first_idx, result, output_offset);
			break;
		}
		case ExpressionType::WINDOW_LAST_VALUE: {
			idx_t n = 1;
			payload_collection.CopyCell(0, FindPrevStart(ignore_nulls, bounds.window_start, bounds.window_end, n),
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
						payload_collection.CopyCell(0, nth_index, result, output_offset);
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

	// Push the last chunk
	output.Append(output_chunk);
}

using WindowExpressions = vector<BoundWindowExpression *>;

static void ComputeWindowExpressions(WindowExpressions &window_exprs, ChunkCollection &input,
                                     ChunkCollection &window_results, ChunkCollection &over,
                                     WindowAggregationMode mode) {
	//	Idempotency
	if (input.Count() == 0) {
		return;
	}
	//	Pick out a function for the OVER clause
	auto over_expr = window_exprs[0];

	//	Set bits for the start of each partition
	vector<validity_t> partition_bits(ValidityMask::EntryCount(input.Count()), 0);
	ValidityMask partition_mask(partition_bits.data());
	partition_mask.SetValid(0);

	for (idx_t c = 0; c < over_expr->partitions.size(); ++c) {
		MaskColumn(partition_mask, over, c);
	}

	//	Set bits for the start of each peer group.
	//	Partitions also break peer groups, so start with the partition bits.
	const auto sort_col_count = over_expr->partitions.size() + over_expr->orders.size();
	ValidityMask order_mask(partition_mask, input.Count());
	for (idx_t c = over_expr->partitions.size(); c < sort_col_count; ++c) {
		MaskColumn(order_mask, over, c);
	}

	//	Compute the functions columnwise
	for (idx_t expr_idx = 0; expr_idx < window_exprs.size(); ++expr_idx) {
		ChunkCollection output;
		ComputeWindowExpression(window_exprs[expr_idx], input, output, over, partition_mask, order_mask, mode);
		window_results.Fuse(output);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
static void GeneratePartition(WindowOperatorState &state, WindowGlobalState &gstate, const idx_t hash_bin) {
	auto &op = (PhysicalWindow &)gstate.op;
	WindowExpressions window_exprs;
	for (idx_t expr_idx = 0; expr_idx < op.select_list.size(); ++expr_idx) {
		D_ASSERT(op.select_list[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto wexpr = reinterpret_cast<BoundWindowExpression *>(op.select_list[expr_idx].get());
		window_exprs.emplace_back(wexpr);
	}

	//	Get rid of any stale data
	state.chunks.Reset();
	state.window_results.Reset();
	state.position = 0;
	state.global_sort_state = nullptr;

	//	Pick out a function for the OVER clause
	auto over_expr = window_exprs[0];

	// There are three types of partitions:
	// 1. No partition (no sorting)
	// 2. One partition (sorting, but no hashing)
	// 3. Multiple partitions (sorting and hashing)
	const auto input_types = gstate.chunks.Types();
	const auto over_types = gstate.over_collection.Types();

	if (gstate.counts.empty() && hash_bin == 0) {
		ChunkCollection &input = gstate.chunks;
		ChunkCollection output;
		ChunkCollection &over = gstate.over_collection;

		const auto has_sorting = over_expr->partitions.size() + over_expr->orders.size();
		if (has_sorting && input.Count() > 0) {
			// 2. One partition
			SortCollectionForPartition(state, over_expr, input, over, nullptr, 0, 0);

			// Overwrite the collections with the sorted data
			ScanSortedPartition(state, input, input_types, over, over_types);
		}

		ComputeWindowExpressions(window_exprs, input, output, over, gstate.mode);
		state.chunks.Merge(input);
		state.window_results.Merge(output);

	} else if (hash_bin < gstate.counts.size() && gstate.counts[hash_bin] > 0) {
		// 3. Multiple partitions
		const auto hash_mask = hash_t(gstate.counts.size() - 1);
		SortCollectionForPartition(state, over_expr, gstate.chunks, gstate.over_collection, &gstate.hash_collection,
		                           hash_bin, hash_mask);

		// Scan the sorted data into new Collections
		ChunkCollection input;
		ChunkCollection output;
		ChunkCollection over;
		ScanSortedPartition(state, input, input_types, over, over_types);

		ComputeWindowExpressions(window_exprs, input, output, over, gstate.mode);
		state.chunks.Merge(input);
		state.window_results.Merge(output);
	}
}

static void Scan(WindowOperatorState &state, DataChunk &chunk) {
	ChunkCollection &big_data = state.chunks;
	ChunkCollection &window_results = state.window_results;

	if (state.position >= big_data.Count()) {
		return;
	}

	// just return what was computed before, appending the result cols of the window expressions at the end
	auto &proj_ch = big_data.GetChunkForRow(state.position);
	auto &wind_ch = window_results.GetChunkForRow(state.position);

	idx_t out_idx = 0;
	D_ASSERT(proj_ch.size() == wind_ch.size());
	chunk.SetCardinality(proj_ch);
	for (idx_t col_idx = 0; col_idx < proj_ch.ColumnCount(); col_idx++) {
		chunk.data[out_idx++].Reference(proj_ch.data[col_idx]);
	}
	for (idx_t col_idx = 0; col_idx < wind_ch.ColumnCount(); col_idx++) {
		chunk.data[out_idx++].Reference(wind_ch.data[col_idx]);
	}
	chunk.Verify();

	state.position += STANDARD_VECTOR_SIZE;
}

SinkResultType PhysicalWindow::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p,
                                    DataChunk &input) const {
	auto &lstate = (WindowLocalState &)lstate_p;
	lstate.chunks.Append(input);

	// Compute the over columns and the hash values for this block (if any)
	const auto over_idx = 0;
	auto over_expr = reinterpret_cast<BoundWindowExpression *>(select_list[over_idx].get());

	const auto sort_col_count = over_expr->partitions.size() + over_expr->orders.size();
	if (sort_col_count > 0) {
		DataChunk over_chunk;
		MaterializeOverForWindow(over_expr, input, over_chunk);

		if (!over_expr->partitions.empty()) {
			if (lstate.counts.empty()) {
				lstate.counts.resize(lstate.partition_count, 0);
			}

			DataChunk hash_chunk;
			HashChunk(lstate.counts, hash_chunk, over_chunk, over_expr->partitions.size());
			lstate.hash_collection.Append(hash_chunk);
			D_ASSERT(lstate.chunks.Count() == lstate.hash_collection.Count());
		}

		lstate.over_collection.Append(over_chunk);
		D_ASSERT(lstate.chunks.Count() == lstate.over_collection.Count());
	}
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalWindow::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &lstate = (WindowLocalState &)lstate_p;
	if (lstate.chunks.Count() == 0) {
		return;
	}
	auto &gstate = (WindowGlobalState &)gstate_p;
	lock_guard<mutex> glock(gstate.lock);
	gstate.chunks.Merge(lstate.chunks);
	gstate.over_collection.Merge(lstate.over_collection);
	gstate.hash_collection.Merge(lstate.hash_collection);
	if (gstate.counts.empty()) {
		gstate.counts = lstate.counts;
	} else {
		D_ASSERT(gstate.counts.size() == lstate.counts.size());
		for (idx_t i = 0; i < gstate.counts.size(); ++i) {
			gstate.counts[i] += lstate.counts[i];
		}
	}
}

unique_ptr<LocalSinkState> PhysicalWindow::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<WindowLocalState>(*this);
}

unique_ptr<GlobalSinkState> PhysicalWindow::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<WindowGlobalState>(*this, context);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class WindowGlobalSourceState : public GlobalSourceState {
public:
	explicit WindowGlobalSourceState(const PhysicalWindow &op) : op(op), next_part(0) {
	}

	const PhysicalWindow &op;
	//! The output read position.
	atomic<idx_t> next_part;

public:
	idx_t MaxThreads() override {
		auto &state = (WindowGlobalState &)*op.sink_state;

		// If there is only one partition, we have to process it on one thread.
		if (state.counts.empty()) {
			return 1;
		}

		idx_t max_threads = 0;
		for (const auto count : state.counts) {
			if (count > 0) {
				max_threads++;
			}
		}

		return max_threads;
	}
};

unique_ptr<LocalSourceState> PhysicalWindow::GetLocalSourceState(ExecutionContext &context,
                                                                 GlobalSourceState &gstate) const {
	return make_unique<WindowOperatorState>(*this, context);
}

unique_ptr<GlobalSourceState> PhysicalWindow::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<WindowGlobalSourceState>(*this);
}

void PhysicalWindow::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                             LocalSourceState &lstate_p) const {
	auto &state = (WindowOperatorState &)lstate_p;
	auto &global_source = (WindowGlobalSourceState &)gstate_p;
	auto &gstate = (WindowGlobalState &)*sink_state;

	do {
		if (state.position >= state.chunks.Count()) {
			auto hash_bin = global_source.next_part++;
			for (; hash_bin < state.partitions; hash_bin = global_source.next_part++) {
				if (gstate.counts[hash_bin] > 0) {
					break;
				}
			}
			GeneratePartition(state, gstate, hash_bin);
		}
		Scan(state, chunk);
		if (chunk.size() != 0) {
			return;
		} else {
			break;
		}
	} while (true);
	D_ASSERT(chunk.size() == 0);
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
