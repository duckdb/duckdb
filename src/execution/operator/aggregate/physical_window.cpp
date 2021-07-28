#include "duckdb/execution/operator/aggregate/physical_window.hpp"

#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/window_segment_tree.hpp"
#include "duckdb/parallel/task_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/common/windows_undefs.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>

namespace duckdb {

using counts_t = std::vector<size_t>;

//	Global sink state
class WindowGlobalState : public GlobalOperatorState {
public:
	WindowGlobalState(PhysicalWindow &op_p, ClientContext &context) : op(op_p) {
	}

	PhysicalWindow &op;
	mutex lock;
	ChunkCollection chunks;
	ChunkCollection over_collection;
	ChunkCollection hash_collection;
	counts_t counts;
};

//	Per-thread sink state
class WindowLocalState : public LocalSinkState {
public:
	explicit WindowLocalState(PhysicalWindow &op_p, const unsigned partition_bits = 10)
	    : op(op_p), partition_count(size_t(1) << partition_bits) {
	}

	PhysicalWindow &op;
	ChunkCollection chunks;
	ChunkCollection over_collection;
	ChunkCollection hash_collection;
	const size_t partition_count;
	counts_t counts;
};

// this implements a sorted window functions variant
PhysicalWindow::PhysicalWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                               idx_t estimated_cardinality, PhysicalOperatorType type)
    : PhysicalSink(type, move(types), estimated_cardinality), select_list(move(select_list)) {
}

template <typename W>
class BitArray {
public:
	using bits_t = std::vector<W>;

	static const auto BITS_PER_WORD = std::numeric_limits<W>::digits;
	static const auto ZEROS = std::numeric_limits<W>::min();
	static const auto ONES = std::numeric_limits<W>::max();

	class reference { // NOLINT
	public:
		friend BitArray;

		reference(const reference &r) = default;

		reference &operator=(bool x) noexcept {
			auto b = parent.Block(pos);
			auto s = parent.Shift(pos);
			auto w = parent.GetBlock(b);
			if (parent.TestBit(w, s) != x) {
				parent.SetBlock(b, parent.FlipBit(w, s));
			}
			return *this;
		}

		reference &operator=(const reference &r) noexcept {
			return *this = bool(r);
		}

		explicit operator bool() const noexcept {
			return parent[pos];
		}

		bool operator~() const noexcept {
			return !parent[pos];
		}

	private:
		explicit reference(BitArray &parent_p, size_t pos_p) : parent(parent_p), pos(pos_p) {
		}

		BitArray &parent;
		size_t pos;
	};

	static size_t Block(const size_t &pos) {
		return pos / BITS_PER_WORD;
	}

	static unsigned Shift(const size_t &pos) {
		return pos % BITS_PER_WORD;
	}

	static bool TestBit(W w, unsigned s) {
		return (w >> s) & 0x01;
	}

	static W SetBit(W w, unsigned s) {
		return w | (W(1) << s);
	}

	static W ClearBit(W w, unsigned s) {
		return w & ~(W(1) << s);
	}

	static W FlipBit(W w, unsigned s) {
		return w ^ (W(1) << s);
	}

	explicit BitArray(const size_t &count, const W &init = 0)
	    : bits(count ? Block(count - 1) + 1 : 0, init), count(count) {
	}

	size_t Count() const {
		return count;
	}

	const W &GetBlock(size_t b) const {
		return bits[b];
	}

	W &GetBlock(size_t b) {
		return bits[b];
	}

	void SetBlock(size_t b, const W &block) {
		GetBlock(b) = block;
	}

	bool operator[](size_t pos) const {
		return TestBit(GetBlock(Block(pos)), Shift(pos));
	}

	reference operator[](size_t pos) {
		return reference(*this, pos);
	}

private:
	bits_t bits;
	size_t count;
};

template <typename INPUT_TYPE>
struct ChunkIterator {

	ChunkIterator(ChunkCollection &collection, const idx_t col_idx)
	    : collection(collection), col_idx(col_idx), chunk_begin(0), chunk_end(0), ch_idx(0), data(nullptr),
	      validity(nullptr) {
		Update(0);
	}

	void Update(idx_t r) {
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

	bool IsValid(idx_t r) {
		return validity->RowIsValid(r - chunk_begin);
	}

	INPUT_TYPE GetValue(idx_t r) {
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

template <typename MASK_TYPE, typename INPUT_TYPE>
static void MaskTypedColumn(MASK_TYPE &mask, ChunkCollection &over_collection, const idx_t c) {
	ChunkIterator<INPUT_TYPE> ci(over_collection, c);

	//	Record the first value
	idx_t r = 0;
	auto prev_valid = ci.IsValid(r);
	auto prev = ci.GetValue(r);
	++r;

	//	Process complete blocks
	const auto row_count = over_collection.Count();
	const auto complete_block_count = mask.Block(row_count);
	for (idx_t b = mask.Block(r); b < complete_block_count; ++b) {
		auto block = mask.GetBlock(b);

		//	Skip the block if it is all boundaries.
		if (block == mask.ONES) {
			r -= (r % mask.BITS_PER_WORD);
			r += mask.BITS_PER_WORD;
			continue;
		}

		//	Scan the rows in the complete block
		for (unsigned shift = mask.Shift(r); shift < mask.BITS_PER_WORD; ++shift, ++r) {
			//	Update the chunk for this row
			ci.Update(r);

			auto curr_valid = ci.IsValid(r);
			auto curr = ci.GetValue(r);
			if (!mask.TestBit(block, shift)) {
				if (curr_valid != prev_valid || (curr_valid && !Equals::Operation(curr, prev))) {
					block = mask.SetBit(block, shift);
				}
			}
			prev_valid = curr_valid;
			prev = curr;
		}
		mask.SetBlock(b, block);
	}

	// Finish last ragged block
	if (r < row_count) {
		auto block = mask.GetBlock(complete_block_count);
		if (block != mask.ONES) {
			for (unsigned shift = mask.Shift(r); r < row_count; ++shift, ++r) {
				//	Update the chunk for this row
				ci.Update(r);

				auto curr_valid = ci.IsValid(r);
				auto curr = ci.GetValue(r);
				if (!mask.TestBit(block, shift)) {
					if (curr_valid != prev_valid || (curr_valid && !Equals::Operation(curr, prev))) {
						block = mask.SetBit(block, shift);
					}
				}
				prev_valid = curr_valid;
				prev = curr;
			}
			mask.SetBlock(complete_block_count, block);
		}
	}
}

template <typename W>
static void MaskColumn(BitArray<W> &mask, ChunkCollection &over_collection, const idx_t c) {
	using MASK_TYPE = BitArray<W>;

	auto &vector = over_collection.GetChunk(0).data[c];
	switch (vector.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		MaskTypedColumn<MASK_TYPE, int8_t>(mask, over_collection, c);
		break;
	case PhysicalType::INT16:
		MaskTypedColumn<MASK_TYPE, int16_t>(mask, over_collection, c);
		break;
	case PhysicalType::INT32:
		MaskTypedColumn<MASK_TYPE, int32_t>(mask, over_collection, c);
		break;
	case PhysicalType::INT64:
		MaskTypedColumn<MASK_TYPE, int64_t>(mask, over_collection, c);
		break;
	case PhysicalType::UINT8:
		MaskTypedColumn<MASK_TYPE, uint8_t>(mask, over_collection, c);
		break;
	case PhysicalType::UINT16:
		MaskTypedColumn<MASK_TYPE, uint16_t>(mask, over_collection, c);
		break;
	case PhysicalType::UINT32:
		MaskTypedColumn<MASK_TYPE, uint32_t>(mask, over_collection, c);
		break;
	case PhysicalType::UINT64:
		MaskTypedColumn<MASK_TYPE, uint64_t>(mask, over_collection, c);
		break;
	case PhysicalType::INT128:
		MaskTypedColumn<MASK_TYPE, hugeint_t>(mask, over_collection, c);
		break;
	case PhysicalType::FLOAT:
		MaskTypedColumn<MASK_TYPE, float>(mask, over_collection, c);
		break;
	case PhysicalType::DOUBLE:
		MaskTypedColumn<MASK_TYPE, double>(mask, over_collection, c);
		break;
	case PhysicalType::VARCHAR:
		MaskTypedColumn<MASK_TYPE, string_t>(mask, over_collection, c);
		break;
	case PhysicalType::INTERVAL:
		MaskTypedColumn<MASK_TYPE, interval_t>(mask, over_collection, c);
		break;
	default:
		throw NotImplementedException("Type for comparison");
		break;
	}
}

template <typename W>
static idx_t FindNextStart(const BitArray<W> &mask, idx_t l, const idx_t r) {
	while (l < r) {
		//	If l is aligned with the start of a block, and the block is blank, then skip forward one block.
		const auto block = mask.GetBlock(mask.Block(l));
		auto shift = mask.Shift(l);
		if (!block && !shift) {
			l += mask.BITS_PER_WORD;
			continue;
		}

		// Loop over the block
		for (; shift < mask.BITS_PER_WORD; ++shift, ++l) {
			if (mask.TestBit(block, shift)) {
				return MinValue(l, r);
			}
		}
	}

	//	Didn't find a start so return the end of the range
	return r;
}

template <typename W>
static idx_t FindPrevStart(const BitArray<W> &mask, const idx_t l, idx_t r) {
	while (l < r) {
		// If r is aligned with the start of a block, and the previous block is blank,
		// then skip backwards one block.
		const auto block = mask.GetBlock(mask.Block(r - 1));
		auto shift = mask.Shift(r);
		if (!block && !shift) {
			// r is nonzero (> l) and word aligned, so this will not underflow.
			r -= mask.BITS_PER_WORD;
			continue;
		}

		// Loop backwards over the block
		// shift is probing r-1 >= l >= 0
		for (shift = mask.Shift(r - 1) + 1; shift-- > 0; --r) {
			if (mask.TestBit(block, shift)) {
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

static OrderByNullType NormaliseNullOrder(OrderType type, OrderByNullType null_order) {
	if (type != OrderType::DESCENDING) {
		return null_order;
	}

	switch (null_order) {
	case OrderByNullType::NULLS_FIRST:
		return OrderByNullType::NULLS_LAST;
	case OrderByNullType::NULLS_LAST:
		return OrderByNullType::NULLS_FIRST;
	default:
		throw InternalException("Unknown NULL order type");
	}
}

static void SortCollectionForPartition(BoundWindowExpression *wexpr, ChunkCollection &input,
                                       ChunkCollection &sort_collection) {
	if (input.Count() == 0) {
		return;
	}
	vector<OrderType> orders;
	vector<OrderByNullType> null_order_types;

	// we sort by both 1) partition by expression list and 2) order by expressions
	for (idx_t prt_idx = 0; prt_idx < wexpr->partitions.size(); prt_idx++) {
		orders.push_back(OrderType::ASCENDING);
		null_order_types.push_back(OrderByNullType::NULLS_FIRST);
	}

	for (const auto &order : wexpr->orders) {
		orders.push_back(order.type);
		null_order_types.push_back(NormaliseNullOrder(order.type, order.null_order));
	}

	auto sorted_vector = unique_ptr<idx_t[]>(new idx_t[input.Count()]);
	sort_collection.Sort(orders, null_order_types, sorted_vector.get());

	input.Reorder(sorted_vector.get());
	sort_collection.Reorder(sorted_vector.get());
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

	D_ASSERT(over_types.size() > 0);

	over_chunk.Initialize(over_types);
	executor.Execute(input_chunk, over_chunk);

	over_chunk.Verify();
}

struct WindowBoundariesState {
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
struct OperationCompare : public std::binary_function<T, T, bool> {
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

static void UpdateWindowBoundaries(BoundWindowExpression *wexpr, const idx_t input_size, const idx_t row_idx,
                                   ChunkCollection &over_collection, ChunkCollection &boundary_start_collection,
                                   ChunkCollection &boundary_end_collection, const BitArray<uint64_t> &partition_mask,
                                   const BitArray<uint64_t> &order_mask, WindowBoundariesState &bounds) {

	// RANGE sorting parameters
	const auto order_col = wexpr->partitions.size();
	const auto range_sense = wexpr->orders.empty() ? OrderType::INVALID : wexpr->orders[0].type;

	if (wexpr->partitions.size() + wexpr->orders.size() > 0) {

		// determine partition and peer group boundaries to ultimately figure out window size
		bounds.is_same_partition = !partition_mask[row_idx];
		bounds.is_peer = !order_mask[row_idx];

		// when the partition changes, recompute the boundaries
		if (!bounds.is_same_partition) {
			bounds.partition_start = row_idx;
			bounds.peer_start = row_idx;

			// find end of partition
			bounds.partition_end = input_size;
			if (!wexpr->partitions.empty()) {
				bounds.partition_end = FindNextStart(partition_mask, bounds.partition_start + 1, input_size);
			}

			// Find valid ordering values for the new partition
			// so we can exclude NULLs from RANGE expression computations
			bounds.valid_start = bounds.partition_start;
			bounds.valid_end = bounds.partition_end;

			if ((bounds.valid_start < bounds.valid_end) && (wexpr->start == WindowBoundary::EXPR_PRECEDING_RANGE ||
			                                                wexpr->end == WindowBoundary::EXPR_PRECEDING_RANGE)) {
				// Exclude any leading NULLs
				if (CellIsNull(over_collection, order_col, bounds.valid_start)) {
					bounds.valid_start = FindNextStart(order_mask, bounds.valid_start + 1, bounds.valid_end);
				}
			}

			if ((bounds.valid_start < bounds.valid_end) && (wexpr->start == WindowBoundary::EXPR_FOLLOWING_RANGE ||
			                                                wexpr->end == WindowBoundary::EXPR_FOLLOWING_RANGE)) {
				// Exclude any trailing NULLs
				if (CellIsNull(over_collection, order_col, bounds.valid_end - 1)) {
					bounds.valid_end = FindPrevStart(order_mask, bounds.valid_start, bounds.valid_end);
				}
			}

		} else if (!bounds.is_peer) {
			bounds.peer_start = row_idx;
		}

		if (BoundaryNeedsPeer(wexpr->end) || wexpr->type == ExpressionType::WINDOW_CUME_DIST) {
			bounds.peer_end = bounds.partition_end;
			if (!wexpr->orders.empty()) {
				bounds.peer_end = FindNextStart(order_mask, bounds.peer_start + 1, bounds.partition_end);
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

	switch (wexpr->start) {
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
		bounds.window_start = (int64_t)row_idx - GetCell<int64_t>(boundary_start_collection, 0,
		                                                          wexpr->start_expr->IsScalar() ? 0 : row_idx);
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_ROWS: {
		bounds.window_start =
		    row_idx + GetCell<int64_t>(boundary_start_collection, 0, wexpr->start_expr->IsScalar() ? 0 : row_idx);
		break;
	}
	case WindowBoundary::EXPR_PRECEDING_RANGE: {
		const auto expr_idx = wexpr->start_expr->IsScalar() ? 0 : row_idx;
		if (CellIsNull(boundary_start_collection, 0, expr_idx)) {
			bounds.window_start = bounds.peer_start;
		} else {
			bounds.window_start =
			    FindOrderedRangeBound<true>(over_collection, order_col, range_sense, bounds.valid_start, row_idx,
			                                boundary_start_collection, expr_idx);
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_RANGE: {
		const auto expr_idx = wexpr->start_expr->IsScalar() ? 0 : row_idx;
		if (CellIsNull(boundary_start_collection, 0, expr_idx)) {
			bounds.window_start = bounds.peer_start;
		} else {
			bounds.window_start = FindOrderedRangeBound<true>(over_collection, order_col, range_sense, row_idx,
			                                                  bounds.valid_end, boundary_start_collection, expr_idx);
		}
		break;
	}
	default:
		throw InternalException("Unsupported window start boundary");
	}

	switch (wexpr->end) {
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
		bounds.window_end = (int64_t)row_idx -
		                    GetCell<int64_t>(boundary_end_collection, 0, wexpr->end_expr->IsScalar() ? 0 : row_idx) + 1;
		break;
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
		bounds.window_end =
		    row_idx + GetCell<int64_t>(boundary_end_collection, 0, wexpr->end_expr->IsScalar() ? 0 : row_idx) + 1;
		break;
	case WindowBoundary::EXPR_PRECEDING_RANGE: {
		const auto expr_idx = wexpr->end_expr->IsScalar() ? 0 : row_idx;
		if (CellIsNull(boundary_end_collection, 0, expr_idx)) {
			bounds.window_end = bounds.peer_end;
		} else {
			bounds.window_end =
			    FindOrderedRangeBound<false>(over_collection, order_col, range_sense, bounds.valid_start, row_idx,
			                                 boundary_end_collection, expr_idx);
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_RANGE: {
		const auto expr_idx = wexpr->end_expr->IsScalar() ? 0 : row_idx;
		if (CellIsNull(boundary_end_collection, 0, expr_idx)) {
			bounds.window_end = bounds.peer_end;
		} else {
			bounds.window_end = FindOrderedRangeBound<false>(over_collection, order_col, range_sense, row_idx,
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
                                    ChunkCollection &over, const BitArray<uint64_t> &partition_mask,
                                    const BitArray<uint64_t> &order_mask) {

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

	// evaluate boundaries if present. Parser has checked boundary types.
	ChunkCollection boundary_start_collection;
	if (wexpr->start_expr) {
		MaterializeExpression(wexpr->start_expr.get(), input, boundary_start_collection, wexpr->start_expr->IsScalar());
	}

	ChunkCollection boundary_end_collection;
	if (wexpr->end_expr) {
		MaterializeExpression(wexpr->end_expr.get(), input, boundary_end_collection, wexpr->end_expr->IsScalar());
	}

	// build a segment tree for frame-adhering aggregates
	// see http://www.vldb.org/pvldb/vol8/p1058-leis.pdf
	unique_ptr<WindowSegmentTree> segment_tree = nullptr;

	if (wexpr->aggregate) {
		segment_tree = make_unique<WindowSegmentTree>(*(wexpr->aggregate), wexpr->bind_info.get(), wexpr->return_type,
		                                              &payload_collection);
	}

	WindowBoundariesState bounds;
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
		UpdateWindowBoundaries(wexpr, input.Count(), row_idx, over, boundary_start_collection, boundary_end_collection,
		                       partition_mask, order_mask, bounds);
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
			if (payload_collection.ColumnCount() != 1) {
				throw BinderException("NTILE needs a parameter");
			}
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

			if (val_idx >= int64_t(bounds.partition_start) && val_idx < int64_t(bounds.partition_end)) {
				payload_collection.CopyCell(0, val_idx, result, output_offset);
			} else if (wexpr->default_expr) {
				const auto source_row = wexpr->default_expr->IsScalar() ? 0 : row_idx;
				leadlag_default_collection.CopyCell(0, source_row, result, output_offset);
			} else {
				FlatVector::SetNull(result, output_offset, true);
			}
			break;
		}
		case ExpressionType::WINDOW_FIRST_VALUE:
			payload_collection.CopyCell(0, bounds.window_start, result, output_offset);
			break;
		case ExpressionType::WINDOW_LAST_VALUE:
			payload_collection.CopyCell(0, bounds.window_end - 1, result, output_offset);
			break;
		case ExpressionType::WINDOW_NTH_VALUE: {
			if (payload_collection.ColumnCount() != 2) {
				throw BinderException("NTH_VALUE needs a parameter");
			}
			// Returns value evaluated at the row that is the n'th row of the window frame (counting from 1);
			// returns NULL if there is no such row.
			if (CellIsNull(payload_collection, 1, row_idx)) {
				FlatVector::SetNull(result, output_offset, true);
			} else {
				auto n_param = GetCell<int64_t>(payload_collection, 1, row_idx);
				int64_t n_total = bounds.window_end - bounds.window_start;
				if (0 < n_param && n_param <= n_total) {
					payload_collection.CopyCell(0, bounds.window_start + n_param - 1, result, output_offset);
				} else {
					FlatVector::SetNull(result, output_offset, true);
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
                                     ChunkCollection &window_results, ChunkCollection &over) {
	//	Idempotency
	if (input.Count() == 0) {
		return;
	}
	//	Pick out a function for the OVER clause
	auto over_expr = window_exprs[0];

	//	Sort the partition
	const auto sort_col_count = over_expr->partitions.size() + over_expr->orders.size();
	if (sort_col_count > 0) {
		SortCollectionForPartition(over_expr, input, over);
	}

	//	Set bits for the start of each partition
	BitArray<uint64_t> partition_bits(input.Count());
	partition_bits[0] = true;

	for (idx_t c = 0; c < over_expr->partitions.size(); ++c) {
		MaskColumn(partition_bits, over, c);
	}

	//	Set bits for the start of each peer group.
	//	Partitions also break peer groups, so start with the partition bits.
	auto order_bits = partition_bits;
	for (idx_t c = over_expr->partitions.size(); c < sort_col_count; ++c) {
		MaskColumn(order_bits, over, c);
	}

	//	Compute the functions columnwise
	for (idx_t expr_idx = 0; expr_idx < window_exprs.size(); ++expr_idx) {
		ChunkCollection output;
		ComputeWindowExpression(window_exprs[expr_idx], input, output, over, partition_bits, order_bits);
		window_results.Fuse(output);
	}
}

static void AppendCollection(const ChunkCollection &source, ChunkCollection &target, SelectionVector &sel,
                             const idx_t source_count, const idx_t chunk_idx) {

	DataChunk chunk;
	chunk.Initialize(source.Types());
	source.GetChunk(chunk_idx).Copy(chunk, sel, source_count);
	target.Append(chunk);
}

static void ExtractPartition(WindowGlobalState &gstate, ChunkCollection &chunks, ChunkCollection &over_collection,
                             const hash_t hash_bin, const hash_t hash_mask) {

	//	Copy the partition data so we can work with it on this thread
	ChunkCollection &hashes = gstate.hash_collection;
	SelectionVector sel;
	for (idx_t chunk_idx = 0; chunk_idx < hashes.ChunkCount(); ++chunk_idx) {
		//	Build a selection vector of matching hashes
		auto &hash_chunk = hashes.GetChunk(chunk_idx);
		auto hash_size = hash_chunk.size();
		auto hash_data = FlatVector::GetData<hash_t>(hash_chunk.data[0]);
		sel.Initialize(hash_size);
		idx_t bin_size = 0;
		for (idx_t i = 0; i < hash_size; ++i) {
			if ((hash_data[i] & hash_mask) == hash_bin) {
				sel.set_index(bin_size++, i);
			}
		}

		//	Copy the data for each collection
		if (bin_size == 0) {
			continue;
		}

		AppendCollection(gstate.chunks, chunks, sel, bin_size, chunk_idx);
		AppendCollection(gstate.over_collection, over_collection, sel, bin_size, chunk_idx);
	}
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
idx_t PhysicalWindow::MaxThreads(ClientContext &context) {
	// Recursive CTE can cause us to be called befor Finalize,
	// so we have to check and fall back to the cardinality estimate
	// in that case
	if (!this->sink_state.get()) {
		return (estimated_cardinality + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE + 1;
	}
	auto &state = (WindowGlobalState &)*this->sink_state;

	// If there is only one partition, we have to process it on one thread.
	if (state.counts.empty()) {
		return 1;
	}

	idx_t max_threads = 0;
	for (const auto count : state.counts) {
		max_threads += int(count > 0);
	}

	return max_threads;
}

//	Global read state
class WindowParallelState : public ParallelState {
public:
	WindowParallelState() : next_part(0) {
	}
	//! The output read position.
	atomic<idx_t> next_part;
};

unique_ptr<ParallelState> PhysicalWindow::GetParallelState() {
	auto result = make_unique<WindowParallelState>();
	return move(result);
}

// Per-thread read state
class PhysicalWindowOperatorState : public PhysicalOperatorState {
public:
	PhysicalWindowOperatorState(PhysicalOperator &op, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), parallel_state(nullptr), initialized(false) {
	}

	ParallelState *parallel_state;
	bool initialized;

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
};

unique_ptr<PhysicalOperatorState> PhysicalWindow::GetOperatorState() {
	return make_unique<PhysicalWindowOperatorState>(*this, children.empty() ? nullptr : children[0].get());
}

static void GeneratePartition(PhysicalWindowOperatorState &state, WindowGlobalState &gstate, const idx_t hash_bin) {
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

	if (gstate.counts.empty() && hash_bin == 0) {
		ChunkCollection &big_data = gstate.chunks;
		ChunkCollection output;
		ChunkCollection &over_collection = gstate.over_collection;
		ComputeWindowExpressions(window_exprs, big_data, output, over_collection);
		state.chunks.Merge(big_data);
		state.window_results.Merge(output);
	} else if (hash_bin < gstate.counts.size() && gstate.counts[hash_bin] > 0) {
		ChunkCollection input;
		ChunkCollection output;
		ChunkCollection over;
		const auto hash_mask = hash_t(gstate.counts.size() - 1);
		ExtractPartition(gstate, input, over, hash_bin, hash_mask);
		ComputeWindowExpressions(window_exprs, input, output, over);
		state.chunks.Merge(input);
		state.window_results.Merge(output);
	}
}

static void Scan(PhysicalWindowOperatorState &state, DataChunk &chunk) {
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

void PhysicalWindow::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                      PhysicalOperatorState *state_p) const {
	auto &state = *reinterpret_cast<PhysicalWindowOperatorState *>(state_p);
	auto &gstate = (WindowGlobalState &)*sink_state;

	if (!state.initialized) {
		// initialize thread-local operator state
		state.partitions = gstate.counts.size();
		state.next_part = 0;
		// record parallel state (if any)
		state.parallel_state = nullptr;
		auto &task = context.task;
		// check if there is any parallel state to fetch
		state.parallel_state = nullptr;
		auto task_info = task.task_info.find(this);
		if (task_info != task.task_info.end()) {
			// parallel scan init
			state.parallel_state = task_info->second;
		}
		state.initialized = true;
	}

	if (!state.parallel_state) {
		// sequential scan
		if (state.position >= state.chunks.Count()) {
			auto hash_bin = state.next_part++;
			for (; hash_bin < state.partitions; hash_bin = state.next_part++) {
				if (gstate.counts[hash_bin] > 0) {
					break;
				}
			}
			GeneratePartition(state, gstate, hash_bin);
		}
		Scan(state, chunk);
		if (chunk.size() != 0) {
			return;
		}
	} else {
		// parallel scan
		auto &parallel_state = *reinterpret_cast<WindowParallelState *>(state.parallel_state);
		do {
			if (state.position >= state.chunks.Count()) {
				auto hash_bin = parallel_state.next_part++;
				for (; hash_bin < state.partitions; hash_bin = parallel_state.next_part++) {
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
	}
	D_ASSERT(chunk.size() == 0);
}

void PhysicalWindow::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_p,
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
}

void PhysicalWindow::Combine(ExecutionContext &context, GlobalOperatorState &gstate_p, LocalSinkState &lstate_p) {
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

bool PhysicalWindow::FinalizeInternal(ClientContext &context, unique_ptr<GlobalOperatorState> gstate_p) {
	this->sink_state = move(gstate_p);
	return true;
}

bool PhysicalWindow::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> gstate_p) {
	return FinalizeInternal(context, move(gstate_p));
}

unique_ptr<LocalSinkState> PhysicalWindow::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<WindowLocalState>(*this);
}

unique_ptr<GlobalOperatorState> PhysicalWindow::GetGlobalState(ClientContext &context) {
	return make_unique<WindowGlobalState>(*this, context);
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
