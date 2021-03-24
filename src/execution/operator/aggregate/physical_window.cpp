#include "duckdb/execution/operator/aggregate/physical_window.hpp"

#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/window_segment_tree.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

#include <cmath>
#include <numeric>

namespace duckdb {

using Slice = ChunkCollection::slice_t;

class WindowGlobalState : public GlobalOperatorState {
public:
	WindowGlobalState(PhysicalWindow &op_p, ClientContext &context) : op(op_p) {
	}

	PhysicalWindow &op;
	std::mutex lock;
	ChunkCollection chunks;
	ChunkCollection window_results;
};

class WindowLocalState : public LocalSinkState {
public:
	explicit WindowLocalState(PhysicalWindow &op_p) : op(op_p) {
	}

	PhysicalWindow &op;
	ChunkCollection chunks;
};

//! The operator state of the window
class PhysicalWindowOperatorState : public PhysicalOperatorState {
public:
	PhysicalWindowOperatorState(PhysicalOperator &op, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), position(0) {
	}

	idx_t position;
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

	//	Finish last ragged block
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
static idx_t FindNextStart(const BitArray<W> &mask, idx_t l, idx_t r) {
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
				return std::min(l, r);
			}
		}
	}

	//	Didn't find a start so return the end of the range
	return r;
}

static idx_t MaterializeExpressions(Expression **exprs, idx_t expr_count, ChunkCollection &input,
                                    ChunkCollection &output, const Slice range, bool scalar = false) {
	const auto begin_chunk = input.LocateChunk(range.first);
	const auto result = begin_chunk * STANDARD_VECTOR_SIZE;

	if (expr_count == 0) {
		return result;
	}

	const auto end_chunk = input.LocateChunk(range.second - 1) + 1;

	vector<LogicalType> types;
	ExpressionExecutor executor;
	for (idx_t expr_idx = 0; expr_idx < expr_count; ++expr_idx) {
		types.push_back(exprs[expr_idx]->return_type);
		executor.AddExpression(*exprs[expr_idx]);
	}

	for (auto i = begin_chunk; i < end_chunk; i++) {
		DataChunk chunk;
		chunk.Initialize(types);

		executor.Execute(input.GetChunk(i), chunk);

		chunk.Verify();
		output.Append(chunk);

		if (scalar) {
			break;
		}
	}

	return result;
}

static idx_t MaterializeExpression(Expression *expr, ChunkCollection &input, ChunkCollection &output, const Slice range,
                                   bool scalar = false) {
	return MaterializeExpressions(&expr, 1, input, output, range, scalar);
}

static bool CompatibleSorts(const BoundWindowExpression *a, const BoundWindowExpression *b) {
	// check if the partitions are equivalent
	if (a->partitions.size() != b->partitions.size()) {
		return false;
	}
	for (idx_t i = 0; i < a->partitions.size(); i++) {
		if (!a->partitions[i]->Equals(b->partitions[i].get())) {
			return false;
		}
	}
	// check if the orderings are equivalent
	if (a->orders.size() != b->orders.size()) {
		return false;
	}
	for (idx_t i = 0; i < a->orders.size(); i++) {
		if (a->orders[i].type != b->orders[i].type) {
			return false;
		}
		if (!a->orders[i].expression->Equals(b->orders[i].expression.get())) {
			return false;
		}
	}
	return true;
}

using counts_t = std::vector<size_t>;

static counts_t OffsetsFromCounts(const counts_t &counts) {
	counts_t offsets(1, 0);
	offsets.reserve(counts.size() + 1);
	for (size_t i = 1; i <= counts.size(); ++i) {
		offsets.emplace_back(offsets[i - 1] + counts[i - 1]);
	}

	return offsets;
}

static std::vector<Slice> SlicesFromCounts(const counts_t &counts, const idx_t page_size = STANDARD_VECTOR_SIZE) {
	std::vector<Slice> result;

	Slice slice(0, 0);
	for (const auto &count : counts) {
		if (count) {
			slice.second += count;
			//	Emit a slice when it is either big enough or ends on a page boundary
			if ((slice.second - slice.first) >= page_size || (0 == slice.second % page_size)) {
				result.emplace_back(slice);
				slice.first = slice.second;
			}
		}
	}

	//	Finish any ragged slices
	if (slice.first != slice.second) {
		result.emplace_back(slice);
	}

	return result;
}

static void PartitionChunk(counts_t &counts, Vector &hash_vector, DataChunk &sort_chunk, const idx_t partition_cols,
                           const hash_t partition_mask) {
	D_ASSERT(hash_vector.GetType().id() == LogicalTypeId::HASH);
	const auto count = sort_chunk.size();
	VectorOperations::Hash(sort_chunk.data[0], hash_vector, count);
	for (idx_t prt_idx = 1; prt_idx < partition_cols; ++prt_idx) {
		VectorOperations::CombineHash(hash_vector, sort_chunk.data[prt_idx], count);
	}

	auto hashes = FlatVector::GetData<hash_t>(hash_vector);
	for (idx_t i = 0; i < count; ++i) {
		const auto bin = (hashes[i] & partition_mask);
		++counts[bin];
	}
}

static void SortPartitionedCollectionForWindow(BoundWindowExpression *wexpr, ChunkCollection &input,
                                               ChunkCollection &output, ChunkCollection &over_collection,
                                               const idx_t begin, const idx_t end) {
	if (begin == end) {
		return;
	}
	vector<OrderType> orders;
	vector<OrderByNullType> null_order_types;

	// we sort by both 1) partition by expression list and 2) order by expressions
	for (idx_t prt_idx = 0; prt_idx < wexpr->partitions.size(); prt_idx++) {
		orders.push_back(OrderType::ASCENDING);
		null_order_types.push_back(OrderByNullType::NULLS_FIRST);
	}

	for (idx_t ord_idx = 0; ord_idx < wexpr->orders.size(); ord_idx++) {
		orders.push_back(wexpr->orders[ord_idx].type);
		null_order_types.push_back(wexpr->orders[ord_idx].null_order);
	}

	const Slice slice {begin, end};
	auto sorted_vector = unique_ptr<idx_t[]>(new idx_t[end - begin]);
	over_collection.SortSlice(slice, orders, null_order_types, sorted_vector.get());

	input.ReorderSlice(slice, sorted_vector.get());
	output.ReorderSlice(slice, sorted_vector.get());
	over_collection.ReorderSlice(slice, sorted_vector.get());
}

static counts_t DistributePartitionsForWindow(BoundWindowExpression *wexpr, ChunkCollection &input,
                                              ChunkCollection &output, ChunkCollection &over_collection,
                                              const unsigned partition_bits = 10) {
	//	If there are no partition columns, then there is one partition
	const auto partition_cols = wexpr->partitions.size();
	const auto num_rows = over_collection.Count();
	if (partition_cols == 0) {
		return counts_t(1, num_rows);
	}

	//	Radix sort the partitions
	const auto partition_count = size_t(1) << partition_bits;
	counts_t counts(partition_count, 0);

	//	First pass: hash and count the partition sizes
	const auto partition_mask = hash_t(partition_count - 1);
	ChunkCollection hash_collection;
	vector<LogicalType> hash_types(1, LogicalTypeId::HASH);
	for (const auto &sort_chunk : over_collection.Chunks()) {
		DataChunk hash_chunk;
		hash_chunk.Initialize(hash_types);
		hash_chunk.SetCardinality(*sort_chunk);
		PartitionChunk(counts, hash_chunk.data[0], *sort_chunk, partition_cols, partition_mask);
		hash_collection.Append(hash_chunk);
	}
	auto offsets = OffsetsFromCounts(counts);

	//	Second pass: Compute the reordering
	auto order_vector = unique_ptr<idx_t[]>(new idx_t[over_collection.Count()]);
	auto order = order_vector.get();
	idx_t r = 0;
	for (const auto &hash_chunk : hash_collection.Chunks()) {
		const auto hashes = FlatVector::GetData<hash_t>(hash_chunk->data[0]);
		for (idx_t i = 0; i < hash_chunk->size(); ++i, ++r) {
			const auto bin = (hashes[i] & partition_mask);
			order[offsets[bin]++] = r;
		}
	}

	//	Reorder all the collections into the bins
	input.Reorder(order);
	over_collection.Reorder(order);
	output.Reorder(order);

	return counts;
}

static void MaterializeOverCollectionForWindow(BoundWindowExpression *wexpr, ChunkCollection &input,
                                               ChunkCollection &output, ChunkCollection &over_collection) {
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

	// create a ChunkCollection for the results of the expressions in the window definitions
	for (idx_t i = 0; i < input.ChunkCount(); i++) {
		DataChunk over_chunk;
		over_chunk.Initialize(over_types);

		executor.Execute(input.GetChunk(i), over_chunk);

		over_chunk.Verify();
		over_collection.Append(over_chunk);
	}

	D_ASSERT(input.Count() == over_collection.Count());
}

static counts_t SortCollectionForWindow(BoundWindowExpression *wexpr, ChunkCollection &input, ChunkCollection &output,
                                        ChunkCollection &over_collection, const unsigned partition_bits = 10) {
	MaterializeOverCollectionForWindow(wexpr, input, output, over_collection);
	auto counts = DistributePartitionsForWindow(wexpr, input, output, over_collection, partition_bits);

	//	Sort the partitions independently
	idx_t begin = 0;
	for (size_t i = 0; i < counts.size(); ++i) {
		auto end = begin + counts[i];
		SortPartitionedCollectionForWindow(wexpr, input, output, over_collection, begin, end);
		begin = end;
	}

	return counts;
}

struct WindowBoundariesState {
	idx_t partition_start = 0;
	idx_t partition_end = 0;
	idx_t peer_start = 0;
	idx_t peer_end = 0;
	int64_t window_start = -1;
	int64_t window_end = -1;
	bool is_same_partition = false;
	bool is_peer = false;
};

static bool WindowNeedsRank(BoundWindowExpression *wexpr) {
	return wexpr->type == ExpressionType::WINDOW_PERCENT_RANK || wexpr->type == ExpressionType::WINDOW_RANK ||
	       wexpr->type == ExpressionType::WINDOW_RANK_DENSE || wexpr->type == ExpressionType::WINDOW_CUME_DIST;
}

static void UpdateWindowBoundaries(BoundWindowExpression *wexpr, const Slice range, const idx_t row_idx,
                                   const idx_t local_row, ChunkCollection &boundary_start_collection,
                                   ChunkCollection &boundary_end_collection, const BitArray<uint64_t> &partition_mask,
                                   const BitArray<uint64_t> &order_mask, WindowBoundariesState &bounds) {

	if (wexpr->partitions.size() + wexpr->orders.size() > 0) {

		// determine partition and peer group boundaries to ultimately figure out window size
		bounds.is_same_partition = !partition_mask[row_idx];
		bounds.is_peer = !order_mask[row_idx];

		// when the partition changes, recompute the boundaries
		if (!bounds.is_same_partition) {
			bounds.partition_start = row_idx;
			bounds.peer_start = row_idx;

			// find end of partition
			bounds.partition_end = range.second;
			if (!wexpr->partitions.empty()) {
				bounds.partition_end = FindNextStart(partition_mask, bounds.partition_start + 1, range.second);
			}

		} else if (!bounds.is_peer) {
			bounds.peer_start = row_idx;
		}

		if (wexpr->end == WindowBoundary::CURRENT_ROW_RANGE || wexpr->type == ExpressionType::WINDOW_CUME_DIST) {
			bounds.peer_end = bounds.partition_end;
			if (!wexpr->orders.empty()) {
				bounds.peer_end = FindNextStart(order_mask, bounds.peer_start + 1, bounds.partition_end);
			}
		}
	} else {
		bounds.is_same_partition = false;
		bounds.is_peer = true;
		bounds.partition_end = range.second;
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
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		D_ASSERT(0); // disallowed
		break;
	case WindowBoundary::EXPR_PRECEDING: {
		D_ASSERT(boundary_start_collection.ColumnCount() > 0);
		bounds.window_start =
		    (int64_t)row_idx -
		    boundary_start_collection.GetValue(0, wexpr->start_expr->IsScalar() ? 0 : local_row).GetValue<int64_t>();
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING: {
		D_ASSERT(boundary_start_collection.ColumnCount() > 0);
		bounds.window_start =
		    row_idx +
		    boundary_start_collection.GetValue(0, wexpr->start_expr->IsScalar() ? 0 : local_row).GetValue<int64_t>();
		break;
	}

	default:
		throw NotImplementedException("Unsupported boundary");
	}

	switch (wexpr->end) {
	case WindowBoundary::UNBOUNDED_PRECEDING:
		D_ASSERT(0); // disallowed
		break;
	case WindowBoundary::CURRENT_ROW_ROWS:
		bounds.window_end = row_idx + 1;
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		bounds.window_end = bounds.peer_end;
		break;
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		bounds.window_end = bounds.partition_end;
		break;
	case WindowBoundary::EXPR_PRECEDING:
		D_ASSERT(boundary_end_collection.ColumnCount() > 0);
		bounds.window_end =
		    (int64_t)row_idx -
		    boundary_end_collection.GetValue(0, wexpr->end_expr->IsScalar() ? 0 : local_row).GetValue<int64_t>() + 1;
		break;
	case WindowBoundary::EXPR_FOLLOWING:
		D_ASSERT(boundary_end_collection.ColumnCount() > 0);
		bounds.window_end =
		    row_idx +
		    boundary_end_collection.GetValue(0, wexpr->end_expr->IsScalar() ? 0 : local_row).GetValue<int64_t>() + 1;

		break;
	default:
		throw NotImplementedException("Unsupported boundary");
	}

	// clamp windows to partitions if they should exceed
	if (bounds.window_start < (int64_t)bounds.partition_start) {
		bounds.window_start = bounds.partition_start;
	}
	if ((idx_t)bounds.window_end > bounds.partition_end) {
		bounds.window_end = bounds.partition_end;
	}

	if (bounds.window_start < 0 || bounds.window_end < 0) {
		throw Exception("Failed to compute window boundaries");
	}
}

static void ComputeWindowExpression(BoundWindowExpression *wexpr, ChunkCollection &input, ChunkCollection &output,
                                    const BitArray<uint64_t> &partition_mask, const BitArray<uint64_t> &order_mask,
                                    const idx_t output_col, const Slice range) {

	// TODO we could evaluate those expressions in parallel

	// evaluate inner expressions of window functions, could be more complex
	ChunkCollection payload_collection;
	vector<Expression *> exprs;
	for (auto &child : wexpr->children) {
		exprs.push_back(child.get());
	}
	// TODO: child may be a scalar, don't need to materialize the whole collection then
	// We only need to evaluate local data (payloads, boundaries, etc.) on the [range) range,
	// so we need to know the delta between the computed row index and the operator index.
	const auto local_delta = MaterializeExpressions(exprs.data(), exprs.size(), input, payload_collection, range);

	ChunkCollection leadlag_offset_collection;
	ChunkCollection leadlag_default_collection;
	if (wexpr->type == ExpressionType::WINDOW_LEAD || wexpr->type == ExpressionType::WINDOW_LAG) {
		if (wexpr->offset_expr) {
			MaterializeExpression(wexpr->offset_expr.get(), input, leadlag_offset_collection, range,
			                      wexpr->offset_expr->IsScalar());
		}
		if (wexpr->default_expr) {
			MaterializeExpression(wexpr->default_expr.get(), input, leadlag_default_collection, range,
			                      wexpr->default_expr->IsScalar());
		}
	}

	// evaluate boundaries if present.
	ChunkCollection boundary_start_collection;
	if (wexpr->start_expr &&
	    (wexpr->start == WindowBoundary::EXPR_PRECEDING || wexpr->start == WindowBoundary::EXPR_FOLLOWING)) {
		MaterializeExpression(wexpr->start_expr.get(), input, boundary_start_collection, range,
		                      wexpr->start_expr->IsScalar());
	}
	ChunkCollection boundary_end_collection;
	if (wexpr->end_expr &&
	    (wexpr->end == WindowBoundary::EXPR_PRECEDING || wexpr->end == WindowBoundary::EXPR_FOLLOWING)) {
		MaterializeExpression(wexpr->end_expr.get(), input, boundary_end_collection, range,
		                      wexpr->end_expr->IsScalar());
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
	for (idx_t row_idx = range.first; row_idx < range.second; row_idx++) {
		const auto local_row = row_idx - local_delta;
		// special case, OVER (), aggregate over everything
		UpdateWindowBoundaries(wexpr, range, row_idx, local_row, boundary_start_collection, boundary_end_collection,
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

		Value res;

		// if no values are read for window, result is NULL
		if (bounds.window_start >= bounds.window_end) {
			output.SetValue(output_col, row_idx, res);
			continue;
		}

		switch (wexpr->type) {
		case ExpressionType::WINDOW_AGGREGATE: {
			res = segment_tree->Compute(bounds.window_start - local_delta, bounds.window_end - local_delta);
			break;
		}
		case ExpressionType::WINDOW_ROW_NUMBER: {
			res = Value::Numeric(wexpr->return_type, row_idx - bounds.partition_start + 1);
			break;
		}
		case ExpressionType::WINDOW_RANK_DENSE: {
			res = Value::Numeric(wexpr->return_type, dense_rank);
			break;
		}
		case ExpressionType::WINDOW_RANK: {
			res = Value::Numeric(wexpr->return_type, rank);
			break;
		}
		case ExpressionType::WINDOW_PERCENT_RANK: {
			int64_t denom = (int64_t)bounds.partition_end - bounds.partition_start - 1;
			double percent_rank = denom > 0 ? ((double)rank - 1) / denom : 0;
			res = Value(percent_rank);
			break;
		}
		case ExpressionType::WINDOW_CUME_DIST: {
			int64_t denom = (int64_t)bounds.partition_end - bounds.partition_start;
			double cume_dist = denom > 0 ? ((double)(bounds.peer_end - bounds.partition_start)) / denom : 0;
			res = Value(cume_dist);
			break;
		}
		case ExpressionType::WINDOW_NTILE: {
			if (payload_collection.ColumnCount() != 1) {
				throw Exception("NTILE needs a parameter");
			}
			auto n_param = payload_collection.GetValue(0, local_row).GetValue<int64_t>();
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
			res = Value::Numeric(wexpr->return_type, result_ntile);
			break;
		}
		case ExpressionType::WINDOW_LEAD:
		case ExpressionType::WINDOW_LAG: {
			Value def_val = Value(wexpr->return_type);
			idx_t offset = 1;
			if (wexpr->offset_expr) {
				offset = leadlag_offset_collection.GetValue(0, wexpr->offset_expr->IsScalar() ? 0 : local_row)
				             .GetValue<int64_t>();
			}
			if (wexpr->default_expr) {
				def_val = leadlag_default_collection.GetValue(0, wexpr->default_expr->IsScalar() ? 0 : local_row);
			}
			if (wexpr->type == ExpressionType::WINDOW_LEAD) {
				auto lead_idx = row_idx + offset;
				if (lead_idx < bounds.partition_end) {
					res = payload_collection.GetValue(0, lead_idx - local_delta);
				} else {
					res = def_val;
				}
			} else {
				int64_t lag_idx = (int64_t)row_idx - offset;
				if (lag_idx >= 0 && (idx_t)lag_idx >= bounds.partition_start) {
					res = payload_collection.GetValue(0, lag_idx - local_delta);
				} else {
					res = def_val;
				}
			}

			break;
		}
		case ExpressionType::WINDOW_FIRST_VALUE: {
			res = payload_collection.GetValue(0, bounds.window_start - local_delta);
			break;
		}
		case ExpressionType::WINDOW_LAST_VALUE: {
			res = payload_collection.GetValue(0, bounds.window_end - 1 - local_delta);
			break;
		}
		default:
			throw NotImplementedException("Window aggregate type %s", ExpressionTypeToString(wexpr->type));
		}

		output.SetValue(output_col, row_idx, res);
	}
}

class TaskCounter {
public:
	explicit TaskCounter(TaskScheduler &scheduler_p)
	    : scheduler(scheduler_p), token(scheduler_p.CreateProducer()), task_count(0), tasks_completed(0) {
	}

	void AddTask(unique_ptr<Task> task) {
		++task_count;
		scheduler.ScheduleTask(*token, move(task));
	}

	void FinishTask() const {
		++tasks_completed;
	}

	void Finish() {
		while (tasks_completed < task_count) {
			unique_ptr<Task> task;
			if (scheduler.GetTaskFromProducer(*token, task)) {
				task->Execute();
				task.reset();
			}
		}
	}

private:
	TaskScheduler &scheduler;
	unique_ptr<ProducerToken> token;
	size_t task_count;
	mutable std::atomic<size_t> tasks_completed;
};

class ComputeWindowExpressionTask : public Task {
public:
	using Mask = BitArray<uint64_t>;

	ComputeWindowExpressionTask(TaskCounter &parent_p, BoundWindowExpression *wexpr_p, ChunkCollection &input_p,
	                            ChunkCollection &output_p, const Mask &partition_mask_p, const Mask &order_mask_p,
	                            const idx_t output_col_p, const Slice range_p)
	    : parent(parent_p), wexpr(wexpr_p), input(input_p), output(output_p), partition_mask(partition_mask_p),
	      order_mask(order_mask_p), output_col(output_col_p), range(range_p) {
	}

	void Execute() override {
		ComputeWindowExpression(wexpr, input, output, partition_mask, order_mask, output_col, range);
		parent.FinishTask();
	}

private:
	TaskCounter &parent;
	BoundWindowExpression *wexpr;
	ChunkCollection &input;
	ChunkCollection &output;
	const Mask &partition_mask;
	const Mask &order_mask;
	const idx_t output_col;
	const Slice range;
};

void PhysicalWindow::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) {
	auto state = reinterpret_cast<PhysicalWindowOperatorState *>(state_p);

	auto &gstate = (WindowGlobalState &)*sink_state;

	ChunkCollection &big_data = gstate.chunks;
	ChunkCollection &window_results = gstate.window_results;

	if (big_data.Count() == 0) {
		return;
	}

	if (state->position >= big_data.Count()) {
		return;
	}

	// just return what was computed before, appending the result cols of the window expressions at the end
	auto &proj_ch = big_data.GetChunkForRow(state->position);
	auto &wind_ch = window_results.GetChunkForRow(state->position);

	idx_t out_idx = 0;
	D_ASSERT(proj_ch.size() == wind_ch.size());
	chunk.SetCardinality(proj_ch);
	for (idx_t col_idx = 0; col_idx < proj_ch.ColumnCount(); col_idx++) {
		chunk.data[out_idx++].Reference(proj_ch.data[col_idx]);
	}
	for (idx_t col_idx = 0; col_idx < wind_ch.ColumnCount(); col_idx++) {
		chunk.data[out_idx++].Reference(wind_ch.data[col_idx]);
	}
	state->position += STANDARD_VECTOR_SIZE;
}

unique_ptr<PhysicalOperatorState> PhysicalWindow::GetOperatorState() {
	return make_unique<PhysicalWindowOperatorState>(*this, children[0].get());
}

void PhysicalWindow::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_p,
                          DataChunk &input) {
	auto &lstate = (WindowLocalState &)lstate_p;
	lstate.chunks.Append(input);
}

void PhysicalWindow::Combine(ExecutionContext &context, GlobalOperatorState &gstate_p, LocalSinkState &lstate_p) {
	auto &gstate = (WindowGlobalState &)gstate_p;
	auto &lstate = (WindowLocalState &)lstate_p;
	lock_guard<mutex> glock(gstate.lock);
	gstate.chunks.Merge(lstate.chunks);
}

void PhysicalWindow::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> gstate_p) {
	this->sink_state = move(gstate_p);
	auto &gstate = (WindowGlobalState &)*this->sink_state;

	ChunkCollection &big_data = gstate.chunks;
	ChunkCollection &window_results = gstate.window_results;

	if (big_data.Count() == 0) {
		return;
	}

	vector<LogicalType> window_types;
	for (idx_t expr_idx = 0; expr_idx < select_list.size(); expr_idx++) {
		window_types.push_back(select_list[expr_idx]->return_type);
	}

	for (idx_t i = 0; i < big_data.ChunkCount(); i++) {
		DataChunk window_chunk;
		window_chunk.Initialize(window_types);
		window_chunk.SetCardinality(big_data.GetChunk(i).size());
		for (idx_t col_idx = 0; col_idx < window_chunk.ColumnCount(); col_idx++) {
			window_chunk.data[col_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(window_chunk.data[col_idx], true);
		}

		window_chunk.Verify();
		window_results.Append(window_chunk);
	}

	D_ASSERT(window_results.ColumnCount() == select_list.size());
	// we can have multiple window functions

	auto &scheduler = TaskScheduler::GetScheduler(context);

	// Process the window functions by sharing the partition/order definitions
	vector<idx_t> remaining(select_list.size());
	std::iota(remaining.begin(), remaining.end(), 0);
	while (!remaining.empty()) {
		// Find all functions that share the partitioning of the first remaining expression
		const auto over_idx = remaining[0];
		D_ASSERT(select_list[over_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto over_expr = reinterpret_cast<BoundWindowExpression *>(select_list[over_idx].get());

		vector<idx_t> matching;
		vector<idx_t> unprocessed;
		for (const auto &expr_idx : remaining) {
			D_ASSERT(select_list[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
			auto wexpr = reinterpret_cast<BoundWindowExpression *>(select_list[expr_idx].get());
			if (CompatibleSorts(over_expr, wexpr)) {
				matching.emplace_back(expr_idx);
			} else {
				unprocessed.emplace_back(expr_idx);
			}
		}
		remaining.swap(unprocessed);

		// sort by partition and order clause in window def
		ChunkCollection over_collection;
		const auto sort_col_count = over_expr->partitions.size() + over_expr->orders.size();
		counts_t counts(1, big_data.Count());
		if (sort_col_count > 0) {
			counts = SortCollectionForWindow(over_expr, big_data, window_results, over_collection);
		}

		//	Set bits for the start of each partition
		BitArray<uint64_t> partition_mask(over_collection.Count());
		if (partition_mask.Count() > 0) {
			//	Pre-populate with the hash boundaries
			const auto ranges = SlicesFromCounts(counts, 1);
			for (const auto &range : ranges) {
				partition_mask[range.first] = true;
			}
		}
		for (idx_t c = 0; c < over_expr->partitions.size(); ++c) {
			MaskColumn(partition_mask, over_collection, c);
		}

		//	Set bits for the start of each peer group.
		//	Partitions also break peer groups, so start with the partition bits.
		BitArray<uint64_t> order_mask(partition_mask);
		for (idx_t c = over_expr->partitions.size(); c < sort_col_count; ++c) {
			MaskColumn(order_mask, over_collection, c);
		}

		//	Go through the counts and convert them to non-empty partition ranges.
		const auto ranges = SlicesFromCounts(counts);

		//	Compute the functions with matching sorts in parallel
		TaskCounter tasks(scheduler);
		for (const auto &expr_idx : matching) {
			D_ASSERT(select_list[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
			auto wexpr = reinterpret_cast<BoundWindowExpression *>(select_list[expr_idx].get());
			// reuse partition and order clause in window def
			for (const auto &range : ranges) {
				auto task = make_unique<ComputeWindowExpressionTask>(tasks, wexpr, big_data, window_results,
				                                                     partition_mask, order_mask, expr_idx, range);
				tasks.AddTask(move(task));
			}
		}
		tasks.Finish();
	}
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
