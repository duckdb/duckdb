#include "duckdb/execution/window_executor.hpp"

#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/subtract.hpp"

#include "duckdb/common/array.hpp"

namespace duckdb {

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
		for (++shift; shift-- > 0 && l < r; --r) {
			// l < r ensures n == 1 if result is supposed to be NULL because of EXCLUDE
			if (mask.RowIsValid(block, shift) && --n == 0) {
				return MaxValue(l, r - 1);
			}
		}
	}

	//	Didn't find a start so return the start of the range
	return l;
}

template <typename T>
static T GetCell(const DataChunk &chunk, idx_t column, idx_t index) {
	D_ASSERT(chunk.ColumnCount() > column);
	auto &source = chunk.data[column];
	const auto data = FlatVector::GetData<T>(source);
	return data[index];
}

static bool CellIsNull(const DataChunk &chunk, idx_t column, idx_t index) {
	D_ASSERT(chunk.ColumnCount() > column);
	auto &source = chunk.data[column];
	return FlatVector::IsNull(source, index);
}

static void CopyCell(const DataChunk &chunk, idx_t column, idx_t index, Vector &target, idx_t target_offset) {
	D_ASSERT(chunk.ColumnCount() > column);
	auto &source = chunk.data[column];
	VectorOperations::Copy(source, target, index + 1, index, target_offset);
}

//===--------------------------------------------------------------------===//
// WindowColumnIterator
//===--------------------------------------------------------------------===//
template <typename T>
struct WindowColumnIterator {
	using iterator = WindowColumnIterator<T>;
	using iterator_category = std::random_access_iterator_tag;
	using difference_type = std::ptrdiff_t;
	using value_type = T;
	using reference = T;
	using pointer = idx_t;

	explicit WindowColumnIterator(const WindowInputColumn &coll_p, pointer pos_p = 0) : coll(&coll_p), pos(pos_p) {
	}

	//	Forward iterator
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

	//	Bidirectional iterator
	inline iterator &operator--() {
		--pos;
		return *this;
	}
	inline iterator operator--(int) {
		auto result = *this;
		--(*this);
		return result;
	}

	//	Random Access
	inline iterator &operator+=(difference_type n) {
		pos += UnsafeNumericCast<pointer>(n);
		return *this;
	}
	inline iterator &operator-=(difference_type n) {
		pos -= UnsafeNumericCast<pointer>(n);
		return *this;
	}

	inline reference operator[](difference_type m) const {
		return coll->GetCell<T>(pos + m);
	}

	friend inline iterator &operator+(const iterator &a, difference_type n) {
		return iterator(a.coll, a.pos + n);
	}

	friend inline iterator operator-(const iterator &a, difference_type n) {
		return iterator(a.coll, a.pos - n);
	}

	friend inline iterator operator+(difference_type n, const iterator &a) {
		return a + n;
	}
	friend inline difference_type operator-(const iterator &a, const iterator &b) {
		return difference_type(a.pos - b.pos);
	}

	friend inline bool operator==(const iterator &a, const iterator &b) {
		return a.pos == b.pos;
	}
	friend inline bool operator!=(const iterator &a, const iterator &b) {
		return a.pos != b.pos;
	}
	friend inline bool operator<(const iterator &a, const iterator &b) {
		return a.pos < b.pos;
	}
	friend inline bool operator<=(const iterator &a, const iterator &b) {
		return a.pos <= b.pos;
	}
	friend inline bool operator>(const iterator &a, const iterator &b) {
		return a.pos > b.pos;
	}
	friend inline bool operator>=(const iterator &a, const iterator &b) {
		return a.pos >= b.pos;
	}

private:
	optional_ptr<const WindowInputColumn> coll;
	pointer pos;
};

template <typename T, typename OP>
struct OperationCompare : public std::function<bool(T, T)> {
	inline bool operator()(const T &lhs, const T &val) const {
		return OP::template Operation<T>(lhs, val);
	}
};

template <typename T, typename OP, bool FROM>
static idx_t FindTypedRangeBound(const WindowInputColumn &over, const idx_t order_begin, const idx_t order_end,
                                 const WindowBoundary range, WindowInputExpression &boundary, const idx_t chunk_idx,
                                 const FrameBounds &prev) {
	D_ASSERT(!boundary.CellIsNull(chunk_idx));
	const auto val = boundary.GetCell<T>(chunk_idx);

	OperationCompare<T, OP> comp;

	// Check that the value we are searching for is in range.
	if (range == WindowBoundary::EXPR_PRECEDING_RANGE) {
		//	Preceding but value past the end
		const auto cur_val = over.GetCell<T>(order_end);
		if (comp(cur_val, val)) {
			throw OutOfRangeException("Invalid RANGE PRECEDING value");
		}
	} else {
		//	Following but value before beginning
		D_ASSERT(range == WindowBoundary::EXPR_FOLLOWING_RANGE);
		const auto cur_val = over.GetCell<T>(order_begin);
		if (comp(val, cur_val)) {
			throw OutOfRangeException("Invalid RANGE FOLLOWING value");
		}
	}

	//	Try to reuse the previous bounds to restrict the search.
	//	This is only valid if the previous bounds were non-empty
	//	Only inject the comparisons if the previous bounds are a strict subset.
	WindowColumnIterator<T> begin(over, order_begin);
	WindowColumnIterator<T> end(over, order_end);
	if (prev.start < prev.end) {
		if (order_begin < prev.start && prev.start < order_end) {
			const auto first = over.GetCell<T>(prev.start);
			if (!comp(val, first)) {
				//	prev.first <= val, so we can start further forward
				begin += UnsafeNumericCast<int64_t>(prev.start - order_begin);
			}
		}
		if (order_begin < prev.end && prev.end < order_end) {
			const auto second = over.GetCell<T>(prev.end - 1);
			if (!comp(second, val)) {
				//	val <= prev.second, so we can end further back
				// (prev.second is the largest peer)
				end -= UnsafeNumericCast<int64_t>(order_end - prev.end - 1);
			}
		}
	}

	if (FROM) {
		return idx_t(std::lower_bound(begin, end, val, comp));
	} else {
		return idx_t(std::upper_bound(begin, end, val, comp));
	}
}

template <typename OP, bool FROM>
static idx_t FindRangeBound(const WindowInputColumn &over, const idx_t order_begin, const idx_t order_end,
                            const WindowBoundary range, WindowInputExpression &boundary, const idx_t chunk_idx,
                            const FrameBounds &prev) {
	D_ASSERT(boundary.chunk.ColumnCount() == 1);
	D_ASSERT(boundary.chunk.data[0].GetType().InternalType() == over.input_expr.ptype);

	switch (over.input_expr.ptype) {
	case PhysicalType::INT8:
		return FindTypedRangeBound<int8_t, OP, FROM>(over, order_begin, order_end, range, boundary, chunk_idx, prev);
	case PhysicalType::INT16:
		return FindTypedRangeBound<int16_t, OP, FROM>(over, order_begin, order_end, range, boundary, chunk_idx, prev);
	case PhysicalType::INT32:
		return FindTypedRangeBound<int32_t, OP, FROM>(over, order_begin, order_end, range, boundary, chunk_idx, prev);
	case PhysicalType::INT64:
		return FindTypedRangeBound<int64_t, OP, FROM>(over, order_begin, order_end, range, boundary, chunk_idx, prev);
	case PhysicalType::UINT8:
		return FindTypedRangeBound<uint8_t, OP, FROM>(over, order_begin, order_end, range, boundary, chunk_idx, prev);
	case PhysicalType::UINT16:
		return FindTypedRangeBound<uint16_t, OP, FROM>(over, order_begin, order_end, range, boundary, chunk_idx, prev);
	case PhysicalType::UINT32:
		return FindTypedRangeBound<uint32_t, OP, FROM>(over, order_begin, order_end, range, boundary, chunk_idx, prev);
	case PhysicalType::UINT64:
		return FindTypedRangeBound<uint64_t, OP, FROM>(over, order_begin, order_end, range, boundary, chunk_idx, prev);
	case PhysicalType::INT128:
		return FindTypedRangeBound<hugeint_t, OP, FROM>(over, order_begin, order_end, range, boundary, chunk_idx, prev);
	case PhysicalType::UINT128:
		return FindTypedRangeBound<uhugeint_t, OP, FROM>(over, order_begin, order_end, range, boundary, chunk_idx,
		                                                 prev);
	case PhysicalType::FLOAT:
		return FindTypedRangeBound<float, OP, FROM>(over, order_begin, order_end, range, boundary, chunk_idx, prev);
	case PhysicalType::DOUBLE:
		return FindTypedRangeBound<double, OP, FROM>(over, order_begin, order_end, range, boundary, chunk_idx, prev);
	case PhysicalType::INTERVAL:
		return FindTypedRangeBound<interval_t, OP, FROM>(over, order_begin, order_end, range, boundary, chunk_idx,
		                                                 prev);
	default:
		throw InternalException("Unsupported column type for RANGE");
	}
}

template <bool FROM>
static idx_t FindOrderedRangeBound(const WindowInputColumn &over, const OrderType range_sense, const idx_t order_begin,
                                   const idx_t order_end, const WindowBoundary range, WindowInputExpression &boundary,
                                   const idx_t chunk_idx, const FrameBounds &prev) {
	switch (range_sense) {
	case OrderType::ASCENDING:
		return FindRangeBound<LessThan, FROM>(over, order_begin, order_end, range, boundary, chunk_idx, prev);
	case OrderType::DESCENDING:
		return FindRangeBound<GreaterThan, FROM>(over, order_begin, order_end, range, boundary, chunk_idx, prev);
	default:
		throw InternalException("Unsupported ORDER BY sense for RANGE");
	}
}

struct WindowBoundariesState {
	static inline bool IsScalar(const unique_ptr<Expression> &expr) {
		return expr ? expr->IsScalar() : true;
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

	static inline bool ExpressionNeedsPeer(const ExpressionType &type) {
		switch (type) {
		case ExpressionType::WINDOW_RANK:
		case ExpressionType::WINDOW_RANK_DENSE:
		case ExpressionType::WINDOW_PERCENT_RANK:
		case ExpressionType::WINDOW_CUME_DIST:
			return true;
		default:
			return false;
		}
	}

	WindowBoundariesState(const BoundWindowExpression &wexpr, const idx_t input_size);

	void Update(const idx_t row_idx, const WindowInputColumn &range_collection, const idx_t chunk_idx,
	            WindowInputExpression &boundary_start, WindowInputExpression &boundary_end,
	            const ValidityMask &partition_mask, const ValidityMask &order_mask);

	void Bounds(DataChunk &bounds, idx_t row_idx, const WindowInputColumn &range, const idx_t count,
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

	idx_t next_pos = 0;
	idx_t partition_start = 0;
	idx_t partition_end = 0;
	idx_t peer_start = 0;
	idx_t peer_end = 0;
	idx_t valid_start = 0;
	idx_t valid_end = 0;
	idx_t window_start = NumericLimits<idx_t>::Maximum();
	idx_t window_end = NumericLimits<idx_t>::Maximum();
	FrameBounds prev;
};

//===--------------------------------------------------------------------===//
// WindowBoundariesState
//===--------------------------------------------------------------------===//
void WindowBoundariesState::Update(const idx_t row_idx, const WindowInputColumn &range_collection,
                                   const idx_t chunk_idx, WindowInputExpression &boundary_start,
                                   WindowInputExpression &boundary_end, const ValidityMask &partition_mask,
                                   const ValidityMask &order_mask) {

	if (partition_count + order_count > 0) {

		// determine partition and peer group boundaries to ultimately figure out window size
		const auto is_same_partition = !partition_mask.RowIsValidUnsafe(row_idx);
		const auto is_peer = !order_mask.RowIsValidUnsafe(row_idx);
		const auto is_jump = (next_pos != row_idx);

		// when the partition changes, recompute the boundaries
		if (!is_same_partition || is_jump) {
			if (is_jump) {
				idx_t n = 1;
				partition_start = FindPrevStart(partition_mask, 0, row_idx + 1, n);
				n = 1;
				peer_start = FindPrevStart(order_mask, 0, row_idx + 1, n);
			} else {
				partition_start = row_idx;
				peer_start = row_idx;
			}

			// find end of partition
			partition_end = input_size;
			if (partition_count) {
				idx_t n = 1;
				partition_end = FindNextStart(partition_mask, partition_start + 1, input_size, n);
			}

			// Find valid ordering values for the new partition
			// so we can exclude NULLs from RANGE expression computations
			valid_start = partition_start;
			valid_end = partition_end;

			if ((valid_start < valid_end) && has_preceding_range) {
				// Exclude any leading NULLs
				if (range_collection.CellIsNull(valid_start)) {
					idx_t n = 1;
					valid_start = FindNextStart(order_mask, valid_start + 1, valid_end, n);
				}
			}

			if ((valid_start < valid_end) && has_following_range) {
				// Exclude any trailing NULLs
				if (range_collection.CellIsNull(valid_end - 1)) {
					idx_t n = 1;
					valid_end = FindPrevStart(order_mask, valid_start, valid_end, n);
				}

				//	Reset range hints
				prev.start = valid_start;
				prev.end = valid_end;
			}
		} else if (!is_peer) {
			peer_start = row_idx;
		}

		if (needs_peer) {
			peer_end = partition_end;
			if (order_count) {
				idx_t n = 1;
				peer_end = FindNextStart(order_mask, peer_start + 1, partition_end, n);
			}
		}

	} else {
		//	OVER()
		partition_end = input_size;
		peer_end = partition_end;
	}
	next_pos = row_idx + 1;

	// determine window boundaries depending on the type of expression
	switch (start_boundary) {
	case WindowBoundary::UNBOUNDED_PRECEDING:
		window_start = partition_start;
		break;
	case WindowBoundary::CURRENT_ROW_ROWS:
		window_start = row_idx;
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		window_start = peer_start;
		break;
	case WindowBoundary::EXPR_PRECEDING_ROWS: {
		int64_t computed_start;
		if (!TrySubtractOperator::Operation(static_cast<int64_t>(row_idx), boundary_start.GetCell<int64_t>(chunk_idx),
		                                    computed_start)) {
			window_start = partition_start;
		} else {
			window_start = UnsafeNumericCast<idx_t>(MaxValue<int64_t>(computed_start, 0));
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_ROWS: {
		int64_t computed_start;
		if (!TryAddOperator::Operation(static_cast<int64_t>(row_idx), boundary_start.GetCell<int64_t>(chunk_idx),
		                               computed_start)) {
			window_start = partition_start;
		} else {
			window_start = UnsafeNumericCast<idx_t>(MaxValue<int64_t>(computed_start, 0));
		}
		break;
	}
	case WindowBoundary::EXPR_PRECEDING_RANGE: {
		if (boundary_start.CellIsNull(chunk_idx)) {
			window_start = peer_start;
		} else {
			prev.start = FindOrderedRangeBound<true>(range_collection, range_sense, valid_start, row_idx,
			                                         start_boundary, boundary_start, chunk_idx, prev);
			window_start = prev.start;
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_RANGE: {
		if (boundary_start.CellIsNull(chunk_idx)) {
			window_start = peer_start;
		} else {
			prev.start = FindOrderedRangeBound<true>(range_collection, range_sense, row_idx, valid_end, start_boundary,
			                                         boundary_start, chunk_idx, prev);
			window_start = prev.start;
		}
		break;
	}
	default:
		throw InternalException("Unsupported window start boundary");
	}

	switch (end_boundary) {
	case WindowBoundary::CURRENT_ROW_ROWS:
		window_end = row_idx + 1;
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		window_end = peer_end;
		break;
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		window_end = partition_end;
		break;
	case WindowBoundary::EXPR_PRECEDING_ROWS: {
		int64_t computed_start;
		if (!TrySubtractOperator::Operation(int64_t(row_idx + 1), boundary_end.GetCell<int64_t>(chunk_idx),
		                                    computed_start)) {
			window_end = partition_end;
		} else {
			window_end = UnsafeNumericCast<idx_t>(MaxValue<int64_t>(computed_start, 0));
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_ROWS: {
		int64_t computed_start;
		if (!TryAddOperator::Operation(int64_t(row_idx + 1), boundary_end.GetCell<int64_t>(chunk_idx),
		                               computed_start)) {
			window_end = partition_end;
		} else {
			window_end = UnsafeNumericCast<idx_t>(MaxValue<int64_t>(computed_start, 0));
		}
		break;
	}
	case WindowBoundary::EXPR_PRECEDING_RANGE: {
		if (boundary_end.CellIsNull(chunk_idx)) {
			window_end = peer_end;
		} else {
			prev.end = FindOrderedRangeBound<false>(range_collection, range_sense, valid_start, row_idx, end_boundary,
			                                        boundary_end, chunk_idx, prev);
			window_end = prev.end;
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_RANGE: {
		if (boundary_end.CellIsNull(chunk_idx)) {
			window_end = peer_end;
		} else {
			prev.end = FindOrderedRangeBound<false>(range_collection, range_sense, row_idx, valid_end, end_boundary,
			                                        boundary_end, chunk_idx, prev);
			window_end = prev.end;
		}
		break;
	}
	default:
		throw InternalException("Unsupported window end boundary");
	}

	// clamp windows to partitions if they should exceed
	if (window_start < partition_start) {
		window_start = partition_start;
	}
	if (window_start > partition_end) {
		window_start = partition_end;
	}
	if (window_end < partition_start) {
		window_end = partition_start;
	}
	if (window_end > partition_end) {
		window_end = partition_end;
	}
}

static bool HasPrecedingRange(const BoundWindowExpression &wexpr) {
	return (wexpr.start == WindowBoundary::EXPR_PRECEDING_RANGE || wexpr.end == WindowBoundary::EXPR_PRECEDING_RANGE);
}

static bool HasFollowingRange(const BoundWindowExpression &wexpr) {
	return (wexpr.start == WindowBoundary::EXPR_FOLLOWING_RANGE || wexpr.end == WindowBoundary::EXPR_FOLLOWING_RANGE);
}

WindowBoundariesState::WindowBoundariesState(const BoundWindowExpression &wexpr, const idx_t input_size)
    : type(wexpr.type), input_size(input_size), start_boundary(wexpr.start), end_boundary(wexpr.end),
      partition_count(wexpr.partitions.size()), order_count(wexpr.orders.size()),
      range_sense(wexpr.orders.empty() ? OrderType::INVALID : wexpr.orders[0].type),
      has_preceding_range(HasPrecedingRange(wexpr)), has_following_range(HasFollowingRange(wexpr)),
      // if we have EXCLUDE GROUP / TIES, we also need peer boundaries
      needs_peer(BoundaryNeedsPeer(wexpr.end) || ExpressionNeedsPeer(wexpr.type) ||
                 wexpr.exclude_clause >= WindowExcludeMode::GROUP) {
}

void WindowBoundariesState::Bounds(DataChunk &bounds, idx_t row_idx, const WindowInputColumn &range, const idx_t count,
                                   WindowInputExpression &boundary_start, WindowInputExpression &boundary_end,
                                   const ValidityMask &partition_mask, const ValidityMask &order_mask) {
	bounds.Reset();
	D_ASSERT(bounds.ColumnCount() == 6);
	auto partition_begin_data = FlatVector::GetData<idx_t>(bounds.data[PARTITION_BEGIN]);
	auto partition_end_data = FlatVector::GetData<idx_t>(bounds.data[PARTITION_END]);
	auto peer_begin_data = FlatVector::GetData<idx_t>(bounds.data[PEER_BEGIN]);
	auto peer_end_data = FlatVector::GetData<idx_t>(bounds.data[PEER_END]);
	auto window_begin_data = FlatVector::GetData<int64_t>(bounds.data[WINDOW_BEGIN]);
	auto window_end_data = FlatVector::GetData<int64_t>(bounds.data[WINDOW_END]);
	for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
		Update(row_idx, range, chunk_idx, boundary_start, boundary_end, partition_mask, order_mask);
		*partition_begin_data++ = partition_start;
		*partition_end_data++ = partition_end;
		if (needs_peer) {
			*peer_begin_data++ = peer_start;
			*peer_end_data++ = peer_end;
		}
		*window_begin_data++ = UnsafeNumericCast<int64_t>(window_start);
		*window_end_data++ = UnsafeNumericCast<int64_t>(window_end);
	}
	bounds.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// WindowExecutorBoundsState
//===--------------------------------------------------------------------===//
class WindowExecutorBoundsState : public WindowExecutorLocalState {
public:
	explicit WindowExecutorBoundsState(const WindowExecutorGlobalState &gstate);
	~WindowExecutorBoundsState() override {
	}

	virtual void UpdateBounds(idx_t row_idx, DataChunk &input_chunk, const WindowInputColumn &range);

	// Frame management
	const ValidityMask &partition_mask;
	const ValidityMask &order_mask;
	DataChunk bounds;
	WindowBoundariesState state;

	// evaluate boundaries if present. Parser has checked boundary types.
	WindowInputExpression boundary_start;
	WindowInputExpression boundary_end;
};

WindowExecutorBoundsState::WindowExecutorBoundsState(const WindowExecutorGlobalState &gstate)
    : WindowExecutorLocalState(gstate), partition_mask(gstate.partition_mask), order_mask(gstate.order_mask),
      state(gstate.executor.wexpr, gstate.payload_count),
      boundary_start(gstate.executor.wexpr.start_expr.get(), gstate.executor.context),
      boundary_end(gstate.executor.wexpr.end_expr.get(), gstate.executor.context) {
	vector<LogicalType> bounds_types(6, LogicalType(LogicalTypeId::UBIGINT));
	bounds.Initialize(Allocator::Get(gstate.executor.context), bounds_types);
}

void WindowExecutorBoundsState::UpdateBounds(idx_t row_idx, DataChunk &input_chunk, const WindowInputColumn &range) {
	// Evaluate the row-level arguments
	boundary_start.Execute(input_chunk);
	boundary_end.Execute(input_chunk);

	const auto count = input_chunk.size();
	bounds.Reset();
	state.Bounds(bounds, row_idx, range, count, boundary_start, boundary_end, partition_mask, order_mask);
}

//===--------------------------------------------------------------------===//
// ExclusionFilter
//===--------------------------------------------------------------------===//

//! Handles window exclusion by piggybacking on the filtering logic.
//! (needed for first_value, last_value, nth_value)
class ExclusionFilter {
public:
	ExclusionFilter(const WindowExcludeMode exclude_mode_p, idx_t total_count, const ValidityMask &src)
	    : mode(exclude_mode_p), mask_src(src) {
		mask.Initialize(total_count);

		// copy the data from mask_src
		FetchFromSource(0, total_count);
	}

	//! Copy the entries from mask_src to mask, in the index range [begin, end)
	void FetchFromSource(idx_t begin, idx_t end);
	//! Apply the current exclusion to the validity mask
	//! (offset is the current row's index within the chunk)
	void ApplyExclusion(DataChunk &bounds, idx_t row_idx, idx_t offset);
	//! Reset the validity mask to match mask_src
	//! (offset is the current row's index within the chunk)
	void ResetMask(idx_t row_idx, idx_t offset);

	//! The current peer group's begin
	idx_t curr_peer_begin;
	//! The current peer group's end
	idx_t curr_peer_end;
	//! The window exclusion mode
	WindowExcludeMode mode;
	//! The validity mask representing the exclusion
	ValidityMask mask;
	//! The validity mask upon which mask is based
	const ValidityMask &mask_src;
	//! A validity mask consisting of only one entries (needed if no ignore_nulls mask is supplied)
	ValidityMask all_ones_mask;
};

void ExclusionFilter::FetchFromSource(idx_t begin, idx_t end) {
	idx_t begin_entry_idx;
	idx_t end_entry_idx;
	idx_t idx_in_entry;
	mask.GetEntryIndex(begin, begin_entry_idx, idx_in_entry);
	mask.GetEntryIndex(end - 1, end_entry_idx, idx_in_entry);
	auto dst = mask.GetData() + begin_entry_idx;
	for (idx_t entry_idx = begin_entry_idx; entry_idx <= end_entry_idx; ++entry_idx) {
		*dst++ = mask_src.GetValidityEntry(entry_idx);
	}
}

void ExclusionFilter::ApplyExclusion(DataChunk &bounds, idx_t row_idx, idx_t offset) {
	// flip the bits in mask according to the window exclusion mode
	switch (mode) {
	case WindowExcludeMode::CURRENT_ROW:
		mask.SetInvalid(row_idx);
		break;
	case WindowExcludeMode::TIES:
	case WindowExcludeMode::GROUP: {
		if (curr_peer_end == row_idx || offset == 0) {
			// new peer group or input chunk: set entire peer group to invalid
			auto peer_begin = FlatVector::GetData<const idx_t>(bounds.data[PEER_BEGIN]);
			auto peer_end = FlatVector::GetData<const idx_t>(bounds.data[PEER_END]);
			curr_peer_begin = peer_begin[offset];
			curr_peer_end = peer_end[offset];
			for (idx_t i = curr_peer_begin; i < curr_peer_end; i++) {
				mask.SetInvalid(i);
			}
		}
		if (mode == WindowExcludeMode::TIES) {
			mask.Set(row_idx, mask_src.RowIsValid(row_idx));
		}
		break;
	}
	default:
		break;
	}
}

void ExclusionFilter::ResetMask(idx_t row_idx, idx_t offset) {
	// flip the bits that were modified in ApplyExclusion back
	switch (mode) {
	case WindowExcludeMode::CURRENT_ROW:
		mask.Set(row_idx, mask_src.RowIsValid(row_idx));
		break;
	case WindowExcludeMode::TIES:
		mask.SetInvalid(row_idx);
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case WindowExcludeMode::GROUP:
		if (curr_peer_end == row_idx + 1) {
			// if we've reached the peer group's end, restore the entire peer group
			FetchFromSource(curr_peer_begin, curr_peer_end);
		}
		break;
	default:
		break;
	}
}

//===--------------------------------------------------------------------===//
// WindowValueLocalState
//===--------------------------------------------------------------------===//

//! A class representing the state of the first_value, last_value and nth_value functions
class WindowValueLocalState : public WindowExecutorBoundsState {
public:
	WindowValueLocalState(const WindowExecutorGlobalState &gstate, const ValidityMask &ignore_nulls)
	    : WindowExecutorBoundsState(gstate)

	{
		if (gstate.executor.wexpr.exclude_clause == WindowExcludeMode::NO_OTHER) {
			exclusion_filter = nullptr;
			ignore_nulls_exclude = &ignore_nulls;
		} else {
			// create the exclusion filter based on ignore_nulls
			exclusion_filter =
			    make_uniq<ExclusionFilter>(gstate.executor.wexpr.exclude_clause, gstate.payload_count, ignore_nulls);
			ignore_nulls_exclude = &exclusion_filter->mask;
		}
	}

	//! The exclusion filter handling exclusion
	unique_ptr<ExclusionFilter> exclusion_filter;
	//! The validity mask that combines both the NULLs and exclusion information
	const ValidityMask *ignore_nulls_exclude;
};

//===--------------------------------------------------------------------===//
// WindowExecutor
//===--------------------------------------------------------------------===//
static void PrepareInputExpressions(const vector<unique_ptr<Expression>> &exprs, ExpressionExecutor &executor,
                                    DataChunk &chunk) {
	if (exprs.empty()) {
		return;
	}

	vector<LogicalType> types;
	for (idx_t expr_idx = 0; expr_idx < exprs.size(); ++expr_idx) {
		types.push_back(exprs[expr_idx]->return_type);
		executor.AddExpression(*exprs[expr_idx]);
	}

	if (!types.empty()) {
		auto &allocator = executor.GetAllocator();
		chunk.Initialize(allocator, types);
	}
}

WindowExecutor::WindowExecutor(BoundWindowExpression &wexpr, ClientContext &context) : wexpr(wexpr), context(context) {
}

WindowExecutorGlobalState::WindowExecutorGlobalState(const WindowExecutor &executor, const idx_t payload_count,
                                                     const ValidityMask &partition_mask, const ValidityMask &order_mask)
    : executor(executor), payload_count(payload_count), partition_mask(partition_mask), order_mask(order_mask),
      payload_executor(executor.context), range((HasPrecedingRange(executor.wexpr) || HasFollowingRange(executor.wexpr))
                                                    ? executor.wexpr.orders[0].expression.get()
                                                    : nullptr,
                                                executor.context, payload_count) {

	// TODO: child may be a scalar, don't need to materialize the whole collection then

	// evaluate inner expressions of window functions, could be more complex
	PrepareInputExpressions(executor.wexpr.children, payload_executor, payload_chunk);
}

unique_ptr<WindowExecutorGlobalState> WindowExecutor::GetGlobalState(const idx_t payload_count,
                                                                     const ValidityMask &partition_mask,
                                                                     const ValidityMask &order_mask) const {
	return make_uniq<WindowExecutorGlobalState>(*this, payload_count, partition_mask, order_mask);
}

unique_ptr<WindowExecutorLocalState> WindowExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowExecutorBoundsState>(gstate);
}

//===--------------------------------------------------------------------===//
// WindowAggregateExecutor
//===--------------------------------------------------------------------===//
class WindowAggregateExecutorGlobalState : public WindowExecutorGlobalState {
public:
	bool IsConstantAggregate();
	bool IsCustomAggregate();
	bool IsDistinctAggregate();

	WindowAggregateExecutorGlobalState(const WindowAggregateExecutor &executor, const idx_t payload_count,
	                                   const ValidityMask &partition_mask, const ValidityMask &order_mask);

	ExpressionExecutor filter_executor;
	SelectionVector filter_sel;

	// aggregate computation algorithm
	unique_ptr<WindowAggregator> aggregator;
	// aggregate global state
	unique_ptr<WindowAggregatorState> gsink;
};

bool WindowAggregateExecutorGlobalState::IsConstantAggregate() {
	const auto &wexpr = executor.wexpr;

	if (!wexpr.aggregate) {
		return false;
	}
	// window exclusion cannot be handled by constant aggregates
	if (wexpr.exclude_clause != WindowExcludeMode::NO_OTHER) {
		return false;
	}

	//	COUNT(*) is already handled efficiently by segment trees.
	if (wexpr.children.empty()) {
		return false;
	}

	/*
	    The default framing option is RANGE UNBOUNDED PRECEDING, which
	    is the same as RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
	    ROW; it sets the frame to be all rows from the partition start
	    up through the current row's last peer (a row that the window's
	    ORDER BY clause considers equivalent to the current row; all
	    rows are peers if there is no ORDER BY). In general, UNBOUNDED
	    PRECEDING means that the frame starts with the first row of the
	    partition, and similarly UNBOUNDED FOLLOWING means that the
	    frame ends with the last row of the partition, regardless of
	    RANGE, ROWS or GROUPS mode. In ROWS mode, CURRENT ROW means that
	    the frame starts or ends with the current row; but in RANGE or
	    GROUPS mode it means that the frame starts or ends with the
	    current row's first or last peer in the ORDER BY ordering. The
	    offset PRECEDING and offset FOLLOWING options vary in meaning
	    depending on the frame mode.
	*/
	switch (wexpr.start) {
	case WindowBoundary::UNBOUNDED_PRECEDING:
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		if (!wexpr.orders.empty()) {
			return false;
		}
		break;
	default:
		return false;
	}

	switch (wexpr.end) {
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		if (!wexpr.orders.empty()) {
			return false;
		}
		break;
	default:
		return false;
	}

	return true;
}

bool WindowAggregateExecutorGlobalState::IsDistinctAggregate() {
	const auto &wexpr = executor.wexpr;

	if (!wexpr.aggregate) {
		return false;
	}

	return wexpr.distinct;
}

bool WindowAggregateExecutorGlobalState::IsCustomAggregate() {
	const auto &wexpr = executor.wexpr;
	const auto &mode = reinterpret_cast<const WindowAggregateExecutor &>(executor).mode;

	if (!wexpr.aggregate) {
		return false;
	}

	if (!AggregateObject(wexpr).function.window) {
		return false;
	}

	return (mode < WindowAggregationMode::COMBINE);
}

void WindowExecutor::Evaluate(idx_t row_idx, DataChunk &input_chunk, Vector &result, WindowExecutorLocalState &lstate,
                              WindowExecutorGlobalState &gstate) const {
	auto &lbstate = lstate.Cast<WindowExecutorBoundsState>();
	lbstate.UpdateBounds(row_idx, input_chunk, gstate.range);

	const auto count = input_chunk.size();
	EvaluateInternal(gstate, lstate, result, count, row_idx);

	result.Verify(count);
}

WindowAggregateExecutor::WindowAggregateExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                 WindowAggregationMode mode)
    : WindowExecutor(wexpr, context), mode(mode) {
}

WindowAggregateExecutorGlobalState::WindowAggregateExecutorGlobalState(const WindowAggregateExecutor &executor,
                                                                       const idx_t group_count,
                                                                       const ValidityMask &partition_mask,
                                                                       const ValidityMask &order_mask)
    : WindowExecutorGlobalState(executor, group_count, partition_mask, order_mask), filter_executor(executor.context) {
	auto &wexpr = executor.wexpr;
	auto &context = executor.context;
	auto return_type = wexpr.return_type;
	auto arg_types = payload_chunk.GetTypes();
	const auto &mode = reinterpret_cast<const WindowAggregateExecutor &>(executor).mode;

	// Force naive for SEPARATE mode or for (currently!) unsupported functionality
	const auto force_naive =
	    !ClientConfig::GetConfig(context).enable_optimizer || mode == WindowAggregationMode::SEPARATE;
	AggregateObject aggr(wexpr);
	if (force_naive || (wexpr.distinct && wexpr.exclude_clause != WindowExcludeMode::NO_OTHER)) {
		aggregator = make_uniq<WindowNaiveAggregator>(aggr, arg_types, return_type, wexpr.exclude_clause);
	} else if (IsDistinctAggregate()) {
		// build a merge sort tree
		// see https://dl.acm.org/doi/pdf/10.1145/3514221.3526184
		aggregator = make_uniq<WindowDistinctAggregator>(aggr, arg_types, return_type, wexpr.exclude_clause, context);
	} else if (IsConstantAggregate()) {
		aggregator = make_uniq<WindowConstantAggregator>(aggr, arg_types, return_type, wexpr.exclude_clause);
	} else if (IsCustomAggregate()) {
		aggregator = make_uniq<WindowCustomAggregator>(aggr, arg_types, return_type, wexpr.exclude_clause);
	} else {
		// build a segment tree for frame-adhering aggregates
		// see http://www.vldb.org/pvldb/vol8/p1058-leis.pdf
		aggregator = make_uniq<WindowSegmentTree>(aggr, arg_types, return_type, mode, wexpr.exclude_clause);
	}

	gsink = aggregator->GetGlobalState(group_count, partition_mask);

	// evaluate the FILTER clause and stuff it into a large mask for compactness and reuse
	if (wexpr.filter_expr) {
		filter_executor.AddExpression(*wexpr.filter_expr);
		filter_sel.Initialize(STANDARD_VECTOR_SIZE);
	}
}

unique_ptr<WindowExecutorGlobalState> WindowAggregateExecutor::GetGlobalState(const idx_t payload_count,
                                                                              const ValidityMask &partition_mask,
                                                                              const ValidityMask &order_mask) const {
	return make_uniq<WindowAggregateExecutorGlobalState>(*this, payload_count, partition_mask, order_mask);
}

void WindowAggregateExecutor::Sink(DataChunk &input_chunk, const idx_t input_idx, const idx_t total_count,
                                   WindowExecutorGlobalState &gstate) const {
	auto &gastate = gstate.Cast<WindowAggregateExecutorGlobalState>();
	auto &filter_sel = gastate.filter_sel;
	auto &filter_executor = gastate.filter_executor;
	auto &payload_executor = gastate.payload_executor;
	auto &payload_chunk = gastate.payload_chunk;
	auto &aggregator = gastate.aggregator;
	auto &gsink = gastate.gsink;

	// TODO we could evaluate those expressions in parallel
	idx_t filtered = 0;
	SelectionVector *filtering = nullptr;
	if (wexpr.filter_expr) {
		filtering = &filter_sel;
		filtered = filter_executor.SelectExpression(input_chunk, filter_sel);
	}

	if (!wexpr.children.empty()) {
		payload_chunk.Reset();
		payload_executor.Execute(input_chunk, payload_chunk);
		payload_chunk.Verify();
	} else if (aggregator) {
		//	Zero-argument aggregate (e.g., COUNT(*)
		payload_chunk.SetCardinality(input_chunk);
	}

	D_ASSERT(aggregator);
	aggregator->Sink(*gsink, payload_chunk, filtering, filtered);

	WindowExecutor::Sink(input_chunk, input_idx, total_count, gstate);
}

static void ApplyWindowStats(const WindowBoundary &boundary, FrameDelta &delta, BaseStatistics *base, bool is_start) {
	// Avoid overflow by clamping to the frame bounds
	auto base_stats = delta;

	switch (boundary) {
	case WindowBoundary::UNBOUNDED_PRECEDING:
		if (is_start) {
			delta.end = 0;
			return;
		}
		break;
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		if (!is_start) {
			delta.begin = 0;
			return;
		}
		break;
	case WindowBoundary::CURRENT_ROW_ROWS:
		delta.begin = delta.end = 0;
		return;
	case WindowBoundary::EXPR_PRECEDING_ROWS:
		if (base && base->GetStatsType() == StatisticsType::NUMERIC_STATS && NumericStats::HasMinMax(*base)) {
			//	Preceding so negative offset from current row
			base_stats.begin = NumericStats::GetMin<int64_t>(*base);
			base_stats.end = NumericStats::GetMax<int64_t>(*base);
			if (delta.begin < base_stats.end && base_stats.end < delta.end) {
				delta.begin = -base_stats.end;
			}
			if (delta.begin < base_stats.begin && base_stats.begin < delta.end) {
				delta.end = -base_stats.begin + 1;
			}
		}
		return;
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
		if (base && base->GetStatsType() == StatisticsType::NUMERIC_STATS && NumericStats::HasMinMax(*base)) {
			base_stats.begin = NumericStats::GetMin<int64_t>(*base);
			base_stats.end = NumericStats::GetMax<int64_t>(*base);
			if (base_stats.end < delta.end) {
				delta.end = base_stats.end + 1;
			}
		}
		return;

	case WindowBoundary::CURRENT_ROW_RANGE:
	case WindowBoundary::EXPR_PRECEDING_RANGE:
	case WindowBoundary::EXPR_FOLLOWING_RANGE:
		return;
	default:
		break;
	}

	if (is_start) {
		throw InternalException("Unsupported window start boundary");
	} else {
		throw InternalException("Unsupported window end boundary");
	}
}

void WindowAggregateExecutor::Finalize(WindowExecutorGlobalState &gstate) const {
	auto &gastate = gstate.Cast<WindowAggregateExecutorGlobalState>();
	auto &aggregator = gastate.aggregator;
	auto &gsink = gastate.gsink;
	D_ASSERT(aggregator);

	//	Estimate the frame statistics
	//	Default to the entire partition if we don't know anything
	FrameStats stats;
	const auto count = NumericCast<int64_t>(gastate.payload_count);

	//	First entry is the frame start
	stats[0] = FrameDelta(-count, count);
	auto base = wexpr.expr_stats.empty() ? nullptr : wexpr.expr_stats[0].get();
	ApplyWindowStats(wexpr.start, stats[0], base, true);

	//	Second entry is the frame end
	stats[1] = FrameDelta(-count, count);
	base = wexpr.expr_stats.empty() ? nullptr : wexpr.expr_stats[1].get();
	ApplyWindowStats(wexpr.end, stats[1], base, false);

	aggregator->Finalize(*gsink, stats);
}

class WindowAggregateExecutorLocalState : public WindowExecutorBoundsState {
public:
	WindowAggregateExecutorLocalState(const WindowExecutorGlobalState &gstate, const WindowAggregator &aggregator)
	    : WindowExecutorBoundsState(gstate), aggregator_state(aggregator.GetLocalState()) {
	}

public:
	// state of aggregator
	unique_ptr<WindowAggregatorState> aggregator_state;

	void NextRank(idx_t partition_begin, idx_t peer_begin, idx_t row_idx);
};

unique_ptr<WindowExecutorLocalState>
WindowAggregateExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	auto &gastate = gstate.Cast<WindowAggregateExecutorGlobalState>();
	auto res = make_uniq<WindowAggregateExecutorLocalState>(gstate, *gastate.aggregator);
	return std::move(res);
}

void WindowAggregateExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                               Vector &result, idx_t count, idx_t row_idx) const {
	auto &gastate = gstate.Cast<WindowAggregateExecutorGlobalState>();
	auto &lastate = lstate.Cast<WindowAggregateExecutorLocalState>();
	auto &aggregator = gastate.aggregator;
	auto &gsink = gastate.gsink;
	D_ASSERT(aggregator);

	auto &agg_state = *lastate.aggregator_state;

	aggregator->Evaluate(*gsink, agg_state, lastate.bounds, result, count, row_idx);
}

//===--------------------------------------------------------------------===//
// WindowRowNumberExecutor
//===--------------------------------------------------------------------===//
WindowRowNumberExecutor::WindowRowNumberExecutor(BoundWindowExpression &wexpr, ClientContext &context)
    : WindowExecutor(wexpr, context) {
}

void WindowRowNumberExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                               Vector &result, idx_t count, idx_t row_idx) const {
	auto &lbstate = lstate.Cast<WindowExecutorBoundsState>();
	auto partition_begin = FlatVector::GetData<const idx_t>(lbstate.bounds.data[PARTITION_BEGIN]);
	auto rdata = FlatVector::GetData<int64_t>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		rdata[i] = NumericCast<int64_t>(row_idx - partition_begin[i] + 1);
	}
}

//===--------------------------------------------------------------------===//
// WindowPeerState
//===--------------------------------------------------------------------===//
class WindowPeerState : public WindowExecutorBoundsState {
public:
	explicit WindowPeerState(const WindowExecutorGlobalState &gstate) : WindowExecutorBoundsState(gstate) {
	}

public:
	uint64_t dense_rank = 1;
	uint64_t rank_equal = 0;
	uint64_t rank = 1;

	void NextRank(idx_t partition_begin, idx_t peer_begin, idx_t row_idx);
};

void WindowPeerState::NextRank(idx_t partition_begin, idx_t peer_begin, idx_t row_idx) {
	if (partition_begin == row_idx) {
		dense_rank = 1;
		rank = 1;
		rank_equal = 0;
	} else if (peer_begin == row_idx) {
		dense_rank++;
		rank += rank_equal;
		rank_equal = 0;
	}
	rank_equal++;
}

WindowRankExecutor::WindowRankExecutor(BoundWindowExpression &wexpr, ClientContext &context)
    : WindowExecutor(wexpr, context) {
}

unique_ptr<WindowExecutorLocalState> WindowRankExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowPeerState>(gstate);
}

void WindowRankExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                          Vector &result, idx_t count, idx_t row_idx) const {
	auto &lpeer = lstate.Cast<WindowPeerState>();
	auto partition_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_BEGIN]);
	auto peer_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PEER_BEGIN]);
	auto rdata = FlatVector::GetData<int64_t>(result);

	//	Reset to "previous" row
	lpeer.rank = (peer_begin[0] - partition_begin[0]) + 1;
	lpeer.rank_equal = (row_idx - peer_begin[0]);

	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		lpeer.NextRank(partition_begin[i], peer_begin[i], row_idx);
		rdata[i] = NumericCast<int64_t>(lpeer.rank);
	}
}

WindowDenseRankExecutor::WindowDenseRankExecutor(BoundWindowExpression &wexpr, ClientContext &context)
    : WindowExecutor(wexpr, context) {
}

unique_ptr<WindowExecutorLocalState>
WindowDenseRankExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowPeerState>(gstate);
}

void WindowDenseRankExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                               Vector &result, idx_t count, idx_t row_idx) const {
	auto &lpeer = lstate.Cast<WindowPeerState>();

	auto &order_mask = gstate.order_mask;
	auto partition_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_BEGIN]);
	auto peer_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PEER_BEGIN]);
	auto rdata = FlatVector::GetData<int64_t>(result);

	//	Reset to "previous" row
	lpeer.rank = (peer_begin[0] - partition_begin[0]) + 1;
	lpeer.rank_equal = (row_idx - peer_begin[0]);

	//	The previous dense rank is the number of order mask bits in [partition_begin, row_idx)
	lpeer.dense_rank = 0;

	auto order_begin = partition_begin[0];
	idx_t begin_idx;
	idx_t begin_offset;
	order_mask.GetEntryIndex(order_begin, begin_idx, begin_offset);

	auto order_end = row_idx;
	idx_t end_idx;
	idx_t end_offset;
	order_mask.GetEntryIndex(order_end, end_idx, end_offset);

	//	If they are in the same entry, just loop
	if (begin_idx == end_idx) {
		const auto entry = order_mask.GetValidityEntry(begin_idx);
		for (; begin_offset < end_offset; ++begin_offset) {
			lpeer.dense_rank += order_mask.RowIsValid(entry, begin_offset);
		}
	} else {
		// Count the ragged bits at the start of the partition
		if (begin_offset) {
			const auto entry = order_mask.GetValidityEntry(begin_idx);
			for (; begin_offset < order_mask.BITS_PER_VALUE; ++begin_offset) {
				lpeer.dense_rank += order_mask.RowIsValid(entry, begin_offset);
				++order_begin;
			}
			++begin_idx;
		}

		//	Count the the aligned bits.
		ValidityMask tail_mask(order_mask.GetData() + begin_idx);
		lpeer.dense_rank += tail_mask.CountValid(order_end - order_begin);
	}

	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		lpeer.NextRank(partition_begin[i], peer_begin[i], row_idx);
		rdata[i] = NumericCast<int64_t>(lpeer.dense_rank);
	}
}

WindowPercentRankExecutor::WindowPercentRankExecutor(BoundWindowExpression &wexpr, ClientContext &context)
    : WindowExecutor(wexpr, context) {
}

unique_ptr<WindowExecutorLocalState>
WindowPercentRankExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowPeerState>(gstate);
}

void WindowPercentRankExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                                 Vector &result, idx_t count, idx_t row_idx) const {
	auto &lpeer = lstate.Cast<WindowPeerState>();
	auto partition_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_END]);
	auto peer_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PEER_BEGIN]);
	auto rdata = FlatVector::GetData<double>(result);

	//	Reset to "previous" row
	lpeer.rank = (peer_begin[0] - partition_begin[0]) + 1;
	lpeer.rank_equal = (row_idx - peer_begin[0]);

	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		lpeer.NextRank(partition_begin[i], peer_begin[i], row_idx);
		auto denom = NumericCast<int64_t>(partition_end[i] - partition_begin[i] - 1);
		double percent_rank = denom > 0 ? ((double)lpeer.rank - 1) / denom : 0;
		rdata[i] = percent_rank;
	}
}

//===--------------------------------------------------------------------===//
// WindowCumeDistExecutor
//===--------------------------------------------------------------------===//
WindowCumeDistExecutor::WindowCumeDistExecutor(BoundWindowExpression &wexpr, ClientContext &context)
    : WindowExecutor(wexpr, context) {
}

void WindowCumeDistExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                              Vector &result, idx_t count, idx_t row_idx) const {
	auto &lbstate = lstate.Cast<WindowExecutorBoundsState>();
	auto partition_begin = FlatVector::GetData<const idx_t>(lbstate.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(lbstate.bounds.data[PARTITION_END]);
	auto peer_end = FlatVector::GetData<const idx_t>(lbstate.bounds.data[PEER_END]);
	auto rdata = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		auto denom = NumericCast<int64_t>(partition_end[i] - partition_begin[i]);
		double cume_dist = denom > 0 ? ((double)(peer_end[i] - partition_begin[i])) / denom : 0;
		rdata[i] = cume_dist;
	}
}

//===--------------------------------------------------------------------===//
// WindowValueGlobalState
//===--------------------------------------------------------------------===//

class WindowValueGlobalState : public WindowExecutorGlobalState {
public:
	WindowValueGlobalState(const WindowExecutor &executor, const idx_t payload_count,
	                       const ValidityMask &partition_mask, const ValidityMask &order_mask)
	    : WindowExecutorGlobalState(executor, payload_count, partition_mask, order_mask)

	{
		auto types = payload_chunk.GetTypes();
		if (!types.empty()) {
			payload_collection.Initialize(Allocator::Get(executor.context), types, payload_count);
		}
	}

	// The partition values
	DataChunk payload_collection;
	// IGNORE NULLS
	ValidityMask ignore_nulls;
};

//===--------------------------------------------------------------------===//
// WindowValueExecutor
//===--------------------------------------------------------------------===//
WindowValueExecutor::WindowValueExecutor(BoundWindowExpression &wexpr, ClientContext &context)
    : WindowExecutor(wexpr, context) {
}

WindowNtileExecutor::WindowNtileExecutor(BoundWindowExpression &wexpr, ClientContext &context)
    : WindowValueExecutor(wexpr, context) {
}

unique_ptr<WindowExecutorGlobalState> WindowValueExecutor::GetGlobalState(const idx_t payload_count,
                                                                          const ValidityMask &partition_mask,
                                                                          const ValidityMask &order_mask) const {
	return make_uniq<WindowValueGlobalState>(*this, payload_count, partition_mask, order_mask);
}

void WindowValueExecutor::Sink(DataChunk &input_chunk, const idx_t input_idx, const idx_t total_count,
                               WindowExecutorGlobalState &gstate) const {
	auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	auto &payload_chunk = gvstate.payload_chunk;
	auto &payload_executor = gvstate.payload_executor;
	auto &payload_collection = gvstate.payload_collection;
	auto &ignore_nulls = gvstate.ignore_nulls;

	// Single pass over the input to produce the global data.
	// Vectorisation for the win...

	// Set up a validity mask for IGNORE NULLS
	bool check_nulls = false;
	if (wexpr.ignore_nulls) {
		switch (wexpr.type) {
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

	if (!wexpr.children.empty()) {
		payload_chunk.Reset();
		payload_executor.Execute(input_chunk, payload_chunk);
		payload_chunk.Verify();
		payload_collection.Append(payload_chunk, true);

		// process payload chunks while they are still piping hot
		if (check_nulls) {
			const auto count = input_chunk.size();

			payload_chunk.Flatten();
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

	WindowExecutor::Sink(input_chunk, input_idx, total_count, gstate);
}

unique_ptr<WindowExecutorLocalState> WindowValueExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	if (wexpr.type == ExpressionType::WINDOW_FIRST_VALUE || wexpr.type == ExpressionType::WINDOW_LAST_VALUE ||
	    wexpr.type == ExpressionType::WINDOW_NTH_VALUE) {
		auto &ignore_nulls = gstate.Cast<WindowValueGlobalState>().ignore_nulls;
		return make_uniq<WindowValueLocalState>(gstate, ignore_nulls);
	} else {
		return make_uniq<WindowExecutorBoundsState>(gstate);
	}
}

void WindowNtileExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                           Vector &result, idx_t count, idx_t row_idx) const {
	auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	auto &payload_collection = gvstate.payload_collection;
	D_ASSERT(payload_collection.ColumnCount() == 1);
	auto &lbstate = lstate.Cast<WindowExecutorBoundsState>();
	auto partition_begin = FlatVector::GetData<const idx_t>(lbstate.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(lbstate.bounds.data[PARTITION_END]);
	auto rdata = FlatVector::GetData<int64_t>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		if (CellIsNull(payload_collection, 0, row_idx)) {
			FlatVector::SetNull(result, i, true);
		} else {
			auto n_param = GetCell<int64_t>(payload_collection, 0, row_idx);
			if (n_param < 1) {
				throw InvalidInputException("Argument for ntile must be greater than zero");
			}
			// With thanks from SQLite's ntileValueFunc()
			auto n_total = NumericCast<int64_t>(partition_end[i] - partition_begin[i]);
			if (n_param > n_total) {
				// more groups allowed than we have values
				// map every entry to a unique group
				n_param = n_total;
			}
			int64_t n_size = (n_total / n_param);
			// find the row idx within the group
			D_ASSERT(row_idx >= partition_begin[i]);
			auto adjusted_row_idx = NumericCast<int64_t>(row_idx - partition_begin[i]);
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
			rdata[i] = result_ntile;
		}
	}
}

//===--------------------------------------------------------------------===//
// WindowLeadLagLocalState
//===--------------------------------------------------------------------===//
class WindowLeadLagLocalState : public WindowExecutorBoundsState {
public:
	explicit WindowLeadLagLocalState(const WindowExecutorGlobalState &gstate)
	    : WindowExecutorBoundsState(gstate),
	      leadlag_offset(gstate.executor.wexpr.offset_expr.get(), gstate.executor.context),
	      leadlag_default(gstate.executor.wexpr.default_expr.get(), gstate.executor.context) {
	}

	void UpdateBounds(idx_t row_idx, DataChunk &input_chunk, const WindowInputColumn &range) override;

public:
	// LEAD/LAG Evaluation
	WindowInputExpression leadlag_offset;
	WindowInputExpression leadlag_default;
};

void WindowLeadLagLocalState::UpdateBounds(idx_t row_idx, DataChunk &input_chunk, const WindowInputColumn &range) {
	// Evaluate the row-level arguments
	leadlag_offset.Execute(input_chunk);
	leadlag_default.Execute(input_chunk);

	WindowExecutorBoundsState::UpdateBounds(row_idx, input_chunk, range);
}

WindowLeadLagExecutor::WindowLeadLagExecutor(BoundWindowExpression &wexpr, ClientContext &context)
    : WindowValueExecutor(wexpr, context) {
}

unique_ptr<WindowExecutorLocalState>
WindowLeadLagExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowLeadLagLocalState>(gstate);
}

void WindowLeadLagExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                             Vector &result, idx_t count, idx_t row_idx) const {
	auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	auto &payload_collection = gvstate.payload_collection;
	auto &ignore_nulls = gvstate.ignore_nulls;
	auto &llstate = lstate.Cast<WindowLeadLagLocalState>();

	bool can_shift = ignore_nulls.AllValid();
	if (wexpr.offset_expr) {
		can_shift = can_shift && wexpr.offset_expr->IsFoldable();
	}
	if (wexpr.default_expr) {
		can_shift = can_shift && wexpr.default_expr->IsFoldable();
	}

	auto partition_begin = FlatVector::GetData<const idx_t>(llstate.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(llstate.bounds.data[PARTITION_END]);
	const auto row_end = row_idx + count;
	for (idx_t i = 0; i < count;) {
		int64_t offset = 1;
		if (wexpr.offset_expr) {
			offset = llstate.leadlag_offset.GetCell<int64_t>(i);
		}
		int64_t val_idx = (int64_t)row_idx;
		if (wexpr.type == ExpressionType::WINDOW_LEAD) {
			val_idx = AddOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(val_idx, offset);
		} else {
			val_idx = SubtractOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(val_idx, offset);
		}

		idx_t delta = 0;
		if (val_idx < (int64_t)row_idx) {
			// Count backwards
			delta = idx_t(row_idx - idx_t(val_idx));
			val_idx = int64_t(FindPrevStart(ignore_nulls, partition_begin[i], row_idx, delta));
		} else if (val_idx > (int64_t)row_idx) {
			delta = idx_t(idx_t(val_idx) - row_idx);
			val_idx = int64_t(FindNextStart(ignore_nulls, row_idx + 1, partition_end[i], delta));
		}
		// else offset is zero, so don't move.

		if (can_shift) {
			if (!delta) {
				//	Copy source[index:index+width] => result[i:]
				const auto index = NumericCast<idx_t>(val_idx);
				const auto source_limit = partition_end[i] - index;
				const auto target_limit = MinValue(partition_end[i], row_end) - row_idx;
				const auto width = MinValue(source_limit, target_limit);
				auto &source = payload_collection.data[0];
				VectorOperations::Copy(source, result, index + width, index, i);
				i += width;
				row_idx += width;
			} else if (wexpr.default_expr) {
				llstate.leadlag_default.CopyCell(result, i, delta);
				i += delta;
				row_idx += delta;
			} else {
				for (idx_t nulls = MinValue(delta, count - i); nulls--; ++i, ++row_idx) {
					FlatVector::SetNull(result, i, true);
				}
			}
		} else {
			if (!delta) {
				CopyCell(payload_collection, 0, NumericCast<idx_t>(val_idx), result, i);
			} else if (wexpr.default_expr) {
				llstate.leadlag_default.CopyCell(result, i);
			} else {
				FlatVector::SetNull(result, i, true);
			}
			++i;
			++row_idx;
		}
	}
}

WindowFirstValueExecutor::WindowFirstValueExecutor(BoundWindowExpression &wexpr, ClientContext &context)
    : WindowValueExecutor(wexpr, context) {
}

void WindowFirstValueExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                                Vector &result, idx_t count, idx_t row_idx) const {
	auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	auto &payload_collection = gvstate.payload_collection;
	auto &lvstate = lstate.Cast<WindowValueLocalState>();
	auto window_begin = FlatVector::GetData<const idx_t>(lvstate.bounds.data[WINDOW_BEGIN]);
	auto window_end = FlatVector::GetData<const idx_t>(lvstate.bounds.data[WINDOW_END]);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {

		if (lvstate.exclusion_filter) {
			lvstate.exclusion_filter->ApplyExclusion(lvstate.bounds, row_idx, i);
		}

		if (window_begin[i] >= window_end[i]) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		//	Same as NTH_VALUE(..., 1)
		idx_t n = 1;
		const auto first_idx = FindNextStart(*lvstate.ignore_nulls_exclude, window_begin[i], window_end[i], n);
		if (!n) {
			CopyCell(payload_collection, 0, first_idx, result, i);
		} else {
			FlatVector::SetNull(result, i, true);
		}

		if (lvstate.exclusion_filter) {
			lvstate.exclusion_filter->ResetMask(row_idx, i);
		}
	}
}

WindowLastValueExecutor::WindowLastValueExecutor(BoundWindowExpression &wexpr, ClientContext &context)
    : WindowValueExecutor(wexpr, context) {
}

void WindowLastValueExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                               Vector &result, idx_t count, idx_t row_idx) const {
	auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	auto &payload_collection = gvstate.payload_collection;
	auto &lvstate = lstate.Cast<WindowValueLocalState>();
	auto window_begin = FlatVector::GetData<const idx_t>(lvstate.bounds.data[WINDOW_BEGIN]);
	auto window_end = FlatVector::GetData<const idx_t>(lvstate.bounds.data[WINDOW_END]);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {

		if (lvstate.exclusion_filter) {
			lvstate.exclusion_filter->ApplyExclusion(lvstate.bounds, row_idx, i);
		}

		if (window_begin[i] >= window_end[i]) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		idx_t n = 1;
		const auto last_idx = FindPrevStart(*lvstate.ignore_nulls_exclude, window_begin[i], window_end[i], n);
		if (!n) {
			CopyCell(payload_collection, 0, last_idx, result, i);
		} else {
			FlatVector::SetNull(result, i, true);
		}

		if (lvstate.exclusion_filter) {
			lvstate.exclusion_filter->ResetMask(row_idx, i);
		}
	}
}

WindowNthValueExecutor::WindowNthValueExecutor(BoundWindowExpression &wexpr, ClientContext &context)
    : WindowValueExecutor(wexpr, context) {
}

void WindowNthValueExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                              Vector &result, idx_t count, idx_t row_idx) const {
	auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	auto &payload_collection = gvstate.payload_collection;
	D_ASSERT(payload_collection.ColumnCount() == 2);

	auto &lvstate = lstate.Cast<WindowValueLocalState>();
	auto window_begin = FlatVector::GetData<const idx_t>(lvstate.bounds.data[WINDOW_BEGIN]);
	auto window_end = FlatVector::GetData<const idx_t>(lvstate.bounds.data[WINDOW_END]);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {

		if (lvstate.exclusion_filter) {
			lvstate.exclusion_filter->ApplyExclusion(lvstate.bounds, row_idx, i);
		}

		if (window_begin[i] >= window_end[i]) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		// Returns value evaluated at the row that is the n'th row of the window frame (counting from 1);
		// returns NULL if there is no such row.
		if (CellIsNull(payload_collection, 1, row_idx)) {
			FlatVector::SetNull(result, i, true);
		} else {
			auto n_param = GetCell<int64_t>(payload_collection, 1, row_idx);
			if (n_param < 1) {
				FlatVector::SetNull(result, i, true);
			} else {
				auto n = idx_t(n_param);
				const auto nth_index = FindNextStart(*lvstate.ignore_nulls_exclude, window_begin[i], window_end[i], n);
				if (!n) {
					CopyCell(payload_collection, 0, nth_index, result, i);
				} else {
					FlatVector::SetNull(result, i, true);
				}
			}
		}

		if (lvstate.exclusion_filter) {
			lvstate.exclusion_filter->ResetMask(row_idx, i);
		}
	}
}

} // namespace duckdb
