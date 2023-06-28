#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/execution/window_executor.hpp"

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
		for (++shift; shift-- > 0; --r) {
			if (mask.RowIsValid(block, shift) && --n == 0) {
				return MaxValue(l, r - 1);
			}
		}
	}

	//	Didn't find a start so return the start of the range
	return l;
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

	explicit WindowColumnIterator(WindowInputColumn &coll_p, pointer pos_p = 0) : coll(&coll_p), pos(pos_p) {
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
		pos += n;
		return *this;
	}
	inline iterator &operator-=(difference_type n) {
		pos -= n;
		return *this;
	}

	inline reference operator[](difference_type m) const {
		return coll->GetCell<T>(pos + m);
	}

	friend inline iterator &operator+(const iterator &a, difference_type n) {
		return iterator(a.coll, a.pos + n);
	}

	friend inline iterator &operator-(const iterator &a, difference_type n) {
		return iterator(a.coll, a.pos - n);
	}

	friend inline iterator &operator+(difference_type n, const iterator &a) {
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
	optional_ptr<WindowInputColumn> coll;
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
                                 WindowInputExpression &boundary, const idx_t chunk_idx, const FrameBounds &prev) {
	D_ASSERT(!boundary.CellIsNull(chunk_idx));
	const auto val = boundary.GetCell<T>(chunk_idx);

	OperationCompare<T, OP> comp;
	WindowColumnIterator<T> begin(over, order_begin);
	WindowColumnIterator<T> end(over, order_end);

	if (order_begin < prev.first && prev.first < order_end) {
		const auto first = over.GetCell<T>(prev.first);
		if (!comp(val, first)) {
			//	prev.first <= val, so we can start further forward
			begin += (prev.first - order_begin);
		}
	}
	if (order_begin <= prev.second && prev.second < order_end) {
		const auto second = over.GetCell<T>(prev.second);
		if (!comp(second, val)) {
			//	val <= prev.second, so we can end further back
			// (prev.second is the largest peer)
			end -= (order_end - prev.second - 1);
		}
	}

	if (FROM) {
		return idx_t(std::lower_bound(begin, end, val, comp));
	} else {
		return idx_t(std::upper_bound(begin, end, val, comp));
	}
}

template <typename OP, bool FROM>
static idx_t FindRangeBound(WindowInputColumn &over, const idx_t order_begin, const idx_t order_end,
                            WindowInputExpression &boundary, const idx_t chunk_idx, const FrameBounds &prev) {
	D_ASSERT(boundary.chunk.ColumnCount() == 1);
	D_ASSERT(boundary.chunk.data[0].GetType().InternalType() == over.input_expr.ptype);

	switch (over.input_expr.ptype) {
	case PhysicalType::INT8:
		return FindTypedRangeBound<int8_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::INT16:
		return FindTypedRangeBound<int16_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::INT32:
		return FindTypedRangeBound<int32_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::INT64:
		return FindTypedRangeBound<int64_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::UINT8:
		return FindTypedRangeBound<uint8_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::UINT16:
		return FindTypedRangeBound<uint16_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::UINT32:
		return FindTypedRangeBound<uint32_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::UINT64:
		return FindTypedRangeBound<uint64_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::INT128:
		return FindTypedRangeBound<hugeint_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::FLOAT:
		return FindTypedRangeBound<float, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::DOUBLE:
		return FindTypedRangeBound<double, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::INTERVAL:
		return FindTypedRangeBound<interval_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	default:
		throw InternalException("Unsupported column type for RANGE");
	}
}

template <bool FROM>
static idx_t FindOrderedRangeBound(WindowInputColumn &over, const OrderType range_sense, const idx_t order_begin,
                                   const idx_t order_end, WindowInputExpression &boundary, const idx_t chunk_idx,
                                   const FrameBounds &prev) {
	switch (range_sense) {
	case OrderType::ASCENDING:
		return FindRangeBound<LessThan, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case OrderType::DESCENDING:
		return FindRangeBound<GreaterThan, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	default:
		throw InternalException("Unsupported ORDER BY sense for RANGE");
	}
}

//===--------------------------------------------------------------------===//
// WindowBoundariesState
//===--------------------------------------------------------------------===//
void WindowBoundariesState::Update(const idx_t row_idx, WindowInputColumn &range_collection, const idx_t chunk_idx,
                                   WindowInputExpression &boundary_start, WindowInputExpression &boundary_end,
                                   const ValidityMask &partition_mask, const ValidityMask &order_mask) {

	if (partition_count + order_count > 0) {

		// determine partition and peer group boundaries to ultimately figure out window size
		const auto is_same_partition = !partition_mask.RowIsValidUnsafe(row_idx);
		const auto is_peer = !order_mask.RowIsValidUnsafe(row_idx);

		// when the partition changes, recompute the boundaries
		if (!is_same_partition) {
			partition_start = row_idx;
			peer_start = row_idx;

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
				prev.first = valid_start;
				prev.second = valid_end;
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

	// determine window boundaries depending on the type of expression
	window_start = -1;
	window_end = -1;

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
		if (!TrySubtractOperator::Operation(int64_t(row_idx), boundary_start.GetCell<int64_t>(chunk_idx),
		                                    window_start)) {
			throw OutOfRangeException("Overflow computing ROWS PRECEDING start");
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_ROWS: {
		if (!TryAddOperator::Operation(int64_t(row_idx), boundary_start.GetCell<int64_t>(chunk_idx), window_start)) {
			throw OutOfRangeException("Overflow computing ROWS FOLLOWING start");
		}
		break;
	}
	case WindowBoundary::EXPR_PRECEDING_RANGE: {
		if (boundary_start.CellIsNull(chunk_idx)) {
			window_start = peer_start;
		} else {
			prev.first = FindOrderedRangeBound<true>(range_collection, range_sense, valid_start, row_idx,
			                                         boundary_start, chunk_idx, prev);
			window_start = prev.first;
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_RANGE: {
		if (boundary_start.CellIsNull(chunk_idx)) {
			window_start = peer_start;
		} else {
			prev.first = FindOrderedRangeBound<true>(range_collection, range_sense, row_idx, valid_end, boundary_start,
			                                         chunk_idx, prev);
			window_start = prev.first;
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
	case WindowBoundary::EXPR_PRECEDING_ROWS:
		if (!TrySubtractOperator::Operation(int64_t(row_idx + 1), boundary_end.GetCell<int64_t>(chunk_idx),
		                                    window_end)) {
			throw OutOfRangeException("Overflow computing ROWS PRECEDING end");
		}
		break;
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
		if (!TryAddOperator::Operation(int64_t(row_idx + 1), boundary_end.GetCell<int64_t>(chunk_idx), window_end)) {
			throw OutOfRangeException("Overflow computing ROWS FOLLOWING end");
		}
		break;
	case WindowBoundary::EXPR_PRECEDING_RANGE: {
		if (boundary_end.CellIsNull(chunk_idx)) {
			window_end = peer_end;
		} else {
			prev.second = FindOrderedRangeBound<false>(range_collection, range_sense, valid_start, row_idx,
			                                           boundary_end, chunk_idx, prev);
			window_end = prev.second;
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_RANGE: {
		if (boundary_end.CellIsNull(chunk_idx)) {
			window_end = peer_end;
		} else {
			prev.second = FindOrderedRangeBound<false>(range_collection, range_sense, row_idx, valid_end, boundary_end,
			                                           chunk_idx, prev);
			window_end = prev.second;
		}
		break;
	}
	default:
		throw InternalException("Unsupported window end boundary");
	}

	// clamp windows to partitions if they should exceed
	if (window_start < (int64_t)partition_start) {
		window_start = partition_start;
	}
	if (window_start > (int64_t)partition_end) {
		window_start = partition_end;
	}
	if (window_end < (int64_t)partition_start) {
		window_end = partition_start;
	}
	if (window_end > (int64_t)partition_end) {
		window_end = partition_end;
	}

	if (window_start < 0 || window_end < 0) {
		throw InternalException("Failed to compute window boundaries");
	}
}

void WindowBoundariesState::Bounds(DataChunk &bounds, idx_t row_idx, WindowInputColumn &range, const idx_t count,
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
		*window_begin_data++ = window_start;
		*window_end_data++ = window_end;
	}
	bounds.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// WindowExecutor
//===--------------------------------------------------------------------===//
static void PrepareInputExpressions(vector<unique_ptr<Expression>> &exprs, ExpressionExecutor &executor,
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

WindowExecutor::WindowExecutor(BoundWindowExpression &wexpr, ClientContext &context, const idx_t count)
    : wexpr(wexpr), state(wexpr, count), payload_collection(), payload_executor(context),
      leadlag_offset(wexpr.offset_expr.get(), context), leadlag_default(wexpr.default_expr.get(), context),
      boundary_start(wexpr.start_expr.get(), context), boundary_end(wexpr.end_expr.get(), context),
      range((state.has_preceding_range || state.has_following_range) ? wexpr.orders[0].expression.get() : nullptr,
            context, count)

{
	// TODO: child may be a scalar, don't need to materialize the whole collection then

	// evaluate inner expressions of window functions, could be more complex
	PrepareInputExpressions(wexpr.children, payload_executor, payload_chunk);

	auto types = payload_chunk.GetTypes();
	if (!types.empty()) {
		payload_collection.Initialize(Allocator::Get(context), types);
	}

	vector<LogicalType> bounds_types(6, LogicalType(LogicalTypeId::UBIGINT));
	bounds.Initialize(Allocator::Get(context), bounds_types);
}

void WindowValueExecutor::Sink(DataChunk &input_chunk, const idx_t input_idx, const idx_t total_count) {
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

	WindowExecutor::Sink(input_chunk, input_idx, total_count);
}

//===--------------------------------------------------------------------===//
// WindowAggregateExecutor
//===--------------------------------------------------------------------===//
bool WindowAggregateExecutor::IsConstantAggregate() {
	if (!wexpr.aggregate) {
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

bool WindowAggregateExecutor::IsCustomAggregate() {
	if (!wexpr.aggregate) {
		return false;
	}

	if (!AggregateObject(wexpr).function.window) {
		return false;
	}

	return (mode < WindowAggregationMode::COMBINE);
}

void WindowExecutor::Evaluate(idx_t row_idx, DataChunk &input_chunk, Vector &result, const ValidityMask &partition_mask,
                              const ValidityMask &order_mask) {
	// Evaluate the row-level arguments
	boundary_start.Execute(input_chunk);
	boundary_end.Execute(input_chunk);

	leadlag_offset.Execute(input_chunk);
	leadlag_default.Execute(input_chunk);

	const auto count = input_chunk.size();
	bounds.Reset();
	state.Bounds(bounds, row_idx, range, input_chunk.size(), boundary_start, boundary_end, partition_mask, order_mask);

	EvaluateInternal(bounds, result, count, row_idx);

	result.Verify(count);
}

WindowAggregateExecutor::WindowAggregateExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                 const ValidityMask &partition_mask, const idx_t count,
                                                 WindowAggregationMode mode)
    : WindowExecutor(wexpr, context, count), mode(mode), filter_executor(context) {
	// TODO we could evaluate those expressions in parallel

	//	Check for constant aggregate
	if (IsConstantAggregate()) {
		aggregate_state =
		    make_uniq<WindowConstantAggregate>(AggregateObject(wexpr), wexpr.return_type, partition_mask, count);
	} else if (IsCustomAggregate()) {
		aggregate_state = make_uniq<WindowCustomAggregate>(AggregateObject(wexpr), wexpr.return_type, count);
	} else if (wexpr.aggregate) {
		// build a segment tree for frame-adhering aggregates
		// see http://www.vldb.org/pvldb/vol8/p1058-leis.pdf
		aggregate_state = make_uniq<WindowSegmentTree>(AggregateObject(wexpr), wexpr.return_type, count, mode);
	}

	// evaluate the FILTER clause and stuff it into a large mask for compactness and reuse
	if (wexpr.filter_expr) {
		filter_executor.AddExpression(*wexpr.filter_expr);
		filter_sel.Initialize(STANDARD_VECTOR_SIZE);
	}
}

void WindowAggregateExecutor::Sink(DataChunk &input_chunk, const idx_t input_idx, const idx_t total_count) {
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
	} else if (aggregate_state) {
		//	Zero-argument aggregate (e.g., COUNT(*)
		payload_chunk.SetCardinality(input_chunk);
	}

	D_ASSERT(aggregate_state);
	aggregate_state->Sink(payload_chunk, filtering, filtered);

	WindowExecutor::Sink(input_chunk, input_idx, total_count);
}

void WindowAggregateExecutor::Finalize() {
	D_ASSERT(aggregate_state);
	aggregate_state->Finalize();
}

void WindowAggregateExecutor::EvaluateInternal(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	D_ASSERT(aggregate_state);
	auto window_begin = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_BEGIN]);
	auto window_end = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_END]);
	aggregate_state->Evaluate(window_begin, window_end, result, count);
}

//===--------------------------------------------------------------------===//
// WindowRowNumberExecutor
//===--------------------------------------------------------------------===//
void WindowRowNumberExecutor::EvaluateInternal(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	auto partition_begin = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_BEGIN]);
	auto rdata = FlatVector::GetData<int64_t>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		rdata[i] = row_idx - partition_begin[i] + 1;
	}
}

//===--------------------------------------------------------------------===//
// WindowPeerExecutor
//===--------------------------------------------------------------------===//
void WindowPeerExecutor::NextRank(idx_t partition_begin, idx_t peer_begin, idx_t row_idx) {
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

void WindowRankExecutor::EvaluateInternal(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	auto partition_begin = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_BEGIN]);
	auto peer_begin = FlatVector::GetData<const idx_t>(bounds.data[PEER_BEGIN]);
	auto rdata = FlatVector::GetData<int64_t>(result);

	//	Reset to "previous" row
	rank = (peer_begin[0] - partition_begin[0]) + 1;
	rank_equal = (row_idx - peer_begin[0]);

	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		NextRank(partition_begin[i], peer_begin[i], row_idx);
		rdata[i] = rank;
	}
}

void WindowDenseRankExecutor::EvaluateInternal(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	auto partition_begin = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_BEGIN]);
	auto peer_begin = FlatVector::GetData<const idx_t>(bounds.data[PEER_BEGIN]);
	auto rdata = FlatVector::GetData<int64_t>(result);

	//	Reset to "previous" row
	rank = (peer_begin[0] - partition_begin[0]) + 1;
	rank_equal = (row_idx - peer_begin[0]);

	//	The dense rank is the number of order mask bits between partition_begin and peer_begin
	dense_rank = 1;

	auto order_begin = partition_begin[0];
	idx_t begin_idx;
	idx_t begin_offset;
	order_mask.GetEntryIndex(order_begin, begin_idx, begin_offset);

	auto order_end = peer_begin[0];
	idx_t end_idx;
	idx_t end_offset;
	order_mask.GetEntryIndex(order_end, end_idx, end_offset);

	//	If they are in the same entry, just loop
	if (begin_idx == end_idx) {
		const auto entry = order_mask.GetValidityEntry(begin_idx);
		for (; begin_offset < end_offset; ++begin_offset) {
			dense_rank += order_mask.RowIsValid(entry, begin_offset);
		}
	} else {
		// Count the ragged bits at the start of the partition
		if (begin_offset) {
			const auto entry = order_mask.GetValidityEntry(begin_idx);
			for (; begin_offset < order_mask.BITS_PER_VALUE; ++begin_offset) {
				dense_rank += order_mask.RowIsValid(entry, begin_offset);
				++order_begin;
			}
			++begin_idx;
		}

		//	Count the the aligned bits.
		ValidityMask tail_mask(order_mask.GetData() + begin_idx);
		dense_rank += tail_mask.CountValid(order_end - order_begin);
	}

	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		NextRank(partition_begin[i], peer_begin[i], row_idx);
		rdata[i] = dense_rank;
	}
}

void WindowPercentRankExecutor::EvaluateInternal(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	auto partition_begin = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_END]);
	auto peer_begin = FlatVector::GetData<const idx_t>(bounds.data[PEER_BEGIN]);
	auto rdata = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		NextRank(partition_begin[i], peer_begin[i], row_idx);
		int64_t denom = partition_end[i] - partition_begin[i] - 1;
		double percent_rank = denom > 0 ? ((double)rank - 1) / denom : 0;
		rdata[i] = percent_rank;
	}
}

void WindowCumeDistExecutor::EvaluateInternal(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	auto partition_begin = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_END]);
	auto peer_end = FlatVector::GetData<const idx_t>(bounds.data[PEER_END]);
	auto rdata = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		int64_t denom = partition_end[i] - partition_begin[i];
		double cume_dist = denom > 0 ? ((double)(peer_end[i] - partition_begin[i])) / denom : 0;
		rdata[i] = cume_dist;
	}
}

//===--------------------------------------------------------------------===//
// WindowValueExecutor
//===--------------------------------------------------------------------===//
void WindowNtileExecutor::EvaluateInternal(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	D_ASSERT(payload_collection.ColumnCount() == 1);
	auto partition_begin = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_END]);
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
			int64_t n_total = partition_end[i] - partition_begin[i];
			if (n_param > n_total) {
				// more groups allowed than we have values
				// map every entry to a unique group
				n_param = n_total;
			}
			int64_t n_size = (n_total / n_param);
			// find the row idx within the group
			D_ASSERT(row_idx >= partition_begin[i]);
			int64_t adjusted_row_idx = row_idx - partition_begin[i];
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

void WindowLeadLagExecutor::EvaluateInternal(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	auto partition_begin = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_END]);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		int64_t offset = 1;
		if (wexpr.offset_expr) {
			offset = leadlag_offset.GetCell<int64_t>(i);
		}
		int64_t val_idx = (int64_t)row_idx;
		if (wexpr.type == ExpressionType::WINDOW_LEAD) {
			val_idx += offset;
		} else {
			val_idx -= offset;
		}

		idx_t delta = 0;
		if (val_idx < (int64_t)row_idx) {
			// Count backwards
			delta = idx_t(row_idx - val_idx);
			val_idx = FindPrevStart(ignore_nulls, partition_begin[i], row_idx, delta);
		} else if (val_idx > (int64_t)row_idx) {
			delta = idx_t(val_idx - row_idx);
			val_idx = FindNextStart(ignore_nulls, row_idx + 1, partition_end[i], delta);
		}
		// else offset is zero, so don't move.

		if (!delta) {
			CopyCell(payload_collection, 0, val_idx, result, i);
		} else if (wexpr.default_expr) {
			leadlag_default.CopyCell(result, i);
		} else {
			FlatVector::SetNull(result, i, true);
		}
	}
}

void WindowFirstValueExecutor::EvaluateInternal(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	auto window_begin = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_BEGIN]);
	auto window_end = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_END]);
	auto &rmask = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		if (window_begin[i] >= window_end[i]) {
			rmask.SetInvalid(i);
			continue;
		}
		//	Same as NTH_VALUE(..., 1)
		idx_t n = 1;
		const auto first_idx = FindNextStart(ignore_nulls, window_begin[i], window_end[i], n);
		if (!n) {
			CopyCell(payload_collection, 0, first_idx, result, i);
		} else {
			FlatVector::SetNull(result, i, true);
		}
	}
}

void WindowLastValueExecutor::EvaluateInternal(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	auto window_begin = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_BEGIN]);
	auto window_end = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_END]);
	auto &rmask = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		if (window_begin[i] >= window_end[i]) {
			rmask.SetInvalid(i);
			continue;
		}
		idx_t n = 1;
		const auto last_idx = FindPrevStart(ignore_nulls, window_begin[i], window_end[i], n);
		if (!n) {
			CopyCell(payload_collection, 0, last_idx, result, i);
		} else {
			FlatVector::SetNull(result, i, true);
		}
	}
}

void WindowNthValueExecutor::EvaluateInternal(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	D_ASSERT(payload_collection.ColumnCount() == 2);

	auto window_begin = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_BEGIN]);
	auto window_end = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_END]);
	auto &rmask = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		if (window_begin[i] >= window_end[i]) {
			rmask.SetInvalid(i);
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
				const auto nth_index = FindNextStart(ignore_nulls, window_begin[i], window_end[i], n);
				if (!n) {
					CopyCell(payload_collection, 0, nth_index, result, i);
				} else {
					FlatVector::SetNull(result, i, true);
				}
			}
		}
	}
}

} // namespace duckdb
