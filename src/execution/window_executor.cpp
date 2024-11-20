#include "duckdb/execution/window_executor.hpp"

#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include "duckdb/common/array.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowCollection
//===--------------------------------------------------------------------===//
WindowCollection::WindowCollection(BufferManager &buffer_manager, idx_t count, const vector<LogicalType> &types)
    : all_valids(types.size()), types(types), count(count), buffer_manager(buffer_manager) {
	if (!types.empty()) {
		inputs = make_uniq<ColumnDataCollection>(buffer_manager, types);
	}

	validities.resize(types.size());

	// Atomic vectors can't be constructed with a given value
	for (auto &all_valid : all_valids) {
		all_valid = true;
	}
}

void WindowCollection::GetCollection(idx_t row_idx, ColumnDataCollectionSpec &spec) {
	if (spec.second && row_idx == spec.first + spec.second->Count()) {
		return;
	}

	lock_guard<mutex> collection_guard(lock);

	auto collection = make_uniq<ColumnDataCollection>(buffer_manager, types);
	spec = {row_idx, collection.get()};
	Range probe {row_idx, collections.size()};
	auto i = std::upper_bound(ranges.begin(), ranges.end(), probe);
	ranges.insert(i, probe);
	collections.emplace_back(std::move(collection));
}

void WindowCollection::Combine(const ColumnSet &validity_cols) {
	lock_guard<mutex> collection_guard(lock);

	// If there are no columns (COUNT(*)) then this is a NOP
	if (types.empty()) {
		return;
	}

	// Have we already combined?
	if (inputs->Count()) {
		D_ASSERT(collections.empty());
		D_ASSERT(ranges.empty());
		return;
	}

	// If there are columns, we should have data
	D_ASSERT(!collections.empty());
	D_ASSERT(!ranges.empty());

	for (auto &range : ranges) {
		inputs->Combine(*collections[range.second]);
	}
	collections.clear();
	ranges.clear();

	if (validity_cols.empty()) {
		return;
	}

	D_ASSERT(inputs.get());

	//	Find all columns with NULLs
	vector<column_t> invalid_cols;
	for (auto &col_idx : validity_cols) {
		if (!all_valids[col_idx]) {
			invalid_cols.emplace_back(col_idx);
			validities[col_idx].Initialize(inputs->Count());
		}
	}

	if (invalid_cols.empty()) {
		return;
	}

	WindowCursor cursor(*this, invalid_cols);
	idx_t target_offset = 0;
	while (cursor.Scan()) {
		const auto count = cursor.chunk.size();
		for (idx_t i = 0; i < invalid_cols.size(); ++i) {
			auto &other = FlatVector::Validity(cursor.chunk.data[i]);
			const auto col_idx = invalid_cols[i];
			validities[col_idx].SliceInPlace(other, target_offset, 0, count);
		}
		target_offset += count;
	}
}

WindowBuilder::WindowBuilder(WindowCollection &collection) : collection(collection) {
}

void WindowBuilder::Sink(DataChunk &chunk, idx_t input_idx) {
	// Check whether we need a a new collection
	if (!sink.second || input_idx < sink.first || sink.first + sink.second->Count() < input_idx) {
		collection.GetCollection(input_idx, sink);
		D_ASSERT(sink.second);
		sink.second->InitializeAppend(appender);
	}
	sink.second->Append(appender, chunk);

	// Record NULLs
	for (column_t col_idx = 0; col_idx < chunk.ColumnCount(); ++col_idx) {
		if (!collection.all_valids[col_idx]) {
			continue;
		}

		// Column was valid, make sure it still is.
		UnifiedVectorFormat data;
		chunk.data[col_idx].ToUnifiedFormat(chunk.size(), data);
		if (!data.validity.AllValid()) {
			collection.all_valids[col_idx] = false;
		}
	}
}

WindowCursor::WindowCursor(const WindowCollection &paged, vector<column_t> column_ids) : paged(paged) {
	D_ASSERT(paged.collections.empty());
	D_ASSERT(paged.ranges.empty());
	if (column_ids.empty()) {
		//	For things like COUNT(*) set the state up to contain the whole range
		state.segment_index = 0;
		state.chunk_index = 0;
		state.current_row_index = 0;
		state.next_row_index = paged.size();
		state.properties = ColumnDataScanProperties::ALLOW_ZERO_COPY;
		chunk.SetCapacity(state.next_row_index);
		chunk.SetCardinality(state.next_row_index);
		return;
	} else if (chunk.data.empty()) {
		auto &inputs = paged.inputs;
		D_ASSERT(inputs.get());
		inputs->InitializeScan(state, std::move(column_ids));
		inputs->InitializeScanChunk(state, chunk);
	}
}

WindowCursor::WindowCursor(const WindowCollection &paged, column_t col_idx)
    : WindowCursor(paged, vector<column_t>(1, col_idx)) {
}

struct WindowInputExpression {
	WindowInputExpression(DataChunk &chunk, column_t col_idx)
	    : ptype(PhysicalType::INVALID), scalar(true), chunk(chunk), col_idx(col_idx) {
		if (col_idx < chunk.data.size()) {
			auto &col = chunk.data[col_idx];
			ptype = col.GetType().InternalType();
			scalar = (col.GetVectorType() == VectorType::CONSTANT_VECTOR);
		}
	}

	inline PhysicalType InternalType() const {
		return ptype;
	}

	template <typename T>
	inline T GetCell(idx_t i) const {
		D_ASSERT(!chunk.data.empty());
		const auto data = FlatVector::GetData<T>(chunk.data[col_idx]);
		return data[scalar ? 0 : i];
	}

	inline bool CellIsNull(idx_t i) const {
		D_ASSERT(!chunk.data.empty());
		auto &col = chunk.data[col_idx];

		if (scalar) {
			return ConstantVector::IsNull(col);
		}
		return FlatVector::IsNull(col, i);
	}

	inline void CopyCell(Vector &target, idx_t target_offset, idx_t width = 1) const {
		D_ASSERT(!chunk.data.empty());
		auto &source = chunk.data[col_idx];
		auto source_offset = scalar ? 0 : target_offset;
		VectorOperations::Copy(source, target, source_offset + width, source_offset, target_offset);
	}

private:
	PhysicalType ptype;
	bool scalar;
	DataChunk &chunk;
	const column_t col_idx;
};

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

	explicit WindowColumnIterator(WindowCursor &coll, pointer pos = 0) : coll(&coll), pos(pos) {
	}

	//	Forward iterator
	inline reference operator*() const {
		return coll->GetCell<T>(0, pos);
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
		return coll->GetCell<T>(0, pos + m);
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
	// optional_ptr does not allow us to modify this, but the constructor enforces it.
	WindowCursor *coll;
	pointer pos;
};

template <typename T, typename OP>
struct OperationCompare : public std::function<bool(T, T)> {
	inline bool operator()(const T &lhs, const T &val) const {
		return OP::template Operation<T>(lhs, val);
	}
};

template <typename T, typename OP, bool FROM>
static idx_t FindTypedRangeBound(WindowCursor &over, const idx_t order_begin, const idx_t order_end,
                                 const WindowBoundary range, WindowInputExpression &boundary, const idx_t chunk_idx,
                                 const FrameBounds &prev) {
	D_ASSERT(!boundary.CellIsNull(chunk_idx));
	const auto val = boundary.GetCell<T>(chunk_idx);

	OperationCompare<T, OP> comp;

	// Check that the value we are searching for is in range.
	if (range == WindowBoundary::EXPR_PRECEDING_RANGE) {
		//	Preceding but value past the current value
		const auto cur_val = over.GetCell<T>(0, order_end - 1);
		if (comp(cur_val, val)) {
			throw OutOfRangeException("Invalid RANGE PRECEDING value");
		}
	} else {
		//	Following but value before the current value
		D_ASSERT(range == WindowBoundary::EXPR_FOLLOWING_RANGE);
		const auto cur_val = over.GetCell<T>(0, order_begin);
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
			const auto first = over.GetCell<T>(0, prev.start);
			if (!comp(val, first)) {
				//	prev.first <= val, so we can start further forward
				begin += UnsafeNumericCast<int64_t>(prev.start - order_begin);
			}
		}
		if (order_begin < prev.end && prev.end < order_end) {
			const auto second = over.GetCell<T>(0, prev.end - 1);
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
static idx_t FindRangeBound(WindowCursor &over, const idx_t order_begin, const idx_t order_end,
                            const WindowBoundary range, WindowInputExpression &boundary, const idx_t chunk_idx,
                            const FrameBounds &prev) {
	switch (boundary.InternalType()) {
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
static idx_t FindOrderedRangeBound(WindowCursor &over, const OrderType range_sense, const idx_t order_begin,
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

	WindowBoundariesState(const BoundWindowExpression &wexpr, const idx_t input_size);

	// Generate the partition start indices
	void PartitionBegin(DataChunk &bounds, idx_t row_idx, const idx_t count, bool is_jump,
	                    const ValidityMask &partition_mask);
	void PartitionEnd(DataChunk &bounds, idx_t row_idx, const idx_t count, bool is_jump,
	                  const ValidityMask &partition_mask);
	void PeerBegin(DataChunk &bounds, idx_t row_idx, const idx_t count, bool is_jump,
	               const ValidityMask &partition_mask, const ValidityMask &order_mask);
	void PeerEnd(DataChunk &bounds, idx_t row_idx, const idx_t count, const ValidityMask &partition_mask,
	             const ValidityMask &order_mask);
	void ValidBegin(DataChunk &bounds, idx_t row_idx, const idx_t count, bool is_jump,
	                const ValidityMask &partition_mask, const ValidityMask &order_mask,
	                optional_ptr<WindowCursor> range);
	void ValidEnd(DataChunk &bounds, idx_t row_idx, const idx_t count, bool is_jump, const ValidityMask &partition_mask,
	              const ValidityMask &order_mask, optional_ptr<WindowCursor> range);
	void FrameBegin(DataChunk &bounds, idx_t row_idx, const idx_t count, WindowInputExpression &boundary_begin,
	                optional_ptr<WindowCursor> range);
	void FrameEnd(DataChunk &bounds, idx_t row_idx, const idx_t count, WindowInputExpression &boundary_end,
	              optional_ptr<WindowCursor> range);

	static void ClampFrame(const idx_t count, idx_t *values, const idx_t *begin, const idx_t *end) {
		for (idx_t i = 0; i < count; ++i) {
			values[i] = MinValue(MaxValue(values[i], begin[i]), end[i]);
		}
	}

	void Bounds(DataChunk &bounds, idx_t row_idx, optional_ptr<WindowCursor> range, const idx_t count,
	            WindowInputExpression &boundary_start, WindowInputExpression &boundary_end,
	            const ValidityMask &partition_mask, const ValidityMask &order_mask);

	// Cached lookups
	WindowBoundsSet required;
	const ExpressionType type;
	const idx_t input_size;
	const WindowBoundary start_boundary;
	const WindowBoundary end_boundary;
	const size_t partition_count;
	const size_t order_count;
	const OrderType range_sense;
	const bool has_preceding_range;
	const bool has_following_range;

	// Carried between chunks
	idx_t next_pos = 0;
	idx_t partition_start = 0;
	idx_t partition_end = 0;
	idx_t peer_start = 0;
	idx_t valid_start = 0;
	idx_t valid_end = 0;

	FrameBounds prev;
};

//===--------------------------------------------------------------------===//
// WindowBoundariesState
//===--------------------------------------------------------------------===//
static bool HasPrecedingRange(const BoundWindowExpression &wexpr) {
	return (wexpr.start == WindowBoundary::EXPR_PRECEDING_RANGE || wexpr.end == WindowBoundary::EXPR_PRECEDING_RANGE);
}

static bool HasFollowingRange(const BoundWindowExpression &wexpr) {
	return (wexpr.start == WindowBoundary::EXPR_FOLLOWING_RANGE || wexpr.end == WindowBoundary::EXPR_FOLLOWING_RANGE);
}

static WindowBoundsSet GetWindowBounds(const BoundWindowExpression &wexpr) {
	const auto partition_count = wexpr.partitions.size();
	const auto order_count = wexpr.orders.size();

	WindowBoundsSet result;
	switch (wexpr.type) {
	case ExpressionType::WINDOW_ROW_NUMBER:
		result.insert(PARTITION_BEGIN);
		break;
	case ExpressionType::WINDOW_RANK_DENSE:
	case ExpressionType::WINDOW_RANK:
		result.insert(PARTITION_BEGIN);
		result.insert(PEER_BEGIN);
		break;
	case ExpressionType::WINDOW_PERCENT_RANK:
		result.insert(PARTITION_BEGIN);
		result.insert(PARTITION_END);
		result.insert(PEER_BEGIN);
		break;
	case ExpressionType::WINDOW_CUME_DIST:
		result.insert(PARTITION_BEGIN);
		result.insert(PARTITION_END);
		result.insert(PEER_END);
		break;
	case ExpressionType::WINDOW_NTILE:
	case ExpressionType::WINDOW_LEAD:
	case ExpressionType::WINDOW_LAG:
		result.insert(PARTITION_BEGIN);
		result.insert(PARTITION_END);
		break;
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_LAST_VALUE:
	case ExpressionType::WINDOW_NTH_VALUE:
	case ExpressionType::WINDOW_AGGREGATE:
		result.insert(PARTITION_BEGIN);
		result.insert(PARTITION_END);
		result.insert(FRAME_BEGIN);
		result.insert(FRAME_END);

		// if we have EXCLUDE GROUP / TIES, we also need peer boundaries
		if (wexpr.exclude_clause != WindowExcludeMode::NO_OTHER) {
			result.insert(PEER_BEGIN);
			result.insert(PEER_END);
		}

		// If the frames are RANGE, then we need peer boundaries
		// If they are preceding or following, we also need to know
		// where the valid values begin or end.
		switch (wexpr.start) {
		case WindowBoundary::CURRENT_ROW_RANGE:
			result.insert(PEER_BEGIN);
			break;
		case WindowBoundary::EXPR_PRECEDING_RANGE:
			result.insert(PEER_BEGIN);
			result.insert(VALID_BEGIN);
			result.insert(VALID_END);
			break;
		case WindowBoundary::EXPR_FOLLOWING_RANGE:
			result.insert(PEER_BEGIN);
			result.insert(VALID_END);
			break;
		default:
			break;
		}

		switch (wexpr.end) {
		case WindowBoundary::CURRENT_ROW_RANGE:
			result.insert(PEER_END);
			break;
		case WindowBoundary::EXPR_PRECEDING_RANGE:
			result.insert(PEER_END);
			result.insert(VALID_BEGIN);
			break;
		case WindowBoundary::EXPR_FOLLOWING_RANGE:
			result.insert(PEER_END);
			result.insert(VALID_BEGIN);
			result.insert(VALID_END);
			break;
		default:
			break;
		}
		break;
	default:
		throw InternalException("Window aggregate type %s", ExpressionTypeToString(wexpr.type));
	}

	//	Internal dependencies
	if (result.count(VALID_END)) {
		result.insert(PARTITION_END);
		if (HasFollowingRange(wexpr)) {
			result.insert(VALID_BEGIN);
		}
	}
	if (result.count(VALID_BEGIN)) {
		result.insert(PARTITION_BEGIN);
		result.insert(PARTITION_END);
	}
	if (result.count(PEER_END)) {
		result.insert(PARTITION_END);
		if (order_count) {
			result.insert(PEER_BEGIN);
		}
	}
	if (result.count(PARTITION_END) && (partition_count + order_count)) {
		result.insert(PARTITION_BEGIN);
	}

	return result;
}

WindowBoundariesState::WindowBoundariesState(const BoundWindowExpression &wexpr, const idx_t input_size)
    : required(GetWindowBounds(wexpr)), type(wexpr.type), input_size(input_size), start_boundary(wexpr.start),
      end_boundary(wexpr.end), partition_count(wexpr.partitions.size()), order_count(wexpr.orders.size()),
      range_sense(wexpr.orders.empty() ? OrderType::INVALID : wexpr.orders[0].type),
      has_preceding_range(HasPrecedingRange(wexpr)), has_following_range(HasFollowingRange(wexpr)) {
}

void WindowBoundariesState::Bounds(DataChunk &bounds, idx_t row_idx, optional_ptr<WindowCursor> range,
                                   const idx_t count, WindowInputExpression &boundary_start,
                                   WindowInputExpression &boundary_end, const ValidityMask &partition_mask,
                                   const ValidityMask &order_mask) {
	bounds.Reset();
	D_ASSERT(bounds.ColumnCount() == 8);

	const auto is_jump = (next_pos != row_idx);
	if (required.count(PARTITION_BEGIN)) {
		PartitionBegin(bounds, row_idx, count, is_jump, partition_mask);
	}
	if (required.count(PARTITION_END)) {
		PartitionEnd(bounds, row_idx, count, is_jump, partition_mask);
	}
	if (required.count(PEER_BEGIN)) {
		PeerBegin(bounds, row_idx, count, is_jump, partition_mask, order_mask);
	}
	if (required.count(PEER_END)) {
		PeerEnd(bounds, row_idx, count, partition_mask, order_mask);
	}
	if (required.count(VALID_BEGIN)) {
		ValidBegin(bounds, row_idx, count, is_jump, partition_mask, order_mask, range);
	}
	if (required.count(VALID_END)) {
		ValidEnd(bounds, row_idx, count, is_jump, partition_mask, order_mask, range);
	}
	if (required.count(FRAME_BEGIN)) {
		FrameBegin(bounds, row_idx, count, boundary_start, range);
	}
	if (required.count(FRAME_END)) {
		FrameEnd(bounds, row_idx, count, boundary_end, range);
	}
	next_pos += count;

	bounds.SetCardinality(count);
}

void WindowBoundariesState::PartitionBegin(DataChunk &bounds, idx_t row_idx, const idx_t count, bool is_jump,
                                           const ValidityMask &partition_mask) {
	auto partition_begin_data = FlatVector::GetData<idx_t>(bounds.data[PARTITION_BEGIN]);

	//	OVER()
	if (partition_count + order_count == 0) {
		for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
			partition_begin_data[chunk_idx] = 0;
		}
		return;
	}

	for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
		// determine partition and peer group boundaries to ultimately figure out window size
		const auto is_same_partition = !partition_mask.RowIsValidUnsafe(row_idx);

		// when the partition changes, recompute the boundaries
		if (!is_same_partition || is_jump) {
			if (is_jump) {
				idx_t n = 1;
				partition_start = FindPrevStart(partition_mask, 0, row_idx + 1, n);
				is_jump = false;
			} else {
				partition_start = row_idx;
			}
		}

		partition_begin_data[chunk_idx] = partition_start;
	}
}

void WindowBoundariesState::PartitionEnd(DataChunk &bounds, idx_t row_idx, const idx_t count, bool is_jump,
                                         const ValidityMask &partition_mask) {
	auto partition_end_data = FlatVector::GetData<idx_t>(bounds.data[PARTITION_END]);

	//	OVER()
	if (partition_count + order_count == 0) {
		for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
			partition_end_data[chunk_idx] = input_size;
		}
		return;
	}

	auto partition_begin_data = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_BEGIN]);
	for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
		// determine partition and peer group boundaries to ultimately figure out window size
		const auto is_same_partition = !partition_mask.RowIsValidUnsafe(row_idx);

		// when the partition changes, recompute the boundaries
		if (!is_same_partition || is_jump) {
			// find end of partition
			partition_end = input_size;
			if (partition_count) {
				const auto partition_begin = partition_begin_data[chunk_idx];
				idx_t n = 1;
				partition_end = FindNextStart(partition_mask, partition_begin + 1, input_size, n);
			}
			is_jump = false;
		}

		partition_end_data[chunk_idx] = partition_end;
	}
}

void WindowBoundariesState::PeerBegin(DataChunk &bounds, idx_t row_idx, const idx_t count, bool is_jump,
                                      const ValidityMask &partition_mask, const ValidityMask &order_mask) {

	auto peer_begin_data = FlatVector::GetData<idx_t>(bounds.data[PEER_BEGIN]);

	//	OVER()
	if (partition_count + order_count == 0) {
		for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
			peer_begin_data[chunk_idx] = 0;
		}
		return;
	}

	for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
		// determine partition and peer group boundaries to ultimately figure out window size
		const auto is_same_partition = !partition_mask.RowIsValidUnsafe(row_idx);
		const auto is_peer = !order_mask.RowIsValidUnsafe(row_idx);

		// when the partition changes, recompute the boundaries
		if (!is_same_partition || is_jump) {
			// find end of partition
			if (is_jump) {
				idx_t n = 1;
				peer_start = FindPrevStart(order_mask, 0, row_idx + 1, n);
			} else {
				peer_start = row_idx;
			}
			is_jump = false;
		} else if (!is_peer) {
			peer_start = row_idx;
		}

		peer_begin_data[chunk_idx] = peer_start;
	}
}

void WindowBoundariesState::PeerEnd(DataChunk &bounds, idx_t row_idx, const idx_t count,
                                    const ValidityMask &partition_mask, const ValidityMask &order_mask) {
	//	OVER()
	if (!order_count) {
		bounds.data[PEER_END].Reference(bounds.data[PARTITION_END]);
		return;
	}

	auto partition_end_data = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_END]);
	auto peer_begin_data = FlatVector::GetData<const idx_t>(bounds.data[PEER_BEGIN]);
	auto peer_end_data = FlatVector::GetData<idx_t>(bounds.data[PEER_END]);
	for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
		idx_t n = 1;
		const auto peer_start = peer_begin_data[chunk_idx];
		const auto partition_end = partition_end_data[chunk_idx];
		peer_end_data[chunk_idx] = FindNextStart(order_mask, peer_start + 1, partition_end, n);
	}
}

void WindowBoundariesState::ValidBegin(DataChunk &bounds, idx_t row_idx, const idx_t count, bool is_jump,
                                       const ValidityMask &partition_mask, const ValidityMask &order_mask,
                                       optional_ptr<WindowCursor> range) {
	auto partition_begin_data = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_BEGIN]);
	auto partition_end_data = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_END]);
	auto valid_begin_data = FlatVector::GetData<idx_t>(bounds.data[VALID_BEGIN]);

	//	OVER()
	D_ASSERT(partition_count + order_count != 0);
	D_ASSERT(range);

	for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
		const auto is_same_partition = !partition_mask.RowIsValidUnsafe(row_idx);

		if (!is_same_partition || is_jump) {
			// Find valid ordering values for the new partition
			// so we can exclude NULLs from RANGE expression computations
			valid_start = partition_begin_data[chunk_idx];
			const auto valid_end = partition_end_data[chunk_idx];

			if ((valid_start < valid_end) && has_preceding_range) {
				// Exclude any leading NULLs
				if (range->CellIsNull(0, valid_start)) {
					idx_t n = 1;
					valid_start = FindNextStart(order_mask, valid_start + 1, valid_end, n);
				}
			}
		}

		valid_begin_data[chunk_idx] = valid_start;
	}
}

void WindowBoundariesState::ValidEnd(DataChunk &bounds, idx_t row_idx, const idx_t count, bool is_jump,
                                     const ValidityMask &partition_mask, const ValidityMask &order_mask,
                                     optional_ptr<WindowCursor> range) {
	auto partition_end_data = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_END]);
	auto valid_begin_data = FlatVector::GetData<const idx_t>(bounds.data[VALID_BEGIN]);
	auto valid_end_data = FlatVector::GetData<idx_t>(bounds.data[VALID_END]);

	//	OVER()
	D_ASSERT(partition_count + order_count != 0);
	D_ASSERT(range);

	for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
		const auto is_same_partition = !partition_mask.RowIsValidUnsafe(row_idx);

		if (!is_same_partition || is_jump) {
			// Find valid ordering values for the new partition
			// so we can exclude NULLs from RANGE expression computations
			valid_end = partition_end_data[chunk_idx];

			if ((valid_start < valid_end) && has_following_range) {
				// Exclude any trailing NULLs
				const auto valid_start = valid_begin_data[chunk_idx];
				if (range->CellIsNull(0, valid_end - 1)) {
					idx_t n = 1;
					valid_end = FindPrevStart(order_mask, valid_start, valid_end, n);
				}

				//	Reset range hints
				prev.start = valid_start;
				prev.end = valid_end;
			}
		}

		valid_end_data[chunk_idx] = valid_end;
	}
}

void WindowBoundariesState::FrameBegin(DataChunk &bounds, idx_t row_idx, const idx_t count,
                                       WindowInputExpression &boundary_begin, optional_ptr<WindowCursor> range) {
	auto partition_begin_data = FlatVector::GetData<idx_t>(bounds.data[PARTITION_BEGIN]);
	auto partition_end_data = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_END]);
	auto peer_begin_data = FlatVector::GetData<idx_t>(bounds.data[PEER_BEGIN]);
	auto valid_begin_data = FlatVector::GetData<const idx_t>(bounds.data[VALID_BEGIN]);
	auto valid_end_data = FlatVector::GetData<const idx_t>(bounds.data[VALID_END]);
	auto frame_begin_data = FlatVector::GetData<idx_t>(bounds.data[FRAME_BEGIN]);

	idx_t window_start = NumericLimits<idx_t>::Maximum();

	switch (start_boundary) {
	case WindowBoundary::UNBOUNDED_PRECEDING:
		bounds.data[FRAME_BEGIN].Reference(bounds.data[PARTITION_BEGIN]);
		// No need to clamp
		return;
	case WindowBoundary::CURRENT_ROW_ROWS:
		for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
			frame_begin_data[chunk_idx] = row_idx;
		}
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		bounds.data[FRAME_BEGIN].Reference(bounds.data[PEER_BEGIN]);
		frame_begin_data = peer_begin_data;
		break;
	case WindowBoundary::EXPR_PRECEDING_ROWS:
		for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
			int64_t computed_start;
			if (!TrySubtractOperator::Operation(static_cast<int64_t>(row_idx),
			                                    boundary_begin.GetCell<int64_t>(chunk_idx), computed_start)) {
				window_start = partition_begin_data[chunk_idx];
			} else {
				window_start = UnsafeNumericCast<idx_t>(MaxValue<int64_t>(computed_start, 0));
			}
			frame_begin_data[chunk_idx] = window_start;
		}
		break;
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
		for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
			int64_t computed_start;
			if (!TryAddOperator::Operation(static_cast<int64_t>(row_idx), boundary_begin.GetCell<int64_t>(chunk_idx),
			                               computed_start)) {
				window_start = partition_begin_data[chunk_idx];
			} else {
				window_start = UnsafeNumericCast<idx_t>(MaxValue<int64_t>(computed_start, 0));
			}
			frame_begin_data[chunk_idx] = window_start;
		}
		break;
	case WindowBoundary::EXPR_PRECEDING_RANGE:
		for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
			if (boundary_begin.CellIsNull(chunk_idx)) {
				window_start = peer_begin_data[chunk_idx];
			} else {
				const auto valid_start = valid_begin_data[chunk_idx];
				prev.end = valid_end_data[chunk_idx];
				window_start = FindOrderedRangeBound<true>(*range, range_sense, valid_start, row_idx + 1,
				                                           start_boundary, boundary_begin, chunk_idx, prev);
				prev.start = window_start;
			}
			frame_begin_data[chunk_idx] = window_start;
		}
		break;
	case WindowBoundary::EXPR_FOLLOWING_RANGE:
		for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
			if (boundary_begin.CellIsNull(chunk_idx)) {
				window_start = peer_begin_data[chunk_idx];
			} else {
				const auto valid_end = valid_end_data[chunk_idx];
				prev.end = valid_end;
				window_start = FindOrderedRangeBound<true>(*range, range_sense, row_idx, valid_end, start_boundary,
				                                           boundary_begin, chunk_idx, prev);
				prev.start = window_start;
			}
			frame_begin_data[chunk_idx] = window_start;
		}
		break;
	default:
		throw InternalException("Unsupported window start boundary");
	}

	ClampFrame(count, frame_begin_data, partition_begin_data, partition_end_data);
}

void WindowBoundariesState::FrameEnd(DataChunk &bounds, idx_t row_idx, const idx_t count,
                                     WindowInputExpression &boundary_end, optional_ptr<WindowCursor> range) {
	auto partition_begin_data = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_BEGIN]);
	auto partition_end_data = FlatVector::GetData<idx_t>(bounds.data[PARTITION_END]);
	auto peer_end_data = FlatVector::GetData<idx_t>(bounds.data[PEER_END]);
	auto valid_begin_data = FlatVector::GetData<const idx_t>(bounds.data[VALID_BEGIN]);
	auto valid_end_data = FlatVector::GetData<const idx_t>(bounds.data[VALID_END]);
	auto frame_end_data = FlatVector::GetData<idx_t>(bounds.data[FRAME_END]);

	idx_t window_end = NumericLimits<idx_t>::Maximum();

	switch (end_boundary) {
	case WindowBoundary::CURRENT_ROW_ROWS:
		for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
			frame_end_data[chunk_idx] = row_idx + 1;
		}
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		bounds.data[FRAME_END].Reference(bounds.data[PEER_END]);
		frame_end_data = peer_end_data;
		break;
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		bounds.data[FRAME_END].Reference(bounds.data[PARTITION_END]);
		// No need to clamp
		return;
	case WindowBoundary::EXPR_PRECEDING_ROWS: {
		for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
			int64_t computed_start;
			if (!TrySubtractOperator::Operation(int64_t(row_idx + 1), boundary_end.GetCell<int64_t>(chunk_idx),
			                                    computed_start)) {
				window_end = partition_end_data[chunk_idx];
			} else {
				window_end = UnsafeNumericCast<idx_t>(MaxValue<int64_t>(computed_start, 0));
			}
			frame_end_data[chunk_idx] = window_end;
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
		for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
			int64_t computed_start;
			if (!TryAddOperator::Operation(int64_t(row_idx + 1), boundary_end.GetCell<int64_t>(chunk_idx),
			                               computed_start)) {
				window_end = partition_end_data[chunk_idx];
			} else {
				window_end = UnsafeNumericCast<idx_t>(MaxValue<int64_t>(computed_start, 0));
			}
			frame_end_data[chunk_idx] = window_end;
		}
		break;
	case WindowBoundary::EXPR_PRECEDING_RANGE:
		for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
			if (boundary_end.CellIsNull(chunk_idx)) {
				window_end = peer_end_data[chunk_idx];
			} else {
				const auto valid_start = valid_begin_data[chunk_idx];
				prev.start = valid_start;
				window_end = FindOrderedRangeBound<false>(*range, range_sense, valid_start, row_idx + 1, end_boundary,
				                                          boundary_end, chunk_idx, prev);
				prev.end = window_end;
			}
			frame_end_data[chunk_idx] = window_end;
		}
		break;
	case WindowBoundary::EXPR_FOLLOWING_RANGE:
		for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
			if (boundary_end.CellIsNull(chunk_idx)) {
				window_end = peer_end_data[chunk_idx];
			} else {
				const auto valid_end = valid_end_data[chunk_idx];
				prev.start = valid_begin_data[chunk_idx];
				window_end = FindOrderedRangeBound<false>(*range, range_sense, row_idx, valid_end, end_boundary,
				                                          boundary_end, chunk_idx, prev);
				prev.end = window_end;
			}
			frame_end_data[chunk_idx] = window_end;
		}
		break;
	default:
		throw InternalException("Unsupported window end boundary");
	}

	ClampFrame(count, frame_end_data, partition_begin_data, partition_end_data);
}

//===--------------------------------------------------------------------===//
// WindowExecutorBoundsState
//===--------------------------------------------------------------------===//
class WindowExecutorBoundsState : public WindowExecutorLocalState {
public:
	explicit WindowExecutorBoundsState(const WindowExecutorGlobalState &gstate);
	~WindowExecutorBoundsState() override {
	}

	virtual void UpdateBounds(WindowExecutorGlobalState &gstate, idx_t row_idx, DataChunk &eval_chunk,
	                          optional_ptr<WindowCursor> range);

	// Frame management
	const ValidityMask &partition_mask;
	const ValidityMask &order_mask;
	DataChunk bounds;
	WindowBoundariesState state;
};

WindowExecutorBoundsState::WindowExecutorBoundsState(const WindowExecutorGlobalState &gstate)
    : WindowExecutorLocalState(gstate), partition_mask(gstate.partition_mask), order_mask(gstate.order_mask),
      state(gstate.executor.wexpr, gstate.payload_count) {
	vector<LogicalType> bounds_types(8, LogicalType(LogicalTypeId::UBIGINT));
	bounds.Initialize(Allocator::Get(gstate.executor.context), bounds_types);
}

void WindowExecutorBoundsState::UpdateBounds(WindowExecutorGlobalState &gstate, idx_t row_idx, DataChunk &eval_chunk,
                                             optional_ptr<WindowCursor> range) {
	// Evaluate the row-level arguments
	WindowInputExpression boundary_start(eval_chunk, gstate.executor.boundary_start_idx);
	WindowInputExpression boundary_end(eval_chunk, gstate.executor.boundary_end_idx);

	const auto count = eval_chunk.size();
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

column_t WindowSharedExpressions::RegisterExpr(const unique_ptr<Expression> &expr, Shared &shared) {
	auto pexpr = expr.get();
	if (!pexpr) {
		return DConstants::INVALID_INDEX;
	}

	//	We need to make separate columns for volatile arguments
	const auto is_volatile = expr->IsVolatile();
	auto i = shared.columns.find(*pexpr);
	if (i != shared.columns.end() && !is_volatile) {
		return i->second.front();
	}

	// New column, find maximum column number
	column_t result = shared.size++;
	shared.columns[*pexpr].emplace_back(result);

	return result;
}

vector<const Expression *> WindowSharedExpressions::GetSortedExpressions(Shared &shared) {
	vector<const Expression *> sorted(shared.size, nullptr);
	for (auto &col : shared.columns) {
		auto &expr = col.first.get();
		for (auto col_idx : col.second) {
			sorted[col_idx] = &expr;
		}
	}

	return sorted;
}
void WindowSharedExpressions::PrepareExecutors(Shared &shared, ExpressionExecutor &exec, DataChunk &chunk) {
	const auto sorted = GetSortedExpressions(shared);
	vector<LogicalType> types;
	for (auto expr : sorted) {
		exec.AddExpression(*expr);
		types.emplace_back(expr->return_type);
	}

	if (!types.empty()) {
		chunk.Initialize(exec.GetAllocator(), types);
	}
}

//===--------------------------------------------------------------------===//
// WindowExecutor
//===--------------------------------------------------------------------===//
WindowExecutor::WindowExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared)
    : wexpr(wexpr), context(context),
      range_expr((HasPrecedingRange(wexpr) || HasFollowingRange(wexpr)) ? wexpr.orders[0].expression.get() : nullptr) {
	if (range_expr) {
		range_idx = shared.RegisterCollection(wexpr.orders[0].expression, false);
	}

	boundary_start_idx = shared.RegisterEvaluate(wexpr.start_expr);
	boundary_end_idx = shared.RegisterEvaluate(wexpr.end_expr);
}

WindowExecutorGlobalState::WindowExecutorGlobalState(const WindowExecutor &executor, const idx_t payload_count,
                                                     const ValidityMask &partition_mask, const ValidityMask &order_mask)
    : executor(executor), payload_count(payload_count), partition_mask(partition_mask), order_mask(order_mask) {
	for (const auto &child : executor.wexpr.children) {
		arg_types.emplace_back(child->return_type);
	}
}

WindowExecutorLocalState::WindowExecutorLocalState(const WindowExecutorGlobalState &gstate) {
}

void WindowExecutorLocalState::Sink(WindowExecutorGlobalState &gstate, DataChunk &sink_chunk, DataChunk &coll_chunk,
                                    idx_t input_idx) {
}

void WindowExecutorLocalState::Finalize(WindowExecutorGlobalState &gstate, CollectionPtr collection) {
	const auto range_idx = gstate.executor.range_idx;
	if (range_idx != DConstants::INVALID_INDEX) {
		range_cursor = make_uniq<WindowCursor>(*collection, range_idx);
	}
}

unique_ptr<WindowExecutorGlobalState> WindowExecutor::GetGlobalState(const idx_t payload_count,
                                                                     const ValidityMask &partition_mask,
                                                                     const ValidityMask &order_mask) const {
	return make_uniq<WindowExecutorGlobalState>(*this, payload_count, partition_mask, order_mask);
}

unique_ptr<WindowExecutorLocalState> WindowExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowExecutorBoundsState>(gstate);
}

void WindowExecutor::Sink(DataChunk &sink_chunk, DataChunk &coll_chunk, const idx_t input_idx,
                          WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate) const {
	lstate.Sink(gstate, sink_chunk, coll_chunk, input_idx);
}

void WindowExecutor::Finalize(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                              CollectionPtr collection) const {
	lstate.Finalize(gstate, collection);
}

//===--------------------------------------------------------------------===//
// WindowAggregateExecutor
//===--------------------------------------------------------------------===//
class WindowAggregateExecutorGlobalState : public WindowExecutorGlobalState {
public:
	WindowAggregateExecutorGlobalState(const WindowAggregateExecutor &executor, const idx_t payload_count,
	                                   const ValidityMask &partition_mask, const ValidityMask &order_mask);

	// aggregate global state
	unique_ptr<WindowAggregatorState> gsink;

	// the filter reference expression.
	const Expression *filter_ref;
};

bool WindowAggregateExecutor::IsConstantAggregate() {
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

bool WindowAggregateExecutor::IsDistinctAggregate() {
	if (!wexpr.aggregate) {
		return false;
	}

	return wexpr.distinct;
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

void WindowExecutor::Evaluate(idx_t row_idx, DataChunk &eval_chunk, Vector &result, WindowExecutorLocalState &lstate,
                              WindowExecutorGlobalState &gstate) const {
	auto &lbstate = lstate.Cast<WindowExecutorBoundsState>();
	lbstate.UpdateBounds(gstate, row_idx, eval_chunk, lstate.range_cursor);

	const auto count = eval_chunk.size();
	EvaluateInternal(gstate, lstate, eval_chunk, result, count, row_idx);

	result.Verify(count);
}

WindowAggregateExecutor::WindowAggregateExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                 WindowSharedExpressions &shared, WindowAggregationMode mode)
    : WindowExecutor(wexpr, context, shared), mode(mode) {
	auto return_type = wexpr.return_type;

	// Force naive for SEPARATE mode or for (currently!) unsupported functionality
	const auto force_naive =
	    !ClientConfig::GetConfig(context).enable_optimizer || mode == WindowAggregationMode::SEPARATE;
	if (force_naive || (wexpr.distinct && wexpr.exclude_clause != WindowExcludeMode::NO_OTHER)) {
		aggregator = make_uniq<WindowNaiveAggregator>(wexpr, wexpr.exclude_clause, shared);
	} else if (IsDistinctAggregate()) {
		// build a merge sort tree
		// see https://dl.acm.org/doi/pdf/10.1145/3514221.3526184
		aggregator = make_uniq<WindowDistinctAggregator>(wexpr, wexpr.exclude_clause, shared, context);
	} else if (IsConstantAggregate()) {
		aggregator = make_uniq<WindowConstantAggregator>(wexpr, wexpr.exclude_clause, shared);
	} else if (IsCustomAggregate()) {
		aggregator = make_uniq<WindowCustomAggregator>(wexpr, wexpr.exclude_clause, shared);
	} else {
		// build a segment tree for frame-adhering aggregates
		// see http://www.vldb.org/pvldb/vol8/p1058-leis.pdf
		aggregator = make_uniq<WindowSegmentTree>(wexpr, mode, wexpr.exclude_clause, shared);
	}

	// Compute the FILTER with the other eval columns.
	// Anyone who needs it can then convert it to the form they need.
	if (wexpr.filter_expr) {
		const auto filter_idx = shared.RegisterSink(wexpr.filter_expr);
		filter_ref = make_uniq<BoundReferenceExpression>(wexpr.filter_expr->return_type, filter_idx);
	}
}

WindowAggregateExecutorGlobalState::WindowAggregateExecutorGlobalState(const WindowAggregateExecutor &executor,
                                                                       const idx_t group_count,
                                                                       const ValidityMask &partition_mask,
                                                                       const ValidityMask &order_mask)
    : WindowExecutorGlobalState(executor, group_count, partition_mask, order_mask),
      filter_ref(executor.filter_ref.get()) {
	gsink = executor.aggregator->GetGlobalState(executor.context, group_count, partition_mask);
}

unique_ptr<WindowExecutorGlobalState> WindowAggregateExecutor::GetGlobalState(const idx_t payload_count,
                                                                              const ValidityMask &partition_mask,
                                                                              const ValidityMask &order_mask) const {
	return make_uniq<WindowAggregateExecutorGlobalState>(*this, payload_count, partition_mask, order_mask);
}

class WindowAggregateExecutorLocalState : public WindowExecutorBoundsState {
public:
	WindowAggregateExecutorLocalState(const WindowExecutorGlobalState &gstate, const WindowAggregator &aggregator)
	    : WindowExecutorBoundsState(gstate), filter_executor(gstate.executor.context) {

		auto &gastate = gstate.Cast<WindowAggregateExecutorGlobalState>();
		aggregator_state = aggregator.GetLocalState(*gastate.gsink);

		// evaluate the FILTER clause and stuff it into a large mask for compactness and reuse
		auto filter_ref = gastate.filter_ref;
		if (filter_ref) {
			filter_executor.AddExpression(*filter_ref);
			filter_sel.Initialize(STANDARD_VECTOR_SIZE);
		}
	}

public:
	// state of aggregator
	unique_ptr<WindowAggregatorState> aggregator_state;
	//! Executor for any filter clause
	ExpressionExecutor filter_executor;
	//! Result of filtering
	SelectionVector filter_sel;
};

unique_ptr<WindowExecutorLocalState>
WindowAggregateExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowAggregateExecutorLocalState>(gstate, *aggregator);
}

void WindowAggregateExecutor::Sink(DataChunk &sink_chunk, DataChunk &coll_chunk, const idx_t input_idx,
                                   WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate) const {
	auto &gastate = gstate.Cast<WindowAggregateExecutorGlobalState>();
	auto &lastate = lstate.Cast<WindowAggregateExecutorLocalState>();
	auto &filter_sel = lastate.filter_sel;
	auto &filter_executor = lastate.filter_executor;

	idx_t filtered = 0;
	SelectionVector *filtering = nullptr;
	if (gastate.filter_ref) {
		filtering = &filter_sel;
		filtered = filter_executor.SelectExpression(sink_chunk, filter_sel);
	}

	D_ASSERT(aggregator);
	auto &gestate = *gastate.gsink;
	auto &lestate = *lastate.aggregator_state;
	aggregator->Sink(gestate, lestate, sink_chunk, coll_chunk, input_idx, filtering, filtered);

	WindowExecutor::Sink(sink_chunk, coll_chunk, input_idx, gstate, lstate);
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

void WindowAggregateExecutor::Finalize(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                       CollectionPtr collection) const {
	WindowExecutor::Finalize(gstate, lstate, collection);

	auto &gastate = gstate.Cast<WindowAggregateExecutorGlobalState>();
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

	auto &lastate = lstate.Cast<WindowAggregateExecutorLocalState>();
	aggregator->Finalize(*gsink, *lastate.aggregator_state, collection, stats);
}

void WindowAggregateExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                               DataChunk &eval_chunk, Vector &result, idx_t count,
                                               idx_t row_idx) const {
	auto &gastate = gstate.Cast<WindowAggregateExecutorGlobalState>();
	auto &lastate = lstate.Cast<WindowAggregateExecutorLocalState>();
	auto &gsink = gastate.gsink;
	D_ASSERT(aggregator);

	auto &agg_state = *lastate.aggregator_state;

	aggregator->Evaluate(*gsink, agg_state, lastate.bounds, result, count, row_idx);
}

//===--------------------------------------------------------------------===//
// WindowRowNumberExecutor
//===--------------------------------------------------------------------===//
WindowRowNumberExecutor::WindowRowNumberExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                 WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {
}

void WindowRowNumberExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                               DataChunk &eval_chunk, Vector &result, idx_t count,
                                               idx_t row_idx) const {
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

WindowRankExecutor::WindowRankExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                       WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {
}

unique_ptr<WindowExecutorLocalState> WindowRankExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowPeerState>(gstate);
}

void WindowRankExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                          DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx) const {
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

WindowDenseRankExecutor::WindowDenseRankExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                 WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {
}

unique_ptr<WindowExecutorLocalState>
WindowDenseRankExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowPeerState>(gstate);
}

void WindowDenseRankExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                               DataChunk &eval_chunk, Vector &result, idx_t count,
                                               idx_t row_idx) const {
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
		ValidityMask tail_mask(order_mask.GetData() + begin_idx, end_idx - begin_idx);
		lpeer.dense_rank += tail_mask.CountValid(order_end - order_begin);
	}

	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		lpeer.NextRank(partition_begin[i], peer_begin[i], row_idx);
		rdata[i] = NumericCast<int64_t>(lpeer.dense_rank);
	}
}

WindowPercentRankExecutor::WindowPercentRankExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                     WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {
}

unique_ptr<WindowExecutorLocalState>
WindowPercentRankExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowPeerState>(gstate);
}

void WindowPercentRankExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                                 DataChunk &eval_chunk, Vector &result, idx_t count,
                                                 idx_t row_idx) const {
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
		auto denom = static_cast<double>(NumericCast<int64_t>(partition_end[i] - partition_begin[i] - 1));
		double percent_rank = denom > 0 ? ((double)lpeer.rank - 1) / denom : 0;
		rdata[i] = percent_rank;
	}
}

//===--------------------------------------------------------------------===//
// WindowCumeDistExecutor
//===--------------------------------------------------------------------===//
WindowCumeDistExecutor::WindowCumeDistExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                               WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {
}

void WindowCumeDistExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                              DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx) const {
	auto &lbstate = lstate.Cast<WindowExecutorBoundsState>();
	auto partition_begin = FlatVector::GetData<const idx_t>(lbstate.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(lbstate.bounds.data[PARTITION_END]);
	auto peer_end = FlatVector::GetData<const idx_t>(lbstate.bounds.data[PEER_END]);
	auto rdata = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		auto denom = static_cast<double>(NumericCast<int64_t>(partition_end[i] - partition_begin[i]));
		double cume_dist = denom > 0 ? ((double)(peer_end[i] - partition_begin[i])) / denom : 0;
		rdata[i] = cume_dist;
	}
}

//===--------------------------------------------------------------------===//
// WindowValueGlobalState
//===--------------------------------------------------------------------===//

class WindowValueGlobalState : public WindowExecutorGlobalState {
public:
	using WindowCollectionPtr = unique_ptr<WindowCollection>;
	WindowValueGlobalState(const WindowValueExecutor &executor, const idx_t payload_count,
	                       const ValidityMask &partition_mask, const ValidityMask &order_mask)
	    : WindowExecutorGlobalState(executor, payload_count, partition_mask, order_mask), ignore_nulls(&all_valid),
	      child_idx(executor.child_idx) {
	}

	void Finalize(CollectionPtr collection) {
		if (child_idx != DConstants::INVALID_INDEX && executor.wexpr.ignore_nulls) {
			lock_guard<mutex> ignore_nulls_guard(lock);
			ignore_nulls = &collection->validities[child_idx];
		}
	}

	// IGNORE NULLS
	mutex lock;
	ValidityMask all_valid;
	optional_ptr<ValidityMask> ignore_nulls;

	const column_t child_idx;
};

//===--------------------------------------------------------------------===//
// WindowValueLocalState
//===--------------------------------------------------------------------===//

//! A class representing the state of the first_value, last_value and nth_value functions
class WindowValueLocalState : public WindowExecutorBoundsState {
public:
	explicit WindowValueLocalState(const WindowValueGlobalState &gvstate)
	    : WindowExecutorBoundsState(gvstate), gvstate(gvstate) {
	}

	//! Finish the sinking and prepare to scan
	void Finalize(WindowExecutorGlobalState &gstate, CollectionPtr collection) override;

	//! The corresponding global value state
	const WindowValueGlobalState &gvstate;
	//! The exclusion filter handler
	unique_ptr<ExclusionFilter> exclusion_filter;
	//! The validity mask that combines both the NULLs and exclusion information
	optional_ptr<ValidityMask> ignore_nulls_exclude;

	//! The state used for reading the collection
	unique_ptr<WindowCursor> cursor;
};

void WindowValueLocalState::Finalize(WindowExecutorGlobalState &gstate, CollectionPtr collection) {
	WindowExecutorBoundsState::Finalize(gstate, collection);

	// Set up the IGNORE NULLS state
	auto ignore_nulls = gvstate.ignore_nulls;
	if (gvstate.executor.wexpr.exclude_clause == WindowExcludeMode::NO_OTHER) {
		exclusion_filter = nullptr;
		ignore_nulls_exclude = ignore_nulls;
	} else {
		// create the exclusion filter based on ignore_nulls
		exclusion_filter =
		    make_uniq<ExclusionFilter>(gvstate.executor.wexpr.exclude_clause, gvstate.payload_count, *ignore_nulls);
		ignore_nulls_exclude = &exclusion_filter->mask;
	}

	// Prepare to scan
	if (!cursor && gvstate.child_idx != DConstants::INVALID_INDEX) {
		cursor = make_uniq<WindowCursor>(*collection, gvstate.child_idx);
	}
}

//===--------------------------------------------------------------------===//
// WindowValueExecutor
//===--------------------------------------------------------------------===//
WindowValueExecutor::WindowValueExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                         WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {

	//	The children have to be handled separately because only the first one is global
	if (!wexpr.children.empty()) {
		child_idx = shared.RegisterCollection(wexpr.children[0], wexpr.ignore_nulls);

		if (wexpr.children.size() > 1) {
			nth_idx = shared.RegisterEvaluate(wexpr.children[1]);
		}
	}

	offset_idx = shared.RegisterEvaluate(wexpr.offset_expr);
	default_idx = shared.RegisterEvaluate(wexpr.default_expr);
}

WindowNtileExecutor::WindowNtileExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                         WindowSharedExpressions &shared)
    : WindowValueExecutor(wexpr, context, shared) {
}

unique_ptr<WindowExecutorGlobalState> WindowValueExecutor::GetGlobalState(const idx_t payload_count,
                                                                          const ValidityMask &partition_mask,
                                                                          const ValidityMask &order_mask) const {
	return make_uniq<WindowValueGlobalState>(*this, payload_count, partition_mask, order_mask);
}

void WindowValueExecutor::Finalize(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                   CollectionPtr collection) const {
	auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	gvstate.Finalize(collection);

	WindowExecutor::Finalize(gstate, lstate, collection);
}

unique_ptr<WindowExecutorLocalState> WindowValueExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	const auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	return make_uniq<WindowValueLocalState>(gvstate);
}

void WindowNtileExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                           DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx) const {
	auto &lvstate = lstate.Cast<WindowValueLocalState>();
	auto &cursor = *lvstate.cursor;
	auto partition_begin = FlatVector::GetData<const idx_t>(lvstate.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(lvstate.bounds.data[PARTITION_END]);
	auto rdata = FlatVector::GetData<int64_t>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		if (cursor.CellIsNull(0, row_idx)) {
			FlatVector::SetNull(result, i, true);
		} else {
			auto n_param = cursor.GetCell<int64_t>(0, row_idx);
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
class WindowLeadLagLocalState : public WindowValueLocalState {
public:
	explicit WindowLeadLagLocalState(const WindowValueGlobalState &gstate) : WindowValueLocalState(gstate) {
	}
};

WindowLeadLagExecutor::WindowLeadLagExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                             WindowSharedExpressions &shared)
    : WindowValueExecutor(wexpr, context, shared) {
}

unique_ptr<WindowExecutorLocalState>
WindowLeadLagExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	const auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	return make_uniq<WindowLeadLagLocalState>(gvstate);
}

void WindowLeadLagExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                             DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx) const {
	auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	auto &ignore_nulls = gvstate.ignore_nulls;
	auto &llstate = lstate.Cast<WindowLeadLagLocalState>();
	auto &cursor = *llstate.cursor;

	WindowInputExpression leadlag_offset(eval_chunk, offset_idx);
	WindowInputExpression leadlag_default(eval_chunk, default_idx);

	bool can_shift = ignore_nulls->AllValid();
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
			offset = leadlag_offset.GetCell<int64_t>(i);
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
			val_idx = int64_t(FindPrevStart(*ignore_nulls, partition_begin[i], row_idx, delta));
		} else if (val_idx > (int64_t)row_idx) {
			delta = idx_t(idx_t(val_idx) - row_idx);
			val_idx = int64_t(FindNextStart(*ignore_nulls, row_idx + 1, partition_end[i], delta));
		}
		// else offset is zero, so don't move.

		if (can_shift) {
			const auto target_limit = MinValue(partition_end[i], row_end) - row_idx;
			if (!delta) {
				//	Copy source[index:index+width] => result[i:]
				auto index = NumericCast<idx_t>(val_idx);
				const auto source_limit = partition_end[i] - index;
				auto width = MinValue(source_limit, target_limit);
				// We may have to scan multiple blocks here, so loop until we have copied everything
				const idx_t col_idx = 0;
				while (width) {
					const auto source_offset = cursor.Seek(index);
					auto &source = cursor.chunk.data[col_idx];
					const auto copied = MinValue<idx_t>(cursor.chunk.size() - source_offset, width);
					VectorOperations::Copy(source, result, source_offset + copied, source_offset, i);
					i += copied;
					row_idx += copied;
					index += copied;
					width -= copied;
				}
			} else if (wexpr.default_expr) {
				const auto width = MinValue(delta, target_limit);
				leadlag_default.CopyCell(result, i, width);
				i += width;
				row_idx += width;
			} else {
				for (idx_t nulls = MinValue(delta, target_limit); nulls--; ++i, ++row_idx) {
					FlatVector::SetNull(result, i, true);
				}
			}
		} else {
			if (!delta) {
				cursor.CopyCell(0, NumericCast<idx_t>(val_idx), result, i);
			} else if (wexpr.default_expr) {
				leadlag_default.CopyCell(result, i);
			} else {
				FlatVector::SetNull(result, i, true);
			}
			++i;
			++row_idx;
		}
	}
}

WindowFirstValueExecutor::WindowFirstValueExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                   WindowSharedExpressions &shared)
    : WindowValueExecutor(wexpr, context, shared) {
}

void WindowFirstValueExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                                DataChunk &eval_chunk, Vector &result, idx_t count,
                                                idx_t row_idx) const {
	auto &lvstate = lstate.Cast<WindowValueLocalState>();
	auto &cursor = *lvstate.cursor;
	auto window_begin = FlatVector::GetData<const idx_t>(lvstate.bounds.data[FRAME_BEGIN]);
	auto window_end = FlatVector::GetData<const idx_t>(lvstate.bounds.data[FRAME_END]);
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
			cursor.CopyCell(0, first_idx, result, i);
		} else {
			FlatVector::SetNull(result, i, true);
		}

		if (lvstate.exclusion_filter) {
			lvstate.exclusion_filter->ResetMask(row_idx, i);
		}
	}
}

WindowLastValueExecutor::WindowLastValueExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                 WindowSharedExpressions &shared)
    : WindowValueExecutor(wexpr, context, shared) {
}

void WindowLastValueExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                               DataChunk &eval_chunk, Vector &result, idx_t count,
                                               idx_t row_idx) const {
	auto &lvstate = lstate.Cast<WindowValueLocalState>();
	auto &cursor = *lvstate.cursor;
	auto window_begin = FlatVector::GetData<const idx_t>(lvstate.bounds.data[FRAME_BEGIN]);
	auto window_end = FlatVector::GetData<const idx_t>(lvstate.bounds.data[FRAME_END]);
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
			cursor.CopyCell(0, last_idx, result, i);
		} else {
			FlatVector::SetNull(result, i, true);
		}

		if (lvstate.exclusion_filter) {
			lvstate.exclusion_filter->ResetMask(row_idx, i);
		}
	}
}

WindowNthValueExecutor::WindowNthValueExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                               WindowSharedExpressions &shared)
    : WindowValueExecutor(wexpr, context, shared) {
}

void WindowNthValueExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                              DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx) const {
	auto &lvstate = lstate.Cast<WindowValueLocalState>();
	auto &cursor = *lvstate.cursor;
	D_ASSERT(cursor.chunk.ColumnCount() == 1);
	auto window_begin = FlatVector::GetData<const idx_t>(lvstate.bounds.data[FRAME_BEGIN]);
	auto window_end = FlatVector::GetData<const idx_t>(lvstate.bounds.data[FRAME_END]);
	WindowInputExpression nth_col(eval_chunk, nth_idx);
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
		if (nth_col.CellIsNull(row_idx)) {
			FlatVector::SetNull(result, i, true);
		} else {
			auto n_param = nth_col.GetCell<int64_t>(row_idx);
			if (n_param < 1) {
				FlatVector::SetNull(result, i, true);
			} else {
				auto n = idx_t(n_param);
				const auto nth_index = FindNextStart(*lvstate.ignore_nulls_exclude, window_begin[i], window_end[i], n);
				if (!n) {
					cursor.CopyCell(0, nth_index, result, i);
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
