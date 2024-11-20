//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/window_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/window_segment_tree.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

// A wrapper for building ColumnDataCollections in parallel
class WindowCollection {
public:
	using ColumnDataCollectionPtr = unique_ptr<ColumnDataCollection>;
	using ColumnDataCollectionSpec = pair<idx_t, optional_ptr<ColumnDataCollection>>;
	using ColumnSet = unordered_set<column_t>;

	WindowCollection(BufferManager &buffer_manager, idx_t count, const vector<LogicalType> &types);

	idx_t ColumnCount() const {
		return types.size();
	}

	idx_t size() const { // NOLINT
		return count;
	}

	const vector<LogicalType> &GetTypes() const {
		return types;
	}

	//! Update a thread-local collection for appending data to a given row
	void GetCollection(idx_t row_idx, ColumnDataCollectionSpec &spec);
	//! Single-threaded, idempotent ordered combining of all the appended data.
	void Combine(const ColumnSet &build_validity);

	//! The collection data. May be null if the column count is 0.
	ColumnDataCollectionPtr inputs;
	//! Global validity mask
	vector<atomic<bool>> all_valids;
	//! Optional validity mask for the entire collection
	vector<ValidityMask> validities;

	//! The collection columns
	const vector<LogicalType> types;
	//! The collection rows
	const idx_t count;

	//! Guard for range updates
	mutex lock;
	//! The paging buffer manager to use
	BufferManager &buffer_manager;
	//! The component column data collections
	vector<ColumnDataCollectionPtr> collections;
	//! The (sorted) collection ranges
	using Range = pair<idx_t, idx_t>;
	vector<Range> ranges;
};

class WindowBuilder {
public:
	explicit WindowBuilder(WindowCollection &collection);

	//! Add a new chunk at the given index
	void Sink(DataChunk &chunk, idx_t input_idx);

	//! The collection we are helping to build
	WindowCollection &collection;
	//! The thread's current input collection
	using ColumnDataCollectionSpec = WindowCollection::ColumnDataCollectionSpec;
	ColumnDataCollectionSpec sink;
	//! The state used for appending to the collection
	ColumnDataAppendState appender;
	//! Are all the sunk rows valid?
	bool all_valid = true;
};

class WindowCursor {
public:
	WindowCursor(const WindowCollection &paged, column_t col_idx);
	WindowCursor(const WindowCollection &paged, vector<column_t> column_ids);

	//! Is the scan in range?
	inline bool RowIsVisible(idx_t row_idx) const {
		return (row_idx < state.next_row_index && state.current_row_index <= row_idx);
	}
	//! The offset of the row in the given state
	inline sel_t RowOffset(idx_t row_idx) const {
		D_ASSERT(RowIsVisible(row_idx));
		return UnsafeNumericCast<sel_t>(row_idx - state.current_row_index);
	}
	//! Scan the next chunk
	inline bool Scan() {
		return paged.inputs->Scan(state, chunk);
	}
	//! Seek to the given row
	inline idx_t Seek(idx_t row_idx) {
		if (!RowIsVisible(row_idx)) {
			D_ASSERT(paged.inputs.get());
			paged.inputs->Seek(row_idx, state, chunk);
		}
		return RowOffset(row_idx);
	}
	//! Check a collection cell for nullity
	bool CellIsNull(idx_t col_idx, idx_t row_idx) {
		D_ASSERT(chunk.ColumnCount() > col_idx);
		auto index = Seek(row_idx);
		auto &source = chunk.data[col_idx];
		return FlatVector::IsNull(source, index);
	}
	//! Read a typed cell
	template <typename T>
	T GetCell(idx_t col_idx, idx_t row_idx) {
		D_ASSERT(chunk.ColumnCount() > col_idx);
		auto index = Seek(row_idx);
		auto &source = chunk.data[col_idx];
		const auto data = FlatVector::GetData<T>(source);
		return data[index];
	}
	//! Copy a single value
	void CopyCell(idx_t col_idx, idx_t row_idx, Vector &target, idx_t target_offset) {
		D_ASSERT(chunk.ColumnCount() > col_idx);
		auto index = Seek(row_idx);
		auto &source = chunk.data[col_idx];
		VectorOperations::Copy(source, target, index + 1, index, target_offset);
	}

	unique_ptr<WindowCursor> Copy() const {
		return make_uniq<WindowCursor>(paged, state.column_ids);
	}

	//! The pageable data
	const WindowCollection &paged;
	//! The state used for reading the collection
	ColumnDataScanState state;
	//! The data chunk read into
	DataChunk chunk;
};

//	Column indexes of the bounds chunk
enum WindowBounds : uint8_t {
	PARTITION_BEGIN,
	PARTITION_END,
	PEER_BEGIN,
	PEER_END,
	VALID_BEGIN,
	VALID_END,
	FRAME_BEGIN,
	FRAME_END
};

// C++ 11 won't do this automatically...
struct WindowBoundsHash {
	inline uint64_t operator()(const WindowBounds &value) const {
		return value;
	}
};

using WindowBoundsSet = unordered_set<WindowBounds, WindowBoundsHash>;

//! A shared set of expressions
struct WindowSharedExpressions {
	struct Shared {
		column_t size = 0;
		expression_map_t<vector<column_t>> columns;
	};

	//! Register a shared expression in a shared set
	static column_t RegisterExpr(const unique_ptr<Expression> &expr, Shared &shared);

	//! Register a shared collection expression
	column_t RegisterCollection(const unique_ptr<Expression> &expr, bool build_validity) {
		auto result = RegisterExpr(expr, coll_shared);
		if (build_validity) {
			coll_validity.insert(result);
		}
		return result;
	}
	//! Register a shared collection expression
	inline column_t RegisterSink(const unique_ptr<Expression> &expr) {
		return RegisterExpr(expr, sink_shared);
	}
	//! Register a shared evaluation expression
	inline column_t RegisterEvaluate(const unique_ptr<Expression> &expr) {
		return RegisterExpr(expr, eval_shared);
	}

	//! Expression layout
	static vector<const Expression *> GetSortedExpressions(Shared &shared);

	//! Expression execution utility
	static void PrepareExecutors(Shared &shared, ExpressionExecutor &exec, DataChunk &chunk);

	//! Prepare collection expressions
	inline void PrepareCollection(ExpressionExecutor &exec, DataChunk &chunk) {
		PrepareExecutors(coll_shared, exec, chunk);
	}

	//! Prepare collection expressions
	inline void PrepareSink(ExpressionExecutor &exec, DataChunk &chunk) {
		PrepareExecutors(sink_shared, exec, chunk);
	}

	//! Prepare collection expressions
	inline void PrepareEvaluate(ExpressionExecutor &exec, DataChunk &chunk) {
		PrepareExecutors(eval_shared, exec, chunk);
	}

	//! Fully materialised shared expressions
	Shared coll_shared;
	//! Sink shared expressions
	Shared sink_shared;
	//! Evaluate shared expressions
	Shared eval_shared;
	//! Requested collection validity masks
	unordered_set<column_t> coll_validity;
};

class WindowExecutorState {
public:
	WindowExecutorState() {};
	virtual ~WindowExecutorState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class WindowExecutor;

class WindowExecutorGlobalState : public WindowExecutorState {
public:
	using CollectionPtr = optional_ptr<WindowCollection>;

	WindowExecutorGlobalState(const WindowExecutor &executor, const idx_t payload_count,
	                          const ValidityMask &partition_mask, const ValidityMask &order_mask);

	const WindowExecutor &executor;

	const idx_t payload_count;
	const ValidityMask &partition_mask;
	const ValidityMask &order_mask;
	vector<LogicalType> arg_types;
};

class WindowExecutorLocalState : public WindowExecutorState {
public:
	using CollectionPtr = optional_ptr<WindowCollection>;

	explicit WindowExecutorLocalState(const WindowExecutorGlobalState &gstate);

	void Sink(WindowExecutorGlobalState &gstate, DataChunk &sink_chunk, DataChunk &coll_chunk, idx_t input_idx);
	virtual void Finalize(WindowExecutorGlobalState &gstate, CollectionPtr collection);

	//! The state used for reading the range collection
	unique_ptr<WindowCursor> range_cursor;
};

class WindowExecutor {
public:
	using CollectionPtr = optional_ptr<WindowCollection>;

	WindowExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);
	virtual ~WindowExecutor() {
	}

	virtual unique_ptr<WindowExecutorGlobalState>
	GetGlobalState(const idx_t payload_count, const ValidityMask &partition_mask, const ValidityMask &order_mask) const;
	virtual unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const;

	virtual void Sink(DataChunk &sink_chunk, DataChunk &coll_chunk, const idx_t input_idx,
	                  WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate) const;

	virtual void Finalize(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
	                      CollectionPtr collection) const;

	void Evaluate(idx_t row_idx, DataChunk &eval_chunk, Vector &result, WindowExecutorLocalState &lstate,
	              WindowExecutorGlobalState &gstate) const;

	// The function
	const BoundWindowExpression &wexpr;
	ClientContext &context;

	// evaluate frame expressions, if needed
	column_t boundary_start_idx = DConstants::INVALID_INDEX;
	column_t boundary_end_idx = DConstants::INVALID_INDEX;

	// evaluate RANGE expressions, if needed
	optional_ptr<Expression> range_expr;
	column_t range_idx = DConstants::INVALID_INDEX;

protected:
	virtual void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
	                              DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx) const = 0;
};

class WindowAggregateExecutor : public WindowExecutor {
public:
	WindowAggregateExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared,
	                        WindowAggregationMode mode);

	bool IsConstantAggregate();
	bool IsCustomAggregate();
	bool IsDistinctAggregate();

	void Sink(DataChunk &sink_chunk, DataChunk &coll_chunk, const idx_t input_idx, WindowExecutorGlobalState &gstate,
	          WindowExecutorLocalState &lstate) const override;
	void Finalize(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
	              CollectionPtr collection) const override;

	unique_ptr<WindowExecutorGlobalState> GetGlobalState(const idx_t payload_count, const ValidityMask &partition_mask,
	                                                     const ValidityMask &order_mask) const override;
	unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const override;

	const WindowAggregationMode mode;

	// aggregate computation algorithm
	unique_ptr<WindowAggregator> aggregator;

	// FILTER reference expression in sink_chunk
	unique_ptr<Expression> filter_ref;

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowRowNumberExecutor : public WindowExecutor {
public:
	WindowRowNumberExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};

//	Base class for non-aggregate functions that use peer boundaries
class WindowRankExecutor : public WindowExecutor {
public:
	WindowRankExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

	unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const override;

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowDenseRankExecutor : public WindowExecutor {
public:
	WindowDenseRankExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

	unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const override;

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowPercentRankExecutor : public WindowExecutor {
public:
	WindowPercentRankExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

	unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const override;

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowCumeDistExecutor : public WindowExecutor {
public:
	WindowCumeDistExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};

// Base class for non-aggregate functions that have a payload
class WindowValueExecutor : public WindowExecutor {
public:
	WindowValueExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

	void Finalize(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
	              CollectionPtr collection) const override;

	unique_ptr<WindowExecutorGlobalState> GetGlobalState(const idx_t payload_count, const ValidityMask &partition_mask,
	                                                     const ValidityMask &order_mask) const override;
	unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const override;

	//! The column index of the value column
	column_t child_idx = DConstants::INVALID_INDEX;
	//! The column index of the Nth column
	column_t nth_idx = DConstants::INVALID_INDEX;
	//! The column index of the offset column
	column_t offset_idx = DConstants::INVALID_INDEX;
	//! The column index of the default value column
	column_t default_idx = DConstants::INVALID_INDEX;
};

//
class WindowNtileExecutor : public WindowValueExecutor {
public:
	WindowNtileExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};
class WindowLeadLagExecutor : public WindowValueExecutor {
public:
	WindowLeadLagExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

	unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const override;

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowFirstValueExecutor : public WindowValueExecutor {
public:
	WindowFirstValueExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowLastValueExecutor : public WindowValueExecutor {
public:
	WindowLastValueExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowNthValueExecutor : public WindowValueExecutor {
public:
	WindowNthValueExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared);

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, DataChunk &eval_chunk,
	                      Vector &result, idx_t count, idx_t row_idx) const override;
};

} // namespace duckdb
