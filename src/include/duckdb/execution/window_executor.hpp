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
	void Combine(bool build_validity);

	//! The collection data. May be null if the column count is 0.
	ColumnDataCollectionPtr inputs;
	//! Optional validity mask for the entire collection
	ValidityMask validity;

	//! The collection columns
	const vector<LogicalType> types;
	//! The collection rows
	const idx_t count;

private:
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
};

class WindowCursor {
public:
	explicit WindowCursor(const WindowCollection &paged);

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

	//! The pageable data
	const WindowCollection &paged;
	//! The state used for reading the collection
	ColumnDataScanState state;
	//! The data chunk read into
	DataChunk chunk;
};

struct WindowInputExpression {
	static void PrepareInputExpression(Expression &expr, ExpressionExecutor &executor, DataChunk &chunk) {
		vector<LogicalType> types;
		types.push_back(expr.return_type);
		executor.AddExpression(expr);

		auto &allocator = executor.GetAllocator();
		chunk.Initialize(allocator, types);
	}

	WindowInputExpression(optional_ptr<Expression> expr_p, ClientContext &context)
	    : expr(expr_p), ptype(PhysicalType::INVALID), scalar(true), executor(context) {
		if (expr) {
			PrepareInputExpression(*expr, executor, chunk);
			ptype = expr->return_type.InternalType();
			scalar = expr->IsScalar();
		}
	}

	void Execute(DataChunk &input_chunk) {
		if (expr) {
			chunk.Reset();
			executor.Execute(input_chunk, chunk);
			chunk.Verify();
			chunk.Flatten();
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

	inline void CopyCell(Vector &target, idx_t target_offset, idx_t width = 1) const {
		D_ASSERT(!chunk.data.empty());
		auto &source = chunk.data[0];
		auto source_offset = scalar ? 0 : target_offset;
		VectorOperations::Copy(source, target, source_offset + width, source_offset, target_offset);
	}

	optional_ptr<Expression> expr;
	PhysicalType ptype;
	bool scalar;
	ExpressionExecutor executor;
	DataChunk chunk;
};

//	Column indexes of the bounds chunk
enum WindowBounds : uint8_t { PARTITION_BEGIN, PARTITION_END, PEER_BEGIN, PEER_END, WINDOW_BEGIN, WINDOW_END };

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
	WindowExecutorGlobalState(const WindowExecutor &executor, const idx_t payload_count,
	                          const ValidityMask &partition_mask, const ValidityMask &order_mask);

	const WindowExecutor &executor;

	const idx_t payload_count;
	const ValidityMask &partition_mask;
	const ValidityMask &order_mask;
	vector<LogicalType> arg_types;

	// evaluate RANGE expressions, if needed
	optional_ptr<Expression> range_expr;
	unique_ptr<WindowCollection> range;
};

class WindowExecutorLocalState : public WindowExecutorState {
public:
	explicit WindowExecutorLocalState(const WindowExecutorGlobalState &gstate);

	void Sink(WindowExecutorGlobalState &gstate, DataChunk &input_chunk, idx_t input_idx);
	virtual void Finalize(WindowExecutorGlobalState &gstate);

	// Argument evaluation
	ExpressionExecutor payload_executor;
	DataChunk payload_chunk;

	//! Range evaluation
	ExpressionExecutor range_executor;
	DataChunk range_chunk;
	//! The state used for building the range collection
	unique_ptr<WindowBuilder> range_builder;
	//! The state used for reading the range collection
	unique_ptr<WindowCursor> range_cursor;
};

class WindowExecutor {
public:
	WindowExecutor(BoundWindowExpression &wexpr, ClientContext &context);
	virtual ~WindowExecutor() {
	}

	virtual unique_ptr<WindowExecutorGlobalState>
	GetGlobalState(const idx_t payload_count, const ValidityMask &partition_mask, const ValidityMask &order_mask) const;
	virtual unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const;

	virtual void Sink(DataChunk &input_chunk, const idx_t input_idx, const idx_t total_count,
	                  WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate) const;

	virtual void Finalize(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate) const;

	void Evaluate(idx_t row_idx, DataChunk &input_chunk, Vector &result, WindowExecutorLocalState &lstate,
	              WindowExecutorGlobalState &gstate) const;

	// The function
	const BoundWindowExpression &wexpr;
	ClientContext &context;

protected:
	virtual void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, Vector &result,
	                              idx_t count, idx_t row_idx) const = 0;
};

class WindowAggregateExecutor : public WindowExecutor {
public:
	WindowAggregateExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowAggregationMode mode);

	void Sink(DataChunk &input_chunk, const idx_t input_idx, const idx_t total_count, WindowExecutorGlobalState &gstate,
	          WindowExecutorLocalState &lstate) const override;
	void Finalize(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate) const override;

	unique_ptr<WindowExecutorGlobalState> GetGlobalState(const idx_t payload_count, const ValidityMask &partition_mask,
	                                                     const ValidityMask &order_mask) const override;
	unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const override;

	const WindowAggregationMode mode;

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, Vector &result,
	                      idx_t count, idx_t row_idx) const override;
};

class WindowRowNumberExecutor : public WindowExecutor {
public:
	WindowRowNumberExecutor(BoundWindowExpression &wexpr, ClientContext &context);

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, Vector &result,
	                      idx_t count, idx_t row_idx) const override;
};

//	Base class for non-aggregate functions that use peer boundaries
class WindowRankExecutor : public WindowExecutor {
public:
	WindowRankExecutor(BoundWindowExpression &wexpr, ClientContext &context);

	unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const override;

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, Vector &result,
	                      idx_t count, idx_t row_idx) const override;
};

class WindowDenseRankExecutor : public WindowExecutor {
public:
	WindowDenseRankExecutor(BoundWindowExpression &wexpr, ClientContext &context);

	unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const override;

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, Vector &result,
	                      idx_t count, idx_t row_idx) const override;
};

class WindowPercentRankExecutor : public WindowExecutor {
public:
	WindowPercentRankExecutor(BoundWindowExpression &wexpr, ClientContext &context);

	unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const override;

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, Vector &result,
	                      idx_t count, idx_t row_idx) const override;
};

class WindowCumeDistExecutor : public WindowExecutor {
public:
	WindowCumeDistExecutor(BoundWindowExpression &wexpr, ClientContext &context);

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, Vector &result,
	                      idx_t count, idx_t row_idx) const override;
};

// Base class for non-aggregate functions that have a payload
class WindowValueExecutor : public WindowExecutor {
public:
	WindowValueExecutor(BoundWindowExpression &wexpr, ClientContext &context);

	void Sink(DataChunk &input_chunk, const idx_t input_idx, const idx_t total_count, WindowExecutorGlobalState &gstate,
	          WindowExecutorLocalState &lstate) const override;

	unique_ptr<WindowExecutorGlobalState> GetGlobalState(const idx_t payload_count, const ValidityMask &partition_mask,
	                                                     const ValidityMask &order_mask) const override;
	unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const override;
};

//
class WindowNtileExecutor : public WindowValueExecutor {
public:
	WindowNtileExecutor(BoundWindowExpression &wexpr, ClientContext &context);

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, Vector &result,
	                      idx_t count, idx_t row_idx) const override;
};
class WindowLeadLagExecutor : public WindowValueExecutor {
public:
	WindowLeadLagExecutor(BoundWindowExpression &wexpr, ClientContext &context);

	unique_ptr<WindowExecutorLocalState> GetLocalState(const WindowExecutorGlobalState &gstate) const override;

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, Vector &result,
	                      idx_t count, idx_t row_idx) const override;
};

class WindowFirstValueExecutor : public WindowValueExecutor {
public:
	WindowFirstValueExecutor(BoundWindowExpression &wexpr, ClientContext &context);

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, Vector &result,
	                      idx_t count, idx_t row_idx) const override;
};

class WindowLastValueExecutor : public WindowValueExecutor {
public:
	WindowLastValueExecutor(BoundWindowExpression &wexpr, ClientContext &context);

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, Vector &result,
	                      idx_t count, idx_t row_idx) const override;
};

class WindowNthValueExecutor : public WindowValueExecutor {
public:
	WindowNthValueExecutor(BoundWindowExpression &wexpr, ClientContext &context);

protected:
	void EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate, Vector &result,
	                      idx_t count, idx_t row_idx) const override;
};

} // namespace duckdb
