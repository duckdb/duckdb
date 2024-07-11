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

// A wrapper for building DataChunks in parallel
class WindowDataChunk {
public:
	// True if the vector data can just be copied to
	static bool IsSimple(const Vector &v);

	static inline bool IsMaskAligned(idx_t begin, idx_t end, idx_t count) {
		return ValidityMask::IsAligned(begin) && (ValidityMask::IsAligned(end) || (end == count));
	}

	explicit WindowDataChunk(DataChunk &chunk);

	void Initialize(Allocator &allocator, const vector<LogicalType> &types, idx_t capacity);

	void Copy(DataChunk &src, idx_t begin);

	//! The wrapped chunk
	DataChunk &chunk;

private:
	//! True if the column is a scalar only value
	vector<bool> is_simple;
	//! Exclusive lock for each column
	vector<mutex> locks;
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

struct WindowInputColumn {
	WindowInputColumn(optional_ptr<Expression> expr_p, ClientContext &context, idx_t count);

	void Copy(DataChunk &input_chunk, idx_t input_idx);

	inline bool CellIsNull(idx_t i) const {
		D_ASSERT(!target.data.empty());
		D_ASSERT(i < count);
		return FlatVector::IsNull((target.data[0]), scalar ? 0 : i);
	}

	template <typename T>
	inline T GetCell(idx_t i) const {
		D_ASSERT(!target.data.empty());
		D_ASSERT(i < count);
		const auto data = FlatVector::GetData<T>(target.data[0]);
		return data[scalar ? 0 : i];
	}

	optional_ptr<Expression> expr;
	PhysicalType ptype;
	const bool scalar;
	const idx_t count;

private:
	DataChunk target;
	WindowDataChunk wtarget;
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
	WindowInputColumn range;
};

class WindowExecutorLocalState : public WindowExecutorState {
public:
	explicit WindowExecutorLocalState(const WindowExecutorGlobalState &gstate);

	void Sink(WindowExecutorGlobalState &gstate, DataChunk &input_chunk, idx_t input_idx);

	// Argument evaluation
	ExpressionExecutor payload_executor;
	DataChunk payload_chunk;

	//! Range evaluation
	ExpressionExecutor range_executor;
	DataChunk range_chunk;
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

	virtual void Finalize(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate) const {
	}

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
