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

	optional_ptr<Expression> expr;
	PhysicalType ptype;
	bool scalar;
	ExpressionExecutor executor;
	DataChunk chunk;
};

struct WindowInputColumn {
	WindowInputColumn(Expression *expr_p, ClientContext &context, idx_t capacity_p)
	    : input_expr(expr_p, context), count(0), capacity(capacity_p) {
		if (input_expr.expr) {
			target = make_uniq<Vector>(input_expr.chunk.data[0].GetType(), capacity);
		}
	}

	void Append(DataChunk &input_chunk) {
		if (input_expr.expr) {
			const auto source_count = input_chunk.size();
			D_ASSERT(count + source_count <= capacity);
			if (!input_expr.scalar || !count) {
				input_expr.Execute(input_chunk);
				auto &source = input_expr.chunk.data[0];
				VectorOperations::Copy(source, *target, source_count, 0, count);
			}
			count += source_count;
		}
	}

	inline bool CellIsNull(idx_t i) const {
		D_ASSERT(target);
		D_ASSERT(i < count);
		return FlatVector::IsNull(*target, input_expr.scalar ? 0 : i);
	}

	template <typename T>
	inline T GetCell(idx_t i) const {
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

//	Column indexes of the bounds chunk
enum WindowBounds : uint8_t { PARTITION_BEGIN, PARTITION_END, PEER_BEGIN, PEER_END, WINDOW_BEGIN, WINDOW_END };

class WindowExecutorState {
public:
	WindowExecutorState() {};
	virtual ~WindowExecutorState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class WindowExecutor {
public:
	WindowExecutor(BoundWindowExpression &wexpr, ClientContext &context, const idx_t payload_count,
	               const ValidityMask &partition_mask, const ValidityMask &order_mask);
	virtual ~WindowExecutor() {
	}

	virtual void Sink(DataChunk &input_chunk, const idx_t input_idx, const idx_t total_count) {
		range.Append(input_chunk);
	}

	virtual void Finalize() {
	}

	virtual unique_ptr<WindowExecutorState> GetExecutorState() const;

	void Evaluate(idx_t row_idx, DataChunk &input_chunk, Vector &result, WindowExecutorState &lstate) const;

protected:
	// The function
	BoundWindowExpression &wexpr;
	ClientContext &context;
	const idx_t payload_count;
	const ValidityMask &partition_mask;
	const ValidityMask &order_mask;

	// Expression collections
	DataChunk payload_collection;
	ExpressionExecutor payload_executor;
	DataChunk payload_chunk;

	// evaluate RANGE expressions, if needed
	WindowInputColumn range;

	virtual void EvaluateInternal(WindowExecutorState &lstate, Vector &result, idx_t count, idx_t row_idx) const = 0;
};

class WindowAggregateExecutor : public WindowExecutor {
public:
	bool IsConstantAggregate();
	bool IsCustomAggregate();

	WindowAggregateExecutor(BoundWindowExpression &wexpr, ClientContext &context, const idx_t payload_count,
	                        const ValidityMask &partition_mask, const ValidityMask &order_mask,
	                        WindowAggregationMode mode);

	void Sink(DataChunk &input_chunk, const idx_t input_idx, const idx_t total_count) override;
	void Finalize() override;

	unique_ptr<WindowExecutorState> GetExecutorState() const override;

	const WindowAggregationMode mode;

protected:
	ExpressionExecutor filter_executor;
	SelectionVector filter_sel;

	// aggregate computation algorithm
	unique_ptr<WindowAggregator> aggregator;

	void EvaluateInternal(WindowExecutorState &lstate, Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowRowNumberExecutor : public WindowExecutor {
public:
	WindowRowNumberExecutor(BoundWindowExpression &wexpr, ClientContext &context, const idx_t payload_count,
	                        const ValidityMask &partition_mask, const ValidityMask &order_mask);

protected:
	void EvaluateInternal(WindowExecutorState &lstate, Vector &result, idx_t count, idx_t row_idx) const override;
};

//	Base class for non-aggregate functions that use peer boundaries
class WindowRankExecutor : public WindowExecutor {
public:
	WindowRankExecutor(BoundWindowExpression &wexpr, ClientContext &context, const idx_t payload_count,
	                   const ValidityMask &partition_mask, const ValidityMask &order_mask);

	unique_ptr<WindowExecutorState> GetExecutorState() const override;

protected:
	void EvaluateInternal(WindowExecutorState &lstate, Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowDenseRankExecutor : public WindowExecutor {
public:
	WindowDenseRankExecutor(BoundWindowExpression &wexpr, ClientContext &context, const idx_t payload_count,
	                        const ValidityMask &partition_mask, const ValidityMask &order_mask);

	unique_ptr<WindowExecutorState> GetExecutorState() const override;

protected:
	void EvaluateInternal(WindowExecutorState &lstate, Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowPercentRankExecutor : public WindowExecutor {
public:
	WindowPercentRankExecutor(BoundWindowExpression &wexpr, ClientContext &context, const idx_t payload_count,
	                          const ValidityMask &partition_mask, const ValidityMask &order_mask);

	unique_ptr<WindowExecutorState> GetExecutorState() const override;

protected:
	void EvaluateInternal(WindowExecutorState &lstate, Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowCumeDistExecutor : public WindowExecutor {
public:
	WindowCumeDistExecutor(BoundWindowExpression &wexpr, ClientContext &context, const idx_t payload_count,
	                       const ValidityMask &partition_mask, const ValidityMask &order_mask);

protected:
	void EvaluateInternal(WindowExecutorState &lstate, Vector &result, idx_t count, idx_t row_idx) const override;
};

// Base class for non-aggregate functions that have a payload
class WindowValueExecutor : public WindowExecutor {
public:
	WindowValueExecutor(BoundWindowExpression &wexpr, ClientContext &context, const idx_t payload_count,
	                    const ValidityMask &partition_mask, const ValidityMask &order_mask);

	void Sink(DataChunk &input_chunk, const idx_t input_idx, const idx_t total_count) override;

protected:
	// IGNORE NULLS
	ValidityMask ignore_nulls;
};

//
class WindowNtileExecutor : public WindowValueExecutor {
public:
	WindowNtileExecutor(BoundWindowExpression &wexpr, ClientContext &context, const idx_t payload_count,
	                    const ValidityMask &partition_mask, const ValidityMask &order_mask);

protected:
	void EvaluateInternal(WindowExecutorState &lstate, Vector &result, idx_t count, idx_t row_idx) const override;
};
class WindowLeadLagExecutor : public WindowValueExecutor {
public:
	WindowLeadLagExecutor(BoundWindowExpression &wexpr, ClientContext &context, const idx_t payload_count,
	                      const ValidityMask &partition_mask, const ValidityMask &order_mask);

	unique_ptr<WindowExecutorState> GetExecutorState() const override;

protected:
	void EvaluateInternal(WindowExecutorState &lstate, Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowFirstValueExecutor : public WindowValueExecutor {
public:
	WindowFirstValueExecutor(BoundWindowExpression &wexpr, ClientContext &context, const idx_t payload_count,
	                         const ValidityMask &partition_mask, const ValidityMask &order_mask);

protected:
	void EvaluateInternal(WindowExecutorState &lstate, Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowLastValueExecutor : public WindowValueExecutor {
public:
	WindowLastValueExecutor(BoundWindowExpression &wexpr, ClientContext &context, const idx_t payload_count,
	                        const ValidityMask &partition_mask, const ValidityMask &order_mask);

protected:
	void EvaluateInternal(WindowExecutorState &lstate, Vector &result, idx_t count, idx_t row_idx) const override;
};

class WindowNthValueExecutor : public WindowValueExecutor {
public:
	WindowNthValueExecutor(BoundWindowExpression &wexpr, ClientContext &context, const idx_t payload_count,
	                       const ValidityMask &partition_mask, const ValidityMask &order_mask);

protected:
	void EvaluateInternal(WindowExecutorState &lstate, Vector &result, idx_t count, idx_t row_idx) const override;
};

} // namespace duckdb
