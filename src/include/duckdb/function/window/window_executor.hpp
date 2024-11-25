//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/window/window_collection.hpp"
#include "duckdb/function/window/window_segment_tree.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

class WindowCollection;

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
