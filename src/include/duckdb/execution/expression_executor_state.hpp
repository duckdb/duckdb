//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/expression_executor_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_result.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {

class Expression;
class BoundFunctionExpression;
class BoundReferenceExpression;
class BoundConstantExpression;
class ExpressionExecutor;
struct ExpressionExecutorState;
struct FunctionLocalState;

//! Decomposed `ref <op> const` or `ref <op> ref` comparison for the bitmap select fast path.
struct BitmapComparisonInfo {
	optional_ptr<const BoundReferenceExpression> ref;
	//! exactly one of `constant` (ref <op> const) or `ref2` (ref <op> ref) is set
	optional_ptr<const BoundConstantExpression> constant;
	optional_ptr<const BoundReferenceExpression> ref2;
	ExpressionType op;
};

struct ExpressionState {
	ExpressionState(const Expression &expr, ExpressionExecutorState &root);
	virtual ~ExpressionState() {
	}

	const Expression &expr;
	ExpressionExecutorState &root;
	vector<unique_ptr<ExpressionState>> child_states;
	vector<LogicalType> types;
	DataChunk intermediate_chunk;
	vector<bool> initialize;

public:
	void AddChild(const Expression &child_expr);
	void Finalize();
	Allocator &GetAllocator();
	bool HasContext();
	DUCKDB_API ClientContext &GetContext();

	void Verify(ExpressionExecutorState &root);

	//! Reset any cached dictionary expression states in this expression state and its children
	virtual void ResetDictionaryStates();

public:
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

struct ExecuteFunctionState : public ExpressionState {
public:
	ExecuteFunctionState(const Expression &expr, ExpressionExecutorState &root);
	~ExecuteFunctionState() override;

public:
	static optional_ptr<FunctionLocalState> GetFunctionState(ExpressionState &state) {
		return state.Cast<ExecuteFunctionState>().local_state.get();
	}

	bool TryExecuteDictionaryExpression(const BoundFunctionExpression &expr, DataChunk &args, ExpressionState &state,
	                                    Vector &result);

	void ResetDictionaryStates() override;

public:
	unique_ptr<FunctionLocalState> local_state;
	//! Set once: this expression is a `ref <op> const` comparison the bitmap select fast path can handle
	bool select_bitmap_capable = false;
	//! Autovec twin of the bound +,-,* callback (nullptr when unavailable)
	bool (*autovec_function)(DataChunk &, Vector &) = nullptr;
	//! Cached comparison decomposition (valid when select_bitmap_capable), so Select does not walk the expression
	BitmapComparisonInfo cmp_info;
	//! Scratch bitmaps, their buffers are allocated lazily by PrepareBitmap only when actually used.
	SelectionResult tmp_sel1, tmp_sel2, tmp_sel3;

private:
	//! Non-constant input columns that may be compatible dictionary vectors
	vector<idx_t> dictionary_input_indices;
	//! Reusable input chunk for dictionary execution (points at the dictionary children); allocated once, then only
	//! re-referenced per call so the hot path does no per-chunk allocation
	DataChunk dictionary_input_chunk;
	//! Vector holding the expression executed on the entire dictionary
	buffer_ptr<DictionaryEntry> output_dictionary;
	//! ID of the input dictionary Vector
	string current_input_dictionary_id;
};

struct ExpressionExecutorState {
	ExpressionExecutorState();

	unique_ptr<ExpressionState> root_state;
	ExpressionExecutor *executor = nullptr;

	void Verify();
};

} // namespace duckdb
