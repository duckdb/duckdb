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

struct BitmapComparisonInfo {
	optional_ptr<const BoundReferenceExpression> ref;
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
	bool select_bitmap_capable = false;
	BitmapComparisonInfo cmp_info;
	SelectionResult tmp_sel1, tmp_sel2;

private:
	bool safe_autovec_arith = false;
	//! The column index of the "unary" input column that may be a dictionary vector
	//! Only valid when the expression is eligible for the dictionary expression optimization
	//! This is the case when the input is "practically unary", i.e., only one non-const input column
	optional_idx input_col_idx;
	//! Non-constant children; several are only usable when they share one selection (dense arithmetic)
	vector<idx_t> dictionary_input_indices;
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
