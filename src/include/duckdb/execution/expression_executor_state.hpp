//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/expression_executor_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/cycle_counter.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {
class Expression;
class ExpressionExecutor;
struct ExpressionExecutorState;
struct FunctionLocalState;

struct ExpressionState {
	ExpressionState(const Expression &expr, ExpressionExecutorState &root);
	virtual ~ExpressionState() {
	}

	const Expression &expr;
	ExpressionExecutorState &root;
	vector<unique_ptr<ExpressionState>> child_states;
	vector<LogicalType> types;
	DataChunk intermediate_chunk;
	string name;
	CycleCounter profiler;

public:
	void AddChild(Expression *expr);
	void Finalize();
};

struct ExecuteFunctionState : public ExpressionState {
	ExecuteFunctionState(const Expression &expr, ExpressionExecutorState &root);
	~ExecuteFunctionState();

	unique_ptr<FunctionLocalState> local_state;

public:
	static FunctionLocalState *GetFunctionState(ExpressionState &state) {
		return ((ExecuteFunctionState &)state).local_state.get();
	}
};

struct ExpressionExecutorState {
	explicit ExpressionExecutorState(const string &name);
	unique_ptr<ExpressionState> root_state;
	ExpressionExecutor *executor;
	CycleCounter profiler;
	string name;
};

} // namespace duckdb
