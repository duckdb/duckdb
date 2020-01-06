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

namespace duckdb {
class Expression;
class ExpressionExecutor;
struct ExpressionExecutorState;

struct ExpressionState {
	ExpressionState(Expression &expr, ExpressionExecutorState &root) : expr(expr), root(root) {
	}
	virtual ~ExpressionState() {
	}

	Expression &expr;
	ExpressionExecutorState &root;
	DataChunk arguments;
	vector<unique_ptr<ExpressionState>> child_states;
public:
	void AddIntermediates(vector<Expression *> expressions);
	virtual void Reset() {
		arguments.Reset();
	}
};

struct ExpressionExecutorState {
	unique_ptr<ExpressionState> root_state;
	ExpressionExecutor *executor;
};

} // namespace duckdb
