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
#include "duckdb/common/profiler.hpp"

namespace duckdb {
class Expression;
class ExpressionExecutor;
class Profiler;
struct ExpressionExecutorState;

struct ExpressionState {
	ExpressionState(Expression &expr, ExpressionExecutorState &root);
	virtual ~ExpressionState() {
	}

	Expression &expr;
	ExpressionExecutorState &root;
	vector<unique_ptr<ExpressionState>> child_states;
	vector<LogicalType> types;
	DataChunk intermediate_chunk;
	string name;
	double time;
	Profiler profiler;

public:
	void AddChild(Expression *expr);
	void Finalize();
};

struct ExpressionExecutorState {
	unique_ptr<ExpressionState> root_state;
	ExpressionExecutor *executor;
};

} // namespace duckdb
