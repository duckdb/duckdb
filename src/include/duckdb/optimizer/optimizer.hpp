//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/common/enums/optimizer_type.hpp"

#include <functional>

namespace duckdb {
class Binder;

class Optimizer {
public:
	Optimizer(Binder &binder, ClientContext &context);

	//! Optimize a plan by running specialized optimizers
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);
	//! Return a reference to the client context of this optimizer
	ClientContext &GetContext();
	//! Whether the specific optimizer is disabled
	bool OptimizerDisabled(OptimizerType type);
	static bool OptimizerDisabled(ClientContext &context, OptimizerType type);

public:
	ClientContext &context;
	Binder &binder;
	ExpressionRewriter rewriter;

private:
	void RunBuiltInOptimizers();
	void RunOptimizer(OptimizerType type, const std::function<void()> &callback);
	void Verify(LogicalOperator &op);

public:
	// helper functions
	unique_ptr<Expression> BindScalarFunction(const string &name, unique_ptr<Expression> c1);
	unique_ptr<Expression> BindScalarFunction(const string &name, unique_ptr<Expression> c1, unique_ptr<Expression> c2);

private:
	unique_ptr<LogicalOperator> plan;

private:
	unique_ptr<Expression> BindScalarFunction(const string &name, vector<unique_ptr<Expression>> children);
};

} // namespace duckdb
