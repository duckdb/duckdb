//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/range/range_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

class RangeExecutor {
protected:
	RangeExecutor(ClientContext &context, const vector<unique_ptr<Expression>> &args_list, bool generate_series_p)
	    : first_fetch(true), generate_series(generate_series_p), expr_executor(context) {

		// we add each expression in the args_list to the expression executor
		vector<LogicalType> args_data_types;
		for (auto &exp : args_list) {
			D_ASSERT(exp->type == ExpressionType::BOUND_REF);
			auto &ref = exp->Cast<BoundReferenceExpression>();
			args_data_types.push_back(exp->return_type);
			expr_executor.AddExpression(ref);
		}

		auto &allocator = Allocator::Get(context);
		args_data.Initialize(allocator, args_data_types);
	}

public:
	~RangeExecutor() {
	}

	//! execute the Range function
	OperatorResultType virtual Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   const vector<unique_ptr<Expression>> &args_list) = 0;

protected:
	bool first_fetch;
	//! indicate whether the function is generate_series (true) or range (false)
	bool generate_series;

	ExpressionExecutor expr_executor;
	//! chunk for the arguments to be stored in
	DataChunk args_data;

protected:
	//! reset the executor's state after having handled one input row
	void virtual Reset() {
		first_fetch = true;
	}

	//! loads arguments from args_list, using the data in the input chunk
	void virtual FetchArguments(DataChunk &input, const vector<unique_ptr<Expression>> &args_list) = 0;
};

} // namespace duckdb
