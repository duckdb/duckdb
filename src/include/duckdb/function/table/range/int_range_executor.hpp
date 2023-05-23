//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/range/int_range_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table/range/range_executor.hpp"

namespace duckdb {

class IntRangeExecutor : public RangeExecutor {
public:
	IntRangeExecutor(ClientContext &context, const vector<unique_ptr<Expression>> &args_list, bool generate_series_p)
	    : RangeExecutor(context, args_list, generate_series_p), current_row(0) {
	}

	virtual ~IntRangeExecutor() {
	}

public:
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           const vector<unique_ptr<Expression>> &args_list) override;

private:
	//! number of already generated rows
	idx_t current_row;

	const hugeint_t DEFAULT_INCREMENT = 1;
	const hugeint_t DEFAULT_START = 0;
	hugeint_t start;
	hugeint_t end;
	hugeint_t increment;

private:
	void Reset() override;
	void FetchArguments(DataChunk &input, const vector<unique_ptr<Expression>> &args_list) override;
};

} // namespace duckdb
