//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/range/time_range_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table/range/range_executor.hpp"

namespace duckdb {

class TimeRangeExecutor : public RangeExecutor {
public:
	TimeRangeExecutor(ClientContext &context, const vector<unique_ptr<Expression>> &args_list, bool generate_series_p)
	    : RangeExecutor(context, args_list, generate_series_p) {
	}

	virtual ~TimeRangeExecutor() {
	}

public:
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           const vector<unique_ptr<Expression>> &args_list) override;

private:
	timestamp_t start;
	timestamp_t end;
	interval_t increment;
	bool positive_increment;

	timestamp_t current_timestamp;

private:
	void FetchArguments(DataChunk &input, const vector<unique_ptr<Expression>> &args_list) override;
	//! determines if are there are still rows to be emitted
	bool Finished(timestamp_t current_value);
	//! set current_timestamp one timestep forward
	void virtual StepForward();
};

} // namespace duckdb
