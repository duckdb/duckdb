//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/physical_window.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! PhysicalStreamingWindow implements streaming window functions (i.e. with an empty OVER clause)
class PhysicalStreamingWindow : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::STREAMING_WINDOW;

	static bool IsStreamingFunction(ClientContext &context, unique_ptr<Expression> &expr);

public:
	PhysicalStreamingWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
	                        idx_t estimated_cardinality,
	                        PhysicalOperatorType type = PhysicalOperatorType::STREAMING_WINDOW);

	//! The projection list of the WINDOW statement
	vector<unique_ptr<Expression>> select_list;

public:
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	OperatorFinalizeResultType FinalExecute(ExecutionContext &context, DataChunk &chunk, GlobalOperatorState &gstate,
	                                        OperatorState &state) const final;

	bool RequiresFinalExecute() const final {
		return true;
	}

	OrderPreservationType OperatorOrder() const override {
		return OrderPreservationType::FIXED_ORDER;
	}

	string ParamsToString() const override;

private:
	void ExecuteFunctions(ExecutionContext &context, DataChunk &chunk, DataChunk &delayed,
	                      GlobalOperatorState &gstate_p, OperatorState &state_p) const;
	void ExecuteInput(ExecutionContext &context, DataChunk &delayed, DataChunk &input, DataChunk &chunk,
	                  GlobalOperatorState &gstate, OperatorState &state) const;
	void ExecuteDelayed(ExecutionContext &context, DataChunk &delayed, DataChunk &input, DataChunk &chunk,
	                    GlobalOperatorState &gstate, OperatorState &state) const;
};

} // namespace duckdb
