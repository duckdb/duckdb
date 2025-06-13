#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/update_extensions_info.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

class PhysicalUnifiedStringDictionary : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::UNIFIED_STRING_DICTIONARY_INSERTION;

public:
	PhysicalUnifiedStringDictionary(PhysicalPlan &physical_plan, vector<LogicalType> types, vector<bool> insert_to_usd,
	                                idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, TYPE, std::move(types), estimated_cardinality) {
		this->insert_to_usd = std::move(insert_to_usd);
	};

	bool ParallelOperator() const override {
		return true;
	}

	bool IsSource() const override {
		return false;
	}

	bool IsSink() const override {
		return false;
	}

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;

private:
	vector<bool> insert_to_usd;
};

} // namespace duckdb
