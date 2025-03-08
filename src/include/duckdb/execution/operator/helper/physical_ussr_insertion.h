#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/update_extensions_info.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

class PhysicalUnifiedString : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::UNIFIED_STRINGS;

public:
	PhysicalUnifiedString(vector<LogicalType> types, vector<bool> insert_to_ussr, idx_t estimated_cardinality)
	    : PhysicalOperator(TYPE, std::move(types), estimated_cardinality) {
		this->insert_to_ussr = std::move(insert_to_ussr);
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

	vector<DataChunk> cached;

	static constexpr const idx_t USSR_CACHING_THRESHOLD = 10;

public:
private:
	vector<bool> insert_to_ussr;
	void USSR_insertion_loop(data_ptr_t dict_strings, idx_t count, ClientContext &context, ValidityMask &validity,
	                         const vector<idx_t> &priority_insertion, bool isParquet, bool exists_prio = false) const;
};

} // namespace duckdb
