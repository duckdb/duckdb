#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/unified_string_dictionary.hpp"
#include "duckdb/main/client_context.hpp"
namespace duckdb {

class UnifiedStringsDictionary;

class PhysicalUnifiedStringDictionary : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::UNIFIED_STRING_DICTIONARY_INSERTION;

	// very much on the conservative side. We try to insert the first few dictionaries and then meassure the growth as a
	// heuristic to determine domain cardinality.
	static constexpr double TOTAL_GROWTH_THRESHOLD = 0.1;
	static constexpr idx_t MIN_DICTIONARY_SEEN = 10;
	static constexpr idx_t MAX_STRINGS_PER_COLUMN = 5000;
	static constexpr idx_t MAX_DICT_SIZE = 3000;

public:
	PhysicalUnifiedStringDictionary(PhysicalPlan &physical_plan, vector<LogicalType> types, vector<bool> insert_to_usd,
	                                bool insert_flat_vectors, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, TYPE, std::move(types), estimated_cardinality),
	      insert_to_usd(std::move(insert_to_usd)), insert_flat_vectors(insert_flat_vectors) {
	}

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
	void InsertConstant(ExecutionContext &context, Vector &vec) const;
	void InsertFlat(ExecutionContext &context, Vector &vec, idx_t count) const;
	void InsertDictionary(ExecutionContext &context, Vector &vec, OperatorState &state_p, GlobalOperatorState &gstate,
	                      idx_t col_idx) const;
	void UpdateDictionaryState(ExecutionContext &context, OperatorState &state_p, GlobalOperatorState &gstate,
	                           Vector &vec, idx_t col_idx, idx_t dict_size) const;

	vector<bool> insert_to_usd;
	bool insert_flat_vectors;
};

} // namespace duckdb
