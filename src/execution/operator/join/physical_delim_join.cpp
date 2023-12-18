#include "duckdb/execution/operator/join/physical_delim_join.hpp"

#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"

namespace duckdb {

PhysicalDelimJoin::PhysicalDelimJoin(PhysicalOperatorType type, vector<LogicalType> types,
                                     unique_ptr<PhysicalOperator> original_join,
                                     vector<const_reference<PhysicalOperator>> delim_scans, idx_t estimated_cardinality)
    : PhysicalOperator(type, std::move(types), estimated_cardinality), join(std::move(original_join)),
      delim_scans(std::move(delim_scans)) {
	D_ASSERT(type == PhysicalOperatorType::LEFT_DELIM_JOIN || type == PhysicalOperatorType::RIGHT_DELIM_JOIN);
}

vector<const_reference<PhysicalOperator>> PhysicalDelimJoin::GetChildren() const {
	vector<const_reference<PhysicalOperator>> result;
	for (auto &child : children) {
		result.push_back(*child);
	}
	result.push_back(*join);
	result.push_back(*distinct);
	return result;
}

string PhysicalDelimJoin::ParamsToString() const {
	return join->ParamsToString();
}

} // namespace duckdb
