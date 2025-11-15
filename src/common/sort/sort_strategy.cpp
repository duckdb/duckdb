#include "duckdb/common/sorting/sort_strategy.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// SortStrategy
//===--------------------------------------------------------------------===//
SortStrategy::SortStrategy(const Types &input_types) : payload_types(input_types) {
}

void SortStrategy::Synchronize(const GlobalSinkState &source, GlobalSinkState &target) const {
}

void SortStrategy::SortColumnData(ExecutionContext &context, hash_t hash_bin, OperatorSinkFinalizeInput &finalize) {
	//	Nothing to sort
	return;
}

unique_ptr<LocalSourceState> SortStrategy::GetLocalSourceState(ExecutionContext &context,
                                                               GlobalSourceState &gstate) const {
	return make_uniq<LocalSourceState>();
}

} // namespace duckdb
