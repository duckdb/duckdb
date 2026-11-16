//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/filter/physical_top_key_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {
struct DynamicFilterData;

//! PhysicalTopKeyFilter is a streaming filter below a grouped aggregate whose Top-N orders by a prefix of the group
//! keys. Every thread keeps the smallest "limit" distinct key tuples that it has seen. The largest of them is not
//! smaller than the limit-th smallest distinct key tuple of the whole input, and neither is the smallest such bound of
//! all threads, which is shared (TopNBoundaryValue). A row is removed only if its key tuple sorts strictly after a
//! bound (ties pass), so that every group that the Top-N can return still receives all of its rows.//!
//! The pruning is conservative. The contract of the operator is:
//!   * it never removes a row of a group whose key tuple is among the "limit" smallest distinct key tuples of its
//!     input (the groups that the Top-N above the aggregate can return), and
//!   * it may pass any other row.
//! Its output is therefore a superset of the rows that are needed, not the rows of exactly "limit" distinct keys: rows
//! pass unfiltered while a thread warms up, while no bound has been published, while a thread works with an older
//! (larger) bound than the current one, after a thread has stopped filtering, and the bound of a thread is only as
//! small as the keys that it has seen itself. The aggregate above can receive rows of - and build - groups that are
//! not part of the result, with a part of their rows only. Only the Top-N above the aggregate decides the result: the
//! groups it returns are complete, and a group that is incomplete sorts after "limit" complete ones.
//! The rows that pass keep their order, so that aggregates that depend on the order of their input see the rows in the
//! order in which they would see them without the filter. That is why this is not a caching operator: in a pipeline
//! that ends in a hash aggregate the cache holds back a small chunk while a later full chunk passes.
class PhysicalTopKeyFilter : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::TOP_KEY_FILTER;

public:
	PhysicalTopKeyFilter(PhysicalPlan &physical_plan, vector<LogicalType> types, vector<BoundOrderByNode> orders,
	                     idx_t limit, shared_ptr<DynamicFilterData> dynamic_filter, idx_t estimated_cardinality);
	~PhysicalTopKeyFilter() override;

	//! The keys and their order
	vector<BoundOrderByNode> orders;
	//! The number of smallest distinct key tuples whose rows are guaranteed to pass. Not an upper bound of the number
	//! of distinct key tuples in the output.
	idx_t limit;
	//! Dynamic table filter on the first key (if any)
	shared_ptr<DynamicFilterData> dynamic_filter;

public:
	void InitializeOutputChunk(Allocator &allocator, DataChunk &chunk) const override;
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;

	bool ParallelOperator() const override {
		return true;
	}
	PipelineExternalInputSupport GetExternalInputSupport() const override {
		return PipelineExternalInputSupport::SUPPORTED;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;
};
} // namespace duckdb
