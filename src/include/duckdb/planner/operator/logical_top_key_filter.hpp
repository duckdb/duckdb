//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_top_key_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
struct DynamicFilterData;

//! LogicalTopKeyFilter is a streaming filter below a grouped aggregate whose Top-N orders by a prefix of the group
//! keys. It removes rows whose key tuple sorts after a bound that is known to be at or after the "limit"-th smallest
//! distinct key tuple of its input, so that the aggregate does not have to aggregate most of the groups that the
//! Top-N discards.//!
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
//! An optimizer that moves or copies this operator has to keep the Top-N on the same keys above it.
class LogicalTopKeyFilter : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_TOP_KEY_FILTER;

public:
	LogicalTopKeyFilter(vector<BoundOrderByNode> orders, idx_t limit);
	~LogicalTopKeyFilter() override;

	//! The keys (expressions over the child) and their order
	vector<BoundOrderByNode> orders;
	//! The number of smallest distinct key tuples whose rows are guaranteed to pass (LIMIT + OFFSET of the Top-N). Not
	//! an upper bound of the number of distinct key tuples in the output.
	idx_t limit;
	//! Dynamic table filter on the first key (if any)
	shared_ptr<DynamicFilterData> dynamic_filter;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
