//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/join/physical_hash_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"
#include "execution/join_hashtable.hpp"
#include "execution/operator/join/physical_join.hpp"
#include "execution/physical_operator.hpp"
#include "planner/operator/logical_join.hpp"

namespace duckdb {

//! PhysicalHashJoin represents a hash loop join between two tables
class PhysicalHashJoin : public PhysicalJoin {
public:
	PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                 vector<JoinCondition> cond, JoinType join_type, bool null_values_are_equal);

	void _GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState(ExpressionExecutor *parent_executor) override;

	unique_ptr<JoinHashTable> hash_table;
};

class PhysicalHashJoinOperatorState : public PhysicalOperatorState {
public:
	PhysicalHashJoinOperatorState(PhysicalOperator *left, PhysicalOperator *right, ExpressionExecutor *parent_executor)
	    : PhysicalOperatorState(left, parent_executor), initialized(false) {
		assert(left && right);
	}

	bool initialized;
	DataChunk join_keys;
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;
};
} // namespace duckdb
