//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/physical_hash_join.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"

#include "execution/join_hashtable.hpp"
#include "execution/physical_operator.hpp"

#include "planner/operator/logical_join.hpp"

namespace duckdb {

//! PhysicalHashJoin represents a hash loop join between two tables
class PhysicalHashJoin : public PhysicalOperator {
  public:
	PhysicalHashJoin(std::unique_ptr<PhysicalOperator> left,
	                 std::unique_ptr<PhysicalOperator> right,
	                 std::vector<JoinCondition> cond, JoinType join_type);

	std::vector<std::string> GetNames() override;
	std::vector<TypeId> GetTypes() override;

	virtual void _GetChunk(ClientContext &context, DataChunk &chunk,
	                       PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

	std::unique_ptr<JoinHashTable> hash_table;
	std::vector<TypeId> join_key_types;
	std::vector<JoinCondition> conditions;
	JoinType type;
};

class PhysicalHashJoinOperatorState : public PhysicalOperatorState {
  public:
	PhysicalHashJoinOperatorState(PhysicalOperator *left,
	                              PhysicalOperator *right,
	                              std::vector<TypeId> &join_key_types,
	                              ExpressionExecutor *parent_executor)
	    : PhysicalOperatorState(left, parent_executor), initialized(false) {
		assert(left && right);
		join_keys.Initialize(join_key_types);
	}

	bool initialized;
	DataChunk join_keys;
	std::unique_ptr<JoinHashTable::ScanStructure> scan_structure;
};
} // namespace duckdb
