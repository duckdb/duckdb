#pragma once

#include "duckdb/optimizer/constraint_propagation/constraint_facts.hpp"
#include "duckdb/optimizer/constraint_propagation/fact_store.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class LogicalComparisonJoin;

//! Extract equi-key masks from a join. Conditions that aren't column-ref
//! equalities (or that aren't provably equivalent to one) are skipped
bool CollectEquiKeys(const FactStore &store, const LogicalComparisonJoin &join, ColumnMask &key0, ColumnMask &key1,
                     idx_t *matched_conditions);

} // namespace duckdb
