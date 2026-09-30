#pragma once

#include "duckdb/optimizer/constraint_propagation/constraint_facts.hpp"
#include "duckdb/optimizer/constraint_propagation/fact_store.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class LogicalComparisonJoin;

//! Extract equi-key masks from a join.
bool CollectEquiKeys(const FactStore &store, const LogicalComparisonJoin &join, ColumnMask &key0, ColumnMask &key1,
                     idx_t *matched_conditions);

//! Collects the equi-key pairs of a comparison join.
bool CollectEquiKeyPairs(const FactStore &store, const LogicalComparisonJoin &join,
                         vector<std::pair<idx_t, idx_t>> &pairs, idx_t *matched_conditions);

} // namespace duckdb
