#pragma once

#include "duckdb/optimizer/constraint_propagation/constraint_facts.hpp"
#include "duckdb/optimizer/constraint_propagation/fact_store.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

class LogicalComparisonJoin;

//! Extract equi-key masks from a join.
bool CollectEquiKeys(const FactStore &store, const LogicalComparisonJoin &join, ColumnMask &key0, ColumnMask &key1,
                     idx_t *matched_conditions);

//! Collects the equi-key pairs of a comparison join.
bool CollectEquiKeyPairs(const FactStore &store, const LogicalComparisonJoin &join,
                         vector<std::pair<idx_t, idx_t>> &pairs, idx_t *matched_conditions);

//! Result of extracting single-column constraint
struct ExtractedConstraint {
	const BoundColumnRefExpression *column = nullptr;
	ValueDomain allowed;
};

//! Flattens nested AND-conjunctions into conjuncts.
void FlattenConjuncts(const Expression &expr, vector<const Expression *> &out);

//! Recognize `col IS NOT NULL` and `col OP const` / `const OP col` (OP: =, >, >=, <, <=).
bool TryExtractConstraint(const Expression &conjunct, ExtractedConstraint &out);

//! Render a scan-level TableFilter.
bool TryExtractTableFilterDomain(const TableFilter &filter, const BoundColumnRefExpression &column_ref,
                                 ValueDomain &out);

} // namespace duckdb
