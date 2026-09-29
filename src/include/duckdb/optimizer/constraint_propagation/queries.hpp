#pragma once

#include "duckdb/optimizer/constraint_propagation/constraint_facts.hpp"

namespace duckdb {

class ConstraintPropagator;
class LogicalOperator;
class LogicalComparisonJoin;
class TableCatalogEntry;

bool IsUniqueOn(const ConstraintPropagator &p, const LogicalOperator &scope, const ColumnMask &cols,
                bool require_null_safe);

bool IsNotNullOn(const ConstraintPropagator &p, const LogicalOperator &scope, const ColumnMask &cols);

bool IsForeignKeyTo(const ConstraintPropagator &p, const LogicalOperator &scope, const ColumnMask &cols,
                    const TableCatalogEntry &target);

//! Every join condition must be a pure column-ref equality
bool EquiKeys(const ConstraintPropagator &p, const LogicalComparisonJoin &join, ColumnMask &left_keys,
              ColumnMask &right_keys);

//! Every row of side `side` has at least one match on the other side.
bool JoinCoverage(const ConstraintPropagator &p, const LogicalComparisonJoin &join, idx_t side);

//! How many output rows side `side` produces per input row.
SideMultiplicity MultiplicityOf(const ConstraintPropagator &p, const LogicalComparisonJoin &join, idx_t side);

} // namespace duckdb
