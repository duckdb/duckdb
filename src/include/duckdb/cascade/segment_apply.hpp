//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/segment_apply.hpp
//
// SegmentApply alternatives: section 3.4 of Galindo-Legaria & Joshi, "Orthogonal
// Optimization of Subqueries and Aggregation" (SIGMOD 2001).
//
// A SegmentApply is Apply whose parameter is a *set* of rows - a segment - rather than
// a single row: `R SA_A E` splits R by the columns A and evaluates E once per segment.
// The paper introduces it (3.4.1) for the shape correlation removal so often produces:
// two instances of the same expression joined together, one of them optionally
// aggregated or filtered, where the join predicate equates the *same column* of both
// instances. Rows whose value in that column differs can never match, so the column can
// partition the relation - that is the segmenting column.
//
// DuckDB's executor has no operator that evaluates a parameterized sub-plan per segment,
// so this pass *recognizes* the alternatives and reports them rather than building them.
// What the paper's reordering primitive (3.4.2) buys on top - the aggregate moving below
// the join that filters the rows it reads - is available through the ordinary rewrite
// machinery, and a query's plan shows whether it was taken.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class LogicalOperator;

//! The segmenting columns of every SegmentApply alternative in a plan, as
//! "table.column" strings, deduplicated and sorted.
vector<string> DescribeSegmentApplyAlternatives(LogicalOperator &plan);

} // namespace duckdb
