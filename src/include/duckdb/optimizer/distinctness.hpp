//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/distinctness.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding_map.hpp"

namespace duckdb {

class ClientContext;
class Expression;
class LogicalAggregate;
class LogicalComparisonJoin;
class LogicalGet;
class LogicalOperator;
class LogicalProjection;

//! The operators that unique sets are forwarded through
enum class DistinctnessPropagation : uint8_t {
	//! Filters, limits, orderings, windows and projections
	UNARY_OPERATORS,
	//! Additionally samples, the left side of SEMI / ANTI / MARK / SINGLE joins, and a side of an INNER join (the
	//! left side of a LEFT join) whose other side is itself distinct on its equality keys
	UNARY_OPERATORS_AND_JOINS
};

//! Structural proofs that the output of an operator has at most one row per value of a set of columns, where NULLs
//! count as one value (like for DISTINCT and GROUP BY). This is the one place where unique sets are inferred.
//! The sources are the groups of an aggregate with a single grouping set and the PRIMARY KEY / UNIQUE NOT NULL
//! constraints of a scanned table. They are forwarded through operators that never duplicate rows (see
//! DistinctnessPropagation). Statistics (distinct counts) are estimates and are never used.
//! The unique sets of an operator are computed once and cached by the identity of the operator, so that a
//! bottom-up pass over a plan stays linear. An instance is therefore only valid for the analysis of a stable plan:
//! once a pass replaces or removes an operator that has been asked about (or one below it), or changes what the
//! unique sets are derived from (the scanned columns, groups and grouping sets, projection expressions, join types
//! and join conditions), the instance must not be used anymore. Rewrites of anything else (e.g. order keys, window
//! partitions) and of operators above everything that has been asked about keep it valid.
class Distinctness {
public:
	//! The maximum number of unique sets that a join forwards
	static constexpr idx_t MAX_UNIQUE_SETS = 32;

public:
	//! A context allows cast proofs that account for registered overrides.
	explicit Distinctness(DistinctnessPropagation propagation, optional_ptr<ClientContext> context = nullptr)
	    : propagation(propagation), context(context) {
	}

	//! Whether the output of the operator has at most one row per value of the keys
	bool IsDistinctOn(LogicalOperator &op, const column_binding_set_t &keys);
	//! Checks expression keys, including aliases and built-in injective casts.
	bool IsDistinctOn(LogicalOperator &op, const vector<unique_ptr<Expression>> &keys);
	//! The sets of columns the output of the operator is distinct on (cached)
	const vector<column_binding_set_t> &GetUniqueSets(LogicalOperator &op);
	//! Gets the equality keys (= or IS NOT DISTINCT FROM) of the left (child_idx 0) or right (child_idx 1) side of a
	//! join. Returns false if there is no equality, or if the key of an equality is not a plain column reference
	static bool GetJoinEqualityKeys(const LogicalComparisonJoin &join, idx_t child_idx, column_binding_set_t &keys);

private:
	bool IsDistinctOnProjectedKeys(LogicalOperator &op, const column_binding_set_t &keys);
	vector<column_binding_set_t> ComputeUniqueSets(LogicalOperator &op);
	static vector<column_binding_set_t> ComputeScanUniqueSets(LogicalGet &get);
	static vector<column_binding_set_t> ComputeAggregateUniqueSets(LogicalAggregate &aggr);
	vector<column_binding_set_t> ComputeProjectionUniqueSets(LogicalProjection &projection);
	vector<column_binding_set_t> ComputeJoinUniqueSets(LogicalComparisonJoin &join);
	bool IsDistinctOnJoinKeys(LogicalComparisonJoin &join, idx_t child_idx);

private:
	const DistinctnessPropagation propagation;
	const optional_ptr<ClientContext> context;
	reference_map_t<LogicalOperator, vector<column_binding_set_t>> unique_sets;
};

} // namespace duckdb
