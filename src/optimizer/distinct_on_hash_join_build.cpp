//===----------------------------------------------------------------------===//
// DistinctOnHashJoinBuildOptimizer
//
// Detects a DISTINCT ON subquery sitting on the build side of an INNER or LEFT
// hash join and rewrites the plan to drop the subquery, leaving the join's
// build-side hash table to deduplicate at chain-insertion time.
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/distinct_on_hash_join_build.hpp"

#include "duckdb/optimizer/build_probe_side_optimizer.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

static bool IsHashJoinEligible(const LogicalComparisonJoin &join) {
	if (join.HasArbitraryConditions()) {
		return false;
	}
	idx_t range_count = 0;
	if (!join.HasEquality(range_count)) {
		return false;
	}
	if (range_count > 0) {
		return false;
	}
	return true;
}

//! Walks down from `start` through pure passthrough LogicalProjection nodes (each
//! projection expression must be a BoundColumnRefExpression) until it finds a
//! LogicalDistinct. Records the chain top-to-bottom in `projection_chain` and returns
//! the unique_ptr slot that owns the LogicalDistinct. Returns nullptr if no such
//! chain reaches a LogicalDistinct.
static optional_ptr<unique_ptr<LogicalOperator>>
FindBuildSideDistinct(unique_ptr<LogicalOperator> &start, vector<reference<LogicalProjection>> &projection_chain) {
	auto *current = &start;
	while (true) {
		if (!*current) {
			return nullptr;
		}
		auto &op = **current;
		if (op.type == LogicalOperatorType::LOGICAL_DISTINCT) {
			return current;
		}
		if (op.type != LogicalOperatorType::LOGICAL_PROJECTION) {
			return nullptr;
		}
		auto &proj = op.Cast<LogicalProjection>();
		if (proj.children.size() != 1) {
			return nullptr;
		}
		// Any projection shape is accepted here: some expressions may be arbitrary
		// (constants, function calls), others may be BoundColumnRefs. TraceBinding
		// checks per-position whether the specific column we're trying to trace is
		// a BoundColumnRef, and fails the trace for that column if it isn't. This
		// is important for DEBUG builds where ColumnLifetimeAnalyzer::Verify inserts
		// a verification projection shaped like [NULL, col_N, NULL, col_{N-1}, ...,
		// NULL, col_0, NULL] between every join and its children — a strict
		// "all expressions must be BoundColumnRef" check would reject it.
		projection_chain.push_back(proj);
		current = &proj.children[0];
	}
}

//! Translate a column binding visible above the projection chain down through it,
//! returning the binding in the LogicalDistinct's binding space. Fails if the
//! specific expression at the traced column_index isn't a BoundColumnRef — i.e.
//! the column we care about is computed by a function, is a constant, or is
//! otherwise not a simple rebinding we can follow.
static bool TraceBinding(ColumnBinding binding, const vector<reference<LogicalProjection>> &projection_chain,
                         ColumnBinding &out) {
	auto cur = binding;
	for (auto &proj_ref : projection_chain) {
		auto &proj = proj_ref.get();
		if (cur.table_index != proj.table_index) {
			// Each projection re-binds its child's columns under its own table_index,
			// so the chain is strictly linear. Anything else is unexpected.
			return false;
		}
		if (cur.column_index >= proj.expressions.size()) {
			return false;
		}
		auto &expr = *proj.expressions[cur.column_index];
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
		cur = expr.Cast<BoundColumnRefExpression>().binding;
	}
	out = cur;
	return true;
}

//! Returns true if `cond` is an equality (or NOT DISTINCT FROM) comparison whose
//! RHS is a plain BoundColumnRefExpression that traces through `projection_chain`
//! to a binding in the LogicalDistinct's binding space, written to `out`.
//!
//! This is intentionally strict: the RHS must be the column ref itself, not an
//! expression wrapping one. The fused build-side hash table keys on the
//! *evaluated* RHS, so any wrapper that is non-injective (LOWER(s), narrowing
//! casts, arithmetic that can collide) would let the HT collapse rows that the
//! original DISTINCT ON kept distinct, producing different results from the
//! unfused query.
static bool ResolveEqualityRhsBinding(const JoinCondition &cond,
                                      const vector<reference<LogicalProjection>> &projection_chain,
                                      ColumnBinding &out) {
	if (!cond.IsComparison()) {
		return false;
	}
	const auto cmp = cond.GetComparisonType();
	if (cmp != ExpressionType::COMPARE_EQUAL && cmp != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		// Non-equality comparators (NOT EQUAL, DISTINCT FROM, range comparators) are
		// applied as residual predicates by the join, not as hash-table keys. Two
		// build rows that the user expected DISTINCT ON to keep distinct could end
		// up colliding on the equality keys alone, and the residual predicate would
		// then be evaluated against whichever row the HT happened to keep first —
		// non-deterministic and observably different from the unfused query.
		return false;
	}
	if (cond.GetRHS().GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	const auto &col = cond.GetRHS().Cast<BoundColumnRefExpression>();
	return TraceBinding(col.binding, projection_chain, out);
}

//! Returns true if flipping the join's children might enable a beneficial fusion.
//! BUILD_SIDE_PROBE_SIDE picks the smaller side as build using raw cardinalities,
//! without knowing about our dedup-on-build compression. We override BPS only
//! when its chosen build (children[1]) is at least as small as the alternative
//! (children[0]) — i.e., when BPS picked children[1] *because* it was smaller and
//! our dedup might compress children[0] enough to make the flip beneficial.
//!
//! When children[0] is already smaller than children[1], BPS would have picked
//! children[0] as build under both cost models (with or without our optimization),
//! so there's nothing to override and flipping would only make things worse.
static bool FlipMightHelp(const LogicalComparisonJoin &join) {
	return join.children[0]->estimated_cardinality >= join.children[1]->estimated_cardinality;
}

//! Returns true if the LogicalDistinct under `slot` (possibly through a chain of
//! passthrough projections) can be fused into the build side of `join`, given that
//! every join condition's RHS columns must trace into the dedup-target columns.
//! On success the LogicalDistinct is spliced out and `join.dedup_build` is set.
static bool TryFuseAtChild1(LogicalComparisonJoin &join) {
	vector<reference<LogicalProjection>> projection_chain;
	auto distinct_slot = FindBuildSideDistinct(join.children[1], projection_chain);
	if (!distinct_slot) {
		return false;
	}
	auto &distinct = (*distinct_slot)->Cast<LogicalDistinct>();
	if (distinct.distinct_type != DistinctType::DISTINCT_ON) {
		return false;
	}
	// ORDER BY inside DISTINCT ON would lower to an ordered first() aggregate;
	// dedup-on-insert has no notion of ordering, so we cannot fuse.
	if (distinct.order_by) {
		return false;
	}
	if (distinct.children.size() != 1) {
		return false;
	}
	// Every distinct target must be a plain column reference. If a target is a computed
	// expression like LOWER(col), the dedup is on the expression value, and dropping
	// duplicates could discard rows whose underlying column values differ from the kept
	// row — unsafe for join correctness.
	column_binding_set_t dedup_bindings;
	for (auto &target : distinct.distinct_targets) {
		if (target->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
		auto &col = target->Cast<BoundColumnRefExpression>();
		dedup_bindings.insert(col.binding);
	}
	// Walk every condition: must be a plain equality with a direct column-ref RHS,
	// and the resolved RHS bindings must form *exactly* the dedup target set.
	//
	// Why exact equality and not just subset? Two directional checks are needed:
	//
	// 1. dedup ⊆ rhs (every dedup column appears in some equality condition):
	//    if a dedup column is missing from the join key, two rows that DISTINCT ON
	//    kept distinct (different in the missing dedup column) hash to the same
	//    HT key on the equality columns alone, and one is silently dropped.
	//    Example: DISTINCT ON (k, w), JOIN ON p.k = b.k. Build rows (k=1,w=10)
	//    and (k=1,w=20) collapse in the HT but produce two output rows in the
	//    unfused query.
	//
	// 2. rhs ⊆ dedup (every join key column is a dedup target):
	//    if a join key column is not a dedup target, two rows that share the
	//    dedup key but differ in the join key would both be kept in the HT
	//    (different keys), but the unfused DISTINCT ON would have collapsed
	//    them to one row — possibly the one whose join key matches the probe,
	//    possibly not (non-deterministic). Allowing the fusion would change the
	//    observable result by always picking the matching row.
	//
	// Combined with C2: each cond.GetRHS() must itself be a BoundColumnRefExpression
	// (no LOWER/CAST/etc.), because the HT keys on the *evaluated* expression and
	// non-injective wrappers can collapse rows the unfused DISTINCT ON kept distinct.
	column_binding_set_t rhs_bindings;
	for (auto &cond : join.conditions) {
		ColumnBinding traced;
		if (!ResolveEqualityRhsBinding(cond, projection_chain, traced)) {
			return false;
		}
		rhs_bindings.insert(traced);
	}
	if (rhs_bindings != dedup_bindings) {
		return false;
	}
	// Splice out the LogicalDistinct: the slot it occupied now points at the
	// distinct's child directly, and the hash table will dedup at insert time.
	// Any LogicalProjection nodes between the join and the distinct stay in place;
	// LogicalDistinct passes through its child's bindings unchanged, so the
	// projections still resolve to the same columns.
	*distinct_slot = std::move(distinct.children[0]);
	join.dedup_build = true;
	return true;
}

static bool TryRewrite(LogicalComparisonJoin &join) {
	if (join.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		// exclude ASOF / DELIM joins
		return false;
	}
	if (join.children.size() != 2) {
		return false;
	}
	if (!IsHashJoinEligible(join)) {
		return false;
	}

	if (join.join_type == JoinType::INNER) {
		// Try children[1] first (the canonical build side after BUILD_SIDE_PROBE_SIDE).
		if (TryFuseAtChild1(join)) {
			return true;
		}
		// Flipping is semantically free for INNER (equality is symmetric, INNER is
		// its own inverse), but we only override BUILD_SIDE_PROBE_SIDE when there's
		// a plausible reason its decision was suboptimal under our cost model. See
		// FlipMightHelp.
		if (!FlipMightHelp(join)) {
			return false;
		}
		BuildProbeSideOptimizer::FlipChildren(join);
		if (TryFuseAtChild1(join)) {
			return true;
		}
		BuildProbeSideOptimizer::FlipChildren(join);
		return false;
	}
	if (join.join_type == JoinType::LEFT) {
		// LEFT joins build on the right side. Only fuse when the distinct is already
		// on children[1]: flipping a LEFT to a RIGHT would put us on a code path
		// where Finalize does a full build-side scan to emit unmatched rows, which
		// is incompatible with our compaction (we'd be scanning rows that no longer
		// exist).
		return TryFuseAtChild1(join);
	}
	if (join.join_type == JoinType::RIGHT) {
		// RIGHT and LEFT are inverses, so flipping a RIGHT to a LEFT is always
		// semantically valid. After the flip, the new build side (the old probe
		// side) doesn't have the "preserved" semantic, so dedup-on-insert is safe
		// against the unmatched-rows scan that blocks fusing a LEFT join's actual
		// build side. We don't try to fuse the original RIGHT's children[1]
		// without flipping, because that would discard rows the RIGHT join is
		// supposed to keep.
		//
		// Same flip-gating heuristic as the INNER branch: only override
		// BUILD_SIDE_PROBE_SIDE when there's a plausible reason it picked
		// suboptimally.
		if (!FlipMightHelp(join)) {
			return false;
		}
		BuildProbeSideOptimizer::FlipChildren(join);
		if (TryFuseAtChild1(join)) {
			return true;
		}
		BuildProbeSideOptimizer::FlipChildren(join);
		return false;
	}
	return false;
}

unique_ptr<LogicalOperator> DistinctOnHashJoinBuildOptimizer::Optimize(unique_ptr<LogicalOperator> op) {
	for (auto &child : op->children) {
		child = Optimize(std::move(child));
	}
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		TryRewrite(join);
	}
	return op;
}

} // namespace duckdb
