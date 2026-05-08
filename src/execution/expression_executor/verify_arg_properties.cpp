#include "duckdb/execution/verify_arg_properties.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/function/arg_properties.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <numeric>

namespace duckdb {

#ifdef DEBUG
namespace {

// Build a row-index permutation sorted by (other-args, target-arg). When `chunk` is null, only
// the target vector contributes to the sort key. Rows that match on every non-target arg end up
// adjacent, sorted by the target arg within each such group.
vector<idx_t> SortRowsByTargetArg(Vector &target_vec, idx_t count, DataChunk *chunk, idx_t target_arg) {
	vector<idx_t> perm(count);
	std::iota(perm.begin(), perm.end(), idx_t(0));
	std::sort(perm.begin(), perm.end(), [&](idx_t a, idx_t b) {
		if (chunk) {
			for (idx_t k = 0; k < chunk->ColumnCount(); k++) {
				if (k == target_arg) {
					continue;
				}
				const auto va = chunk->data[k].GetValue(a);
				const auto vb = chunk->data[k].GetValue(b);
				if (!ValueOperations::DistinctFrom(va, vb)) {
					continue;
				}
				return ValueOperations::DistinctLessThan(va, vb);
			}
		}
		return ValueOperations::DistinctLessThan(target_vec.GetValue(a), target_vec.GetValue(b));
	});
	return perm;
}

// True when every column of `chunk` except `target_arg` has NotDistinctFrom values at rows a, b.
// Returns true when chunk is null (unary case has no other args to compare).
bool RowsShareNonTargetArgs(DataChunk *chunk, idx_t target_arg, idx_t a, idx_t b) {
	if (!chunk) {
		return true;
	}
	for (idx_t k = 0; k < chunk->ColumnCount(); k++) {
		if (k == target_arg) {
			continue;
		}
		if (ValueOperations::DistinctFrom(chunk->data[k].GetValue(a), chunk->data[k].GetValue(b))) {
			return false;
		}
	}
	return true;
}

// Asserts the monotonicity claim on a single ordered pair (in_a <= in_b by sort).
void AssertMonotonicityPair(Monotonicity m, const Value &in_a, const Value &in_b, const Value &out_a,
                            const Value &out_b) {
	// CONSTANT, or equal inputs to a deterministic function: outputs must match.
	if (m == Monotonicity::CONSTANT || !ValueOperations::DistinctFrom(in_a, in_b)) {
		D_ASSERT(!ValueOperations::DistinctFrom(out_a, out_b));
		return;
	}
	const bool strict = IsStrict(m);
	if (IsMonotonicIncreasing(m)) {
		D_ASSERT(strict ? ValueOperations::DistinctLessThan(out_a, out_b)
		                : ValueOperations::DistinctLessThanEquals(out_a, out_b));
	} else {
		D_ASSERT(strict ? ValueOperations::DistinctGreaterThan(out_a, out_b)
		                : ValueOperations::DistinctGreaterThanEquals(out_a, out_b));
	}
}

// Shared per-arg verifier. `chunk` and `target_arg` describe the "other args" context for the
// multi-arg scalar function case; pass nullptr for the single-arg cast case.
void VerifyArgPropertiesForArg(Vector &target_vec, Vector &result, idx_t count, const ArgProperties &props,
                               bool check_monotonicity, bool check_injectivity, DataChunk *chunk, idx_t target_arg) {
	const auto perm = SortRowsByTargetArg(target_vec, count, chunk, target_arg);
	for (idx_t i = 1; i < count; i++) {
		const idx_t a = perm[i - 1];
		const idx_t b = perm[i];
		if (!RowsShareNonTargetArgs(chunk, target_arg, a, b)) {
			continue; // group boundary — claims don't apply across other-arg changes
		}
		const auto in_a = target_vec.GetValue(a);
		const auto in_b = target_vec.GetValue(b);
		const auto out_a = result.GetValue(a);
		const auto out_b = result.GetValue(b);
		// NULL endpoints have no monotonic relation
		if (in_a.IsNull() || in_b.IsNull() || out_a.IsNull() || out_b.IsNull()) {
			continue;
		}
		if (check_monotonicity) {
			AssertMonotonicityPair(props.monotonicity, in_a, in_b, out_a, out_b);
		}
		if (check_injectivity && ValueOperations::DistinctFrom(in_a, in_b)) {
			D_ASSERT(ValueOperations::DistinctFrom(out_a, out_b));
		}
	}
}

void VerifyDeclaredProperty(Vector &target_vec, Vector &result, idx_t count, const ArgProperties &props,
                            DataChunk *chunk, idx_t target_arg) {
	const bool check_monotonicity = IsKnownMonotonic(props.monotonicity);
	const bool check_injectivity = props.injective;
	if (!check_monotonicity && !check_injectivity) {
		return;
	}
	VerifyArgPropertiesForArg(target_vec, result, count, props, check_monotonicity, check_injectivity, chunk,
	                          target_arg);
}

} // namespace
#endif

void VerifyFunctionArgProperties(const BoundFunctionExpression &expr, DataChunk &args, Vector &result) {
#ifdef DEBUG
	if (!expr.function.HasArgProperties() || args.size() < 2) {
		return;
	}
	const auto &all_props = expr.function.GetAllArgProperties();
	for (idx_t arg_idx = 0; arg_idx < all_props.size(); arg_idx++) {
		VerifyDeclaredProperty(args.data[arg_idx], result, args.size(), all_props[arg_idx], &args, arg_idx);
	}
#endif
}

void VerifyCastArgProperties(const BoundCastExpression &expr, Vector &source, Vector &result, idx_t count) {
#ifdef DEBUG
	if (count < 2) {
		return;
	}
	VerifyDeclaredProperty(source, result, count, expr.bound_cast.GetArgProperties(), nullptr, 0);
#endif
}

} // namespace duckdb
