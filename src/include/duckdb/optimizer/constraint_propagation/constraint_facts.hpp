#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class TableCatalogEntry;

class ColumnMask {
public:
	ColumnMask() = default;
	explicit ColumnMask(idx_t width);

	static ColumnMask Empty();
	static ColumnMask FromPositions(const vector<idx_t> &positions, idx_t width);

	idx_t Width() const;
	bool IsEmpty() const;
	idx_t PopCount() const;

	void Set(idx_t position);
	bool Test(idx_t position) const;

	bool IsSubsetOf(const ColumnMask &super) const;
	ColumnMask Union(const ColumnMask &other) const;
	ColumnMask Intersection(const ColumnMask &other) const;
	ColumnMask ShiftedBy(idx_t offset) const;

	template <class FN>
	void ForEachPosition(FN &&fn) const;

	bool operator==(const ColumnMask &other) const;

private:
	vector<uint64_t> words;
};

template <class FN>
void ColumnMask::ForEachPosition(FN &&fn) const {
	for (idx_t w = 0; w < words.size(); w++) {
		uint64_t bits = words[w];
		idx_t base = w * 64;
		while (bits) {
			idx_t offset = 0;
			while (!(bits & 1ULL)) {
				bits >>= 1ULL;
				offset++;
			}
			if (!fn(base + offset)) {
				return;
			}
			bits >>= 1ULL;
		}
	}
}

struct UniqueFact {
	ColumnMask cols;
	bool null_distinct = false;
};

struct FKFact {
	Identifier target_schema;
	Identifier target_name;
	vector<idx_t> cols;
	vector<idx_t> referenced_keys;

	bool IsValid() const {
		return !cols.empty() && cols.size() == referenced_keys.size();
	}

	bool PositionsAllIn(const ColumnMask &mask) const {
		for (auto pos : cols) {
			if (!mask.Test(pos)) {
				return false;
			}
		}
		return true;
	}
};

//! Over-approximation of the values a column can hold at a point in the
//! plan. Two roles, and the distinction is load-bearing:
//!  - PROBE-side fact (transfer kernel): over-approximation of values that
//!    actually occur. Wider = sound.
//!  - ALLOWED-set extracted from a ref-side conjunct: EXACT set of values for
//!    which the conjunct is TRUE. IsSubsetOf is only sound when the right-hand
//!    side is exact — never approximate an IN list as [min, max].
struct ValueDomain {
	//! Column type; only meaningful when values are constrained.
	LogicalType type;
	//! Can the column be NULL here?
	bool null_possible = true;
	//! Contradiction detected (relation provably empty). SubsetOf(bottom, X)
	//! is vacuously true.
	bool bottom = false;

	//! RANGE: v in domain iff
	//!   (!has_lo || v > lo || (lo_inclusive && v == lo)) &&
	//!   (!has_hi || v < hi || (hi_inclusive && v == hi))
	bool has_lo = false, has_hi = false;
	bool lo_inclusive = true, hi_inclusive = true;
	Value lo, hi;

	//! SET: exact finite set of non-null values.
	bool is_set = false;
	vector<Value> values;

	bool HasValueConstraint() const {
		return has_lo || has_hi || is_set;
	}
	bool IsUnconstrained() const {
		return !HasValueConstraint();
	}

	//! this ⊆ other, null-aware. Only sound when `other` is EXACT.
	bool IsSubsetOf(const ValueDomain &other) const;
	//! this := this ∩ other. Only sound when `other` is EXACT (a conjunct's
	//! allowed-set). Used to narrow probe-side facts.
	void IntersectWith(const ValueDomain &other);
};

class ScopeFacts {
public:
	const ColumnMask &NotNull() const {
		return not_null;
	}
	const vector<UniqueFact> &Unique() const {
		return unique;
	}
	const vector<FKFact> &FKs() const {
		return fks;
	}

	void AddUniqueFact(UniqueFact fact);
	void AddFKFact(FKFact fact);

	void SetNotNull(ColumnMask mask) {
		not_null = std::move(mask);
	}
	void AddNotNullBit(idx_t pos) {
		not_null.Set(pos);
	}
	void SetUniqueFacts(vector<UniqueFact> facts) {
		unique = std::move(facts);
	}
	void SetFKFacts(vector<FKFact> facts) {
		fks = std::move(facts);
	}

	const ValueDomain &Domain(idx_t pos) const {
		if (pos < domains.size()) {
			return domains[pos];
		}
		static const ValueDomain ANY;
		return ANY;
	}
	void NarrowDomain(idx_t pos, const ValueDomain &allowed) {
		if (!allowed.null_possible) {
			AddNotNullBit(pos);
		}
		if (pos >= domains.size()) {
			domains.resize(pos + 1);
		}
		domains[pos].IntersectWith(allowed);
	}

	bool IsUniqueOn(const ColumnMask &cols, bool require_null_safe) const;

	const TableCatalogEntry *base_table = nullptr;
	vector<idx_t> base_column;
	vector<ValueDomain> domains;
	bool rows_dropped_below = false;

private:
	ColumnMask not_null;
	vector<UniqueFact> unique;
	vector<FKFact> fks;
};

enum class SideMultiplicity : uint8_t {
	UNKNOWN = 0,
	AT_MOST_ONE = 1,
	EXACTLY_ONE = 2,
};
inline bool operator>=(SideMultiplicity a, SideMultiplicity b) {
	return static_cast<uint8_t>(a) >= static_cast<uint8_t>(b);
}

optional_idx PositionIn(const vector<ColumnBinding> &bindings, const ColumnBinding &b);

} // namespace duckdb
