#include "duckdb/optimizer/constraint_propagation/constraint_facts.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// ValueDomain
//===----------------------------------------------------------------------===//

static bool ValueEquals(const Value &a, const Value &b) {
	D_ASSERT(a.type() == b.type());
	return a == b;
}

static bool ValueLess(const Value &a, const Value &b) {
	D_ASSERT(a.type() == b.type());
	return a < b;
}

//! Is v within d's (inclusive/exclusive) bounds?
static bool ValueInRange(const Value &v, const ValueDomain &d) {
	if (d.has_lo) {
		if (ValueLess(v, d.lo) || (!d.lo_inclusive && ValueEquals(v, d.lo))) {
			return false;
		}
	}
	if (d.has_hi) {
		if (ValueLess(d.hi, v) || (!d.hi_inclusive && ValueEquals(v, d.hi))) {
			return false;
		}
	}
	return true;
}

//! Is v an allowed value of d?
static bool ValueInDomain(const Value &v, const ValueDomain &d) {
	if (d.is_set) {
		for (auto &dv : d.values) {
			if (ValueEquals(v, dv)) {
				return true;
			}
		}
		return false;
	}
	return ValueInRange(v, d);
}

//! Is every value in `values` allowed by `other`?  Covers SET ⊆ SET and SET ⊆ RANGE
static bool ValuesSubsetOfDomain(const vector<Value> &values, const ValueDomain &other) {
	for (auto &v : values) {
		if (!ValueInDomain(v, other)) {
			return false;
		}
	}
	return true;
}

//! RANGE ⊆ SET: only provable when `a` is a single point in `b`.
static bool RangeSubsetOfSet(const ValueDomain &a, const vector<Value> &b) {
	if (!a.has_lo || !a.has_hi || !ValueEquals(a.lo, a.hi) || !a.lo_inclusive || !a.hi_inclusive) {
		return false;
	}
	for (auto &bv : b) {
		if (ValueEquals(a.lo, bv)) {
			return true;
		}
	}
	return false;
}

//! RANGE ⊆ RANGE
static bool RangeSubsetOfRange(const ValueDomain &a, const ValueDomain &b) {
	// a ⊆ b fails if b has a bound that excludes something a allows.
	if (b.has_lo) {
		if (!a.has_lo) {
			return false;
		}
		if (ValueLess(a.lo, b.lo)) {
			return false;
		}
		if (ValueEquals(a.lo, b.lo) && a.lo_inclusive && !b.lo_inclusive) {
			return false;
		}
	}
	if (b.has_hi) {
		if (!a.has_hi) {
			return false;
		}
		if (ValueLess(b.hi, a.hi)) {
			return false;
		}
		if (ValueEquals(a.hi, b.hi) && a.hi_inclusive && !b.hi_inclusive) {
			return false;
		}
	}
	return true;
}

//! SET ∩ SET, SET ∩ RANGE, and RANGE ∩ SET
static ValueDomain FilterValuesByDomain(const vector<Value> &values, const ValueDomain &domain, const LogicalType &type,
                                        bool null_a, bool null_b) {
	ValueDomain r;
	r.type = type;
	r.null_possible = null_a && null_b;
	r.is_set = true;
	for (auto &v : values) {
		if (ValueInDomain(v, domain)) {
			r.values.push_back(v);
		}
	}
	if (r.values.empty() && !r.null_possible) {
		r.bottom = true;
	}
	return r;
}

//! RANGE ∩ RANGE
static ValueDomain IntersectRanges(const ValueDomain &a, const ValueDomain &b) {
	ValueDomain r;
	r.type = a.type;
	r.null_possible = a.null_possible && b.null_possible;

	r.has_lo = a.has_lo;
	r.lo = a.lo;
	r.lo_inclusive = a.lo_inclusive;
	r.has_hi = a.has_hi;
	r.hi = a.hi;
	r.hi_inclusive = a.hi_inclusive;

	if (b.has_lo) {
		if (!r.has_lo || ValueLess(r.lo, b.lo)) {
			r.lo = b.lo;
			r.lo_inclusive = b.lo_inclusive;
			r.has_lo = true;
		} else if (ValueEquals(r.lo, b.lo)) {
			r.lo_inclusive = r.lo_inclusive && b.lo_inclusive;
		}
	}
	if (b.has_hi) {
		if (!r.has_hi || ValueLess(b.hi, r.hi)) {
			r.hi = b.hi;
			r.hi_inclusive = b.hi_inclusive;
			r.has_hi = true;
		} else if (ValueEquals(r.hi, b.hi)) {
			r.hi_inclusive = r.hi_inclusive && b.hi_inclusive;
		}
	}
	if (r.has_lo && r.has_hi &&
	    (ValueLess(r.hi, r.lo) || (ValueEquals(r.lo, r.hi) && !(r.lo_inclusive && r.hi_inclusive)))) {
		r.bottom = true;
	}
	return r;
}

bool ValueDomain::IsSubsetOf(const ValueDomain &other) const {
	if (bottom) {
		return true;
	}
	if (null_possible && !other.null_possible) {
		return false;
	}
	bool self_constrained = HasValueConstraint();
	bool other_constrained = other.HasValueConstraint();
	if (self_constrained && other_constrained && !(type == other.type)) {
		return false;
	}
	if (!other_constrained) {
		return true;
	}
	if (!self_constrained) {
		return false;
	}
	if (is_set) {
		return ValuesSubsetOfDomain(values, other);
	}
	if (other.is_set) {
		return RangeSubsetOfSet(*this, other.values);
	}
	return RangeSubsetOfRange(*this, other);
}

void ValueDomain::IntersectWith(const ValueDomain &other) {
	if (bottom) {
		return;
	}
	if (other.bottom) {
		bottom = true;
		return;
	}
	if (!other.HasValueConstraint()) {
		null_possible = null_possible && other.null_possible;
		return;
	}
	if (IsUnconstrained()) {
		bool combined_null = null_possible && other.null_possible;
		type = other.type;
		has_lo = other.has_lo;
		lo = other.lo;
		lo_inclusive = other.lo_inclusive;
		has_hi = other.has_hi;
		hi = other.hi;
		hi_inclusive = other.hi_inclusive;
		is_set = other.is_set;
		values = other.values;
		null_possible = combined_null;
		return;
	}
	D_ASSERT(type == other.type);
	if (is_set) {
		*this = FilterValuesByDomain(values, other, type, null_possible, other.null_possible);
	} else if (other.is_set) {
		*this = FilterValuesByDomain(other.values, *this, type, null_possible, other.null_possible);
	} else {
		*this = IntersectRanges(*this, other);
	}
}

//===----------------------------------------------------------------------===//
// ColumnMask
//===----------------------------------------------------------------------===//

ColumnMask::ColumnMask(idx_t width) : words((width + 63) / 64, 0) {
}

ColumnMask ColumnMask::Empty() {
	return ColumnMask();
}

ColumnMask ColumnMask::FromPositions(const vector<idx_t> &positions, idx_t width) {
	ColumnMask result(width);
	for (auto p : positions) {
		result.Set(p);
	}
	return result;
}

idx_t ColumnMask::Width() const {
	return words.size() * 64;
}

bool ColumnMask::IsEmpty() const {
	for (auto w : words) {
		if (w) {
			return false;
		}
	}
	return true;
}

idx_t ColumnMask::PopCount() const {
	idx_t count = 0;
	for (auto w : words) {
		while (w) {
			w &= w - 1;
			count++;
		}
	}
	return count;
}

void ColumnMask::Set(idx_t position) {
	idx_t w = position / 64;
	if (w >= words.size()) {
		words.resize(w + 1, 0);
	}
	words[w] |= 1ULL << (position % 64);
}

bool ColumnMask::Test(idx_t position) const {
	idx_t w = position / 64;
	if (w >= words.size()) {
		return false;
	}
	return (words[w] >> (position % 64)) & 1;
}

bool ColumnMask::IsSubsetOf(const ColumnMask &super) const {
	for (idx_t i = 0; i < words.size(); i++) {
		uint64_t super_word = i < super.words.size() ? super.words[i] : 0;
		if (words[i] & ~super_word) {
			return false;
		}
	}
	return true;
}

ColumnMask ColumnMask::Union(const ColumnMask &other) const {
	ColumnMask result = *this;
	if (result.words.size() < other.words.size()) {
		result.words.resize(other.words.size(), 0);
	}
	for (idx_t i = 0; i < other.words.size(); i++) {
		result.words[i] |= other.words[i];
	}
	return result;
}

ColumnMask ColumnMask::Intersection(const ColumnMask &other) const {
	ColumnMask result = *this;
	if (result.words.size() > other.words.size()) {
		result.words.resize(other.words.size());
	}
	for (idx_t i = 0; i < result.words.size(); i++) {
		result.words[i] &= other.words[i];
	}
	return result;
}

ColumnMask ColumnMask::ShiftedBy(idx_t offset) const {
	ColumnMask result(Width() + offset);
	ForEachPosition([&](idx_t p) -> bool {
		result.Set(p + offset);
		return true;
	});
	return result;
}

bool ColumnMask::operator==(const ColumnMask &other) const {
	return IsSubsetOf(other) && other.IsSubsetOf(*this);
}

//===----------------------------------------------------------------------===//
// ScopeFacts
//===----------------------------------------------------------------------===//
void ScopeFacts::AddUniqueFact(UniqueFact fact) {
	// E dominates F iff E.cols ⊆ F.cols AND E.null_distinct <= F.null_distinct
	for (auto &existing : unique) {
		if (existing.cols.IsSubsetOf(fact.cols) && existing.null_distinct <= fact.null_distinct) {
			return; // dominated by an existing fact
		}
	}
	for (idx_t i = unique.size(); i > 0; i--) {
		auto &existing = unique[i - 1];
		if (fact.cols.IsSubsetOf(existing.cols) && fact.null_distinct <= existing.null_distinct) {
			unique.erase(unique.begin() + static_cast<std::ptrdiff_t>(i - 1));
		}
	}
	unique.push_back(std::move(fact));
}
void ScopeFacts::AddFKFact(FKFact fact) {
	for (auto &existing : fks) {
		if (existing.target_schema == fact.target_schema && existing.target_name == fact.target_name &&
		    existing.cols == fact.cols && existing.referenced_keys == fact.referenced_keys) {
			return;
		}
	}
	fks.push_back(std::move(fact));
}

bool ScopeFacts::IsUniqueOn(const ColumnMask &cols, bool require_null_safe) const {
	for (auto &f : unique) {
		if (require_null_safe && f.null_distinct) {
			continue;
		}
		if (f.cols.IsSubsetOf(cols)) {
			return true;
		}
	}
	return false;
}

//===----------------------------------------------------------------------===//
// Helpers
//===----------------------------------------------------------------------===//
optional_idx PositionIn(const vector<ColumnBinding> &bindings, const ColumnBinding &b) {
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (bindings[i] == b) {
			return i;
		}
	}
	return optional_idx();
}

} // namespace duckdb
