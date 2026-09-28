#include "duckdb/optimizer/constraint_propagation/constraint_facts.hpp"

namespace duckdb {

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
