#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/vector.hpp"
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
	ColumnMask cols;
	vector<idx_t> referenced_keys;
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

	bool IsUniqueOn(const ColumnMask &cols, bool require_null_safe) const;

	const TableCatalogEntry *base_table = nullptr;
	vector<idx_t> base_column;
	bool filter_below = false;

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
