#include "duckdb/execution/mark_join_refinement.hpp"
#include "duckdb/execution/join_hashtable.hpp"

namespace duckdb {

MarkJoinRefinementIndex::MarkJoinRefinementIndex() = default;
MarkJoinRefinementIndex::~MarkJoinRefinementIndex() = default;

uint64_t MarkJoinRefinement::NullMask(const DataChunk &keys, idx_t row,
                                     const vector<JoinCondition> &conditions) {
	if (conditions.size() > 64) {
		return 0;
	}
	uint64_t mask = 0;
	for (idx_t col = 0; col < conditions.size(); col++) {
		const auto comparison = conditions[col].GetComparisonType();
		if (keys.data[col].GetType().IsNested() || comparison == ExpressionType::COMPARE_DISTINCT_FROM ||
		    comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			continue;
		}
		if (keys.data[col].GetValue(row).IsNull()) {
			mask |= uint64_t(1) << col;
		}
	}
	return mask;
}

void MarkJoinRefinement::AddChunk(const DataChunk &keys, idx_t chunk,
                                  const vector<JoinCondition> &conditions) {
	for (idx_t row = 0; row < keys.size(); row++) {
		auto &group = groups[NullMask(keys, row, conditions)];
		group.selections[chunk].push_back(UnsafeNumericCast<sel_t>(row));
		group.count++;
	}
}

idx_t MarkJoinRefinement::SizeInBytes() const {
	idx_t size = sizeof(*this) + chunks.capacity() * sizeof(chunks[0]);
	for (auto &entry : groups) {
		size += sizeof(entry);
		for (auto &selection : entry.second.selections) {
			size += sizeof(selection) + selection.second.capacity() * sizeof(sel_t);
		}
		for (auto &index : entry.second.indexes) {
			size += sizeof(index) + sizeof(*index.second) + index.second->columns.capacity() * sizeof(idx_t);
			if (index.second->hash) {
				size += index.second->hash->SizeInBytes() + index.second->hash->capacity * sizeof(ht_entry_t);
			}
			if (index.second->prefix) {
				size += index.second->prefix->SizeInBytes();
			}
		}
	}
	return size;
}

} // namespace duckdb
