#include "duckdb/execution/mark_join_refinement.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/execution/ie_join_union.hpp"

namespace duckdb {

MarkJoinRefinementIndex::MarkJoinRefinementIndex() = default;
MarkJoinRefinementIndex::~MarkJoinRefinementIndex() = default;

uint64_t MarkJoinRefinement::NullMask(const DataChunk &keys, idx_t row, const vector<JoinCondition> &conditions) {
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

MarkPatternClassification MarkJoinRefinement::Classify(uint64_t probe_mask, uint64_t build_mask,
                                                       const vector<JoinCondition> &conditions) {
	MarkPatternClassification result;
	if (conditions.size() > 64) {
		return result;
	}
	result.reducible = true;
	result.dropped = probe_mask | build_mask;
	for (idx_t col = 0; col < conditions.size(); col++) {
		if (result.dropped & (uint64_t(1) << col)) {
			continue;
		}
		result.applicable_count++;
		if (conditions[col].GetLHS().GetReturnType().IsNested()) {
			continue;
		}
		switch (conditions[col].GetComparisonType()) {
		case ExpressionType::COMPARE_EQUAL:
			result.equality_mask |= uint64_t(1) << col;
			break;
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			result.ranges.push_back(col);
			break;
		default:
			break;
		}
	}
	return result;
}

unique_ptr<MarkJoinRefinementIndex>
MarkJoinRefinementIndex::BuildHash(ClientContext &context, const PhysicalOperator &op, MarkJoinRefinementGroup &group,
                                   uint64_t equality_mask, const vector<JoinCondition> &conditions,
                                   const mark_key_fetch_t &fetch) {
	auto built = make_uniq<MarkJoinRefinementIndex>();
	vector<LogicalType> types;
	for (idx_t col = 0; col < conditions.size(); col++) {
		if (equality_mask & (uint64_t(1) << col)) {
			built->columns.push_back(col);
			built->conditions.push_back(conditions[col].Copy());
			types.push_back(conditions[col].GetLHS().GetReturnType());
		}
	}
	built->output_columns.push_back(types.size());
	built->hash = make_uniq<JoinHashTable>(context, op, built->conditions, vector<LogicalType> {LogicalType::UBIGINT},
	                                       JoinType::INNER, 0, built->output_columns, nullptr);
	auto layout_types = types;
	layout_types.push_back(LogicalType::UBIGINT);
	layout_types.push_back(LogicalType::HASH);
	auto layout = make_shared_ptr<TupleDataLayout>();
	layout->Initialize(layout_types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	built->hash->FinishInitWithLayout(layout);
	PartitionedTupleDataAppendState append;
	built->hash->GetSinkCollection().InitializeAppendState(append);
	DataChunk index_keys, payload;
	index_keys.InitializeEmpty(types);
	payload.Initialize(Allocator::Get(context), {LogicalType::UBIGINT});
	for (auto &selection : group.selections) {
		context.InterruptCheck();
		auto &chunk = fetch(selection.first);
		SelectionVector selected(selection.second.data(), selection.second.size());
		index_keys.ReferenceColumns(chunk, built->columns);
		index_keys.Slice(selected, selection.second.size());
		payload.Reset();
		for (auto row : selection.second) {
			payload.data[0].Append(Value::UBIGINT(selection.first * STANDARD_VECTOR_SIZE + row));
		}
		payload.SetChildCardinality(selection.second.size());
		built->hash->Build(append, index_keys, payload);
	}
	built->hash->Unpartition();
	built->hash->AllocatePointerTable();
	built->hash->InitializePointerTable(0, built->hash->capacity);
	built->hash->Finalize(0, built->hash->GetDataCollection().ChunkCount(), false);
	return built;
}

void MarkJoinRefinement::AddChunk(const DataChunk &keys, idx_t chunk, const vector<JoinCondition> &conditions) {
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
			if (index.second->ranges) {
				size += index.second->ranges->SizeInBytes();
			}
			for (auto &result : index.second->probe_results) {
				size += sizeof(result) + result.second->SizeInBytes();
			}
		}
	}
	return size;
}

} // namespace duckdb
