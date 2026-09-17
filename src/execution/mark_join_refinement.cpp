#include "duckdb/execution/mark_join_refinement.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/execution/ie_join_union.hpp"
#include "duckdb/execution/mark_join_row_comparison.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/vector/constant_vector.hpp"

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
	built->hash->finalized = true;
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
		}
	}
	return size;
}

MarkPatternRefiner::MarkPatternRefiner(ClientContext &context, const PhysicalComparisonJoin &op,
                                       MarkJoinRefinement &refinement, mutex &lock, mark_key_fetch_t fetch,
                                       DataChunk &keys, bool matches[], ValidityMask &validity,
                                       mark_candidate_finish_t finish_candidates)
    : context(context), op(op), conditions(op.conditions), refinement(refinement), lock(lock), fetch(std::move(fetch)),
      finish_candidates(std::move(finish_candidates)), chunk(this->fetch(0)), keys(keys), matches(matches),
      validity(validity), comparison(LogicalType::BOOLEAN), comparer(keys, MarkJoinRowComparison::Mode::COMPOSITE) {
	condition_types = keys.GetTypes();
	candidates.InitializeEmpty(condition_types);
}

void MarkPatternRefiner::Fetch(idx_t index) {
	if (cached_chunk != index) {
		fetch(index);
		cached_chunk = index;
	}
}

bool MarkPatternRefiner::Finish(idx_t probe, uint64_t dropped, const SelectionVector &build_selection) {
	comparer.CompareConjunction(keys, probe, candidates, conditions, comparison);
	if (finish_candidates) {
		SelectionVector probe_selection(STANDARD_VECTOR_SIZE);
		for (idx_t row = 0; row < candidates.size(); row++) {
			probe_selection.set_index(row, probe);
		}
		finish_candidates(probe_selection, build_selection, candidates.size(), comparison);
	}
	for (auto value : comparison.Values<bool>()) {
		if (!value.IsValid()) {
			validity.SetInvalid(probe);
			if (dropped) {
				return true;
			}
		} else if (value.GetValue()) {
			matches.get()[probe] = true;
			validity.SetValid(probe);
			return true;
		}
	}
	return false;
}

MarkJoinRefinementIndex &MarkPatternRefiner::BuildEqualityIndex(MarkJoinRefinementGroup &group,
                                                                uint64_t equality_mask) {
	lock_guard<mutex> guard(lock);
	auto &cached = group.indexes[equality_mask];
	if (!cached) {
		cached = MarkJoinRefinementIndex::BuildHash(context, op, group, equality_mask, conditions,
		                                            [&](idx_t index) -> DataChunk & {
			                                            Fetch(index);
			                                            return chunk;
		                                            });
	}
	return *cached;
}

void MarkPatternRefiner::ProbeEqualityIndex(MarkJoinRefinementIndex &index, uint64_t probe_mask, uint64_t dropped,
                                            uint64_t equality_mask) {
	auto &keys = this->keys;
	auto &chunk = this->chunk;
	auto &conditions = this->conditions;
	auto &comparison = this->comparison;
	auto &validity = this->validity;
	auto matches = this->matches.get();
	SelectionVector selected(STANDARD_VECTOR_SIZE);
	idx_t probe_count = 0;
	for (idx_t row = 0; row < keys.size(); row++) {
		if (!matches[row] && validity.RowIsValid(row) &&
		    MarkJoinRefinement::NullMask(keys, row, conditions) == probe_mask) {
			selected.set_index(probe_count++, row);
		}
	}
	DataChunk probe_keys, probe_payload, probe_candidates, build_candidates;
	probe_keys.InitializeEmpty(index.hash->condition_types);
	probe_keys.ReferenceColumns(keys, index.columns);
	probe_keys.Slice(selected, probe_count);
	probe_payload.SetChildCardinality(probe_count);
	probe_candidates.InitializeEmpty(condition_types);
	build_candidates.InitializeEmpty(condition_types);
	vector<idx_t> tail;
	for (idx_t col = 0; col < conditions.size(); col++) {
		if (!((equality_mask | dropped) & (uint64_t(1) << col))) {
			tail.push_back(col);
		}
	}
	TupleDataChunkState key_state;
	TupleDataCollection::InitializeChunkState(key_state, index.hash->condition_types);
	JoinHashTable::ScanStructure cursor(*index.hash, key_state);
	JoinHashTable::ProbeState probe_state;
	index.hash->Probe(cursor, probe_keys, key_state, probe_state);
	Vector build_ids(LogicalType::UBIGINT);
	SelectionVector matched(STANDARD_VECTOR_SIZE), remaining(STANDARD_VECTOR_SIZE);
	SelectionVector left_sel(STANDARD_VECTOR_SIZE), right_sel(STANDARD_VECTOR_SIZE);
	while (cursor.count > 0) {
		context.InterruptCheck();
		const auto match_count = cursor.ResolvePredicates(probe_keys, probe_payload, matched, nullptr);
		if (match_count) {
			cursor.GatherResult(build_ids, matched, match_count, index.output_columns[0]);
			FlatVector::SetSize(build_ids, count_t(match_count));
			auto ids = build_ids.Values<uint64_t>();
			for (idx_t offset = 0; offset < match_count;) {
				const auto chunk_index = ids[offset].GetValue() / STANDARD_VECTOR_SIZE;
				idx_t batch_count = 0;
				do {
					left_sel.set_index(batch_count, selected.get_index(matched.get_index(offset)));
					right_sel.set_index(batch_count++, ids[offset++].GetValue() % STANDARD_VECTOR_SIZE);
				} while (offset < match_count && ids[offset].GetValue() / STANDARD_VECTOR_SIZE == chunk_index);
				Fetch(chunk_index);
				for (auto col : tail) {
					probe_candidates.data[col].Slice(keys.data[col], left_sel, batch_count);
					build_candidates.data[col].Slice(chunk.data[col], right_sel, batch_count);
				}
				probe_candidates.SetChildCardinality(batch_count);
				build_candidates.SetChildCardinality(batch_count);
				MarkJoinRowComparison::CompareTail(probe_candidates, build_candidates, conditions, tail, comparison);
				if (finish_candidates) {
					finish_candidates(left_sel, right_sel, batch_count, comparison);
				}
				auto values = comparison.Values<bool>();
				for (idx_t row = 0; row < batch_count; row++) {
					auto value = values[row];
					if (value.IsValid() && !value.GetValue()) {
						continue;
					}
					const auto original = left_sel.get_index(row);
					if (!value.IsValid() || dropped) {
						if (!matches[original]) {
							validity.SetInvalid(original);
						}
					} else if (value.GetValue()) {
						matches[original] = true;
						validity.SetValid(original);
					}
				}
			}
		}
		idx_t remaining_count = 0;
		for (idx_t row = 0; row < cursor.count; row++) {
			const auto local = cursor.sel_vector.get_index(row);
			const auto original = selected.get_index(local);
			if (!matches[original] && !(dropped && !validity.RowIsValid(original))) {
				remaining.set_index(remaining_count++, local);
			}
		}
		cursor.AdvancePointers(remaining, remaining_count);
	}
}

unique_ptr<IEJoinBuildOrders> MarkPatternRefiner::BuildRangeIndex(ExecutionContext &execution,
                                                                  MarkJoinRefinementGroup &group,
                                                                  const vector<idx_t> &driving,
                                                                  const vector<JoinCondition> &range_conditions) {
	auto &manager = BufferManager::GetBufferManager(context);
	vector<LogicalType> types {condition_types[driving[0]], condition_types[driving[1]], LogicalType::UBIGINT};
	ColumnDataCollection input(manager, types);
	ColumnDataAppendState append;
	input.InitializeAppend(append);
	DataChunk projected;
	projected.Initialize(context, types);
	for (auto &selection : group.selections) {
		context.InterruptCheck();
		Fetch(selection.first);
		projected.Reset();
		for (auto row : selection.second) {
			projected.data[0].Append(chunk.GetValue(driving[0], row));
			projected.data[1].Append(chunk.GetValue(driving[1], row));
			projected.data[2].Append(Value::UBIGINT(selection.first * STANDARD_VECTOR_SIZE + row));
		}
		projected.SetChildCardinality(selection.second.size());
		input.Append(append, projected);
	}
	auto first = IEJoinUnion::SortInput(execution, op, range_conditions, input);
	return IEJoinUnion::PrepareBuild(execution, op, range_conditions, std::move(first));
}

void MarkPatternRefiner::RunRangeJoin(ExecutionContext &execution, IEJoinBuildOrders &build,
                                      const vector<idx_t> &driving, const vector<JoinCondition> &range_conditions,
                                      uint64_t probe_mask, uint64_t dropped, const mark_key_fetch_t &probe_fetch,
                                      idx_t probe_count, AllocatedData &markers) {
	auto &manager = BufferManager::GetBufferManager(context);
	vector<LogicalType> types {condition_types[driving[0]], condition_types[driving[1]], LogicalType::UBIGINT};
	ColumnDataCollection input(manager, types);
	ColumnDataAppendState append;
	input.InitializeAppend(append);
	DataChunk projected;
	projected.Initialize(context, types);
	for (idx_t offset = 0; offset < probe_count; offset += STANDARD_VECTOR_SIZE) {
		context.InterruptCheck();
		auto &probe_keys = probe_fetch(offset / STANDARD_VECTOR_SIZE);
		projected.Reset();
		idx_t count = 0;
		for (idx_t row = 0; row < probe_keys.size(); row++) {
			if (markers.get()[offset + row] ||
			    MarkJoinRefinement::NullMask(probe_keys, row, conditions) != probe_mask) {
				continue;
			}
			projected.data[0].Append(probe_keys.GetValue(driving[0], row));
			projected.data[1].Append(probe_keys.GetValue(driving[1], row));
			projected.data[2].Append(Value::UBIGINT(offset + row));
			count++;
		}
		projected.SetChildCardinality(count);
		input.Append(append, projected);
	}
	auto probes = IEJoinUnion::SortInput(execution, op, range_conditions, input);
	auto ranks = IEJoinUnion::PrepareRanks(execution, op, range_conditions, *probes, build);
	auto left_ids = IEJoinUnion::ExtractColumn(*probes, 2, manager);
	IEJoinCursor<uint64_t> left_id(*left_ids), right_id(*build.row_ids);
	IEJoinUnion joiner(build, *ranks);
	unsafe_vector<idx_t> left, right;
	SelectionVector left_sel(STANDARD_VECTOR_SIZE), right_sel(STANDARD_VECTOR_SIZE);
	idx_t probe_rows[STANDARD_VECTOR_SIZE];
	DataChunk probe_candidates, build_candidates;
	probe_candidates.InitializeEmpty(condition_types);
	build_candidates.InitializeEmpty(condition_types);
	vector<idx_t> tail;
	for (idx_t col = 0; col < conditions.size(); col++) {
		if (col != driving[0] && col != driving[1] && !(dropped & (uint64_t(1) << col))) {
			tail.push_back(col);
		}
	}
	idx_t cached_probe_chunk = DConstants::INVALID_INDEX;
	optional_ptr<DataChunk> probe_keys;
	while (joiner.JoinBlocks(left, right)) {
		context.InterruptCheck();
		for (idx_t pair = 0; pair < left.size();) {
			const auto left_chunk = left_id[left[pair]] / STANDARD_VECTOR_SIZE;
			const auto right_chunk = right_id[right[pair]] / STANDARD_VECTOR_SIZE;
			idx_t batch_count = 0;
			while (pair < left.size()) {
				const auto lhs = left_id[left[pair]];
				const auto rhs = right_id[right[pair]];
				if (lhs / STANDARD_VECTOR_SIZE != left_chunk || rhs / STANDARD_VECTOR_SIZE != right_chunk) {
					break;
				}
				pair++;
				if (markers.get()[lhs] == 2 || (dropped && markers.get()[lhs])) {
					continue;
				}
				left_sel.set_index(batch_count, lhs % STANDARD_VECTOR_SIZE);
				right_sel.set_index(batch_count, rhs % STANDARD_VECTOR_SIZE);
				probe_rows[batch_count++] = lhs;
			}
			if (!batch_count) {
				continue;
			}
			if (cached_probe_chunk != left_chunk) {
				probe_keys = probe_fetch(left_chunk);
				cached_probe_chunk = left_chunk;
			}
			Fetch(right_chunk);
			for (auto col : tail) {
				probe_candidates.data[col].Slice(probe_keys->data[col], left_sel, batch_count);
				build_candidates.data[col].Slice(chunk.data[col], right_sel, batch_count);
			}
			probe_candidates.SetChildCardinality(batch_count);
			build_candidates.SetChildCardinality(batch_count);
			MarkJoinRowComparison::CompareTail(probe_candidates, build_candidates, conditions, tail, comparison);
			if (finish_candidates) {
				finish_candidates(left_sel, right_sel, batch_count, comparison);
			}
			auto values = comparison.Values<bool>();
			for (idx_t row = 0; row < batch_count; row++) {
				auto &marker = markers.get()[probe_rows[row]];
				auto value = values[row];
				if (!value.IsValid() || (dropped && value.GetValue())) {
					if (marker != 2) {
						marker = 1;
					}
				} else if (value.GetValue()) {
					marker = 2;
				}
			}
		}
		if (joiner.lrid > 0 && !left.empty() && left.back() == idx_t(joiner.lrid - 1)) {
			const auto marker = markers.get()[left_id[left.back()]];
			if (marker == 2 || (dropped && marker)) {
				joiner.FinishRow();
			}
		}
	}
}

MarkPatternRefiner::~MarkPatternRefiner() = default;

void MarkPatternRefiner::ApplyMarker(idx_t probe, uint8_t marker) {
	if (marker == 2) {
		matches.get()[probe] = true;
		validity.SetValid(probe);
	} else if (marker == 1 && !matches.get()[probe]) {
		validity.SetInvalid(probe);
	}
}

void MarkPatternRefiner::RefineRangePattern(MarkJoinRefinementGroup &group, idx_t probe, uint64_t probe_mask,
                                            uint64_t build_mask, const vector<idx_t> &ranges) {
	if (!refinement_batches.emplace(probe_mask, build_mask).second) {
		return;
	}
	auto select_driving = [&]() {
		auto driving = ranges;
		std::sort(driving.begin(), driving.end(),
		          [&](idx_t lhs, idx_t rhs) { return PhysicalRangeJoin::LessThan(conditions[lhs], conditions[rhs]); });
		driving.resize(2);
		return driving;
	};
	auto &manager = BufferManager::GetBufferManager(context);
	const auto dropped = probe_mask | build_mask;
	auto driving = select_driving();
	vector<JoinCondition> range_conditions;
	for (auto col : driving) {
		range_conditions.push_back(conditions[col].Copy());
	}
	ThreadContext thread(context);
	ExecutionContext execution(context, thread, nullptr);
	auto &build = [&]() -> IEJoinBuildOrders & {
		lock_guard<mutex> guard(lock);
		const auto mask = (uint64_t(1) << driving[0]) | (uint64_t(1) << driving[1]);
		auto &index = group.indexes[mask];
		if (!index) {
			index = make_uniq<MarkJoinRefinementIndex>();
		}
		if (!index->ranges) {
			index->ranges = BuildRangeIndex(execution, group, driving, range_conditions);
		}
		return *index->ranges;
	}();
	auto markers = manager.GetBufferAllocator().Allocate(keys.size());
	for (idx_t row = 0; row < keys.size(); row++) {
		markers.get()[row] = matches.get()[row] ? 2 : !validity.RowIsValid(row);
	}
	RunRangeJoin(
	    execution, build, driving, range_conditions, probe_mask, dropped,
	    [&](idx_t chunk_index) -> DataChunk & {
		    D_ASSERT(chunk_index == 0);
		    return keys;
	    },
	    keys.size(), markers);
	for (idx_t row = 0; row < keys.size(); row++) {
		ApplyMarker(row, markers.get()[row]);
	}
}

void MarkPatternRefiner::RefineOneRange(MarkJoinRefinementGroup &group, uint64_t probe_mask, uint64_t dropped,
                                        idx_t range_column) {
	auto &index = [&]() -> MarkJoinRefinementIndex & {
		lock_guard<mutex> guard(lock);
		auto &cached = group.indexes[uint64_t(1) << range_column];
		if (!cached) {
			auto built = make_uniq<MarkJoinRefinementIndex>();
			const auto comparison_type = conditions[range_column].GetComparisonType();
			const bool maximum = comparison_type == ExpressionType::COMPARE_LESSTHAN ||
			                     comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO;
			for (auto &selection : group.selections) {
				context.InterruptCheck();
				Fetch(selection.first);
				for (auto row : selection.second) {
					auto value = chunk.GetValue(range_column, row);
					if (built->bound.IsNull() || (maximum ? ValueOperations::GreaterThan(value, built->bound)
					                                      : ValueOperations::LessThan(value, built->bound))) {
						built->bound = std::move(value);
						built->witness = selection.first * STANDARD_VECTOR_SIZE + row;
					}
				}
			}
			cached = std::move(built);
		}
		return *cached;
	}();
	RefineWitnessBatch(index.witness, probe_mask);
}

bool MarkPatternRefiner::RefineExact(MarkJoinRefinementGroup &group, idx_t probe, uint64_t dropped) {
	for (auto &selection : group.selections) {
		context.InterruptCheck();
		Fetch(selection.first);
		SelectionVector selected(STANDARD_VECTOR_SIZE);
		idx_t count = 0;
		for (auto row : selection.second) {
			selected.set_index(count++, row);
		}
		candidates.Reference(chunk);
		candidates.Slice(selected, count);
		if (Finish(probe, dropped, selected)) {
			return true;
		}
	}
	return false;
}

void MarkPatternRefiner::Refine() {
	for (idx_t probe = 0; probe < keys.size(); probe++) {
		if (matches.get()[probe] || !validity.RowIsValid(probe)) {
			continue;
		}
		const auto probe_mask = MarkJoinRefinement::NullMask(keys, probe, conditions);
		for (auto &entry : refinement.groups) {
			auto &group = entry.second;
			const auto classification = MarkJoinRefinement::Classify(probe_mask, entry.first, conditions);
			const auto dropped = classification.dropped;
			const bool reduce = classification.reducible;
			bool finished = false;
			if (classification.equality_mask) {
				if (refinement_batches.emplace(probe_mask, entry.first).second) {
					auto &index = BuildEqualityIndex(group, classification.equality_mask);
					ProbeEqualityIndex(index, probe_mask, dropped, classification.equality_mask);
				}
				finished = matches.get()[probe] || !validity.RowIsValid(probe);
			} else if (reduce && classification.ranges.size() >= 2) {
				RefineRangePattern(group, probe, probe_mask, entry.first, classification.ranges);
				finished = matches.get()[probe] || !validity.RowIsValid(probe);
			} else if (!finish_candidates && reduce && classification.applicable_count == 0) {
				const auto &selection = *group.selections.begin();
				finished = RefineWitness(selection.first * STANDARD_VECTOR_SIZE + selection.second[0], probe, dropped);
			} else if (!finish_candidates && reduce && classification.applicable_count == 1 &&
			           !classification.ranges.empty()) {
				if (refinement_batches.emplace(probe_mask, entry.first).second) {
					RefineOneRange(group, probe_mask, dropped, classification.ranges[0]);
				}
				finished = matches.get()[probe] || (dropped && !validity.RowIsValid(probe));
			} else {
				finished = RefineExact(group, probe, dropped);
			}
			if (finished) {
				break;
			}
		}
	}
}

void MarkPatternRefiner::RefineWitnessBatch(idx_t id, uint64_t probe_mask) {
	SelectionVector selected(STANDARD_VECTOR_SIZE);
	idx_t count = 0;
	for (idx_t row = 0; row < keys.size(); row++) {
		if (!matches.get()[row] && validity.RowIsValid(row) &&
		    MarkJoinRefinement::NullMask(keys, row, conditions) == probe_mask) {
			selected.set_index(count++, row);
		}
	}
	if (!count) {
		return;
	}
	Fetch(id / STANDARD_VECTOR_SIZE);
	DataChunk probes;
	probes.InitializeEmpty(condition_types);
	probes.Reference(keys);
	probes.Slice(selected, count);
	vector<idx_t> columns;
	for (idx_t col = 0; col < conditions.size(); col++) {
		ConstantVector::Reference(candidates.data[col], count_t(count), chunk.data[col], id % STANDARD_VECTOR_SIZE,
		                          chunk.size());
		columns.push_back(col);
	}
	candidates.SetChildCardinality(count);
	MarkJoinRowComparison::CompareTail(probes, candidates, conditions, columns, comparison);
	auto values = comparison.Values<bool>();
	for (idx_t row = 0; row < count; row++) {
		auto value = values[row];
		ApplyMarker(selected[row], !value.IsValid() ? 1 : value.GetValue() ? 2 : 0);
	}
}

bool MarkPatternRefiner::RefineWitness(idx_t id, idx_t probe, uint64_t dropped) {
	Fetch(id / STANDARD_VECTOR_SIZE);
	SelectionVector selected(1);
	selected.set_index(0, id % STANDARD_VECTOR_SIZE);
	candidates.Reference(chunk);
	candidates.Slice(selected, 1);
	return Finish(probe, dropped, selected);
}

} // namespace duckdb
