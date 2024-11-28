#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/reservoir_sample.hpp"

namespace duckdb {

IngestionSample::IngestionSample(idx_t sample_count, int64_t seed)
    : BlockingSample(seed), sample_count(sample_count), allocator(Allocator::DefaultAllocator()) {
	base_reservoir_sample = make_uniq<BaseReservoirSampling>(seed);
	type = SampleType::INGESTION_SAMPLE;
	sample_chunk = nullptr;
	sel_size = 0;
	sel = SelectionVector(STANDARD_VECTOR_SIZE);
}

IngestionSample::IngestionSample(Allocator &allocator, int64_t seed)
    : BlockingSample(seed), sample_count(FIXED_SAMPLE_SIZE), allocator(allocator) {
	base_reservoir_sample = make_uniq<BaseReservoirSampling>(seed);
	type = SampleType::INGESTION_SAMPLE;
	sample_chunk = nullptr;
	sel_size = 0;
	sel = SelectionVector(STANDARD_VECTOR_SIZE);
}

idx_t IngestionSample::NumSamplesCollected() const {
	if (!sample_chunk) {
		return 0;
	}
	return sample_chunk->size();
}

idx_t IngestionSample::GetActiveSampleCount() const {
	auto ret = MaxValue<idx_t>(sel_size, base_reservoir_sample->reservoir_weights.size());
	D_ASSERT(ret <= FIXED_SAMPLE_SIZE);
	return ret;
}

unique_ptr<DataChunk> IngestionSample::GetChunk(idx_t offset) {
	throw InternalException("Invalid Call to Get Chunk");
}

unique_ptr<DataChunk> IngestionSample::GetChunkAndShrink() {
	throw InternalException("Invalid Sampling state");
}

idx_t IngestionSample::GetTuplesSeen() {
	return base_reservoir_sample->num_entries_seen_total;
}

unique_ptr<DataChunk> IngestionSample::CreateNewSampleChunk(vector<LogicalType> &types, idx_t size) const {
	auto new_sample_chunk = make_uniq<DataChunk>();
	new_sample_chunk->Initialize(Allocator::DefaultAllocator(), types, size);
	return new_sample_chunk;
}

SamplingMode IngestionSample::SamplingState() const {
	if (base_reservoir_sample->reservoir_weights.empty()) {
		return SamplingMode::FAST;
	}
	return SamplingMode::SLOW;
}

void IngestionSample::Shrink() {
	Verify();
	if (NumSamplesCollected() <= FIXED_SAMPLE_SIZE || !sample_chunk || destroyed) {
		// sample is destroyed or too small to shrink
		return;
	}

	auto types = sample_chunk->GetTypes();
	auto new_sample_chunk = CreateNewSampleChunk(types, FIXED_SAMPLE_SIZE * FIXED_SAMPLE_SIZE_MULTIPLIER);
	idx_t num_samples_to_keep = GetActiveSampleCount();

	if (SamplingState() != SamplingMode::FAST) {
		base_reservoir_sample->min_weighted_entry_index = base_reservoir_sample->reservoir_weights.top().second;
	}

	std::swap(sample_chunk, new_sample_chunk);
	// perform the copy
	UpdateSampleAppend(*new_sample_chunk, sel, num_samples_to_keep);
	D_ASSERT(sel_size == num_samples_to_keep);
	D_ASSERT(sample_chunk->size() == num_samples_to_keep);
	sel = SelectionVector(0, num_samples_to_keep);
	Verify();
	// We should only have one sample chunk now.
	D_ASSERT(sample_chunk->size() > 0 && sample_chunk->size() <= FIXED_SAMPLE_SIZE);
}

unique_ptr<BlockingSample> IngestionSample::Copy() const {
	auto ret = make_uniq<IngestionSample>(sample_count);

	ret->base_reservoir_sample = base_reservoir_sample->Copy();
	ret->destroyed = destroyed;

	if (!sample_chunk || destroyed) {
		return unique_ptr_cast<IngestionSample, BlockingSample>(std::move(ret));
	}

	D_ASSERT(sample_chunk);

	// create a new sample chunk to store new samples
	auto types = sample_chunk->GetTypes();
	idx_t new_sample_chunk_size = FIXED_SAMPLE_SIZE * FIXED_SAMPLE_SIZE_MULTIPLIER;
	// how many values should be copied
	idx_t values_to_copy = MinValue<idx_t>(GetActiveSampleCount(), FIXED_SAMPLE_SIZE);

	auto new_sample_chunk = CreateNewSampleChunk(types, new_sample_chunk_size);

	SelectionVector const_sel(sel);
	ret->sample_chunk = std::move(new_sample_chunk);
	ret->UpdateSampleAppend(*sample_chunk, const_sel, values_to_copy);
	ret->sel = SelectionVector(0, values_to_copy);
	ret->sel_size = sel_size;
	D_ASSERT(ret->sample_chunk->size() <= FIXED_SAMPLE_SIZE);
	ret->Verify();
	return unique_ptr_cast<IngestionSample, BlockingSample>(std::move(ret));
}

void IngestionSample::ConvertToSlowSample() {
	D_ASSERT(sel_size <= STANDARD_VECTOR_SIZE);
	base_reservoir_sample->FillWeights(sel, sel_size);
}

vector<uint32_t> IngestionSample::GetRandomizedVector(uint32_t size) const {
	vector<uint32_t> ret;
	ret.reserve(size);
	for (uint32_t i = 0; i < size; i++) {
		ret.push_back(i);
	}
	if (size <= 1) {
		return ret;
	}
	if (size == 2) {
		if (base_reservoir_sample->random.NextRandom() > 0.5) {
			std::swap(ret[0], ret[1]);
		}
		return ret;
	}
	idx_t upper_bound = size - 1;
	for (idx_t i = 0; i < upper_bound; i++) {
		uint32_t random_shuffle = base_reservoir_sample->random.NextRandomInteger(static_cast<uint32_t>(i + 1),
		                                                                          static_cast<uint32_t>(upper_bound));
		uint32_t tmp = ret[random_shuffle];
		// basically replacing the tuple that was at index actual_sample_indexes[random_shuffle]
		ret[random_shuffle] = ret[i];
		ret[i] = tmp;
	}
	return ret;
}

void IngestionSample::ShuffleSel() {
	idx_t upper_bound = sel_size - 1;
	auto new_indexes = GetRandomizedVector(sel_size);
	for (idx_t i = 0; i < upper_bound; i++) {
		idx_t tmp = sel.get_index(new_indexes[i]);
		sel.set_index(new_indexes[i], sel.get_index(i));
		sel.set_index(i, tmp);
	}
}

void IngestionSample::SimpleMerge(IngestionSample &other) {
	D_ASSERT(GetPriorityQueueSize() == 0);
	D_ASSERT(other.GetPriorityQueueSize() == 0);

	if (other.GetActiveSampleCount() == 0 && other.GetTuplesSeen() == 0) {
		return;
	}

	if (GetActiveSampleCount() == 0 && GetTuplesSeen() == 0) {
		sel = SelectionVector(other.sel);
		sel_size = other.sel_size;
		base_reservoir_sample->num_entries_seen_total = other.GetTuplesSeen();
		return;
	}

	idx_t total_seen = GetTuplesSeen() + other.GetTuplesSeen();

	auto weight_tuples_this = static_cast<double>(GetTuplesSeen()) / static_cast<double>(total_seen);
	auto weight_tuples_other = static_cast<double>(other.GetTuplesSeen()) / static_cast<double>(total_seen);

	// If weights don't add up to 1, most likely a simple merge occured and no new samples were added.
	// if that is the case, add the missing weight to the lower weighted sample to adjust.
	// this is to avoid cases where if you have a 20k row table and add another 20k rows row by row
	// then eventually the missing weights will add up, and get you a more even distribution
	if (weight_tuples_this + weight_tuples_other < 1) {
		weight_tuples_other += 1 - (weight_tuples_other + weight_tuples_this);
	}

	idx_t keep_from_this = 0;
	idx_t keep_from_other = 0;
	if (weight_tuples_this > weight_tuples_other) {
		keep_from_this =
		    MinValue<idx_t>(static_cast<idx_t>(round(FIXED_SAMPLE_SIZE * weight_tuples_this)), GetActiveSampleCount());
		keep_from_other = MinValue<idx_t>(FIXED_SAMPLE_SIZE - keep_from_this, other.GetActiveSampleCount());
	} else {
		keep_from_other = MinValue<idx_t>(static_cast<idx_t>(round(FIXED_SAMPLE_SIZE * weight_tuples_other)),
		                                  other.GetActiveSampleCount());
		keep_from_this = MinValue<idx_t>(FIXED_SAMPLE_SIZE - keep_from_other, GetActiveSampleCount());
	}

	D_ASSERT(keep_from_this <= GetActiveSampleCount());
	D_ASSERT(keep_from_other <= other.GetActiveSampleCount());
	D_ASSERT(keep_from_other + keep_from_this <= FIXED_SAMPLE_SIZE);
	idx_t size_after_merge = MinValue<idx_t>(keep_from_other + keep_from_this, FIXED_SAMPLE_SIZE);

	// Check if appending the other samples to this will go over the sample chunk size
	if (sample_chunk->size() + keep_from_other > FIXED_SAMPLE_SIZE * FIXED_SAMPLE_SIZE_MULTIPLIER) {
		Shrink();
	}

	D_ASSERT(size_after_merge <= other.GetActiveSampleCount() + GetActiveSampleCount());
	SelectionVector chunk_sel(keep_from_other);
	auto offset = sample_chunk->size();
	for (idx_t i = keep_from_this; i < size_after_merge; i++) {
		if (i >= GetActiveSampleCount()) {
			sel.set_index(GetActiveSampleCount(), offset);
			sel_size += 1;
		} else {
			sel.set_index(i, offset);
		}
		chunk_sel.set_index(i - keep_from_this, other.sel.get_index(i - keep_from_this));
		offset += 1;
	}

	D_ASSERT(GetActiveSampleCount() == size_after_merge);

	// Copy the rows that make it to the sample from other and put them into this.
	UpdateSampleAppend(*other.sample_chunk, chunk_sel, keep_from_other);
	base_reservoir_sample->num_entries_seen_total += other.GetTuplesSeen();

	// if THIS has too many samples now, we conver it to a slower sample.
	if (GetTuplesSeen() >= FIXED_SAMPLE_SIZE * FAST_TO_SLOW_THRESHOLD) {
		ConvertToSlowSample();
	}
	Verify();
}

void IngestionSample::Merge(unique_ptr<BlockingSample> other) {
	if (destroyed || other->destroyed) {
		Destroy();
		return;
	}

	D_ASSERT(other->type == SampleType::INGESTION_SAMPLE);
	auto &other_ingest = other->Cast<IngestionSample>();

	// if the other sample has not collected anything yet return
	if (!other_ingest.sample_chunk) {
		return;
	}

	// this has not collected samples, take over the other
	if (!sample_chunk) {
		base_reservoir_sample = std::move(other->base_reservoir_sample);
		sample_chunk = std::move(other_ingest.sample_chunk);
		sel = SelectionVector(other_ingest.sel);
		sel_size = other_ingest.sel_size;
		Verify();
		return;
	}

	//! Both samples are still in "fast sampling" method
	if (SamplingState() == SamplingMode::FAST && other_ingest.SamplingState() == SamplingMode::FAST) {
		SimpleMerge(other_ingest);
		return;
	}

	// One or none of the samples are in "Fast Sampling" method.
	// When this is the case, switch both to slow sampling
	ConvertToSlowSample();
	other_ingest.ConvertToSlowSample();

	D_ASSERT(SamplingState() == SamplingMode::SLOW && other_ingest.SamplingState() == SamplingMode::SLOW);

	idx_t total_samples = GetActiveSampleCount() + other_ingest.GetActiveSampleCount();
	idx_t total_samples_seen =
	    base_reservoir_sample->num_entries_seen_total + other_ingest.base_reservoir_sample->num_entries_seen_total;
	idx_t num_samples_to_keep = MinValue<idx_t>(
	    FIXED_SAMPLE_SIZE, static_cast<idx_t>(static_cast<double>(total_samples_seen) * SAVE_PERCENTAGE));
	if (num_samples_to_keep < GetActiveSampleCount()) {
		num_samples_to_keep = GetActiveSampleCount();
	}
	D_ASSERT(GetActiveSampleCount() <= num_samples_to_keep);
	D_ASSERT(total_samples <= FIXED_SAMPLE_SIZE * 2);

	// pop from base base_reservoir samples and weights until there are num_samples_to_keep left.
	vector<idx_t> this_indexes_to_replace;
	for (idx_t i = num_samples_to_keep; i < total_samples; i++) {
		auto min_weight_this = base_reservoir_sample->min_weight_threshold;
		auto min_weight_other = other_ingest.base_reservoir_sample->min_weight_threshold;
		// min weight threshol is always positive
		if (min_weight_this > min_weight_other) {
			// pop from other
			other_ingest.base_reservoir_sample->reservoir_weights.pop();
			other_ingest.base_reservoir_sample->UpdateMinWeightThreshold();
		} else {
			auto top_this = PopFromWeightQueue();
			this_indexes_to_replace.push_back(top_this.second);
			base_reservoir_sample->UpdateMinWeightThreshold();
		}
	}

	D_ASSERT(other_ingest.GetPriorityQueueSize() + GetPriorityQueueSize() <= FIXED_SAMPLE_SIZE);
	D_ASSERT(other_ingest.GetPriorityQueueSize() + GetPriorityQueueSize() == num_samples_to_keep);
	D_ASSERT(other_ingest.sample_chunk->GetTypes() == sample_chunk->GetTypes());

	auto min_weight = base_reservoir_sample->min_weight_threshold;
	auto min_weight_index = base_reservoir_sample->min_weighted_entry_index;

	// Prepare a selection vector to copy data from the other sample chunk to this sample chunk
	SelectionVector sel_other(other_ingest.GetPriorityQueueSize());
	D_ASSERT(GetPriorityQueueSize() <= num_samples_to_keep);
	idx_t chunk_offset = 0;

	// now we are adding entries from the other base_reservoir_sampling object to this
	// Depending on how many sample values "this" has, we either need to add to the selection vector
	// Or replace values in "this'" selection vector
	idx_t i = 0;
	while (other_ingest.GetPriorityQueueSize() > 0) {
		auto other_top = other_ingest.PopFromWeightQueue();
		auto other_weight = -other_top.first;
		idx_t index_for_new_pair = chunk_offset + sample_chunk->size();
		if (other_weight < min_weight) {
			min_weight = other_weight;
			min_weight_index = index_for_new_pair;
		}

		// update the sel used to copy values from other to this
		sel_other.set_index(chunk_offset, other_top.second);
		if (i < this_indexes_to_replace.size()) {
			auto replacement_index = this_indexes_to_replace[i];
			sel.set_index(replacement_index, index_for_new_pair);
			other_top.second = replacement_index;
		} else {
			sel.set_index(sel_size, index_for_new_pair);
			other_top.second = sel_size;
			sel_size += 1;
		}

		// make sure that the sample indexes are (this.sample_chunk.size() + chunk_offfset)
		base_reservoir_sample->reservoir_weights.push(other_top);
		chunk_offset += 1;
		i += 1;
	}

	D_ASSERT(GetPriorityQueueSize() == num_samples_to_keep);
	base_reservoir_sample->min_weighted_entry_index = min_weight_index;
	base_reservoir_sample->min_weight_threshold = min_weight;
	D_ASSERT(base_reservoir_sample->min_weight_threshold > 0);
	base_reservoir_sample->num_entries_seen_total = GetTuplesSeen() + other_ingest.GetTuplesSeen();

	// fix, basically you only need to copy the required tuples from other and put them into this. You can
	// save a number of the tuples in THIS.
	UpdateSampleAppend(*other_ingest.sample_chunk, sel_other, chunk_offset);
	if (sample_chunk->size() > FIXED_SAMPLE_SIZE * (FIXED_SAMPLE_SIZE_MULTIPLIER - 3)) {
		Shrink();
	}

	Verify();
}

void IngestionSample::NormalizeWeights(BaseReservoirSampling &base_sampling) {
	vector<std::pair<double, idx_t>> tmp_weights;
	while (!base_sampling.reservoir_weights.empty()) {
		auto top = base_sampling.reservoir_weights.top();
		tmp_weights.push_back(std::move(top));
		base_sampling.reservoir_weights.pop();
	}
	std::sort(tmp_weights.begin(), tmp_weights.end(),
	          [&](std::pair<double, idx_t> a, std::pair<double, idx_t> b) { return a.second < b.second; });
	for (idx_t i = 0; i < tmp_weights.size(); i++) {
		base_sampling.reservoir_weights.emplace(tmp_weights.at(i).first, i);
	}
	base_sampling.SetNextEntry();
}

unique_ptr<BlockingSample> IngestionSample::ConvertToReservoirSample() {
	Shrink();
	Verify();
	if (!sample_chunk || destroyed) {
		auto ret = make_uniq<ReservoirSample>(FIXED_SAMPLE_SIZE);
		ret->Destroy();
		return unique_ptr_cast<ReservoirSample, BlockingSample>(std::move(ret));
	}

	// since this is for serialization, we really need to make sure keep a
	// minimum of 1% of the rows or 2048 rows
	idx_t num_samples_to_keep =
	    MinValue<idx_t>(FIXED_SAMPLE_SIZE, static_cast<idx_t>(SAVE_PERCENTAGE * static_cast<double>(GetTuplesSeen())));

	auto ret = make_uniq<ReservoirSample>(num_samples_to_keep);
	ret->base_reservoir_sample = base_reservoir_sample->Copy();
	if (num_samples_to_keep <= 0) {
		return unique_ptr_cast<ReservoirSample, BlockingSample>(std::move(ret));
	}

	// if we over sampled, make sure we only keep the highest percentage samples
	unordered_set<idx_t> selections_to_delete;
	while (num_samples_to_keep < ret->GetPriorityQueueSize()) {
		auto top = ret->PopFromWeightQueue();
		D_ASSERT(top.second < sel_size);
		selections_to_delete.emplace(top.second);
	}

	// set up reservoir chunk for the reservoir sample
	D_ASSERT(sample_chunk->size() <= FIXED_SAMPLE_SIZE);
	// create a new sample chunk to store new samples
	ret->reservoir_chunk = make_uniq<ReservoirChunk>();
	auto types = sample_chunk->GetTypes();

	ret->reservoir_chunk->chunk.Initialize(Allocator::DefaultAllocator(), sample_chunk->GetTypes(),
	                                       num_samples_to_keep);
	idx_t new_size = ret->reservoir_chunk->chunk.size() + num_samples_to_keep;

	// The current selection vector can potentially have 2048 valid mappings.
	// If we need to save a sample with less rows than that, we need to do the following
	// 1. Create a new selection vector that doesn't point to the rows we are evicting
	SelectionVector new_sel(num_samples_to_keep);
	idx_t offset = 0;
	for (idx_t i = 0; i < num_samples_to_keep + selections_to_delete.size(); i++) {
		if (selections_to_delete.find(i) == selections_to_delete.end()) {
			D_ASSERT(i - offset < num_samples_to_keep);
			new_sel.set_index(i - offset, sel.get_index(i));
		} else {
			offset += 1;
		}
	}
	// 2. Normalize our reservoir weights to not store chunk ids to rows that
	//    have been evicted.
	if (!selections_to_delete.empty()) {
		NormalizeWeights(*ret->base_reservoir_sample);
	}

	D_ASSERT(sample_chunk->GetTypes() == ret->reservoir_chunk->chunk.GetTypes());

	UpdateSampleWithTypes(ret->reservoir_chunk->chunk, *sample_chunk, new_sel, num_samples_to_keep, 0, 0);
	// set the cardinality
	ret->reservoir_chunk->chunk.SetCardinality(new_size);
	ret->base_reservoir_sample->num_entries_seen_total = base_reservoir_sample->num_entries_seen_total;
	if (!ret->base_reservoir_sample->reservoir_weights.empty()) {
		ret->base_reservoir_sample->SetNextEntry();
	}

	D_ASSERT(ret->reservoir_chunk->chunk.size() == num_samples_to_keep);
	// D_ASSERT(ret->GetPriorityQueueSize() == ret->reservoir_chunk->chunk.size());
	return unique_ptr_cast<ReservoirSample, BlockingSample>(std::move(ret));
}

idx_t IngestionSample::FillReservoir(DataChunk &chunk) {

	idx_t ingested_count = 0;
	if (!sample_chunk) {
		if (chunk.size() > FIXED_SAMPLE_SIZE) {
			throw InternalException("Creating sample with DataChunk that is larger than the fixed sample size");
		}
		auto types = chunk.GetTypes();
		// create a new sample chunk to store new samples
		sample_chunk = CreateNewSampleChunk(types, FIXED_SAMPLE_SIZE * FIXED_SAMPLE_SIZE_MULTIPLIER);
	}

	idx_t actual_sample_index_start = GetActiveSampleCount();
	D_ASSERT(sample_chunk->ColumnCount() == chunk.ColumnCount());
	if (sample_chunk->size() < FIXED_SAMPLE_SIZE) {
		ingested_count = MinValue<idx_t>(FIXED_SAMPLE_SIZE - sample_chunk->size(), chunk.size());
		SelectionVector sel_for_input_chunk(ingested_count);
		for (idx_t i = 0; i < ingested_count; i++) {
			sel.set_index(actual_sample_index_start + i, actual_sample_index_start + i);
			sel_for_input_chunk.set_index(i, i);
		}
		UpdateSampleAppend(chunk, sel_for_input_chunk, ingested_count);
	}
	sel_size += ingested_count;
	ShuffleSel();
	D_ASSERT(GetActiveSampleCount() <= FIXED_SAMPLE_SIZE);
	D_ASSERT(GetActiveSampleCount() >= ingested_count);
	// always return how many tuples were ingested
	return ingested_count;
}

void IngestionSample::Destroy() {
	destroyed = true;
}

SelectionVectorHelper IngestionSample::GetReplacementIndexes(idx_t sample_chunk_offset,
                                                             idx_t theoretical_chunk_length) {
	if (GetPriorityQueueSize() == 0 && GetTuplesSeen() <= FIXED_SAMPLE_SIZE * IngestionSample::FAST_TO_SLOW_THRESHOLD) {
		return GetReplacementIndexesFast(sample_chunk_offset, theoretical_chunk_length);
	}
	return GetReplacementIndexesSlow(sample_chunk_offset, theoretical_chunk_length);
}

SelectionVectorHelper IngestionSample::GetReplacementIndexesFast(idx_t sample_chunk_offset, idx_t chunk_length) {
	auto num_to_pop = static_cast<uint32_t>(
	    (static_cast<double>(chunk_length) / static_cast<double>(chunk_length + GetTuplesSeen())) *
	    static_cast<double>(chunk_length));
	D_ASSERT(num_to_pop < chunk_length);

	unordered_map<idx_t, idx_t> replacement_indexes;
	SelectionVector ret_sel(num_to_pop);

	auto random_indexes = GetRandomizedVector(num_to_pop);

	D_ASSERT(sel_size == STANDARD_VECTOR_SIZE);
	// randomize the possible indexes to sample from the theoretical chunk length
	for (idx_t i = 0; i < num_to_pop; i++) {
		// update the selection vector for the reservoir sample
		idx_t random_shuffle = base_reservoir_sample->random.NextRandomInteger(
		    static_cast<uint32_t>(i + 1), static_cast<uint32_t>(GetActiveSampleCount()));
		// basically replacing the tuple that was at sel[random_shuffle].
		auto tmp = sel.get_index(i);
		sel.set_index(random_shuffle, tmp);
		sel.set_index(i, sample_chunk_offset + i);

		ret_sel.set_index(i, random_indexes[i]);
	}

	SelectionVectorHelper ret;
	ret.sel = SelectionVector(ret_sel);
	ret.size = num_to_pop;
	return ret;
}

SelectionVectorHelper IngestionSample::GetReplacementIndexesSlow(const idx_t sample_chunk_offset,
                                                                 const idx_t chunk_length) {
	idx_t remaining = chunk_length;
	unordered_map<idx_t, idx_t> ret_map;
	idx_t sample_chunk_index = 0;

	idx_t base_offset = 0;

	while (true) {
		idx_t offset =
		    base_reservoir_sample->next_index_to_sample - base_reservoir_sample->num_entries_to_skip_b4_next_sample;
		if (offset >= remaining) {
			// not in this chunk! increment current count and go to the next chunk
			base_reservoir_sample->num_entries_to_skip_b4_next_sample += remaining;
			break;
		}
		// in this chunk! replace the element
		// ret[index_in_new_chunk] = index_in_sample_chunk (the sample chunk offset will be applied later)
		// D_ASSERT(sample_chunk_index == ret.size());
		ret_map[base_offset + offset] = sample_chunk_index;
		double r2 = base_reservoir_sample->random.NextRandom(base_reservoir_sample->min_weight_threshold, 1);
		// replace element in our max_heap
		// first get the top most pair
		const auto top = PopFromWeightQueue();
		const auto index = top.second;
		const auto index_in_sample_chunk = sample_chunk_offset + sample_chunk_index;
		sel.set_index(index, index_in_sample_chunk);
		base_reservoir_sample->ReplaceElementWithIndex(index, r2, false);

		sample_chunk_index += 1;
		// shift the chunk forward
		remaining -= offset;
		base_offset += offset;
	}

	// create set of random indexes to update the ingestion sample selection vector.
	// auto random_indexes = GetRandomizedVector(static_cast<uint32_t>(ret_map.size()));

	// create selection vector to return
	SelectionVector ret_sel(ret_map.size());
	D_ASSERT(sel_size == STANDARD_VECTOR_SIZE);
	for (auto &kv : ret_map) {
		ret_sel.set_index(kv.second, kv.first);
	}
	SelectionVectorHelper ret;
	ret.sel = SelectionVector(ret_sel);
	ret.size = static_cast<uint32_t>(ret_map.size());
	return ret;
}

void IngestionSample::Finalize() {
	return;
}

bool IngestionSample::ValidSampleType(const LogicalType &type) {
	return type.IsNumeric();
}

void IngestionSample::UpdateSampleWithTypes(DataChunk &this_, DataChunk &other, SelectionVector &sel,
                                            idx_t source_count, idx_t source_offset, idx_t target_offset) {
	D_ASSERT(sample_chunk->GetTypes() == other.GetTypes());
	auto types = sample_chunk->GetTypes();

	for (idx_t i = 0; i < sample_chunk->ColumnCount(); i++) {
		auto col_type = types[i];
		if (ValidSampleType(col_type)) {
			D_ASSERT(this_.data[i].GetVectorType() == VectorType::FLAT_VECTOR);
			VectorOperations::Copy(other.data[i], this_.data[i], sel, source_count, source_offset, target_offset);
		}
	}
}

void IngestionSample::UpdateSampleAppend(DataChunk &other, SelectionVector &sel, idx_t append_count) {
	idx_t new_size = sample_chunk->size() + append_count;
	if (other.size() == 0) {
		return;
	}
	D_ASSERT(sample_chunk->GetTypes() == other.GetTypes());

	UpdateSampleWithTypes(*sample_chunk, other, sel, append_count, 0, sample_chunk->size());
	sample_chunk->SetCardinality(new_size);
}

void IngestionSample::AddToReservoir(DataChunk &chunk) {
	if (destroyed || chunk.size() == 0) {
		return;
	}

	idx_t tuples_consumed = FillReservoir(chunk);
	base_reservoir_sample->num_entries_seen_total += tuples_consumed;
	D_ASSERT(sample_chunk->size() >= 1);

	if (tuples_consumed == chunk.size()) {
		return;
	}

	// the chunk filled the first FIXED_SAMPLE_SIZE chunk but still has tuples remaining
	// slice the chunk and call AddToReservoir again.
	if (tuples_consumed != chunk.size() && tuples_consumed != 0) {
		// Fill reservoir consumed some of the chunk to reach FIXED_SAMPLE_SIZE
		// now we need to
		// So we slice it and call AddToReservoir
		auto slice = make_uniq<DataChunk>();
		auto samples_remaining = chunk.size() - tuples_consumed;
		auto types = chunk.GetTypes();
		SelectionVector input_sel(samples_remaining);
		for (idx_t i = 0; i < samples_remaining; i++) {
			input_sel.set_index(i, tuples_consumed + i);
		}
		slice->Initialize(Allocator::DefaultAllocator(), types, samples_remaining);
		slice->Slice(chunk, input_sel, samples_remaining);
		slice->SetCardinality(samples_remaining);
		AddToReservoir(*slice);
		return;
	}

	// at this point our sample_chunk has at least FIXED SAMPLE SIZE samples.
	D_ASSERT(sample_chunk->size() >= FIXED_SAMPLE_SIZE);

	auto chunk_sel = GetReplacementIndexes(sample_chunk->size(), chunk.size());

	if (chunk_sel.size == 0) {
		// not adding any samples
		return;
	}
	idx_t size = chunk_sel.size;
	D_ASSERT(size <= chunk.size());

	UpdateSampleAppend(chunk, chunk_sel.sel, size);

	base_reservoir_sample->num_entries_seen_total += chunk.size();
	D_ASSERT(base_reservoir_sample->reservoir_weights.size() == 0 ||
	         base_reservoir_sample->reservoir_weights.size() == FIXED_SAMPLE_SIZE);

	Verify();

	// if we are over the threshold, we ned to swith to slow sampling.
	if (SamplingState() == SamplingMode::FAST && GetTuplesSeen() >= FIXED_SAMPLE_SIZE * FAST_TO_SLOW_THRESHOLD) {
		ConvertToSlowSample();
	}
	if (sample_chunk->size() >= FIXED_SAMPLE_SIZE * (FIXED_SAMPLE_SIZE_MULTIPLIER - 3)) {
		Shrink();
	}
}

void IngestionSample::Verify() {
#ifdef DEBUG
	if (destroyed) {
		return;
	}
	if (GetPriorityQueueSize() == 0) {
		D_ASSERT(GetActiveSampleCount() <= FIXED_SAMPLE_SIZE);
		D_ASSERT(GetTuplesSeen() >= GetActiveSampleCount());
		return;
	}
	if (NumSamplesCollected() > FIXED_SAMPLE_SIZE) {
		D_ASSERT(GetPriorityQueueSize() == FIXED_SAMPLE_SIZE);
	} else if (NumSamplesCollected() <= FIXED_SAMPLE_SIZE && GetPriorityQueueSize() > 0) {
		// it's possible to collect more samples than your priority queue size.
		// see sample_converts_to_reservoir_sample.test
		D_ASSERT(NumSamplesCollected() >= GetPriorityQueueSize());
	}
	auto base_reservoir_copy = base_reservoir_sample->Copy();
	unordered_map<idx_t, idx_t> index_count;
	while (!base_reservoir_copy->reservoir_weights.empty()) {
		auto &pair = base_reservoir_copy->reservoir_weights.top();
		if (index_count.find(pair.second) == index_count.end()) {
			index_count[pair.second] = 1;
			base_reservoir_copy->reservoir_weights.pop();
		} else {
			index_count[pair.second] += 1;
			Printer::Print("duplicate index in reservoir weights " + to_string(pair.second));
			base_reservoir_copy->reservoir_weights.pop();
			D_ASSERT(false);
		}
	}
	// TODO: Verify the Sel as well. No duplicate indices.

	if (sample_chunk) {
		sample_chunk->Verify();
	}
#endif
}

} // namespace duckdb
