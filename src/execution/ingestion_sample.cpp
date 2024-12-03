#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/reservoir_sample.hpp"

namespace duckdb {

ReservoirSample::ReservoirSample(idx_t sample_count, unique_ptr<ReservoirChunk> reservoir_chunk)
    : ReservoirSample(sample_count, 1) {
	if (reservoir_chunk) {
		this->reservoir_chunk = std::move(reservoir_chunk);
		sel_size = this->reservoir_chunk->chunk.size();
		sel = SelectionVector(0, sel_size);
		ExpandSerializedSample();
	}
	internal_sample = true;
}

ReservoirSample::ReservoirSample(Allocator &allocator, int64_t seed)
    : ReservoirSample(allocator, FIXED_SAMPLE_SIZE, seed) {
	internal_sample = true;
}

ReservoirSample::ReservoirSample(Allocator &allocator, idx_t sample_count, int64_t seed)
    : BlockingSample(seed), sample_count(sample_count), allocator(allocator) {
	base_reservoir_sample = make_uniq<BaseReservoirSampling>(seed);
	type = SampleType::RESERVOIR_SAMPLE;
	reservoir_chunk = nullptr;
	internal_sample = false;
	sel = SelectionVector(sample_count);
	sel_size = 0;
}

ReservoirSample::ReservoirSample(idx_t sample_count, int64_t seed)
    : ReservoirSample(Allocator::DefaultAllocator(), sample_count, seed) {
	internal_sample = true;
}

void PrintSel(SelectionVector sel, idx_t sel_size) {
	for (idx_t i = 0; i < sel_size; i++) {
		Printer::Print(to_string(sel.get_index(i)));
	}
}

idx_t ReservoirSample::NumSamplesCollected() const {
	if (!reservoir_chunk) {
		return 0;
	}
	return reservoir_chunk->chunk.size();
}

SamplingMode ReservoirSample::SamplingState() const {
	if (base_reservoir_sample->reservoir_weights.empty()) {
		return SamplingMode::FAST;
	}
	return SamplingMode::SLOW;
}

idx_t ReservoirSample::GetActiveSampleCount() const {
	switch (SamplingState()) {
	case SamplingMode::FAST:
		return sel_size;
	case SamplingMode::SLOW:
		return base_reservoir_sample->reservoir_weights.size();
	default:
		throw InternalException("Sampling State is INVALID");
	}
}

idx_t ReservoirSample::GetTuplesSeen() const {
	return base_reservoir_sample->num_entries_seen_total;
}

DataChunk &ReservoirSample::Chunk() {
	D_ASSERT(reservoir_chunk);
	return reservoir_chunk->chunk;
}

unique_ptr<DataChunk> ReservoirSample::GetChunk(idx_t offset, bool destroy) {
	if (destroyed || !reservoir_chunk || Chunk().size() == 0) {
		return nullptr;
	}
	// cannot destory internal samples.
	D_ASSERT(destroy != internal_sample);
	auto ret = make_uniq<DataChunk>();
	idx_t ret_chunk_size = STANDARD_VECTOR_SIZE;
	idx_t samples_2_return = 0;
	auto samples_2_keep = GetActiveSampleCount();
	if (internal_sample) {
		D_ASSERT(sample_count == STANDARD_VECTOR_SIZE);
		samples_2_return = MinValue<idx_t>(ret_chunk_size, static_cast<idx_t>(GetTuplesSeen() * SAVE_PERCENTAGE));
		if (offset >= samples_2_return) {
			return nullptr;
		}
		samples_2_keep = 0;
	} else {
		if (samples_2_keep > STANDARD_VECTOR_SIZE) {
			samples_2_keep = samples_2_keep - STANDARD_VECTOR_SIZE;
			samples_2_return = STANDARD_VECTOR_SIZE;
		} else {
			samples_2_return = samples_2_keep;
			samples_2_keep = 0;
		}
	}

	if (samples_2_return == 0) {
		return nullptr;
	}

	SelectionVector ret_sel(samples_2_return);

	for (idx_t i = samples_2_keep; i < (samples_2_keep + samples_2_return); i++) {
		if (destroy) {
			D_ASSERT(!internal_sample);
			// pop samples and reduce size of selection vector.
			if (SamplingState() == SamplingMode::SLOW) {
				auto top = PopFromWeightQueue();
				ret_sel.set_index(i - samples_2_keep, sel.get_index(top.second));
			} else {
				ret_sel.set_index(i - samples_2_keep, sel.get_index(i));
			}
			sel_size -= 1;
		} else {
			// samples 2 keep should be zero since we don't store more than 2048 values.
			D_ASSERT(samples_2_keep == 0);
			D_ASSERT(internal_sample);
			ret_sel.set_index(i, sel.get_index(i));
		}
	}

	auto reservoir_types = Chunk().GetTypes();

	ret->Initialize(allocator, reservoir_types, samples_2_return);
	ret->Slice(Chunk(), ret_sel, samples_2_return);
	ret->SetCardinality(samples_2_return);
	return ret;
}

unique_ptr<ReservoirChunk> ReservoirSample::CreateNewSampleChunk(vector<LogicalType> &types, idx_t size) const {
	auto new_sample_chunk = make_uniq<ReservoirChunk>();
	new_sample_chunk->chunk.Initialize(Allocator::DefaultAllocator(), types, size);

	// set the NULL columns correctly
	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		if (!ValidSampleType(types[col_idx]) && internal_sample) {
			new_sample_chunk->chunk.data[col_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(new_sample_chunk->chunk.data[col_idx], true);
		}
	}
	return new_sample_chunk;
}

void ReservoirSample::Shrink() {
	Verify();
	if (NumSamplesCollected() <= FIXED_SAMPLE_SIZE || !reservoir_chunk || destroyed) {
		// sample is destroyed or too small to shrink
		return;
	}

	auto types = Chunk().GetTypes();
	auto new_sample_chunk = CreateNewSampleChunk(types, GetReservoirChunkCapacity());
	idx_t num_samples_to_keep = GetActiveSampleCount();

	if (SamplingState() != SamplingMode::FAST) {
		base_reservoir_sample->min_weighted_entry_index = base_reservoir_sample->reservoir_weights.top().second;
	}

	// perform the copy
	UpdateSampleAppend(new_sample_chunk->chunk, reservoir_chunk->chunk, sel, num_samples_to_keep);
	// swap the two chunks
	std::swap(reservoir_chunk, new_sample_chunk);
	D_ASSERT(sel_size == num_samples_to_keep);
	D_ASSERT(Chunk().size() == num_samples_to_keep);
	sel = SelectionVector(num_samples_to_keep);
	for (idx_t i = 0; i < sel_size; i++) {
		sel.set_index(i, i);
	}
	Verify();
	// We should only have one sample chunk now.
	D_ASSERT(Chunk().size() > 0 && Chunk().size() <= num_samples_to_keep);
}

unique_ptr<BlockingSample> ReservoirSample::Copy() const {

	// only internal samples can be copied.
	D_ASSERT(internal_sample);
	auto ret = make_uniq<ReservoirSample>(sample_count);

	ret->base_reservoir_sample = base_reservoir_sample->Copy();
	ret->destroyed = destroyed;

	if (!reservoir_chunk || destroyed) {
		return unique_ptr_cast<ReservoirSample, BlockingSample>(std::move(ret));
	}

	D_ASSERT(reservoir_chunk);

	// create a new sample chunk to store new samples
	auto types = reservoir_chunk->chunk.GetTypes();
	// how many values should be copied
	idx_t values_to_copy = MinValue<idx_t>(GetActiveSampleCount(), sample_count);

	auto new_sample_chunk = CreateNewSampleChunk(types, GetReservoirChunkCapacity());

	SelectionVector const_sel(sel);
	ret->reservoir_chunk = std::move(new_sample_chunk);
	ret->UpdateSampleAppend(ret->reservoir_chunk->chunk, reservoir_chunk->chunk, const_sel, values_to_copy);
	ret->sel = SelectionVector(0, values_to_copy);
	ret->sel_size = sel_size;
	D_ASSERT(ret->reservoir_chunk->chunk.size() <= sample_count);
	ret->Verify();
	return unique_ptr_cast<ReservoirSample, BlockingSample>(std::move(ret));
}

void ReservoirSample::ConvertToSlowSample() {
	D_ASSERT(sel_size <= sample_count);
	base_reservoir_sample->FillWeights(sel, sel_size);
}

vector<uint32_t> ReservoirSample::GetRandomizedVector(uint32_t size) const {
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

void ReservoirSample::ShuffleSel() {
	auto new_indexes = GetRandomizedVector(static_cast<uint32_t>(sel_size));
	for (idx_t i = 0; i < sel_size; i++) {
		idx_t tmp = sel.get_index(new_indexes[i]);
		sel.set_index(new_indexes[i], sel.get_index(i));
		sel.set_index(i, tmp);
	}
}

void ReservoirSample::SimpleMerge(ReservoirSample &other) {
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
	D_ASSERT(internal_sample);
	D_ASSERT(sample_count == FIXED_SAMPLE_SIZE);
	D_ASSERT(sample_count == other.sample_count);
	auto sample_count_double = static_cast<double>(sample_count);

	if (weight_tuples_this > weight_tuples_other) {
		keep_from_this = MinValue<idx_t>(static_cast<idx_t>(round(sample_count_double * weight_tuples_this)),
		                                 GetActiveSampleCount());
		keep_from_other = MinValue<idx_t>(sample_count - keep_from_this, other.GetActiveSampleCount());
	} else {
		keep_from_other = MinValue<idx_t>(static_cast<idx_t>(round(sample_count_double * weight_tuples_other)),
		                                  other.GetActiveSampleCount());
		keep_from_this = MinValue<idx_t>(sample_count - keep_from_other, GetActiveSampleCount());
	}

	D_ASSERT(keep_from_this <= GetActiveSampleCount());
	D_ASSERT(keep_from_other <= other.GetActiveSampleCount());
	D_ASSERT(keep_from_other + keep_from_this <= FIXED_SAMPLE_SIZE);
	idx_t size_after_merge = MinValue<idx_t>(keep_from_other + keep_from_this, FIXED_SAMPLE_SIZE);

	// Check if appending the other samples to this will go over the sample chunk size
	if (reservoir_chunk->chunk.size() + keep_from_other > GetReservoirChunkCapacity()) {
		Shrink();
	}

	D_ASSERT(size_after_merge <= other.GetActiveSampleCount() + GetActiveSampleCount());
	SelectionVector chunk_sel(keep_from_other);
	auto offset = reservoir_chunk->chunk.size();
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
	UpdateSampleAppend(reservoir_chunk->chunk, other.reservoir_chunk->chunk, chunk_sel, keep_from_other);
	base_reservoir_sample->num_entries_seen_total += other.GetTuplesSeen();

	// if THIS has too many samples now, we conver it to a slower sample.
	if (GetTuplesSeen() >= FIXED_SAMPLE_SIZE * FAST_TO_SLOW_THRESHOLD) {
		ConvertToSlowSample();
	}
	Verify();
}

void ReservoirSample::WeightedMerge(ReservoirSample &other_ingest) {
	D_ASSERT(SamplingState() == SamplingMode::SLOW);
	D_ASSERT(other_ingest.SamplingState() == SamplingMode::SLOW);

	// Find out how many samples we want to keep.
	idx_t total_samples = GetActiveSampleCount() + other_ingest.GetActiveSampleCount();
	idx_t total_samples_seen =
	    base_reservoir_sample->num_entries_seen_total + other_ingest.base_reservoir_sample->num_entries_seen_total;
	idx_t num_samples_to_keep = MinValue<idx_t>(total_samples, MinValue<idx_t>(sample_count, total_samples_seen));

	D_ASSERT(GetActiveSampleCount() <= num_samples_to_keep);
	D_ASSERT(total_samples <= FIXED_SAMPLE_SIZE * 2);

	// pop from base base_reservoir weights until there are num_samples_to_keep left.
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
	D_ASSERT(other_ingest.reservoir_chunk->chunk.GetTypes() == reservoir_chunk->chunk.GetTypes());

	// Prepare a selection vector to copy data from the other sample chunk to this sample chunk
	SelectionVector sel_other(other_ingest.GetPriorityQueueSize());
	D_ASSERT(GetPriorityQueueSize() <= num_samples_to_keep);
	D_ASSERT(other_ingest.GetPriorityQueueSize() >= this_indexes_to_replace.size());
	idx_t chunk_offset = 0;

	// Now push weights from other.base_reservoir_sample to this
	// Depending on how many sample values "this" has, we either need to add to the selection vector
	// Or replace values in "this'" selection vector
	idx_t i = 0;
	while (other_ingest.GetPriorityQueueSize() > 0) {
		auto other_top = other_ingest.PopFromWeightQueue();
		idx_t index_for_new_pair = chunk_offset + reservoir_chunk->chunk.size();

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

	base_reservoir_sample->UpdateMinWeightThreshold();
	D_ASSERT(base_reservoir_sample->min_weight_threshold > 0);
	base_reservoir_sample->num_entries_seen_total = GetTuplesSeen() + other_ingest.GetTuplesSeen();

	// basically you only need to copy the required tuples from other and put them into this. You can
	// save a number of the tuples in THIS.
	UpdateSampleAppend(reservoir_chunk->chunk, other_ingest.reservoir_chunk->chunk, sel_other, chunk_offset);
	if (reservoir_chunk->chunk.size() > FIXED_SAMPLE_SIZE * (FIXED_SAMPLE_SIZE_MULTIPLIER - 3)) {
		Shrink();
	}

	Verify();
}

void ReservoirSample::Merge(unique_ptr<BlockingSample> other) {
	if (destroyed || other->destroyed) {
		Destroy();
		return;
	}

	D_ASSERT(other->type == SampleType::RESERVOIR_SAMPLE);
	auto &other_ingest = other->Cast<ReservoirSample>();

	// if the other sample has not collected anything yet return
	if (!other_ingest.reservoir_chunk) {
		return;
	}

	// this has not collected samples, take over the other
	if (!reservoir_chunk) {
		base_reservoir_sample = std::move(other->base_reservoir_sample);
		reservoir_chunk = std::move(other_ingest.reservoir_chunk);
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
	WeightedMerge(other_ingest);
}

void ReservoirSample::NormalizeWeights() {
	vector<std::pair<double, idx_t>> tmp_weights;
	while (!base_reservoir_sample->reservoir_weights.empty()) {
		auto top = base_reservoir_sample->reservoir_weights.top();
		tmp_weights.push_back(std::move(top));
		base_reservoir_sample->reservoir_weights.pop();
	}
	std::sort(tmp_weights.begin(), tmp_weights.end(),
	          [&](std::pair<double, idx_t> a, std::pair<double, idx_t> b) { return a.second < b.second; });
	for (idx_t i = 0; i < tmp_weights.size(); i++) {
		base_reservoir_sample->reservoir_weights.emplace(tmp_weights.at(i).first, i);
	}
	base_reservoir_sample->SetNextEntry();
}

unique_ptr<BlockingSample> ReservoirSample::PrepareForSerialization() {
	Shrink();
	Verify();
	if (!reservoir_chunk || destroyed) {
		auto ret = make_uniq<ReservoirSample>(FIXED_SAMPLE_SIZE);
		ret->Destroy();
		return unique_ptr_cast<ReservoirSample, BlockingSample>(std::move(ret));
	}

	// since this is for serialization, we really need to make sure keep a
	// minimum of 1% of the rows or 2048 rows
	idx_t num_samples_to_keep =
	    MinValue<idx_t>(FIXED_SAMPLE_SIZE, static_cast<idx_t>(SAVE_PERCENTAGE * static_cast<double>(GetTuplesSeen())));

	auto ret = make_uniq<ReservoirSample>(sample_count);
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
	D_ASSERT(reservoir_chunk->chunk.size() <= FIXED_SAMPLE_SIZE);
	// create a new sample chunk to store new samples
	auto types = reservoir_chunk->chunk.GetTypes();
	ret->reservoir_chunk = CreateNewSampleChunk(types, num_samples_to_keep);

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
	// 2. Update row_ids in our weights so that they don't store rows ids to
	//    indexes in the selection vector that have been evicted.
	if (!selections_to_delete.empty()) {
		ret->NormalizeWeights();
	}

	D_ASSERT(reservoir_chunk->chunk.GetTypes() == ret->reservoir_chunk->chunk.GetTypes());

	UpdateSampleAppend(ret->reservoir_chunk->chunk, reservoir_chunk->chunk, new_sel, num_samples_to_keep);
	// set the cardinality
	ret->reservoir_chunk->chunk.SetCardinality(num_samples_to_keep);
	ret->base_reservoir_sample->num_entries_seen_total = base_reservoir_sample->num_entries_seen_total;
	if (!ret->base_reservoir_sample->reservoir_weights.empty()) {
		ret->base_reservoir_sample->SetNextEntry();
	}

	D_ASSERT(ret->reservoir_chunk->chunk.size() == num_samples_to_keep);
	return unique_ptr_cast<ReservoirSample, BlockingSample>(std::move(ret));
}

void ReservoirSample::ExpandSerializedSample() {
	if (!reservoir_chunk) {
		return;
	}

	auto types = reservoir_chunk->chunk.GetTypes();
	auto new_res_chunk = CreateNewSampleChunk(types, GetReservoirChunkCapacity());
	auto copy_count = reservoir_chunk->chunk.size();
	SelectionVector tmp_sel = SelectionVector(0, copy_count);
	UpdateSampleAppend(new_res_chunk->chunk, reservoir_chunk->chunk, tmp_sel, copy_count);
	new_res_chunk->chunk.SetCardinality(copy_count);
	std::swap(reservoir_chunk, new_res_chunk);
}

idx_t ReservoirSample::GetReservoirChunkCapacity() const {
	return sample_count + (FIXED_SAMPLE_SIZE_MULTIPLIER * FIXED_SAMPLE_SIZE);
}

idx_t ReservoirSample::FillReservoir(DataChunk &chunk) {

	idx_t ingested_count = 0;
	if (!reservoir_chunk) {
		if (chunk.size() > FIXED_SAMPLE_SIZE) {
			throw InternalException("Creating sample with DataChunk that is larger than the fixed sample size");
		}
		auto types = chunk.GetTypes();
		// create a new sample chunk to store new samples
		reservoir_chunk = CreateNewSampleChunk(types, GetReservoirChunkCapacity());
	}

	idx_t actual_sample_index_start = GetActiveSampleCount();
	D_ASSERT(reservoir_chunk->chunk.ColumnCount() == chunk.ColumnCount());
	if (reservoir_chunk->chunk.size() < sample_count) {
		ingested_count = MinValue<idx_t>(sample_count - reservoir_chunk->chunk.size(), chunk.size());
		SelectionVector sel_for_input_chunk(ingested_count);
		for (idx_t i = 0; i < ingested_count; i++) {
			sel.set_index(actual_sample_index_start + i, actual_sample_index_start + i);
			sel_for_input_chunk.set_index(i, i);
		}
		UpdateSampleAppend(reservoir_chunk->chunk, chunk, sel_for_input_chunk, ingested_count);
		sel_size += ingested_count;
		ShuffleSel();
	}
	D_ASSERT(GetActiveSampleCount() <= sample_count);
	D_ASSERT(GetActiveSampleCount() >= ingested_count);
	// always return how many tuples were ingested
	return ingested_count;
}

void ReservoirSample::Destroy() {
	destroyed = true;
}

SelectionVectorHelper ReservoirSample::GetReplacementIndexes(idx_t sample_chunk_offset,
                                                             idx_t theoretical_chunk_length) {
	if (SamplingState() == SamplingMode::FAST) {
		return GetReplacementIndexesFast(sample_chunk_offset, theoretical_chunk_length);
	}
	return GetReplacementIndexesSlow(sample_chunk_offset, theoretical_chunk_length);
}

SelectionVectorHelper ReservoirSample::GetReplacementIndexesFast(idx_t sample_chunk_offset, idx_t chunk_length) {

	// how much weight to the other tuples have compared to the ones in this chunk?
	// In fast sampling, num_to_pop should always be >= 1.
	auto weight_tuples_other = static_cast<double>(chunk_length) / static_cast<double>(GetTuplesSeen() + chunk_length);
	auto num_to_pop = static_cast<uint32_t>(round(weight_tuples_other * sample_count));
	D_ASSERT(num_to_pop >= 1);
	D_ASSERT(num_to_pop <= sample_count);

	unordered_map<idx_t, idx_t> replacement_indexes;
	SelectionVector chunk_sel(num_to_pop);

	auto random_indexes_chunk = GetRandomizedVector(static_cast<uint32_t>(chunk_length));
	for (idx_t i = 0; i < num_to_pop; i++) {
		// update the selection vector for the reservoir sample
		chunk_sel.set_index(i, random_indexes_chunk[i]);
		// sel is already random, so we update the indexes incrementally
		sel.set_index(i, sample_chunk_offset + i);
	}
	ShuffleSel();

	D_ASSERT(sel_size == sample_count);

	SelectionVectorHelper ret;
	ret.sel = SelectionVector(chunk_sel);
	ret.size = num_to_pop;
	return ret;
}

SelectionVectorHelper ReservoirSample::GetReplacementIndexesSlow(const idx_t sample_chunk_offset,
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

	// create selection vector to return
	SelectionVector ret_sel(ret_map.size());
	D_ASSERT(sel_size == sample_count);
	for (auto &kv : ret_map) {
		ret_sel.set_index(kv.second, kv.first);
	}
	SelectionVectorHelper ret;
	ret.sel = SelectionVector(ret_sel);
	ret.size = static_cast<uint32_t>(ret_map.size());
	return ret;
}

void ReservoirSample::Finalize() {
	return;
}

bool ReservoirSample::ValidSampleType(const LogicalType &type) {
	return type.IsNumeric();
}

void ReservoirSample::UpdateSampleAppend(DataChunk &this_, DataChunk &other, SelectionVector &other_sel,
                                         idx_t append_count) const {
	idx_t new_size = this_.size() + append_count;
	if (other.size() == 0) {
		return;
	}
	D_ASSERT(this_.GetTypes() == other.GetTypes());

	// UpdateSampleAppend(this_, other, other_sel, append_count);
	D_ASSERT(this_.GetTypes() == other.GetTypes());
	auto types = reservoir_chunk->chunk.GetTypes();

	for (idx_t i = 0; i < reservoir_chunk->chunk.ColumnCount(); i++) {
		auto col_type = types[i];
		if (ValidSampleType(col_type) || !internal_sample) {
			D_ASSERT(this_.data[i].GetVectorType() == VectorType::FLAT_VECTOR);
			VectorOperations::Copy(other.data[i], this_.data[i], other_sel, append_count, 0, this_.size());
		}
	}
	this_.SetCardinality(new_size);
}

void ReservoirSample::AddToReservoir(DataChunk &chunk) {
	if (destroyed || chunk.size() == 0) {
		return;
	}

	idx_t tuples_consumed = FillReservoir(chunk);
	base_reservoir_sample->num_entries_seen_total += tuples_consumed;
	D_ASSERT(reservoir_chunk->chunk.size() >= 1);

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

	// at this point we should have collected at least sample count samples
	D_ASSERT(GetActiveSampleCount() >= sample_count);

	auto chunk_sel = GetReplacementIndexes(reservoir_chunk->chunk.size(), chunk.size());

	if (chunk_sel.size == 0) {
		// not adding any samples
		return;
	}
	idx_t size = chunk_sel.size;
	D_ASSERT(size <= chunk.size());

	UpdateSampleAppend(reservoir_chunk->chunk, chunk, chunk_sel.sel, size);

	base_reservoir_sample->num_entries_seen_total += chunk.size();
	D_ASSERT(base_reservoir_sample->reservoir_weights.size() == 0 ||
	         base_reservoir_sample->reservoir_weights.size() == sample_count);

	Verify();

	// if we are over the threshold, we ned to swith to slow sampling.
	if (SamplingState() == SamplingMode::FAST && GetTuplesSeen() >= FIXED_SAMPLE_SIZE * FAST_TO_SLOW_THRESHOLD) {
		ConvertToSlowSample();
	}
	if (reservoir_chunk->chunk.size() >= (GetReservoirChunkCapacity() - (FIXED_SAMPLE_SIZE * 3))) {
		Shrink();
	}
}

void ReservoirSample::Verify() {
#ifdef DEBUG
	if (destroyed) {
		return;
	}
	if (GetPriorityQueueSize() == 0) {
		D_ASSERT(GetActiveSampleCount() <= sample_count);
		D_ASSERT(GetTuplesSeen() >= GetActiveSampleCount());
		return;
	}
	if (NumSamplesCollected() > sample_count) {
		D_ASSERT(GetPriorityQueueSize() == sample_count);
	} else if (NumSamplesCollected() <= sample_count && GetPriorityQueueSize() > 0) {
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

	if (reservoir_chunk) {
		reservoir_chunk->chunk.Verify();
	}
#endif
}

} // namespace duckdb
