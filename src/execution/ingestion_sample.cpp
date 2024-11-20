#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/reservoir_sample.hpp"

namespace duckdb {

idx_t IngestionSample::NumSamplesCollected() const {
	if (!sample_chunk) {
		return 0;
	}
	return sample_chunk->size();
}

idx_t IngestionSample::NumActiveSamples() const {
	auto ret = MaxValue<idx_t>(actual_sample_indexes.size(), base_reservoir_sample->reservoir_weights.size());
	D_ASSERT(ret <= FIXED_SAMPLE_SIZE);
	return ret;
}

unique_ptr<DataChunk> IngestionSample::GetChunk(idx_t offset) {
	throw InternalException("Invalid Call to Get Chunk");
}

unique_ptr<DataChunk> IngestionSample::GetChunkAndShrink() {
	throw InternalException("Invalid Sampling state");
}

unique_ptr<DataChunk> IngestionSample::CreateNewSampleChunk(vector<LogicalType> &types, idx_t size) const {
	auto new_sample_chunk = make_uniq<DataChunk>();
	new_sample_chunk->Initialize(Allocator::DefaultAllocator(), types, size);
	for (idx_t col_idx = 0; col_idx < new_sample_chunk->ColumnCount(); col_idx++) {
		auto type = types[col_idx];
		// TODO: should the validity mask be the capacity or the size?
		FlatVector::Validity(new_sample_chunk->data[col_idx]).Initialize(size);

		if (!ValidSampleType(type)) {
			new_sample_chunk->data[col_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(new_sample_chunk->data[col_idx], true);
		}
	}

	return new_sample_chunk;
}

SampleCopyHelper IngestionSample::GetSelToCopyData(idx_t sel_size) const {
	SampleCopyHelper ret;
	if (SamplingState() == SamplingMode::FAST) {
		D_ASSERT(actual_sample_indexes.size() >= sel_size);
		auto indexes_copy = actual_sample_indexes;
		return SelFromSimpleIndexes(indexes_copy);
	}
	D_ASSERT(base_reservoir_sample->reservoir_weights.size() >= sel_size);
	auto base_copy = base_reservoir_sample->Copy();
	vector<std::pair<double, idx_t>> weights_indexes;
	D_ASSERT(sel_size <= base_copy->reservoir_weights.size());
	for (idx_t i = 0; i < sel_size; i++) {
		weights_indexes.push_back(base_copy->reservoir_weights.top());
		base_copy->reservoir_weights.pop();
	}
	// create one large chunk from the collected chunk samples.
	D_ASSERT(sample_chunk->size() != 0);

	// set up selection vector to copy IngestionSample to ReservoirSample
	return SelFromReservoirWeights(weights_indexes);
}

// Get a selection vector referring to the actual sample when the sample is in "Slow" mode
SampleCopyHelper IngestionSample::SelFromReservoirWeights(vector<std::pair<double, idx_t>> &weights_indexes) const {
	idx_t sample_to_keep = weights_indexes.size();
	SampleCopyHelper ret;
	ret.sel = SelectionVector(sample_to_keep);
	for (idx_t i = 0; i < sample_to_keep; i++) {
		ret.sel.set_index(i, weights_indexes[i].second);
		ret.reservoir_weights.emplace(weights_indexes[i].first, i);
	}
	return ret;
}

// Get a selection vector referring to the actual sample when the sample is in "Fast" mode
SampleCopyHelper IngestionSample::SelFromSimpleIndexes(vector<idx_t> &actual_indexes) const {
	idx_t sample_to_keep = actual_indexes.size();
	SampleCopyHelper ret;
	ret.sel = SelectionVector(sample_to_keep);
	vector<idx_t> new_indexes;
	// reservoir weights should be empty. We are about to construct them again with indexes in the new_sample_chunk
	for (idx_t i = 0; i < sample_to_keep; i++) {
		new_indexes.push_back(i);
		ret.sel.set_index(i, actual_indexes[i]);
	}
	ret.actual_indexes = new_indexes;
	return ret;
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
	idx_t num_samples_to_keep = NumActiveSamples();
	auto sample_copy_helper = GetSelToCopyData(num_samples_to_keep);

	actual_sample_indexes = sample_copy_helper.actual_indexes;
	base_reservoir_sample->reservoir_weights = sample_copy_helper.reservoir_weights;
	if (SamplingState() != SamplingMode::FAST) {
		base_reservoir_sample->min_weighted_entry_index = base_reservoir_sample->reservoir_weights.top().second;
	}

	std::swap(sample_chunk, new_sample_chunk);
	// perform the copy
	UpdateSampleAppend(*new_sample_chunk, sample_copy_helper.sel, num_samples_to_keep);
	D_ASSERT(sample_chunk->size() == num_samples_to_keep);
	Verify();
	// We should only have one sample chunk now.
	D_ASSERT(sample_chunk->size() > 0 && sample_chunk->size() <= FIXED_SAMPLE_SIZE);
}

unique_ptr<BlockingSample> IngestionSample::Copy() const {
	return Copy(false);
}

unique_ptr<BlockingSample> IngestionSample::Copy(bool for_serialization) const {
	auto ret = make_uniq<IngestionSample>(sample_count);

	ret->base_reservoir_sample = base_reservoir_sample->Copy();
	ret->destroyed = destroyed;

	if (!sample_chunk || destroyed) {
		return unique_ptr_cast<IngestionSample, BlockingSample>(std::move(ret));
	}

	D_ASSERT(sample_chunk);

	// create a new sample chunk to store new samples
	auto types = sample_chunk->GetTypes();
	idx_t new_sample_chunk_size =
	    for_serialization ? NumActiveSamples() : FIXED_SAMPLE_SIZE * FIXED_SAMPLE_SIZE_MULTIPLIER;
	// how many values should be copied
	idx_t values_to_copy = MinValue<idx_t>(NumActiveSamples(), FIXED_SAMPLE_SIZE);

	auto new_sample_chunk = CreateNewSampleChunk(types, new_sample_chunk_size);

	// set up selection vector to copy IngestionSample to ReservoirSample
	SampleCopyHelper sel = GetSelToCopyData(values_to_copy);
	ret->actual_sample_indexes = sel.actual_indexes;
	ret->base_reservoir_sample->reservoir_weights = sel.reservoir_weights;

	if (SamplingState() != SamplingMode::FAST) {
		ret->base_reservoir_sample->min_weighted_entry_index =
		    ret->base_reservoir_sample->reservoir_weights.top().second;
	}

	ret->sample_chunk = std::move(new_sample_chunk);
	ret->UpdateSampleAppend(*sample_chunk, sel.sel, values_to_copy);
	D_ASSERT(ret->sample_chunk->size() <= FIXED_SAMPLE_SIZE);
	ret->Verify();
	return unique_ptr_cast<IngestionSample, BlockingSample>(std::move(ret));
}

void IngestionSample::ConvertToSlowSample() {
	base_reservoir_sample->FillWeights(actual_sample_indexes);
	actual_sample_indexes.clear();
}

void IngestionSample::SimpleMerge(IngestionSample &other) {
	D_ASSERT(GetPriorityQueueSize() == 0);
	D_ASSERT(other.GetPriorityQueueSize() == 0);

	if (other.actual_sample_indexes.empty() && other.GetTuplesSeen() == 0) {
		return;
	}

	if (actual_sample_indexes.empty() && GetTuplesSeen() == 0) {
		actual_sample_indexes = std::move(other.actual_sample_indexes);
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
		keep_from_this = MinValue<idx_t>(static_cast<idx_t>(round(FIXED_SAMPLE_SIZE * weight_tuples_this)),
		                                 actual_sample_indexes.size());
		keep_from_other = MinValue<idx_t>(FIXED_SAMPLE_SIZE - keep_from_this, other.actual_sample_indexes.size());
	} else {
		keep_from_other = MinValue<idx_t>(static_cast<idx_t>(round(FIXED_SAMPLE_SIZE * weight_tuples_other)),
		                                  other.actual_sample_indexes.size());
		keep_from_this = MinValue<idx_t>(FIXED_SAMPLE_SIZE - keep_from_other, actual_sample_indexes.size());
	}

	D_ASSERT(keep_from_this <= actual_sample_indexes.size());
	D_ASSERT(keep_from_other <= other.actual_sample_indexes.size());
	D_ASSERT(keep_from_other + keep_from_this <= FIXED_SAMPLE_SIZE);
	idx_t size_after_merge = MinValue<idx_t>(keep_from_other + keep_from_this, FIXED_SAMPLE_SIZE);

	// Check if appending the other samples to this will go over the sample chunk size
	if (sample_chunk->size() + keep_from_other > FIXED_SAMPLE_SIZE * FIXED_SAMPLE_SIZE_MULTIPLIER) {
		Shrink();
	}

	D_ASSERT(size_after_merge <= other.actual_sample_indexes.size() + actual_sample_indexes.size());
	SelectionVector sel(keep_from_other);
	auto offset = sample_chunk->size();
	for (idx_t i = keep_from_this; i < size_after_merge; i++) {
		if (i >= actual_sample_indexes.size()) {
			actual_sample_indexes.push_back(offset);
		} else {
			actual_sample_indexes[i] = offset;
		}
		sel.set_index(i - keep_from_this, other.actual_sample_indexes[i - keep_from_this]);
		offset += 1;
	}

	D_ASSERT(actual_sample_indexes.size() == size_after_merge);

	// fix, basically you only need to copy the required tuples from other and put them into this. You can
	// save a number of the tuples in THIS.
	UpdateSampleAppend(*other.sample_chunk, sel, keep_from_other);
	base_reservoir_sample->num_entries_seen_total += other.GetTuplesSeen();

	if (GetTuplesSeen() >= FIXED_SAMPLE_SIZE * IngestionSample::FAST_TO_SLOW_THRESHOLD) {
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
		actual_sample_indexes = other_ingest.actual_sample_indexes;
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

	idx_t total_samples = GetPriorityQueueSize() + other_ingest.GetPriorityQueueSize();
	idx_t num_samples_to_keep = MinValue<idx_t>(FIXED_SAMPLE_SIZE, total_samples);

	D_ASSERT(total_samples <= FIXED_SAMPLE_SIZE * 2);

	// pop from base base_reservoir samples and weights until there are num_samples_to_keep left.
	for (idx_t i = num_samples_to_keep; i < total_samples; i++) {
		auto min_weight_this = base_reservoir_sample->min_weight_threshold;
		auto min_weight_other = other_ingest.base_reservoir_sample->min_weight_threshold;
		// min weight threshol is always positive
		if (min_weight_this > min_weight_other) {
			// pop from other
			other_ingest.base_reservoir_sample->reservoir_weights.pop();
			other_ingest.base_reservoir_sample->UpdateMinWeightThreshold();
		} else {
			base_reservoir_sample->reservoir_weights.pop();
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
	// while also filling in the selection vector we wil use to copy values.
	// TODO: This can be faster. Just pop weights from other until you have
	// num samples to keep. Then copy with an offset.
	while (other_ingest.GetPriorityQueueSize() > 0) {
		auto other_top = other_ingest.PopFromWeightQueue();
		auto other_weight = -other_top.first;
		idx_t index_for_new_pair = chunk_offset + sample_chunk->size();
		if (other_weight < min_weight) {
			min_weight = other_weight;
			min_weight_index = index_for_new_pair;
		}

		sel_other.set_index(chunk_offset, other_top.second);

		// make sure that the sample indexes are (this.sample_chunk.size() + chunk_offfset)
		other_top.second = index_for_new_pair;
		base_reservoir_sample->reservoir_weights.push(other_top);
		chunk_offset += 1;
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

idx_t IngestionSample::GetTuplesSeen() {
	return base_reservoir_sample->num_entries_seen_total;
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
	// minimum of 1% or 2048 values
	idx_t num_samples_to_keep =
	    MinValue<idx_t>(FIXED_SAMPLE_SIZE, static_cast<idx_t>(SAVE_PERCENTAGE * static_cast<double>(GetTuplesSeen())));

	auto copy_helper = GetSelToCopyData(num_samples_to_keep);

	auto ret = make_uniq<ReservoirSample>(num_samples_to_keep);
	if (num_samples_to_keep <= 0) {
		ret->base_reservoir_sample = base_reservoir_sample->Copy();
		return unique_ptr_cast<ReservoirSample, BlockingSample>(std::move(ret));
	}

	// set up reservoir chunk for the reservoir sample
	D_ASSERT(sample_chunk->size() <= FIXED_SAMPLE_SIZE);
	// create a new sample chunk to store new samples
	ret->reservoir_chunk = make_uniq<ReservoirChunk>();
	auto types = sample_chunk->GetTypes();

	// TODO: this could use CreateNewSampleChunk
	ret->reservoir_chunk->chunk.Initialize(Allocator::DefaultAllocator(), sample_chunk->GetTypes(),
	                                       num_samples_to_keep);
	for (idx_t col_idx = 0; col_idx < ret->reservoir_chunk->chunk.ColumnCount(); col_idx++) {
		FlatVector::Validity(ret->reservoir_chunk->chunk.data[col_idx]).Initialize(num_samples_to_keep);
	}

	idx_t new_size = ret->reservoir_chunk->chunk.size() + num_samples_to_keep;

	// now do the copy.
	// TODO: We can use update append for this, but UpdateAppend needs to be fixed
	//       to update append to a random chunk
	D_ASSERT(sample_chunk->GetTypes() == ret->reservoir_chunk->chunk.GetTypes());
	for (idx_t i = 0; i < sample_chunk->ColumnCount(); i++) {
		auto col_type = types[i];
		if (ValidSampleType(col_type)) {
			D_ASSERT(sample_chunk->data[i].GetVectorType() == VectorType::FLAT_VECTOR);
			VectorOperations::Copy(sample_chunk->data[i], ret->reservoir_chunk->chunk.data[i], copy_helper.sel,
			                       num_samples_to_keep, 0, 0);
		} else {
			ret->reservoir_chunk->chunk.data[i].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(ret->reservoir_chunk->chunk.data[i], true);
		}
	}
	// set the cardinality
	ret->reservoir_chunk->chunk.SetCardinality(new_size);
	ret->base_reservoir_sample->reservoir_weights = copy_helper.reservoir_weights;
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

	idx_t actual_sample_index_start = actual_sample_indexes.size();
	D_ASSERT(sample_chunk->ColumnCount() == chunk.ColumnCount());
	if (sample_chunk->size() < FIXED_SAMPLE_SIZE) {
		ingested_count = MinValue<idx_t>(FIXED_SAMPLE_SIZE - sample_chunk->size(), chunk.size());
		SelectionVector sel(ingested_count);
		for (idx_t i = 0; i < ingested_count; i++) {
			actual_sample_indexes.emplace_back(i + actual_sample_index_start);
			sel.set_index(i, i);
		}
		UpdateSampleAppend(chunk, sel, ingested_count);
	}
	D_ASSERT(actual_sample_indexes.size() <= FIXED_SAMPLE_SIZE);
	D_ASSERT(actual_sample_indexes.size() >= ingested_count);
	// always return how many tuples were ingested
	return ingested_count;
}

IngestionSample::IngestionSample(idx_t sample_count, int64_t seed)
    : BlockingSample(seed), sample_count(sample_count), allocator(Allocator::DefaultAllocator()) {
	base_reservoir_sample = make_uniq<BaseReservoirSampling>(seed);
	type = SampleType::INGESTION_SAMPLE;
	sample_chunk = nullptr;
}

IngestionSample::IngestionSample(Allocator &allocator, int64_t seed)
    : BlockingSample(seed), sample_count(FIXED_SAMPLE_SIZE), allocator(allocator) {
	base_reservoir_sample = make_uniq<BaseReservoirSampling>(seed);
	type = SampleType::INGESTION_SAMPLE;
	sample_chunk = nullptr;
}

void IngestionSample::Destroy() {
	destroyed = true;
}

unordered_map<idx_t, idx_t> IngestionSample::GetReplacementIndexes(idx_t sample_chunk_offset,
                                                                   idx_t theoretical_chunk_length) {
	if (GetPriorityQueueSize() == 0 && GetTuplesSeen() <= FIXED_SAMPLE_SIZE * IngestionSample::FAST_TO_SLOW_THRESHOLD) {
		return GetReplacementIndexesFast(sample_chunk_offset, theoretical_chunk_length);
	}
	return GetReplacementIndexesSlow(sample_chunk_offset, theoretical_chunk_length);
}

unordered_map<idx_t, idx_t> IngestionSample::GetReplacementIndexesFast(idx_t sample_chunk_offset,
                                                                       idx_t theoretical_chunk_length) {
	idx_t num_to_pop = static_cast<idx_t>((static_cast<double>(theoretical_chunk_length) /
	                                       static_cast<double>(theoretical_chunk_length + GetTuplesSeen())) *
	                                      static_cast<double>(theoretical_chunk_length));

	unordered_map<idx_t, idx_t> replacement_indexes;
	replacement_indexes.reserve(num_to_pop);
	vector<idx_t> indexes;

	for (idx_t i = 0; i < theoretical_chunk_length; i++) {
		indexes.push_back(i);
	}

	// randomize the possible indexes to sample from the theoretical chunk length
	for (idx_t i = 0; i < num_to_pop; i++) {
		idx_t random_shuffle = base_reservoir_sample->random.NextRandomInteger(
		    static_cast<uint32_t>(i + 1), static_cast<uint32_t>(actual_sample_indexes.size()));
		// basically replacing the tuple that was at index actual_sample_indexes[random_shuffle]
		actual_sample_indexes[random_shuffle] = actual_sample_indexes[i];
		actual_sample_indexes[i] = sample_chunk_offset + i;

		// also shuffle the first num_to_pop values in indexes to get a random range of values from the incoming chunk
		idx_t random_index_from_sample = base_reservoir_sample->random.NextRandomInteger(
		    static_cast<uint32_t>(i), static_cast<uint32_t>(theoretical_chunk_length));
		idx_t tmp = indexes[random_index_from_sample];
		indexes[random_index_from_sample] = indexes[i];
		indexes[i] = tmp;
		replacement_indexes[indexes[i]] = i;
	}

	return replacement_indexes;
}

unordered_map<idx_t, idx_t> IngestionSample::GetReplacementIndexesSlow(idx_t sample_chunk_offset,
                                                                       idx_t theoretical_chunk_length) {
	idx_t remaining = theoretical_chunk_length;
	unordered_map<idx_t, idx_t> ret;
	idx_t sample_chunk_index = 0;

	idx_t base_offset = 0;

	while (true) {
		idx_t offset =
		    base_reservoir_sample->next_index_to_sample - base_reservoir_sample->num_entries_to_skip_b4_next_sample;
		if (offset >= remaining) {
			// not in this chunk! increment current count and go to the next chunk
			base_reservoir_sample->num_entries_to_skip_b4_next_sample += remaining;
			return ret;
		}
		// in this chunk! replace the element
		// ret[index_in_new_chunk] = index_in_sample_chunk (the sample chunk offset will be applied later)
		// D_ASSERT(sample_chunk_index == ret.size());
		ret[base_offset + offset] = sample_chunk_index;
		double r2 = base_reservoir_sample->random.NextRandom(base_reservoir_sample->min_weight_threshold, 1);
		// replace element in our max_hep
		// sample_chunk_offset + sample_chunk_index
		base_reservoir_sample->ReplaceElementWithIndex(sample_chunk_offset + sample_chunk_index, r2);

		sample_chunk_index += 1;
		// shift the chunk forward
		remaining -= offset;
		base_offset += offset;
	}
}

void IngestionSample::Finalize() {
	return;
}

bool IngestionSample::ValidSampleType(const LogicalType &type) {
	return type.IsNumeric();
}

void IngestionSample::UpdateSampleWithTypes(DataChunk &other, SelectionVector &sel, idx_t source_count,
                                            idx_t source_offset, idx_t target_offset) {
	D_ASSERT(sample_chunk->GetTypes() == other.GetTypes());
	auto types = sample_chunk->GetTypes();

	for (idx_t i = 0; i < sample_chunk->ColumnCount(); i++) {
		auto col_type = types[i];
		if (ValidSampleType(col_type)) {
			D_ASSERT(sample_chunk->data[i].GetVectorType() == VectorType::FLAT_VECTOR);
			VectorOperations::Copy(other.data[i], sample_chunk->data[i], sel, source_count, source_offset,
			                       target_offset);
		}
	}
}

void IngestionSample::UpdateSampleAppend(DataChunk &other, SelectionVector &sel, idx_t append_count) {
	idx_t new_size = sample_chunk->size() + append_count;
	if (other.size() == 0) {
		return;
	}
	D_ASSERT(sample_chunk->GetTypes() == other.GetTypes());

	UpdateSampleWithTypes(other, sel, append_count, 0, sample_chunk->size());
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
		SelectionVector sel(samples_remaining);
		for (idx_t i = 0; i < samples_remaining; i++) {
			sel.set_index(i, tuples_consumed + i);
		}
		slice->Initialize(Allocator::DefaultAllocator(), types, samples_remaining);
		slice->Slice(chunk, sel, samples_remaining);
		slice->SetCardinality(samples_remaining);
		AddToReservoir(*slice);
		return;
	}

	// at this point our sample_chunk has at least FIXED SAMPLE SIZE samples.
	D_ASSERT(sample_chunk->size() >= FIXED_SAMPLE_SIZE);

	auto sample_chunk_ind_to_data_chunk_index = GetReplacementIndexes(sample_chunk->size(), chunk.size());

	if (sample_chunk_ind_to_data_chunk_index.empty()) {
		// not adding any samples
		return;
	}
	idx_t size = sample_chunk_ind_to_data_chunk_index.size();
	D_ASSERT(size <= chunk.size());
	SelectionVector sel(size);
	for (auto &input_idx_res_idx : sample_chunk_ind_to_data_chunk_index) {
		auto ind_in_input_chunk = input_idx_res_idx.first;
		auto ind_in_sample_chunk = input_idx_res_idx.second;
		sel.set_index(ind_in_sample_chunk, ind_in_input_chunk);
	}
	const SelectionVector const_sel(sel);

	UpdateSampleAppend(chunk, sel, size);

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
		D_ASSERT(actual_sample_indexes.size() <= FIXED_SAMPLE_SIZE);
		D_ASSERT(GetTuplesSeen() >= actual_sample_indexes.size());
		return;
	}
	if (NumSamplesCollected() > FIXED_SAMPLE_SIZE) {
		D_ASSERT(GetPriorityQueueSize() == FIXED_SAMPLE_SIZE);
	} else if (NumSamplesCollected() <= FIXED_SAMPLE_SIZE && GetPriorityQueueSize() > 0) {
		D_ASSERT(NumSamplesCollected() == GetPriorityQueueSize());
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
			base_reservoir_copy->reservoir_weights.pop();
		}
	}

	if (sample_chunk) {
		sample_chunk->Verify();
	}
#endif
}

} // namespace duckdb
