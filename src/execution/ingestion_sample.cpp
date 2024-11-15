
#include "duckdb/execution/reservoir_sample.hpp"

#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

idx_t IngestionSample::NumSamplesCollected() {
	if (!sample_chunk) {
		return 0;
	}
	return sample_chunk->size();
}

unique_ptr<DataChunk> IngestionSample::GetChunk(idx_t offset) {
	Printer::Print("Get chunk called");
	Shrink();
	D_ASSERT(sample_chunk->size() <= 1);
	auto ret = make_uniq<DataChunk>();
	if (!sample_chunk || destroyed) {
		return nullptr;
	}
	idx_t ret_chunk_size = FIXED_SAMPLE_SIZE;
	auto &chunk_to_copy = sample_chunk;
	if (offset + FIXED_SAMPLE_SIZE > chunk_to_copy->size()) {
		ret_chunk_size = chunk_to_copy->size() - offset;
	}
	if (ret_chunk_size == 0) {
		return nullptr;
	}
	auto reservoir_types = chunk_to_copy->GetTypes();
	SelectionVector sel(FIXED_SAMPLE_SIZE);
	for (idx_t i = offset; i < offset + ret_chunk_size; i++) {
		sel.set_index(i - offset, i);
	}
	ret->Initialize(allocator, reservoir_types, FIXED_SAMPLE_SIZE);
	ret->Slice(*chunk_to_copy, sel, FIXED_SAMPLE_SIZE);
	ret->SetCardinality(ret_chunk_size);
	return ret;
}

unique_ptr<DataChunk> IngestionSample::GetChunkAndShrink() {
	throw InternalException("Invalid Sampling state");
}

unique_ptr<DataChunk> IngestionSample::CreateNewSampleChunk(vector<LogicalType> &types) {
	auto new_sample_chunk = make_uniq<DataChunk>();
	new_sample_chunk->Initialize(Allocator::DefaultAllocator(), types,
	                             FIXED_SAMPLE_SIZE * FIXED_SAMPLE_SIZE_MULTIPLIER);
	for (idx_t col_idx = 0; col_idx < new_sample_chunk->ColumnCount(); col_idx++) {
		auto type = types[col_idx];
		// TODO: should the validity mask be the capacity or the size?
		FlatVector::Validity(new_sample_chunk->data[col_idx])
		    .Initialize(FIXED_SAMPLE_SIZE * FIXED_SAMPLE_SIZE_MULTIPLIER);

		if (!ValidSampleType(type)) {
			new_sample_chunk->data[col_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(new_sample_chunk->data[col_idx], true);
		}
	}
	idx_t num_samples_to_keep = actual_sample_indexes.size();
	// set up selection vector to copy IngestionSample to ReservoirSample
	SelectionVector sel(num_samples_to_keep);
	for (idx_t i = 0; i < num_samples_to_keep; i++) {
		sel.set_index(i, actual_sample_indexes[i]);
	}
	return new_sample_chunk;
}

SelectionVector
IngestionSample::CreateSelectionVectorFromReservoirWeights(vector<std::pair<double, idx_t>> &weights_indexes) const {
	idx_t sample_to_keep = weights_indexes.size();
	SelectionVector sel(sample_to_keep);
	// reservoir weights should be empty. We are about to construct them again with indexes in the new_sample_chunk
	D_ASSERT(base_reservoir_sample->reservoir_weights.empty());
	double max_weight = NumericLimits<double>::Minimum();
	idx_t max_weight_index = 0;
	for (idx_t i = 0; i < sample_to_keep; i++) {
		sel.set_index(i, weights_indexes[i].second);
		base_reservoir_sample->reservoir_weights.emplace(weights_indexes[i].first, i);
		if (max_weight < weights_indexes[i].first) {
			max_weight = weights_indexes[i].first;
			max_weight_index = i;
		}
	}
	base_reservoir_sample->min_weighted_entry_index = max_weight_index;
	base_reservoir_sample->min_weight_threshold = -max_weight;
	D_ASSERT(base_reservoir_sample->min_weight_threshold > 0);
	return sel;
}

SelectionVector IngestionSample::CreateSelectionVectorFromSimpleVector(vector<idx_t> &actual_indexes) {
	idx_t sample_to_keep = actual_indexes.size();
	SelectionVector sel(sample_to_keep);
	// reservoir weights should be empty. We are about to construct them again with indexes in the new_sample_chunk
	for (idx_t i = 0; i < sample_to_keep; i++) {
		sel.set_index(i, actual_indexes[i]);
	}
	return sel;
}

void IngestionSample::Shrink() {
	Verify();
	if (NumSamplesCollected() <= FIXED_SAMPLE_SIZE || !sample_chunk) {
		// nothing to shrink, haven't collected enough samples.
		return;
	}

	if (destroyed) {
		return;
	}

	auto types = sample_chunk->GetTypes();
	unique_ptr<DataChunk> new_sample_chunk = nullptr;
	SelectionVector sel;
	// we always only keep a FIXED_SAMPLE_SIZE number of samples.
	idx_t num_samples_to_keep = FIXED_SAMPLE_SIZE;
	if (GetPriorityQueueSize() == 0) {
		new_sample_chunk = CreateNewSampleChunk(types);
		sel = CreateSelectionVectorFromSimpleVector(actual_sample_indexes);
		num_samples_to_keep = actual_sample_indexes.size();
	} else {
		// we will only keep one sample size of samples
		vector<std::pair<double, idx_t>> weights_indexes;
		D_ASSERT(num_samples_to_keep == base_reservoir_sample->reservoir_weights.size());
		D_ASSERT(num_samples_to_keep <= FIXED_SAMPLE_SIZE);
		for (idx_t i = 0; i < num_samples_to_keep; i++) {
			weights_indexes.push_back(base_reservoir_sample->reservoir_weights.top());
			base_reservoir_sample->reservoir_weights.pop();
		}

		// create one large chunk from the collected chunk samples.
		D_ASSERT(sample_chunk->size() != 0);

		// create a new sample chunk to store new samples
		new_sample_chunk = CreateNewSampleChunk(types);

		// set up selection vector to copy IngestionSample to ReservoirSample
		sel = CreateSelectionVectorFromReservoirWeights(weights_indexes);
	}

	std::swap(sample_chunk, new_sample_chunk);
	// first flatten the chunks to expand null constant vector columns that take the place
	// of the un-supported columns.
	// perform the copy
	UpdateSampleCopy(*new_sample_chunk, sel, 0, 0, num_samples_to_keep);
	// sample_chunk->Copy(*new_sample_chunk, sel, num_samples_to_keep, 0);
	D_ASSERT(sample_chunk->size() == num_samples_to_keep);
	// sample_chunk = std::move(new_sample_chunk);

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

	// create one large chunk from the collected chunk samples.
	// before calling copy(), shrink() must be called.
	// Shrink() cannot be called within copy since copy is const.
	D_ASSERT(sample_chunk);

	// create a new sample chunk to store new samples
	auto new_sample_chunk = make_uniq<DataChunk>();
	auto types = sample_chunk->GetTypes();
	D_ASSERT(sample_chunk->size() <= FIXED_SAMPLE_SIZE);
	idx_t new_sample_chunk_size =
	    for_serialization ? sample_chunk->size() : FIXED_SAMPLE_SIZE * FIXED_SAMPLE_SIZE_MULTIPLIER;
	new_sample_chunk->Initialize(Allocator::DefaultAllocator(), types, new_sample_chunk_size);
	for (idx_t col_idx = 0; col_idx < new_sample_chunk->ColumnCount(); col_idx++) {
		auto type = types[col_idx];
		// TODO: should the validity mask be the capacity or the size?
		FlatVector::Validity(new_sample_chunk->data[col_idx]).Initialize(new_sample_chunk_size);
		if (!ValidSampleType(type)) {
			new_sample_chunk->data[col_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(new_sample_chunk->data[col_idx], true);
		}
	}
	// set up selection vector to copy IngestionSample to ReservoirSample
	SelectionVector sel(new_sample_chunk_size);

	for (idx_t i = 0; i < new_sample_chunk_size; i++) {
		sel.set_index(i, i);
	}

	ret->sample_chunk = std::move(new_sample_chunk);
	ret->UpdateSampleCopy(*sample_chunk, sel, 0, 0, sample_chunk->size());
	D_ASSERT(ret->sample_chunk->size() == sample_chunk->size());

	ret->Verify();
	return unique_ptr_cast<IngestionSample, BlockingSample>(std::move(ret));
}

void IngestionSample::SimpleMerge(IngestionSample &other) {
	D_ASSERT(GetPriorityQueueSize() == 0);
	D_ASSERT(other.GetPriorityQueueSize() == 0);

	if (other.actual_sample_indexes.empty() && other.base_reservoir_sample->num_entries_seen_total == 0) {
		return;
	}

	if (actual_sample_indexes.empty() && base_reservoir_sample->num_entries_seen_total == 0) {
		actual_sample_indexes = std::move(other.actual_sample_indexes);
		base_reservoir_sample->num_entries_seen_total = other.base_reservoir_sample->num_entries_seen_total;
		return;
	}

	idx_t total_samples = other.actual_sample_indexes.size() + actual_sample_indexes.size();
	idx_t size_after_merge = MinValue(static_cast<idx_t>(FIXED_SAMPLE_SIZE), total_samples);
	idx_t total_seen =
	    base_reservoir_sample->num_entries_seen_total + other.base_reservoir_sample->num_entries_seen_total;

	auto keep_from_this = (base_reservoir_sample->num_entries_seen_total * size_after_merge) / total_seen;
	auto keep_from_other = size_after_merge - keep_from_this;

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
	base_reservoir_sample->num_entries_seen_total += other.base_reservoir_sample->num_entries_seen_total;
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
		if (!other_ingest.actual_sample_indexes.empty()) {
			actual_sample_indexes = std::move(other_ingest.actual_sample_indexes);
		}
		Verify();
		return;
	}

	//! Both samples are still in "fast sampling" method
	if (SamplingMode() == SamplingMode::FAST && other_ingest.SamplingMode() == SamplingMode::FAST) {
		SimpleMerge(other_ingest);
		if (base_reservoir_sample->num_entries_seen_total >=
		    FIXED_SAMPLE_SIZE * IngestionSample::FAST_TO_SLOW_THRESHOLD) {
			base_reservoir_sample->FillWeights(actual_sample_indexes);
		}
		return;
	}

	// One or none of the samples are in "Fast Sampling" method.
	// When this is the case, switch both to slow sampling.
	if (SamplingMode() == SamplingMode::FAST) {
		// make sure both samples have weights
		base_reservoir_sample->FillWeights(actual_sample_indexes);
	} else if (other_ingest.SamplingMode() == SamplingMode::FAST) {
		// make sure both samples have weights
		other_ingest.base_reservoir_sample->FillWeights(other_ingest.actual_sample_indexes);
	}

	D_ASSERT(SamplingMode() == SamplingMode::SLOW && other_ingest.SamplingMode() == SamplingMode::SLOW);
	// we know both ingestion samples have collected samples,
	// shrink both samples so merging is easier
	Shrink();
	other_ingest.Shrink();

	// make sure both ingestion samples only have 1 sample after the shrink
	D_ASSERT(other_ingest.sample_chunk->size() > 0 && sample_chunk->size() > 0);

	idx_t total_samples = GetPriorityQueueSize() + other_ingest.GetPriorityQueueSize();
	idx_t num_samples_to_keep = MinValue<idx_t>(FIXED_SAMPLE_SIZE, total_samples);
	// after shrink is called on both samples, we should not have more than FIXED_SAMPLE_SIZE
	// samples for sample
	D_ASSERT(total_samples <= FIXED_SAMPLE_SIZE * 2);
	// if there are more than FIXED_SAMPLE_SIZE samples, we want to keep only the
	// highest weighted FIXED_SAMPLE_SIZE samples

	// pop from base base_reservoir samples and weights until there are num_samples_to_keep
	// left.
	for (idx_t i = num_samples_to_keep; i < total_samples; i++) {
		auto min_weight_this = base_reservoir_sample->min_weight_threshold;
		auto min_weight_other = other_ingest.base_reservoir_sample->min_weight_threshold;
		// min weight threshol is always positive
		if (min_weight_this > min_weight_other) {
			// pop from other
			other_ingest.base_reservoir_sample->reservoir_weights.pop();
			if (other_ingest.GetPriorityQueueSize() != 0) {
				other_ingest.base_reservoir_sample->min_weight_threshold =
				    -other_ingest.base_reservoir_sample->reservoir_weights.top().first;
			} else {
				other_ingest.base_reservoir_sample->min_weight_threshold = 1;
			}
		} else {
			base_reservoir_sample->reservoir_weights.pop();
			if (GetPriorityQueueSize() != 0) {
				base_reservoir_sample->min_weight_threshold = -base_reservoir_sample->reservoir_weights.top().first;
			} else {
				base_reservoir_sample->min_weight_threshold = 1;
			}
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
	base_reservoir_sample->num_entries_seen_total =
	    base_reservoir_sample->num_entries_seen_total + other_ingest.base_reservoir_sample->num_entries_seen_total;

	// fix, basically you only need to copy the required tuples from other and put them into this. You can
	// save a number of the tuples in THIS.
	UpdateSampleAppend(*other_ingest.sample_chunk, sel_other, chunk_offset);
	Verify();
}

idx_t IngestionSample::GetTuplesSeen() {
	return base_reservoir_sample->num_entries_seen_total;
}

unique_ptr<BlockingSample> IngestionSample::ConvertToReservoirSampleToSerialize() {
	Shrink();
	Verify();
	if (!sample_chunk || destroyed) {
		auto ret = make_uniq<ReservoirSample>(FIXED_SAMPLE_SIZE);
		ret->Destroy();
		return unique_ptr_cast<ReservoirSample, BlockingSample>(std::move(ret));
	}

	// since this is for serialization, we really need to make sure keep a
	// minimum of 1% or 2048 values
	idx_t num_samples_to_keep = MinValue<idx_t>(
	    FIXED_SAMPLE_SIZE, static_cast<idx_t>(PERCENTAGE_SAMPLE_SIZE * GetTuplesSeen() / (double(100))));

	vector<std::pair<double, idx_t>> weights_indexes;
	if (base_reservoir_sample->reservoir_weights.empty() && sample_chunk->size() > 0) {
		// we've collected samples but haven't assigned weights yet;
		base_reservoir_sample->InitializeReservoirWeights(sample_chunk->size(), sample_chunk->size());
	}

	auto ret = make_uniq<ReservoirSample>(FIXED_SAMPLE_SIZE);
	ret->base_reservoir_sample = base_reservoir_sample->Copy();

	D_ASSERT(num_samples_to_keep <= ret->GetPriorityQueueSize());
	while (num_samples_to_keep < ret->GetPriorityQueueSize()) {
		ret->PopFromWeightQueue();
	}
	D_ASSERT(num_samples_to_keep == ret->GetPriorityQueueSize());

	for (idx_t i = 0; i < num_samples_to_keep; i++) {
		weights_indexes.push_back(ret->PopFromWeightQueue());
	}

	// ingestion sample has already been shrunk so there is only one sample chunk
	D_ASSERT(sample_chunk->size() <= FIXED_SAMPLE_SIZE);
	// create a new sample chunk to store new samples
	ret->reservoir_chunk = make_uniq<ReservoirChunk>();
	ret->reservoir_chunk->chunk.Initialize(Allocator::DefaultAllocator(), sample_chunk->GetTypes(), FIXED_SAMPLE_SIZE);
	for (idx_t col_idx = 0; col_idx < ret->reservoir_chunk->chunk.ColumnCount(); col_idx++) {
		// TODO: should the validity mask be the capacity or the size?
		FlatVector::Validity(ret->reservoir_chunk->chunk.data[col_idx]).Initialize(FIXED_SAMPLE_SIZE);
	}

	// set up selection vector to copy IngestionSample to ReservoirSample
	SelectionVector sel(num_samples_to_keep);
	// make sure the reservoir weights are empty. We will reconstruct the heap with new indexes
	// and the same weights
	D_ASSERT(ret->GetPriorityQueueSize() == 0);
	double max_weight = NumericLimits<double>::Minimum();
	idx_t max_weight_index = 0;
	for (idx_t i = 0; i < num_samples_to_keep; i++) {
		sel.set_index(i, weights_indexes[i].second);
		ret->base_reservoir_sample->reservoir_weights.emplace(weights_indexes[i].first, i);
		if (max_weight < weights_indexes[i].first) {
			max_weight = weights_indexes[i].first;
			max_weight_index = i;
		}
	}
	ret->Verify();
	ret->base_reservoir_sample->min_weighted_entry_index = max_weight_index;
	ret->base_reservoir_sample->min_weight_threshold = -max_weight;
	D_ASSERT(ret->base_reservoir_sample->min_weight_threshold > 0);

	// perform the copy

	// UpdateSampleCopy(ret->reservoir_chunk->chunk, sel, 0, 0, num_samples_to_keep);
	// sample_chunk->Copy(ret->reservoir_chunk->chunk, sel, num_samples_to_keep, 0);
	idx_t new_size = ret->reservoir_chunk->chunk.size() + num_samples_to_keep;

	D_ASSERT(sample_chunk->GetTypes() == ret->reservoir_chunk->chunk.GetTypes());
	auto types = sample_chunk->GetTypes();
	for (idx_t i = 0; i < sample_chunk->ColumnCount(); i++) {
		auto col_type = types[i];
		if (ValidSampleType(col_type)) {
			D_ASSERT(sample_chunk->data[i].GetVectorType() == VectorType::FLAT_VECTOR);
			VectorOperations::Copy(sample_chunk->data[i], ret->reservoir_chunk->chunk.data[i], sel, num_samples_to_keep,
			                       0, 0);
		} else {
			ret->reservoir_chunk->chunk.data[i].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(ret->reservoir_chunk->chunk.data[i], true);
		}
		// When the sample chunk is initialized, any non-numeric types are stored as constant vectors and set to null.
	}

	ret->reservoir_chunk->chunk.SetCardinality(new_size);

	// sample_chunk->Copy(ret->reservoir_chunk->chunk, sel, num_samples_to_keep, 0);
	D_ASSERT(ret->reservoir_chunk->chunk.size() == num_samples_to_keep);
	// ret->reservoir_chunk->chunk.SetCardinality(num_samples_to_keep);
	D_ASSERT(ret->GetPriorityQueueSize() == ret->reservoir_chunk->chunk.size());
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
		sample_chunk = make_uniq<DataChunk>();
		sample_chunk->Initialize(Allocator::DefaultAllocator(), types,
		                         FIXED_SAMPLE_SIZE * FIXED_SAMPLE_SIZE_MULTIPLIER);

		for (idx_t col_idx = 0; col_idx < sample_chunk->ColumnCount(); col_idx++) {
			auto col_type = types[col_idx];
			FlatVector::Validity(sample_chunk->data[col_idx])
			    .Initialize(FIXED_SAMPLE_SIZE * FIXED_SAMPLE_SIZE_MULTIPLIER);
			if (!ValidSampleType(col_type)) {
				sample_chunk->data[col_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(sample_chunk->data[col_idx], true);
			}
		}
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
	                                       (theoretical_chunk_length + base_reservoir_sample->num_entries_seen_total)) *
	                                      theoretical_chunk_length);

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

void IngestionSample::UpdateSampleAppend(DataChunk &other, SelectionVector &sel, idx_t sel_count) {
	idx_t new_size = sample_chunk->size() + sel_count;
	if (other.size() == 0) {
		return;
	}
	D_ASSERT(sample_chunk->GetTypes() == other.GetTypes());

	UpdateSampleWithTypes(other, sel, sel_count, 0, sample_chunk->size());
	sample_chunk->SetCardinality(new_size);
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
		// When the sample chunk is initialized, any non-numeric types are stored as constant vectors and set to null.
	}
}

void IngestionSample::UpdateSampleCopy(DataChunk &other, SelectionVector &sel, idx_t source_offset, idx_t target_offset,
                                       idx_t size) {
	idx_t new_size = sample_chunk->size() + size;
	UpdateSampleWithTypes(other, sel, size, 0, sample_chunk->size());
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
	// sample_chunk->SetCardinality(sample_chunk_size_before_ingestion + sample_chunk_ind_to_data_chunk_index.size());

	Verify();

	// if we are over the threshold, we ned to swith to slow sampling.
	if (SamplingMode() == SamplingMode::FAST && GetTuplesSeen() >= FIXED_SAMPLE_SIZE * FAST_TO_SLOW_THRESHOLD) {
		base_reservoir_sample->FillWeights(actual_sample_indexes);
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

void IngestionSample::PrintWeightsInOrder() {
	vector<double> weights;
	auto copy_base = base_reservoir_sample->Copy();
	while (!copy_base->reservoir_weights.empty()) {
		weights.push_back(copy_base->reservoir_weights.top().first);
		copy_base->reservoir_weights.pop();
	}
	std::sort(weights.begin(), weights.end(), [](double i, double j) { return i < j; });
	for (idx_t pos = 0; pos < weights.size(); pos++) {
		Printer::Print(to_string(pos) + ": " + to_string(weights.at(pos)));
	}
}

} // namespace duckdb
