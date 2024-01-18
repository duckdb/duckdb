#include "duckdb/execution/reservoir_sample.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

void ReservoirChunk::Serialize(Serializer &serializer) const {
	chunk.Serialize(serializer);
}

unique_ptr<ReservoirChunk> ReservoirChunk::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<ReservoirChunk>();
	result->chunk.Deserialize(deserializer);
	return result;
}

ReservoirSample::ReservoirSample(Allocator &allocator, idx_t sample_count, int64_t seed)
    : BlockingSample(seed), allocator(allocator), sample_count(sample_count), reservoir_initialized(false) {
	type = SampleType::RESERVOIR_SAMPLE;
}

ReservoirSample::ReservoirSample(idx_t sample_count, int64_t seed)
    : ReservoirSample(Allocator::DefaultAllocator(), sample_count, seed) {
}

void ReservoirSample::AddToReservoir(DataChunk &input) {
	if (sample_count == 0) {
		// sample count is 0, means no samples were requested
		return;
	}
	base_reservoir_sample->num_entries_seen_total += input.size();
	// Input: A population V of n weighted items
	// Output: A reservoir R with a size m
	// 1: The first m items of V are inserted into R
	// first we need to check if the reservoir already has "m" elements
	if (!reservoir_chunk || Chunk().size() < sample_count) {
		if (FillReservoir(input) == 0) {
			// entire chunk was consumed by reservoir
			return;
		}
	}
	D_ASSERT(reservoir_chunk);
	D_ASSERT(Chunk().size() == sample_count);
	// Initialize the weights if they have not been already
	if (base_reservoir_sample->reservoir_weights.empty()) {
		base_reservoir_sample->InitializeReservoir(Chunk().size(), sample_count);
	}
	// find the position of next_index_to_sample relative to number of seen entries (num_entries_to_skip_b4_next_sample)
	idx_t remaining = input.size();
	idx_t base_offset = 0;
	while (true) {
		idx_t offset =
		    base_reservoir_sample->next_index_to_sample - base_reservoir_sample->num_entries_to_skip_b4_next_sample;
		if (offset >= remaining) {
			// not in this chunk! increment current count and go to the next chunk
			base_reservoir_sample->num_entries_to_skip_b4_next_sample += remaining;
			return;
		}
		// in this chunk! replace the element
		ReplaceElement(input, base_offset + offset);
		// shift the chunk forward
		remaining -= offset;
		base_offset += offset;
	}
}

unique_ptr<BlockingSample> ReservoirSample::Copy() {
	auto ret = make_uniq<ReservoirSample>(Allocator::DefaultAllocator(), sample_count);
	ret->base_reservoir_sample = base_reservoir_sample->Copy();
	ret->reservoir_initialized = reservoir_initialized;
	ret->reservoir_chunk = reservoir_chunk->Copy();
	return ret;
}


unique_ptr<ReservoirChunk> ReservoirChunk::Copy() {
	auto copy = make_uniq<ReservoirChunk>();
	copy->chunk.Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());
	chunk.Copy(copy->chunk);
	return copy;
}


void ReservoirSample::Merge(unique_ptr<BlockingSample> other) {
	D_ASSERT(other->type == SampleType::RESERVOIR_SAMPLE);
	auto reservoir_other = &other->Cast<ReservoirSample>();
	// three ways to merge samples
	if (sample_count != reservoir_other->sample_count) {
		throw NotImplementedException("attempting to merge samples with different sample counts");
	}

	// There are four combinations for reservoir state

	// 1. This reservoir chunk has not yet been initialized.
	if (reservoir_chunk == nullptr) {
		// take ownership of the reservoir_others sample
		reservoir_initialized = true;
		base_reservoir_sample = std::move(other->base_reservoir_sample);
		reservoir_chunk = std::move(reservoir_other->reservoir_chunk);
		return;
	}

	if (reservoir_other->reservoir_chunk == nullptr) {
		return;
	}

	// 2. Both do not have full reservoir chunks
	if (Chunk().size() + reservoir_other->Chunk().size() < sample_count) {
		// the sum of both reservoir chunk sizes is less than sample count.
		// both samples have not yet thrown away tuples or assigned weights to tuples in the sample
		// Therefore we can just grab chunks from other and add them to this sample.
		// all logic to assign weights will automatically be handled in AddReservoir.
		auto chunk = reservoir_other->GetChunkAndShrink();
		while (chunk) {
			AddToReservoir(*chunk);
			chunk = reservoir_other->GetChunkAndShrink();
		}
		return;
	}

	// 3. Only one has a full reservoir chunk
	// merge the one that has not yet been filled into the full one.
	// The one not yet filled has not skipped any tuples yet, so we are not biased to it's sample when
	// using AddToReservoir.
	if (Chunk().size() + reservoir_other->Chunk().size() < (sample_count * 2)) {
		// one of the samples is full, but not the other.
		if (GetPriorityQueueSize() == sample_count) {
			auto chunk = reservoir_other->GetChunkAndShrink();
			while (chunk) {
				AddToReservoir(*chunk);
				chunk = reservoir_other->GetChunkAndShrink();
			}
		} else {
			// other is full
			// grab chunks from this to fill other
			D_ASSERT(reservoir_other->GetPriorityQueueSize() == sample_count);
			auto chunk = GetChunkAndShrink();
			while (chunk) {
				reservoir_other->AddToReservoir(*chunk);
				chunk = GetChunkAndShrink();
			}
			// now take ownership of the sample of other.
			reservoir_initialized = true;
			base_reservoir_sample = std::move(other->base_reservoir_sample);
			reservoir_chunk = std::move(reservoir_other->reservoir_chunk);
		}
		return;
	}

	//  4. this and other both have full reservoirs where each index in the sample has a weight
	//  Each reservoir has sample_count rows/tuples. We only want to keep the highest weighted samples
	//	so we remove sample_count tuples/rows from the reservoirs that have the lowest weights
	//	Then push the rest of the samples from other into this, using the weights the tuples had in other
	idx_t pop_count = Chunk().size() + reservoir_other->Chunk().size() - sample_count;
	D_ASSERT(pop_count == sample_count);

	// store indexes that need to be replaced in this
	// store weights for new values that will be replacing old values
	vector<idx_t> replaceable_indexes;
	auto min_weight_threshold_this = GetMinWeightThreshold();
	auto min_weight_threshold_other = reservoir_other->GetMinWeightThreshold();
	for (idx_t i = 0; i < pop_count; i++) {
		D_ASSERT(GetPriorityQueueSize() + reservoir_other->GetPriorityQueueSize() >= sample_count);
		if (min_weight_threshold_this < min_weight_threshold_other) {
			auto top = PopFromWeightQueue();
			// top.second holds the index of the replaceable tuple
			replaceable_indexes.push_back(top.second);
		} else {
			reservoir_other->PopFromWeightQueue();
		}
		min_weight_threshold_this = GetMinWeightThreshold();
		min_weight_threshold_other = reservoir_other->GetMinWeightThreshold();
	}

	while (replaceable_indexes.size() > 0) {
		auto top_other = reservoir_other->PopFromWeightQueue();
		auto index_to_replace = replaceable_indexes.back();
		ReplaceElement(index_to_replace, reservoir_other->Chunk(), top_other.second, -top_other.first);
		replaceable_indexes.pop_back();
	}

	D_ASSERT(GetPriorityQueueSize() == sample_count);
}

unique_ptr<DataChunk> ReservoirSample::GetChunk(idx_t offset) {
	if (offset >= Chunk().size()) {
		return nullptr;
	}
	if (!reservoir_chunk || Chunk().size() == 0) {
		return nullptr;
	}
	auto ret = make_uniq<DataChunk>();
	idx_t ret_chunk_size = STANDARD_VECTOR_SIZE;
	if (offset + STANDARD_VECTOR_SIZE > Chunk().size()) {
		ret_chunk_size = Chunk().size() - offset;
	}
	auto reservoir_types = Chunk().GetTypes();
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = offset; i < offset + ret_chunk_size; i++) {
		sel.set_index(i - offset, i);
	}
	ret->Initialize(allocator, reservoir_types.begin(), reservoir_types.end(), STANDARD_VECTOR_SIZE);
	ret->Slice(Chunk(), sel, STANDARD_VECTOR_SIZE);
	ret->SetCardinality(ret_chunk_size);
	return ret;
}

unique_ptr<DataChunk> ReservoirSample::GetChunkAndShrink() {
	if (!reservoir_chunk || Chunk().size() == 0) {
		return nullptr;
	}
	if (Chunk().size() > STANDARD_VECTOR_SIZE) {
		// get from the back
		auto ret = make_uniq<DataChunk>();
		auto samples_remaining = Chunk().size() - STANDARD_VECTOR_SIZE;
		auto reservoir_types = Chunk().GetTypes();
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		for (idx_t i = samples_remaining; i < Chunk().size(); i++) {
			sel.set_index(i - samples_remaining, i);
		}
		ret->Initialize(allocator, reservoir_types.begin(), reservoir_types.end(), STANDARD_VECTOR_SIZE);
		ret->Slice(Chunk(), sel, STANDARD_VECTOR_SIZE);
		ret->SetCardinality(STANDARD_VECTOR_SIZE);
		// reduce capacity and cardinality of the sample data chunk
		Chunk().SetCardinality(samples_remaining);
		return ret;
	}
	auto ret = make_uniq<DataChunk>();
	ret->Initialize(allocator, Chunk().GetTypes());
	Chunk().Copy(*ret);
	reservoir_chunk = nullptr;
	return ret;
}

void ReservoirSample::ReplaceElement(DataChunk &input, idx_t index_in_chunk, double with_weight) {
	// replace the entry in the reservoir with Input[index_in_chunk]
	// If index_in_self_chunk is provided, then the
	// 8. The item in R with the minimum key is replaced by item vi
	D_ASSERT(input.ColumnCount() == Chunk().ColumnCount());
	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		Chunk().SetValue(col_idx, base_reservoir_sample->min_weighted_entry_index,
		                 input.GetValue(col_idx, index_in_chunk));
	}
	base_reservoir_sample->ReplaceElement(with_weight);
}

void ReservoirSample::ReplaceElement(idx_t index, DataChunk &input, idx_t index_in_chunk, double with_weight) {
	// replace the entry in the reservoir with Input[index_in_chunk]
	// If index_in_self_chunk is provided, then the
	// 8. The item in R with the minimum key is replaced by item vi
	D_ASSERT(input.ColumnCount() == Chunk().ColumnCount());
	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		Chunk().SetValue(col_idx, index, input.GetValue(col_idx, index_in_chunk));
	}
	base_reservoir_sample->ReplaceElementWithIndex(index, with_weight);
}

void ReservoirSample::InitializeReservoir(DataChunk &input) {
	reservoir_chunk = make_uniq<ReservoirChunk>();
	Chunk().Initialize(allocator, input.GetTypes(), sample_count);
	for (idx_t col_idx = 0; col_idx < Chunk().ColumnCount(); col_idx++) {
		FlatVector::Validity(Chunk().data[col_idx]).Initialize(sample_count);
	}
	reservoir_initialized = true;
}

idx_t ReservoirSample::FillReservoir(DataChunk &input) {
	idx_t chunk_count = input.size();
	input.Flatten();
	auto num_added_samples = reservoir_chunk ? Chunk().size() : 0;
	D_ASSERT(num_added_samples <= sample_count);

	// required count is what we still need to add to the reservoir
	idx_t required_count;
	if (num_added_samples + chunk_count >= sample_count) {
		// have to limit the count of the chunk
		required_count = sample_count - num_added_samples;
	} else {
		// we copy the entire chunk
		required_count = chunk_count;
	}
	input.SetCardinality(required_count);

	// initialize the reservoir
	if (!reservoir_initialized) {
		InitializeReservoir(input);
	}
	Chunk().Append(input, false, nullptr, required_count);
	base_reservoir_sample->InitializeReservoir(required_count, sample_count);

	num_added_samples += required_count;
	Chunk().SetCardinality(num_added_samples);
	// check if there are still elements remaining in the Input data chunk that should be
	// randomly sampled and potentially added. This happens if we are on a boundary
	// for example, input.size() is 1024, but our sample size is 10
	if (required_count == chunk_count) {
		// we are done here
		return 0;
	}
	// we still need to process a part of the chunk
	// create a selection vector of the remaining elements
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = required_count; i < chunk_count; i++) {
		sel.set_index(i - required_count, i);
	}
	// slice the input vector and continue
	input.Slice(sel, chunk_count - required_count);
	return input.size();
}

DataChunk &ReservoirSample::Chunk() {
	D_ASSERT(reservoir_chunk);
	return reservoir_chunk->chunk;
}

void ReservoirSample::Finalize() {
	return;
}

ReservoirSamplePercentage::ReservoirSamplePercentage(Allocator &allocator, double percentage, int64_t seed)
    : BlockingSample(seed), allocator(allocator), sample_percentage(percentage / 100.0), current_count(0),
      is_finalized(false) {
	reservoir_sample_size = idx_t(sample_percentage * RESERVOIR_THRESHOLD);
	current_sample = make_uniq<ReservoirSample>(allocator, reservoir_sample_size, base_reservoir_sample->random.NextRandomInteger());
	type = SampleType::RESERVOIR_PERCENTAGE_SAMPLE;
}

ReservoirSamplePercentage::ReservoirSamplePercentage(double percentage, int64_t seed)
    : ReservoirSamplePercentage(Allocator::DefaultAllocator(), percentage, seed) {
}

void ReservoirSamplePercentage::AddToReservoir(DataChunk &input) {
	base_reservoir_sample->num_entries_seen_total += input.size();
	if (current_count + input.size() > RESERVOIR_THRESHOLD) {
		// we don't have enough space in our current reservoir
		// first check what we still need to append to the current sample
		idx_t append_to_current_sample_count = RESERVOIR_THRESHOLD - current_count;
		idx_t append_to_next_sample = input.size() - append_to_current_sample_count;
		if (append_to_current_sample_count > 0) {
			// we have elements remaining, first add them to the current sample
			if (append_to_next_sample > 0) {
				// we need to also add to the next sample
				DataChunk new_chunk;
				new_chunk.InitializeEmpty(input.GetTypes());
				new_chunk.Slice(input, *FlatVector::IncrementalSelectionVector(), append_to_current_sample_count);
				new_chunk.Flatten();
				current_sample->AddToReservoir(new_chunk);
			} else {
				input.Flatten();
				input.SetCardinality(append_to_current_sample_count);
				current_sample->AddToReservoir(input);
			}
		}
		if (append_to_next_sample > 0) {
			// slice the input for the remainder
			SelectionVector sel(append_to_next_sample);
			for (idx_t i = append_to_current_sample_count; i < append_to_next_sample + append_to_current_sample_count;
			     i++) {
				sel.set_index(i - append_to_current_sample_count, i);
			}
			input.Slice(sel, append_to_next_sample);
		}
		// now our first sample is filled: append it to the set of finished samples
		finished_samples.push_back(std::move(current_sample));

		// allocate a new sample, and potentially add the remainder of the current input to that sample
		current_sample = make_uniq<ReservoirSample>(allocator, reservoir_sample_size, base_reservoir_sample->random.NextRandomInteger());
		if (append_to_next_sample > 0) {
			current_sample->AddToReservoir(input);
		}
		current_count = append_to_next_sample;
	} else {
		// we can just append to the current sample
		current_count += input.size();
		current_sample->AddToReservoir(input);
	}
}

void ReservoirSamplePercentage::Merge(unique_ptr<BlockingSample> other) {
	throw NotImplementedException("Merging Percentage samples is not yet supported");
}

void ReservoirSamplePercentage::PushNewWeightsForSamples() {
	throw NotImplementedException("Cannot Push new weights for Percentage Sample");
}

unique_ptr<BlockingSample> ReservoirSamplePercentage::Copy() {
	throw NotImplementedException("Cannot copy percentage sample");
}

unique_ptr<DataChunk> ReservoirSamplePercentage::GetChunk(idx_t offset) {
	throw NotImplementedException("GetChunk() not implemented for reservoir sample chunks");
}

unique_ptr<DataChunk> ReservoirSamplePercentage::GetChunkAndShrink() {
	if (!is_finalized) {
		Finalize();
	}
	while (!finished_samples.empty()) {
		auto &front = finished_samples.front();
		auto chunk = front->GetChunkAndShrink();
		if (chunk && chunk->size() > 0) {
			return chunk;
		}
		// move to the next sample
		finished_samples.erase(finished_samples.begin());
	}
	return nullptr;
}

void ReservoirSamplePercentage::Finalize() {
	// need to finalize the current sample, if any
	// we are finializing, so we are starting to return chunks. Our last chunk has
	// sample_percentage * RESERVOIR_THRESHOLD entries that hold samples.
	// if our current count is less than the sample_percentage * RESERVOIR_THRESHOLD
	// then we have sampled too much for the current_sample and we need to redo the sample
	// otherwise we can just push the current sample back
	// Imagine sampling 70% of 100 rows (so 70 rows). We allocate sample_percentage * RESERVOIR_THRESHOLD
	// -----------------------------------------
	auto sampled_more_than_required =
	    current_count > sample_percentage * RESERVOIR_THRESHOLD || finished_samples.empty();
	if (current_count > 0 && sampled_more_than_required) {
		// create a new sample
		auto new_sample_size = idx_t(round(sample_percentage * current_count));
		auto new_sample = make_uniq<ReservoirSample>(allocator, new_sample_size, base_reservoir_sample->random.NextRandomInteger());
		while (true) {
			auto chunk = current_sample->GetChunkAndShrink();
			if (!chunk || chunk->size() == 0) {
				break;
			}
			new_sample->AddToReservoir(*chunk);
		}
		finished_samples.push_back(std::move(new_sample));
	} else {
		finished_samples.push_back(std::move(current_sample));
	}
	// when finalizing, current_sample is null. All samples are now in finished samples.
	current_sample = nullptr;
	is_finalized = true;
}

BaseReservoirSampling::BaseReservoirSampling(int64_t seed) : random(seed) {
	next_index_to_sample = 0;
	min_weight_threshold = 0;
	min_weighted_entry_index = 0;
	num_entries_to_skip_b4_next_sample = 0;
	num_entries_seen_total = 0;
}

BaseReservoirSampling::BaseReservoirSampling() : BaseReservoirSampling(-1) {
}

unique_ptr<BaseReservoirSampling> BaseReservoirSampling::Copy() {
	auto ret = make_uniq<BaseReservoirSampling>(-1);
	ret->reservoir_weights = reservoir_weights;
	ret->next_index_to_sample = next_index_to_sample;
	ret->min_weight_threshold = min_weight_threshold;
	ret->min_weighted_entry_index = min_weighted_entry_index;
	ret->num_entries_to_skip_b4_next_sample = num_entries_to_skip_b4_next_sample;
	ret->num_entries_seen_total = num_entries_seen_total;
	return ret;
}

void ReservoirSample::PushNewWeightsForSamples() {
	vector<std::pair<double, idx_t>> pairs_to_push = {
	    std::make_pair(-0.999583, 704),
	    std::make_pair(-0.999584, 1553),
	    std::make_pair(-0.999584, 537),
	    std::make_pair(-0.999584, 1609),
	    std::make_pair(-0.999584, 362),
	    std::make_pair(-0.999584, 340),
	    std::make_pair(-0.999585, 391),
	    std::make_pair(-0.999585, 125),
	    std::make_pair(-0.999585, 542),
	    std::make_pair(-0.999585, 1810),
	    std::make_pair(-0.999585, 152),
	    std::make_pair(-0.999586, 1959),
	    std::make_pair(-0.999586, 1103),
	    std::make_pair(-0.999586, 459),
	    std::make_pair(-0.999586, 1197),
	    std::make_pair(-0.999586, 1096),
	    std::make_pair(-0.999587, 521),
	    std::make_pair(-0.999587, 1074),
	    std::make_pair(-0.999588, 822),
	    std::make_pair(-0.999588, 1319),
	    std::make_pair(-0.999589, 1648),
	    std::make_pair(-0.999589, 1179),
	    std::make_pair(-0.999589, 933),
	    std::make_pair(-0.999589, 230),
	    std::make_pair(-0.999589, 1745),
	    std::make_pair(-0.999589, 166),
	    std::make_pair(-0.999589, 1102),
	    std::make_pair(-0.99959, 1276),
	    std::make_pair(-0.99959, 1401),
	    std::make_pair(-0.99959, 960),
	    std::make_pair(-0.999591, 663),
	    std::make_pair(-0.999591, 935),
	    std::make_pair(-0.999592, 915),
	    std::make_pair(-0.999592, 1674),
	    std::make_pair(-0.999592, 829),
	    std::make_pair(-0.999592, 216),
	    std::make_pair(-0.999592, 848),
	    std::make_pair(-0.999592, 1920),
	    std::make_pair(-0.999593, 262),
	    std::make_pair(-0.999593, 904),
	    std::make_pair(-0.999593, 1289),
	    std::make_pair(-0.999594, 4),
	    std::make_pair(-0.999594, 768),
	    std::make_pair(-0.999594, 2038),
	    std::make_pair(-0.999594, 1338),
	    std::make_pair(-0.999594, 1741),
	    std::make_pair(-0.999594, 1115),
	    std::make_pair(-0.999594, 855),
	    std::make_pair(-0.999595, 164),
	    std::make_pair(-0.999595, 360),
	    std::make_pair(-0.999595, 23),
	    std::make_pair(-0.999596, 1562),
	    std::make_pair(-0.999596, 1093),
	    std::make_pair(-0.999596, 1171),
	    std::make_pair(-0.999596, 1365),
	    std::make_pair(-0.999597, 976),
	    std::make_pair(-0.999597, 1390),
	    std::make_pair(-0.999597, 884),
	    std::make_pair(-0.999597, 1753),
	    std::make_pair(-0.999597, 1187),
	    std::make_pair(-0.999597, 2004),
	    std::make_pair(-0.999597, 1698),
	    std::make_pair(-0.999597, 1987),
	    std::make_pair(-0.999597, 1677),
	    std::make_pair(-0.999598, 632),
	    std::make_pair(-0.999598, 1843),
	    std::make_pair(-0.999598, 788),
	    std::make_pair(-0.999598, 1918),
	    std::make_pair(-0.999599, 1456),
	    std::make_pair(-0.999599, 1517),
	    std::make_pair(-0.999599, 1626),
	    std::make_pair(-0.999599, 1423),
	    std::make_pair(-0.999599, 1426),
	    std::make_pair(-0.9996, 1736),
	    std::make_pair(-0.999601, 376),
	    std::make_pair(-0.999601, 33),
	    std::make_pair(-0.999601, 1684),
	    std::make_pair(-0.999601, 111),
	    std::make_pair(-0.999601, 1970),
	    std::make_pair(-0.999602, 1557),
	    std::make_pair(-0.999602, 1343),
	    std::make_pair(-0.999603, 1806),
	    std::make_pair(-0.999603, 828),
	    std::make_pair(-0.999603, 236),
	    std::make_pair(-0.999603, 1075),
	    std::make_pair(-0.999603, 791),
	    std::make_pair(-0.999603, 1613),
	    std::make_pair(-0.999604, 1470),
	    std::make_pair(-0.999604, 274),
	    std::make_pair(-0.999605, 944),
	    std::make_pair(-0.999605, 1486),
	    std::make_pair(-0.999605, 1984),
	    std::make_pair(-0.999605, 1065),
	    std::make_pair(-0.999605, 1867),
	    std::make_pair(-0.999606, 1063),
	    std::make_pair(-0.999606, 1095),
	    std::make_pair(-0.999607, 69),
	    std::make_pair(-0.999607, 1535),
	    std::make_pair(-0.999608, 499),
	    std::make_pair(-0.999608, 1011),
	    std::make_pair(-0.999608, 2045),
	    std::make_pair(-0.999609, 19),
	    std::make_pair(-0.999609, 947),
	    std::make_pair(-0.999609, 1204),
	    std::make_pair(-0.99961, 1146),
	    std::make_pair(-0.99961, 920),
	    std::make_pair(-0.99961, 37),
	    std::make_pair(-0.99961, 101),
	    std::make_pair(-0.99961, 366),
	    std::make_pair(-0.99961, 762),
	    std::make_pair(-0.999611, 1726),
	    std::make_pair(-0.999611, 358),
	    std::make_pair(-0.999611, 1734),
	    std::make_pair(-0.999612, 1758),
	    std::make_pair(-0.999612, 423),
	    std::make_pair(-0.999612, 1100),
	    std::make_pair(-0.999612, 1397),
	    std::make_pair(-0.999612, 1106),
	    std::make_pair(-0.999613, 806),
	    std::make_pair(-0.999613, 1405),
	    std::make_pair(-0.999613, 1040),
	    std::make_pair(-0.999613, 1536),
	    std::make_pair(-0.999613, 1140),
	    std::make_pair(-0.999613, 80),
	    std::make_pair(-0.999614, 1603),
	    std::make_pair(-0.999615, 192),
	    std::make_pair(-0.999615, 646),
	    std::make_pair(-0.999615, 244),
	    std::make_pair(-0.999615, 1479),
	    std::make_pair(-0.999616, 1170),
	    std::make_pair(-0.999616, 1836),
	    std::make_pair(-0.999616, 350),
	    std::make_pair(-0.999616, 323),
	    std::make_pair(-0.999616, 461),
	    std::make_pair(-0.999616, 364),
	    std::make_pair(-0.999617, 1751),
	    std::make_pair(-0.999617, 1454),
	    std::make_pair(-0.999618, 78),
	    std::make_pair(-0.999618, 1521),
	    std::make_pair(-0.999618, 220),
	    std::make_pair(-0.999618, 979),
	    std::make_pair(-0.999618, 1235),
	    std::make_pair(-0.999618, 1580),
	    std::make_pair(-0.999618, 1544),
	    std::make_pair(-0.999619, 1919),
	    std::make_pair(-0.999619, 969),
	    std::make_pair(-0.999619, 510),
	    std::make_pair(-0.999619, 1624),
	    std::make_pair(-0.999619, 1184),
	    std::make_pair(-0.999619, 115),
	    std::make_pair(-0.99962, 1087),
	    std::make_pair(-0.99962, 298),
	    std::make_pair(-0.99962, 533),
	    std::make_pair(-0.99962, 1372),
	    std::make_pair(-0.99962, 1762),
	    std::make_pair(-0.99962, 410),
	    std::make_pair(-0.999621, 1931),
	    std::make_pair(-0.999621, 1815),
	    std::make_pair(-0.999621, 1914),
	    std::make_pair(-0.999621, 1057),
	    std::make_pair(-0.999621, 1601),
	    std::make_pair(-0.999621, 2047),
	    std::make_pair(-0.999622, 887),
	    std::make_pair(-0.999622, 1380),
	    std::make_pair(-0.999622, 259),
	    std::make_pair(-0.999622, 1632),
	    std::make_pair(-0.999622, 1177),
	    std::make_pair(-0.999623, 1605),
	    std::make_pair(-0.999623, 840),
	    std::make_pair(-0.999623, 182),
	    std::make_pair(-0.999623, 1798),
	    std::make_pair(-0.999624, 1643),
	    std::make_pair(-0.999624, 1930),
	    std::make_pair(-0.999624, 1986),
	    std::make_pair(-0.999624, 102),
	    std::make_pair(-0.999625, 1147),
	    std::make_pair(-0.999625, 113),
	    std::make_pair(-0.999626, 416),
	    std::make_pair(-0.999626, 1761),
	    std::make_pair(-0.999626, 1167),
	    std::make_pair(-0.999626, 428),
	    std::make_pair(-0.999626, 402),
	    std::make_pair(-0.999626, 377),
	    std::make_pair(-0.999627, 672),
	    std::make_pair(-0.999627, 165),
	    std::make_pair(-0.999627, 232),
	    std::make_pair(-0.999627, 1445),
	    std::make_pair(-0.999627, 1481),
	    std::make_pair(-0.999628, 1291),
	    std::make_pair(-0.999628, 442),
	    std::make_pair(-0.999628, 1305),
	    std::make_pair(-0.999628, 953),
	    std::make_pair(-0.999628, 1358),
	    std::make_pair(-0.999628, 1001),
	    std::make_pair(-0.999628, 1061),
	    std::make_pair(-0.999629, 1729),
	    std::make_pair(-0.999629, 203),
	    std::make_pair(-0.999629, 491),
	    std::make_pair(-0.999629, 51),
	    std::make_pair(-0.999629, 1154),
	    std::make_pair(-0.999629, 781),
	    std::make_pair(-0.999629, 557),
	    std::make_pair(-0.99963, 860),
	    std::make_pair(-0.99963, 936),
	    std::make_pair(-0.99963, 1594),
	    std::make_pair(-0.99963, 1113),
	    std::make_pair(-0.99963, 605),
	    std::make_pair(-0.99963, 369),
	    std::make_pair(-0.999631, 1822),
	    std::make_pair(-0.999631, 1738),
	    std::make_pair(-0.999631, 847),
	    std::make_pair(-0.999631, 2015),
	    std::make_pair(-0.999632, 1824),
	    std::make_pair(-0.999632, 1137),
	    std::make_pair(-0.999632, 1887),
	    std::make_pair(-0.999632, 737),
	    std::make_pair(-0.999633, 1306),
	    std::make_pair(-0.999633, 419),
	    std::make_pair(-0.999633, 1739),
	    std::make_pair(-0.999633, 1228),
	    std::make_pair(-0.999633, 1393),
	    std::make_pair(-0.999633, 1977),
	    std::make_pair(-0.999634, 227),
	    std::make_pair(-0.999634, 594),
	    std::make_pair(-0.999634, 938),
	    std::make_pair(-0.999634, 561),
	    std::make_pair(-0.999634, 1829),
	    std::make_pair(-0.999634, 1299),
	    std::make_pair(-0.999635, 463),
	    std::make_pair(-0.999635, 1778),
	    std::make_pair(-0.999635, 1499),
	    std::make_pair(-0.999635, 1398),
	    std::make_pair(-0.999636, 479),
	    std::make_pair(-0.999636, 1812),
	    std::make_pair(-0.999636, 1388),
	    std::make_pair(-0.999637, 1300),
	    std::make_pair(-0.999638, 611),
	    std::make_pair(-0.999638, 1717),
	    std::make_pair(-0.999638, 1482),
	    std::make_pair(-0.999638, 759),
	    std::make_pair(-0.999639, 26),
	    std::make_pair(-0.999639, 1138),
	    std::make_pair(-0.999639, 985),
	    std::make_pair(-0.999639, 1158),
	    std::make_pair(-0.999639, 754),
	    std::make_pair(-0.999639, 680),
	    std::make_pair(-0.99964, 1169),
	    std::make_pair(-0.99964, 1020),
	    std::make_pair(-0.99964, 1608),
	    std::make_pair(-0.99964, 1568),
	    std::make_pair(-0.999641, 93),
	    std::make_pair(-0.999641, 1107),
	    std::make_pair(-0.999641, 1635),
	    std::make_pair(-0.999641, 25),
	    std::make_pair(-0.999642, 487),
	    std::make_pair(-0.999642, 86),
	    std::make_pair(-0.999642, 1066),
	    std::make_pair(-0.999643, 1525),
	    std::make_pair(-0.999643, 1468),
	    std::make_pair(-0.999643, 1272),
	    std::make_pair(-0.999643, 397),
	    std::make_pair(-0.999643, 314),
	    std::make_pair(-0.999643, 685),
	    std::make_pair(-0.999643, 954),
	    std::make_pair(-0.999643, 1645),
	    std::make_pair(-0.999644, 1652),
	    std::make_pair(-0.999644, 837),
	    std::make_pair(-0.999644, 63),
	    std::make_pair(-0.999644, 677),
	    std::make_pair(-0.999644, 1059),
	    std::make_pair(-0.999645, 504),
	    std::make_pair(-0.999645, 1523),
	    std::make_pair(-0.999645, 1345),
	    std::make_pair(-0.999646, 1435),
	    std::make_pair(-0.999646, 1879),
	    std::make_pair(-0.999646, 922),
	    std::make_pair(-0.999646, 140),
	    std::make_pair(-0.999646, 1834),
	    std::make_pair(-0.999646, 1334),
	    std::make_pair(-0.999647, 206),
	    std::make_pair(-0.999647, 1855),
	    std::make_pair(-0.999647, 1112),
	    std::make_pair(-0.999647, 1884),
	    std::make_pair(-0.999647, 444),
	    std::make_pair(-0.999647, 1502),
	    std::make_pair(-0.999647, 322),
	    std::make_pair(-0.999647, 738),
	    std::make_pair(-0.999648, 210),
	    std::make_pair(-0.999648, 1340),
	    std::make_pair(-0.999648, 801),
	    std::make_pair(-0.999648, 321),
	    std::make_pair(-0.999648, 1399),
	    std::make_pair(-0.999649, 1406),
	    std::make_pair(-0.999649, 1647),
	    std::make_pair(-0.999649, 430),
	    std::make_pair(-0.999649, 783),
	    std::make_pair(-0.999649, 770),
	    std::make_pair(-0.999649, 870),
	    std::make_pair(-0.99965, 378),
	    std::make_pair(-0.99965, 1364),
	    std::make_pair(-0.99965, 151),
	    std::make_pair(-0.99965, 532),
	    std::make_pair(-0.99965, 914),
	    std::make_pair(-0.99965, 1419),
	    std::make_pair(-0.999651, 2023),
	    std::make_pair(-0.999651, 1830),
	    std::make_pair(-0.999651, 1034),
	    std::make_pair(-0.999651, 1767),
	    std::make_pair(-0.999652, 1205),
	    std::make_pair(-0.999652, 901),
	    std::make_pair(-0.999652, 614),
	    std::make_pair(-0.999652, 1467),
	    std::make_pair(-0.999652, 456),
	    std::make_pair(-0.999652, 1135),
	    std::make_pair(-0.999652, 1804),
	    std::make_pair(-0.999652, 571),
	    std::make_pair(-0.999653, 121),
	    std::make_pair(-0.999653, 1796),
	    std::make_pair(-0.999653, 607),
	    std::make_pair(-0.999653, 1422),
	    std::make_pair(-0.999653, 851),
	    std::make_pair(-0.999653, 579),
	    std::make_pair(-0.999653, 170),
	    std::make_pair(-0.999654, 1937),
	    std::make_pair(-0.999654, 1885),
	    std::make_pair(-0.999654, 1592),
	    std::make_pair(-0.999654, 896),
	    std::make_pair(-0.999655, 415),
	    std::make_pair(-0.999655, 168),
	    std::make_pair(-0.999655, 895),
	    std::make_pair(-0.999655, 649),
	    std::make_pair(-0.999655, 893),
	    std::make_pair(-0.999655, 1639),
	    std::make_pair(-0.999656, 897),
	    std::make_pair(-0.999656, 1465),
	    std::make_pair(-0.999656, 1442),
	    std::make_pair(-0.999656, 529),
	    std::make_pair(-0.999657, 520),
	    std::make_pair(-0.999657, 1241),
	    std::make_pair(-0.999657, 1210),
	    std::make_pair(-0.999658, 409),
	    std::make_pair(-0.999658, 1585),
	    std::make_pair(-0.999658, 1413),
	    std::make_pair(-0.999658, 1695),
	    std::make_pair(-0.999658, 1618),
	    std::make_pair(-0.999659, 547),
	    std::make_pair(-0.999659, 1950),
	    std::make_pair(-0.999659, 1230),
	    std::make_pair(-0.99966, 1828),
	    std::make_pair(-0.99966, 1901),
	    std::make_pair(-0.99966, 455),
	    std::make_pair(-0.99966, 740),
	    std::make_pair(-0.99966, 451),
	    std::make_pair(-0.999661, 245),
	    std::make_pair(-0.999661, 1994),
	    std::make_pair(-0.999661, 1352),
	    std::make_pair(-0.999661, 47),
	    std::make_pair(-0.999661, 1145),
	    std::make_pair(-0.999661, 2011),
	    std::make_pair(-0.999662, 525),
	    std::make_pair(-0.999662, 1128),
	    std::make_pair(-0.999662, 531),
	    std::make_pair(-0.999663, 261),
	    std::make_pair(-0.999663, 951),
	    std::make_pair(-0.999663, 336),
	    std::make_pair(-0.999664, 813),
	    std::make_pair(-0.999664, 876),
	    std::make_pair(-0.999664, 1025),
	    std::make_pair(-0.999664, 902),
	    std::make_pair(-0.999664, 1709),
	    std::make_pair(-0.999665, 629),
	    std::make_pair(-0.999665, 186),
	    std::make_pair(-0.999665, 1234),
	    std::make_pair(-0.999666, 1256),
	    std::make_pair(-0.999666, 229),
	    std::make_pair(-0.999666, 905),
	    std::make_pair(-0.999666, 693),
	    std::make_pair(-0.999667, 1936),
	    std::make_pair(-0.999667, 603),
	    std::make_pair(-0.999668, 1623),
	    std::make_pair(-0.999668, 1664),
	    std::make_pair(-0.999668, 548),
	    std::make_pair(-0.999668, 432),
	    std::make_pair(-0.999668, 1989),
	    std::make_pair(-0.999668, 373),
	    std::make_pair(-0.999668, 1005),
	    std::make_pair(-0.999668, 1165),
	    std::make_pair(-0.999669, 931),
	    std::make_pair(-0.999669, 1436),
	    std::make_pair(-0.999669, 898),
	    std::make_pair(-0.99967, 1656),
	    std::make_pair(-0.99967, 1384),
	    std::make_pair(-0.99967, 44),
	    std::make_pair(-0.999671, 715),
	    std::make_pair(-0.999671, 5),
	    std::make_pair(-0.999671, 1975),
	    std::make_pair(-0.999671, 1199),
	    std::make_pair(-0.999671, 312),
	    std::make_pair(-0.999671, 1053),
	    std::make_pair(-0.999672, 682),
	    std::make_pair(-0.999672, 587),
	    std::make_pair(-0.999672, 596),
	    std::make_pair(-0.999672, 539),
	    std::make_pair(-0.999673, 812),
	    std::make_pair(-0.999673, 1097),
	    std::make_pair(-0.999673, 1921),
	    std::make_pair(-0.999673, 690),
	    std::make_pair(-0.999674, 725),
	    std::make_pair(-0.999674, 126),
	    std::make_pair(-0.999674, 1817),
	    std::make_pair(-0.999675, 1309),
	    std::make_pair(-0.999675, 654),
	    std::make_pair(-0.999675, 906),
	    std::make_pair(-0.999675, 1575),
	    std::make_pair(-0.999675, 1308),
	    std::make_pair(-0.999675, 233),
	    std::make_pair(-0.999675, 1571),
	    std::make_pair(-0.999675, 43),
	    std::make_pair(-0.999675, 372),
	    std::make_pair(-0.999675, 1287),
	    std::make_pair(-0.999675, 919),
	    std::make_pair(-0.999676, 739),
	    std::make_pair(-0.999676, 1327),
	    std::make_pair(-0.999676, 1148),
	    std::make_pair(-0.999676, 1432),
	    std::make_pair(-0.999676, 1694),
	    std::make_pair(-0.999677, 417),
	    std::make_pair(-0.999677, 2035),
	    std::make_pair(-0.999677, 1496),
	    std::make_pair(-0.999677, 1768),
	    std::make_pair(-0.999677, 563),
	    std::make_pair(-0.999677, 1788),
	    std::make_pair(-0.999677, 243),
	    std::make_pair(-0.999678, 252),
	    std::make_pair(-0.999678, 46),
	    std::make_pair(-0.999678, 460),
	    std::make_pair(-0.999679, 1848),
	    std::make_pair(-0.999679, 253),
	    std::make_pair(-0.999679, 962),
	    std::make_pair(-0.999679, 1895),
	    std::make_pair(-0.999679, 572),
	    std::make_pair(-0.999679, 1491),
	    std::make_pair(-0.99968, 1262),
	    std::make_pair(-0.99968, 695),
	    std::make_pair(-0.99968, 1284),
	    std::make_pair(-0.99968, 480),
	    std::make_pair(-0.999681, 1301),
	    std::make_pair(-0.999681, 706),
	    std::make_pair(-0.999682, 361),
	    std::make_pair(-0.999682, 503),
	    std::make_pair(-0.999682, 1247),
	    std::make_pair(-0.999682, 1574),
	    std::make_pair(-0.999682, 2009),
	    std::make_pair(-0.999683, 154),
	    std::make_pair(-0.999683, 249),
	    std::make_pair(-0.999683, 2014),
	    std::make_pair(-0.999683, 1754),
	    std::make_pair(-0.999683, 696),
	    std::make_pair(-0.999684, 1783),
	    std::make_pair(-0.999684, 1180),
	    std::make_pair(-0.999684, 1957),
	    std::make_pair(-0.999684, 1225),
	    std::make_pair(-0.999684, 1049),
	    std::make_pair(-0.999685, 1),
	    std::make_pair(-0.999685, 1607),
	    std::make_pair(-0.999685, 1551),
	    std::make_pair(-0.999685, 1705),
	    std::make_pair(-0.999685, 1785),
	    std::make_pair(-0.999685, 256),
	    std::make_pair(-0.999685, 400),
	    std::make_pair(-0.999686, 797),
	    std::make_pair(-0.999686, 756),
	    std::make_pair(-0.999686, 2008),
	    std::make_pair(-0.999686, 1874),
	    std::make_pair(-0.999687, 728),
	    std::make_pair(-0.999687, 299),
	    std::make_pair(-0.999687, 1750),
	    std::make_pair(-0.999688, 711),
	    std::make_pair(-0.999688, 830),
	    std::make_pair(-0.999688, 2019),
	    std::make_pair(-0.999688, 344),
	    std::make_pair(-0.999688, 1094),
	    std::make_pair(-0.999689, 850),
	    std::make_pair(-0.999689, 1679),
	    std::make_pair(-0.999689, 1337),
	    std::make_pair(-0.999689, 1563),
	    std::make_pair(-0.999689, 1290),
	    std::make_pair(-0.999689, 1313),
	    std::make_pair(-0.999689, 1484),
	    std::make_pair(-0.999689, 494),
	    std::make_pair(-0.999689, 989),
	    std::make_pair(-0.999689, 2013),
	    std::make_pair(-0.99969, 1433),
	    std::make_pair(-0.99969, 809),
	    std::make_pair(-0.99969, 1130),
	    std::make_pair(-0.99969, 1948),
	    std::make_pair(-0.99969, 83),
	    std::make_pair(-0.99969, 1307),
	    std::make_pair(-0.999691, 865),
	    std::make_pair(-0.999691, 720),
	    std::make_pair(-0.999691, 2021),
	    std::make_pair(-0.999691, 554),
	    std::make_pair(-0.999691, 64),
	    std::make_pair(-0.999691, 1160),
	    std::make_pair(-0.999691, 1651),
	    std::make_pair(-0.999691, 1355),
	    std::make_pair(-0.999691, 1615),
	    std::make_pair(-0.999691, 1000),
	    std::make_pair(-0.999691, 1366),
	    std::make_pair(-0.999692, 71),
	    std::make_pair(-0.999692, 1841),
	    std::make_pair(-0.999692, 1415),
	    std::make_pair(-0.999692, 76),
	    std::make_pair(-0.999693, 1720),
	    std::make_pair(-0.999693, 211),
	    std::make_pair(-0.999694, 1586),
	    std::make_pair(-0.999694, 1814),
	    std::make_pair(-0.999694, 1772),
	    std::make_pair(-0.999694, 1993),
	    std::make_pair(-0.999694, 658),
	    std::make_pair(-0.999694, 1036),
	    std::make_pair(-0.999694, 1266),
	    std::make_pair(-0.999694, 1076),
	    std::make_pair(-0.999696, 1417),
	    std::make_pair(-0.999696, 753),
	    std::make_pair(-0.999696, 1487),
	    std::make_pair(-0.999696, 1017),
	    std::make_pair(-0.999696, 1362),
	    std::make_pair(-0.999696, 965),
	    std::make_pair(-0.999697, 238),
	    std::make_pair(-0.999697, 867),
	    std::make_pair(-0.999697, 1202),
	    std::make_pair(-0.999697, 218),
	    std::make_pair(-0.999697, 493),
	    std::make_pair(-0.999697, 1789),
	    std::make_pair(-0.999697, 1967),
	    std::make_pair(-0.999697, 1490),
	    std::make_pair(-0.999697, 709),
	    std::make_pair(-0.999698, 1037),
	    std::make_pair(-0.999698, 861),
	    std::make_pair(-0.999698, 1149),
	    std::make_pair(-0.999698, 2041),
	    std::make_pair(-0.999698, 1602),
	    std::make_pair(-0.999699, 1035),
	    std::make_pair(-0.999699, 292),
	    std::make_pair(-0.999699, 1712),
	    std::make_pair(-0.999699, 1663),
	    std::make_pair(-0.999699, 68),
	    std::make_pair(-0.999699, 116),
	    std::make_pair(-0.9997, 1446),
	    std::make_pair(-0.9997, 193),
	    std::make_pair(-0.9997, 826),
	    std::make_pair(-0.9997, 795),
	    std::make_pair(-0.9997, 1978),
	    std::make_pair(-0.999701, 394),
	    std::make_pair(-0.999701, 1320),
	    std::make_pair(-0.999701, 562),
	    std::make_pair(-0.999701, 2043),
	    std::make_pair(-0.999701, 996),
	    std::make_pair(-0.999702, 648),
	    std::make_pair(-0.999702, 921),
	    std::make_pair(-0.999702, 1802),
	    std::make_pair(-0.999702, 48),
	    std::make_pair(-0.999702, 302),
	    std::make_pair(-0.999702, 332),
	    std::make_pair(-0.999702, 713),
	    std::make_pair(-0.999702, 880),
	    std::make_pair(-0.999702, 552),
	    std::make_pair(-0.999703, 1058),
	    std::make_pair(-0.999703, 780),
	    std::make_pair(-0.999703, 1528),
	    std::make_pair(-0.999703, 1964),
	    std::make_pair(-0.999704, 1152),
	    std::make_pair(-0.999704, 553),
	    std::make_pair(-0.999704, 1023),
	    std::make_pair(-0.999704, 453),
	    std::make_pair(-0.999704, 128),
	    std::make_pair(-0.999704, 1198),
	    std::make_pair(-0.999705, 120),
	    std::make_pair(-0.999705, 845),
	    std::make_pair(-0.999705, 1588),
	    std::make_pair(-0.999705, 379),
	    std::make_pair(-0.999705, 1395),
	    std::make_pair(-0.999705, 564),
	    std::make_pair(-0.999706, 54),
	    std::make_pair(-0.999706, 917),
	    std::make_pair(-0.999706, 1190),
	    std::make_pair(-0.999706, 1660),
	    std::make_pair(-0.999706, 1466),
	    std::make_pair(-0.999707, 1904),
	    std::make_pair(-0.999707, 742),
	    std::make_pair(-0.999707, 667),
	    std::make_pair(-0.999707, 345),
	    std::make_pair(-0.999707, 581),
	    std::make_pair(-0.999707, 1518),
	    std::make_pair(-0.999707, 722),
	    std::make_pair(-0.999707, 1224),
	    std::make_pair(-0.999708, 117),
	    std::make_pair(-0.999708, 226),
	    std::make_pair(-0.999708, 77),
	    std::make_pair(-0.999709, 1304),
	    std::make_pair(-0.999711, 1872),
	    std::make_pair(-0.999711, 1182),
	    std::make_pair(-0.999712, 2029),
	    std::make_pair(-0.999712, 30),
	    std::make_pair(-0.999712, 767),
	    std::make_pair(-0.999712, 1850),
	    std::make_pair(-0.999712, 1497),
	    std::make_pair(-0.999713, 14),
	    std::make_pair(-0.999713, 1514),
	    std::make_pair(-0.999713, 1552),
	    std::make_pair(-0.999713, 910),
	    std::make_pair(-0.999713, 818),
	    std::make_pair(-0.999713, 1880),
	    std::make_pair(-0.999714, 622),
	    std::make_pair(-0.999714, 1449),
	    std::make_pair(-0.999714, 702),
	    std::make_pair(-0.999714, 1968),
	    std::make_pair(-0.999715, 982),
	    std::make_pair(-0.999715, 55),
	    std::make_pair(-0.999715, 869),
	    std::make_pair(-0.999715, 708),
	    std::make_pair(-0.999715, 1513),
	    std::make_pair(-0.999715, 665),
	    std::make_pair(-0.999716, 421),
	    std::make_pair(-0.999716, 1238),
	    std::make_pair(-0.999716, 1752),
	    std::make_pair(-0.999717, 1385),
	    std::make_pair(-0.999717, 991),
	    std::make_pair(-0.999717, 2032),
	    std::make_pair(-0.999717, 1488),
	    std::make_pair(-0.999717, 1965),
	    std::make_pair(-0.999718, 342),
	    std::make_pair(-0.999718, 474),
	    std::make_pair(-0.999718, 109),
	    std::make_pair(-0.999718, 643),
	    std::make_pair(-0.999718, 1691),
	    std::make_pair(-0.999718, 1024),
	    std::make_pair(-0.999719, 1387),
	    std::make_pair(-0.999719, 1809),
	    std::make_pair(-0.999719, 124),
	    std::make_pair(-0.99972, 1696),
	    std::make_pair(-0.99972, 925),
	    std::make_pair(-0.99972, 726),
	    std::make_pair(-0.99972, 2033),
	    std::make_pair(-0.99972, 676),
	    std::make_pair(-0.999721, 1267),
	    std::make_pair(-0.999721, 1864),
	    std::make_pair(-0.999722, 297),
	    std::make_pair(-0.999722, 1026),
	    std::make_pair(-0.999722, 1958),
	    std::make_pair(-0.999723, 1838),
	    std::make_pair(-0.999723, 139),
	    std::make_pair(-0.999723, 1916),
	    std::make_pair(-0.999723, 1787),
	    std::make_pair(-0.999723, 694),
	    std::make_pair(-0.999724, 1766),
	    std::make_pair(-0.999724, 1342),
	    std::make_pair(-0.999724, 655),
	    std::make_pair(-0.999724, 1545),
	    std::make_pair(-0.999724, 703),
	    std::make_pair(-0.999724, 49),
	    std::make_pair(-0.999724, 1221),
	    std::make_pair(-0.999724, 1404),
	    std::make_pair(-0.999724, 1844),
	    std::make_pair(-0.999725, 276),
	    std::make_pair(-0.999725, 481),
	    std::make_pair(-0.999725, 1842),
	    std::make_pair(-0.999725, 303),
	    std::make_pair(-0.999725, 955),
	    std::make_pair(-0.999725, 1008),
	    std::make_pair(-0.999725, 1614),
	    std::make_pair(-0.999725, 1462),
	    std::make_pair(-0.999725, 671),
	    std::make_pair(-0.999725, 1630),
	    std::make_pair(-0.999725, 194),
	    std::make_pair(-0.999726, 95),
	    std::make_pair(-0.999726, 1153),
	    std::make_pair(-0.999726, 278),
	    std::make_pair(-0.999726, 1168),
	    std::make_pair(-0.999727, 1781),
	    std::make_pair(-0.999727, 1733),
	    std::make_pair(-0.999727, 1216),
	    std::make_pair(-0.999727, 903),
	    std::make_pair(-0.999727, 1865),
	    std::make_pair(-0.999727, 1367),
	    std::make_pair(-0.999728, 1219),
	    std::make_pair(-0.999728, 1595),
	    std::make_pair(-0.999728, 1896),
	    std::make_pair(-0.999728, 1519),
	    std::make_pair(-0.999728, 1477),
	    std::make_pair(-0.999728, 1858),
	    std::make_pair(-0.999729, 1368),
	    std::make_pair(-0.999729, 283),
	    std::make_pair(-0.999729, 222),
	    std::make_pair(-0.999729, 1374),
	    std::make_pair(-0.999729, 1506),
	    std::make_pair(-0.999729, 660),
	    std::make_pair(-0.99973, 1670),
	    std::make_pair(-0.99973, 392),
	    std::make_pair(-0.99973, 97),
	    std::make_pair(-0.99973, 816),
	    std::make_pair(-0.999731, 1932),
	    std::make_pair(-0.999731, 2040),
	    std::make_pair(-0.999731, 1441),
	    std::make_pair(-0.999731, 1875),
	    std::make_pair(-0.999731, 1988),
	    std::make_pair(-0.999731, 927),
	    std::make_pair(-0.999732, 1048),
	    std::make_pair(-0.999732, 16),
	    std::make_pair(-0.999732, 842),
	    std::make_pair(-0.999732, 141),
	    std::make_pair(-0.999732, 24),
	    std::make_pair(-0.999732, 698),
	    std::make_pair(-0.999733, 10),
	    std::make_pair(-0.999733, 1375),
	    std::make_pair(-0.999733, 1854),
	    std::make_pair(-0.999734, 948),
	    std::make_pair(-0.999734, 1389),
	    std::make_pair(-0.999734, 1725),
	    std::make_pair(-0.999734, 38),
	    std::make_pair(-0.999734, 810),
	    std::make_pair(-0.999734, 1819),
	    std::make_pair(-0.999734, 601),
	    std::make_pair(-0.999734, 1261),
	    std::make_pair(-0.999734, 390),
	    std::make_pair(-0.999735, 751),
	    std::make_pair(-0.999736, 1316),
	    std::make_pair(-0.999736, 1780),
	    std::make_pair(-0.999736, 1941),
	    std::make_pair(-0.999736, 524),
	    std::make_pair(-0.999736, 228),
	    std::make_pair(-0.999737, 1790),
	    std::make_pair(-0.999737, 619),
	    std::make_pair(-0.999737, 118),
	    std::make_pair(-0.999737, 403),
	    std::make_pair(-0.999737, 1945),
	    std::make_pair(-0.999737, 1963),
	    std::make_pair(-0.999738, 908),
	    std::make_pair(-0.999738, 1085),
	    std::make_pair(-0.999738, 338),
	    std::make_pair(-0.999738, 1195),
	    std::make_pair(-0.999738, 438),
	    std::make_pair(-0.999738, 799),
	    std::make_pair(-0.999738, 1452),
	    std::make_pair(-0.999739, 1116),
	    std::make_pair(-0.999739, 1007),
	    std::make_pair(-0.999739, 1510),
	    std::make_pair(-0.999739, 1281),
	    std::make_pair(-0.999739, 58),
	    std::make_pair(-0.999739, 1050),
	    std::make_pair(-0.999739, 271),
	    std::make_pair(-0.999739, 967),
	    std::make_pair(-0.999739, 1676),
	    std::make_pair(-0.999739, 2012),
	    std::make_pair(-0.99974, 91),
	    std::make_pair(-0.99974, 1323),
	    std::make_pair(-0.99974, 356),
	    std::make_pair(-0.99974, 251),
	    std::make_pair(-0.99974, 1275),
	    std::make_pair(-0.99974, 89),
	    std::make_pair(-0.999741, 1702),
	    std::make_pair(-0.999741, 1369),
	    std::make_pair(-0.999741, 75),
	    std::make_pair(-0.999741, 441),
	    std::make_pair(-0.999742, 1769),
	    std::make_pair(-0.999742, 407),
	    std::make_pair(-0.999742, 618),
	    std::make_pair(-0.999742, 2024),
	    std::make_pair(-0.999742, 1749),
	    std::make_pair(-0.999742, 774),
	    std::make_pair(-0.999742, 185),
	    std::make_pair(-0.999743, 79),
	    std::make_pair(-0.999743, 237),
	    std::make_pair(-0.999743, 502),
	    std::make_pair(-0.999743, 1902),
	    std::make_pair(-0.999743, 1908),
	    std::make_pair(-0.999743, 341),
	    std::make_pair(-0.999744, 1570),
	    std::make_pair(-0.999744, 339),
	    std::make_pair(-0.999744, 1251),
	    std::make_pair(-0.999744, 1797),
	    std::make_pair(-0.999745, 1644),
	    std::make_pair(-0.999745, 1038),
	    std::make_pair(-0.999746, 153),
	    std::make_pair(-0.999746, 1823),
	    std::make_pair(-0.999746, 1480),
	    std::make_pair(-0.999746, 1773),
	    std::make_pair(-0.999746, 1217),
	    std::make_pair(-0.999746, 1460),
	    std::make_pair(-0.999747, 142),
	    std::make_pair(-0.999747, 723),
	    std::make_pair(-0.999747, 662),
	    std::make_pair(-0.999748, 103),
	    std::make_pair(-0.999749, 541),
	    std::make_pair(-0.999749, 760),
	    std::make_pair(-0.999749, 1151),
	    std::make_pair(-0.999749, 1673),
	    std::make_pair(-0.99975, 67),
	    std::make_pair(-0.99975, 422),
	    std::make_pair(-0.999751, 984),
	    std::make_pair(-0.999751, 1589),
	    std::make_pair(-0.999751, 1060),
	    std::make_pair(-0.999751, 382),
	    std::make_pair(-0.999751, 1220),
	    std::make_pair(-0.999751, 1784),
	    std::make_pair(-0.999752, 1505),
	    std::make_pair(-0.999752, 7),
	    std::make_pair(-0.999752, 82),
	    std::make_pair(-0.999752, 843),
	    std::make_pair(-0.999752, 949),
	    std::make_pair(-0.999752, 909),
	    std::make_pair(-0.999752, 174),
	    std::make_pair(-0.999752, 1118),
	    std::make_pair(-0.999752, 1870),
	    std::make_pair(-0.999752, 929),
	    std::make_pair(-0.999753, 515),
	    std::make_pair(-0.999753, 1890),
	    std::make_pair(-0.999753, 1714),
	    std::make_pair(-0.999753, 130),
	    std::make_pair(-0.999753, 986),
	    std::make_pair(-0.999754, 158),
	    std::make_pair(-0.999754, 741),
	    std::make_pair(-0.999754, 2020),
	    std::make_pair(-0.999754, 445),
	    std::make_pair(-0.999755, 1015),
	    std::make_pair(-0.999755, 137),
	    std::make_pair(-0.999755, 477),
	    std::make_pair(-0.999755, 368),
	    std::make_pair(-0.999755, 1600),
	    std::make_pair(-0.999755, 1735),
	    std::make_pair(-0.999756, 1629),
	    std::make_pair(-0.999756, 199),
	    std::make_pair(-0.999756, 1935),
	    std::make_pair(-0.999756, 1396),
	    std::make_pair(-0.999756, 1311),
	    std::make_pair(-0.999756, 1109),
	    std::make_pair(-0.999756, 1509),
	    std::make_pair(-0.999756, 1980),
	    std::make_pair(-0.999757, 1982),
	    std::make_pair(-0.999757, 446),
	    std::make_pair(-0.999757, 1310),
	    std::make_pair(-0.999757, 2022),
	    std::make_pair(-0.999757, 1646),
	    std::make_pair(-0.999758, 1121),
	    std::make_pair(-0.999758, 647),
	    std::make_pair(-0.999758, 994),
	    std::make_pair(-0.999759, 1533),
	    std::make_pair(-0.999759, 1972),
	    std::make_pair(-0.99976, 1638),
	    std::make_pair(-0.99976, 1764),
	    std::make_pair(-0.99976, 1110),
	    std::make_pair(-0.99976, 1654),
	    std::make_pair(-0.99976, 1743),
	    std::make_pair(-0.999761, 36),
	    std::make_pair(-0.999761, 570),
	    std::make_pair(-0.999761, 106),
	    std::make_pair(-0.999761, 1581),
	    std::make_pair(-0.999761, 195),
	    std::make_pair(-0.999761, 333),
	    std::make_pair(-0.999761, 2018),
	    std::make_pair(-0.999761, 894),
	    std::make_pair(-0.999762, 1004),
	    std::make_pair(-0.999762, 1723),
	    std::make_pair(-0.999762, 184),
	    std::make_pair(-0.999762, 608),
	    std::make_pair(-0.999762, 73),
	    std::make_pair(-0.999762, 2002),
	    std::make_pair(-0.999763, 1249),
	    std::make_pair(-0.999763, 464),
	    std::make_pair(-0.999763, 179),
	    std::make_pair(-0.999763, 1033),
	    std::make_pair(-0.999764, 1576),
	    std::make_pair(-0.999764, 624),
	    std::make_pair(-0.999765, 1120),
	    std::make_pair(-0.999765, 1878),
	    std::make_pair(-0.999765, 1270),
	    std::make_pair(-0.999765, 958),
	    std::make_pair(-0.999766, 1554),
	    std::make_pair(-0.999766, 866),
	    std::make_pair(-0.999767, 1431),
	    std::make_pair(-0.999767, 900),
	    std::make_pair(-0.999767, 1894),
	    std::make_pair(-0.999767, 942),
	    std::make_pair(-0.999767, 225),
	    std::make_pair(-0.999768, 1990),
	    std::make_pair(-0.999769, 803),
	    std::make_pair(-0.999769, 473),
	    std::make_pair(-0.999769, 528),
	    std::make_pair(-0.999769, 1985),
	    std::make_pair(-0.999769, 1123),
	    std::make_pair(-0.999769, 1083),
	    std::make_pair(-0.999769, 413),
	    std::make_pair(-0.999769, 578),
	    std::make_pair(-0.99977, 597),
	    std::make_pair(-0.99977, 1504),
	    std::make_pair(-0.99977, 833),
	    std::make_pair(-0.99977, 255),
	    std::make_pair(-0.99977, 1995),
	    std::make_pair(-0.99977, 746),
	    std::make_pair(-0.99977, 1852),
	    std::make_pair(-0.99977, 1201),
	    std::make_pair(-0.999771, 1055),
	    std::make_pair(-0.999771, 483),
	    std::make_pair(-0.999771, 1252),
	    std::make_pair(-0.999772, 1381),
	    std::make_pair(-0.999772, 744),
	    std::make_pair(-0.999772, 1346),
	    std::make_pair(-0.999773, 1383),
	    std::make_pair(-0.999773, 1332),
	    std::make_pair(-0.999773, 1903),
	    std::make_pair(-0.999774, 2034),
	    std::make_pair(-0.999774, 337),
	    std::make_pair(-0.999774, 1494),
	    std::make_pair(-0.999775, 952),
	    std::make_pair(-0.999775, 304),
	    std::make_pair(-0.999775, 668),
	    std::make_pair(-0.999775, 992),
	    std::make_pair(-0.999775, 1131),
	    std::make_pair(-0.999776, 966),
	    std::make_pair(-0.999776, 52),
	    std::make_pair(-0.999776, 1566),
	    std::make_pair(-0.999776, 1534),
	    std::make_pair(-0.999777, 868),
	    std::make_pair(-0.999777, 536),
	    std::make_pair(-0.999777, 1331),
	    std::make_pair(-0.999778, 497),
	    std::make_pair(-0.999778, 431),
	    std::make_pair(-0.999778, 1183),
	    std::make_pair(-0.999778, 347),
	    std::make_pair(-0.999778, 1088),
	    std::make_pair(-0.999778, 1722),
	    std::make_pair(-0.999779, 315),
	    std::make_pair(-0.999779, 1999),
	    std::make_pair(-0.999779, 789),
	    std::make_pair(-0.999779, 522),
	    std::make_pair(-0.999779, 785),
	    std::make_pair(-0.999779, 1869),
	    std::make_pair(-0.99978, 108),
	    std::make_pair(-0.99978, 385),
	    std::make_pair(-0.99978, 998),
	    std::make_pair(-0.999781, 1671),
	    std::make_pair(-0.999781, 149),
	    std::make_pair(-0.999781, 734),
	    std::make_pair(-0.999781, 729),
	    std::make_pair(-0.999781, 435),
	    std::make_pair(-0.999781, 1274),
	    std::make_pair(-0.999782, 1529),
	    std::make_pair(-0.999782, 916),
	    std::make_pair(-0.999782, 148),
	    std::make_pair(-0.999783, 731),
	    std::make_pair(-0.999783, 1681),
	    std::make_pair(-0.999783, 1665),
	    std::make_pair(-0.999783, 710),
	    std::make_pair(-0.999783, 577),
	    std::make_pair(-0.999783, 889),
	    std::make_pair(-0.999783, 946),
	    std::make_pair(-0.999784, 849),
	    std::make_pair(-0.999784, 412),
	    std::make_pair(-0.999784, 755),
	    std::make_pair(-0.999785, 425),
	    std::make_pair(-0.999785, 2037),
	    std::make_pair(-0.999785, 1129),
	    std::make_pair(-0.999785, 1840),
	    std::make_pair(-0.999785, 1634),
	    std::make_pair(-0.999785, 1565),
	    std::make_pair(-0.999785, 778),
	    std::make_pair(-0.999786, 514),
	    std::make_pair(-0.999786, 1193),
	    std::make_pair(-0.999786, 375),
	    std::make_pair(-0.999786, 617),
	    std::make_pair(-0.999786, 912),
	    std::make_pair(-0.999786, 462),
	    std::make_pair(-0.999786, 1144),
	    std::make_pair(-0.999786, 1655),
	    std::make_pair(-0.999786, 1357),
	    std::make_pair(-0.999787, 1893),
	    std::make_pair(-0.999787, 1416),
	    std::make_pair(-0.999787, 1934),
	    std::make_pair(-0.999787, 1755),
	    std::make_pair(-0.999788, 1282),
	    std::make_pair(-0.999788, 1039),
	    std::make_pair(-0.999788, 59),
	    std::make_pair(-0.999788, 630),
	    std::make_pair(-0.999788, 1939),
	    std::make_pair(-0.999788, 1546),
	    std::make_pair(-0.999788, 1756),
	    std::make_pair(-0.999788, 1227),
	    std::make_pair(-0.999788, 1127),
	    std::make_pair(-0.999789, 2042),
	    std::make_pair(-0.999789, 686),
	    std::make_pair(-0.999789, 1157),
	    std::make_pair(-0.999789, 1851),
	    std::make_pair(-0.99979, 678),
	    std::make_pair(-0.99979, 6),
	    std::make_pair(-0.99979, 501),
	    std::make_pair(-0.999791, 1800),
	    std::make_pair(-0.999791, 523),
	    std::make_pair(-0.999791, 1891),
	    std::make_pair(-0.999792, 1597),
	    std::make_pair(-0.999792, 545),
	    std::make_pair(-0.999792, 1888),
	    std::make_pair(-0.999792, 1876),
	    std::make_pair(-0.999792, 724),
	    std::make_pair(-0.999792, 745),
	    std::make_pair(-0.999792, 1317),
	    std::make_pair(-0.999792, 814),
	    std::make_pair(-0.999792, 879),
	    std::make_pair(-0.999793, 96),
	    std::make_pair(-0.999793, 1086),
	    std::make_pair(-0.999794, 796),
	    std::make_pair(-0.999794, 1839),
	    std::make_pair(-0.999794, 761),
	    std::make_pair(-0.999794, 197),
	    std::make_pair(-0.999794, 1222),
	    std::make_pair(-0.999795, 1471),
	    std::make_pair(-0.999795, 858),
	    std::make_pair(-0.999795, 157),
	    std::make_pair(-0.999796, 1747),
	    std::make_pair(-0.999796, 448),
	    std::make_pair(-0.999796, 639),
	    std::make_pair(-0.999796, 1976),
	    std::make_pair(-0.999796, 1744),
	    std::make_pair(-0.999796, 653),
	    std::make_pair(-0.999797, 1795),
	    std::make_pair(-0.999797, 320),
	    std::make_pair(-0.999797, 1868),
	    std::make_pair(-0.999797, 468),
	    std::make_pair(-0.999797, 213),
	    std::make_pair(-0.999797, 1142),
	    std::make_pair(-0.999798, 1555),
	    std::make_pair(-0.999798, 683),
	    std::make_pair(-0.999798, 2046),
	    std::make_pair(-0.999798, 956),
	    std::make_pair(-0.999798, 1913),
	    std::make_pair(-0.999799, 656),
	    std::make_pair(-0.999799, 719),
	    std::make_pair(-0.999799, 1500),
	    std::make_pair(-0.9998, 1690),
	    std::make_pair(-0.9998, 2039),
	    std::make_pair(-0.999801, 1688),
	    std::make_pair(-0.999801, 90),
	    std::make_pair(-0.999801, 357),
	    std::make_pair(-0.999801, 1268),
	    std::make_pair(-0.999801, 1860),
	    std::make_pair(-0.999801, 567),
	    std::make_pair(-0.999801, 427),
	    std::make_pair(-0.999802, 550),
	    std::make_pair(-0.999802, 1572),
	    std::make_pair(-0.999802, 1909),
	    std::make_pair(-0.999802, 291),
	    std::make_pair(-0.999802, 1045),
	    std::make_pair(-0.999802, 1693),
	    std::make_pair(-0.999802, 8),
	    std::make_pair(-0.999803, 496),
	    std::make_pair(-0.999803, 1348),
	    std::make_pair(-0.999803, 1704),
	    std::make_pair(-0.999803, 486),
	    std::make_pair(-0.999804, 679),
	    std::make_pair(-0.999804, 134),
	    std::make_pair(-0.999804, 326),
	    std::make_pair(-0.999804, 1191),
	    std::make_pair(-0.999804, 1759),
	    std::make_pair(-0.999804, 1328),
	    std::make_pair(-0.999805, 61),
	    std::make_pair(-0.999805, 959),
	    std::make_pair(-0.999805, 110),
	    std::make_pair(-0.999806, 1606),
	    std::make_pair(-0.999806, 1700),
	    std::make_pair(-0.999806, 555),
	    std::make_pair(-0.999807, 1907),
	    std::make_pair(-0.999807, 1953),
	    std::make_pair(-0.999807, 270),
	    std::make_pair(-0.999808, 1672),
	    std::make_pair(-0.999808, 697),
	    std::make_pair(-0.999808, 811),
	    std::make_pair(-0.999809, 765),
	    std::make_pair(-0.999809, 329),
	    std::make_pair(-0.99981, 1542),
	    std::make_pair(-0.99981, 1027),
	    std::make_pair(-0.99981, 911),
	    std::make_pair(-0.99981, 263),
	    std::make_pair(-0.99981, 669),
	    std::make_pair(-0.99981, 631),
	    std::make_pair(-0.99981, 1598),
	    std::make_pair(-0.99981, 1421),
	    std::make_pair(-0.99981, 1476),
	    std::make_pair(-0.999811, 1411),
	    std::make_pair(-0.999811, 1176),
	    std::make_pair(-0.999811, 81),
	    std::make_pair(-0.999811, 836),
	    std::make_pair(-0.999812, 1956),
	    std::make_pair(-0.999812, 447),
	    std::make_pair(-0.999812, 411),
	    std::make_pair(-0.999812, 1912),
	    std::make_pair(-0.999812, 1424),
	    std::make_pair(-0.999812, 1716),
	    std::make_pair(-0.999812, 1126),
	    std::make_pair(-0.999813, 85),
	    std::make_pair(-0.999813, 882),
	    std::make_pair(-0.999813, 150),
	    std::make_pair(-0.999813, 1336),
	    std::make_pair(-0.999813, 1010),
	    std::make_pair(-0.999813, 1325),
	    std::make_pair(-0.999813, 1407),
	    std::make_pair(-0.999813, 838),
	    std::make_pair(-0.999813, 1473),
	    std::make_pair(-0.999813, 1612),
	    std::make_pair(-0.999813, 1508),
	    std::make_pair(-0.999814, 584),
	    std::make_pair(-0.999814, 1382),
	    std::make_pair(-0.999814, 773),
	    std::make_pair(-0.999814, 1295),
	    std::make_pair(-0.999814, 885),
	    std::make_pair(-0.999814, 794),
	    std::make_pair(-0.999814, 1951),
	    std::make_pair(-0.999814, 1341),
	    std::make_pair(-0.999814, 576),
	    std::make_pair(-0.999814, 1775),
	    std::make_pair(-0.999815, 1777),
	    std::make_pair(-0.999815, 1203),
	    std::make_pair(-0.999815, 1370),
	    std::make_pair(-0.999815, 99),
	    std::make_pair(-0.999816, 950),
	    std::make_pair(-0.999816, 114),
	    std::make_pair(-0.999816, 1710),
	    std::make_pair(-0.999816, 556),
	    std::make_pair(-0.999816, 635),
	    std::make_pair(-0.999816, 1111),
	    std::make_pair(-0.999816, 1081),
	    std::make_pair(-0.999816, 1969),
	    std::make_pair(-0.999817, 1926),
	    std::make_pair(-0.999817, 15),
	    std::make_pair(-0.999817, 642),
	    std::make_pair(-0.999817, 500),
	    std::make_pair(-0.999818, 161),
	    std::make_pair(-0.999818, 107),
	    std::make_pair(-0.999818, 18),
	    std::make_pair(-0.999818, 1175),
	    std::make_pair(-0.999819, 593),
	    std::make_pair(-0.999819, 1133),
	    std::make_pair(-0.999819, 495),
	    std::make_pair(-0.999819, 963),
	    std::make_pair(-0.999819, 1910),
	    std::make_pair(-0.999819, 215),
	    std::make_pair(-0.999819, 2006),
	    std::make_pair(-0.999819, 56),
	    std::make_pair(-0.99982, 972),
	    std::make_pair(-0.99982, 2),
	    std::make_pair(-0.99982, 1322),
	    std::make_pair(-0.99982, 354),
	    std::make_pair(-0.99982, 644),
	    std::make_pair(-0.99982, 681),
	    std::make_pair(-0.99982, 1882),
	    std::make_pair(-0.99982, 1361),
	    std::make_pair(-0.99982, 599),
	    std::make_pair(-0.99982, 1947),
	    std::make_pair(-0.99982, 1847),
	    std::make_pair(-0.999821, 1350),
	    std::make_pair(-0.999822, 264),
	    std::make_pair(-0.999822, 1770),
	    std::make_pair(-0.999822, 779),
	    std::make_pair(-0.999822, 1453),
	    std::make_pair(-0.999822, 1863),
	    std::make_pair(-0.999823, 1318),
	    std::make_pair(-0.999823, 1683),
	    std::make_pair(-0.999823, 156),
	    std::make_pair(-0.999824, 1360),
	    std::make_pair(-0.999824, 652),
	    std::make_pair(-0.999824, 159),
	    std::make_pair(-0.999824, 736),
	    std::make_pair(-0.999825, 997),
	    std::make_pair(-0.999825, 1522),
	    std::make_pair(-0.999825, 1475),
	    std::make_pair(-0.999825, 136),
	    std::make_pair(-0.999825, 1779),
	    std::make_pair(-0.999826, 1082),
	    std::make_pair(-0.999826, 1765),
	    std::make_pair(-0.999827, 990),
	    std::make_pair(-0.999827, 1046),
	    std::make_pair(-0.999827, 178),
	    std::make_pair(-0.999827, 1649),
	    std::make_pair(-0.999828, 324),
	    std::make_pair(-0.999828, 328),
	    std::make_pair(-0.999828, 317),
	    std::make_pair(-0.999828, 1084),
	    std::make_pair(-0.999829, 1942),
	    std::make_pair(-0.999829, 1448),
	    std::make_pair(-0.999829, 13),
	    std::make_pair(-0.999829, 1293),
	    std::make_pair(-0.999829, 549),
	    std::make_pair(-0.999829, 1155),
	    std::make_pair(-0.999829, 1104),
	    std::make_pair(-0.99983, 628),
	    std::make_pair(-0.99983, 747),
	    std::make_pair(-0.99983, 1386),
	    std::make_pair(-0.99983, 970),
	    std::make_pair(-0.99983, 575),
	    std::make_pair(-0.999831, 721),
	    std::make_pair(-0.999831, 1827),
	    std::make_pair(-0.999831, 877),
	    std::make_pair(-0.999831, 1255),
	    std::make_pair(-0.999831, 615),
	    std::make_pair(-0.999831, 733),
	    std::make_pair(-0.999832, 1014),
	    std::make_pair(-0.999832, 582),
	    std::make_pair(-0.999832, 405),
	    std::make_pair(-0.999832, 212),
	    std::make_pair(-0.999833, 1425),
	    std::make_pair(-0.999833, 191),
	    std::make_pair(-0.999833, 1162),
	    std::make_pair(-0.999833, 1846),
	    std::make_pair(-0.999833, 1283),
	    std::make_pair(-0.999833, 1811),
	    std::make_pair(-0.999833, 1763),
	    std::make_pair(-0.999834, 1239),
	    std::make_pair(-0.999834, 964),
	    std::make_pair(-0.999834, 1215),
	    std::make_pair(-0.999834, 1392),
	    std::make_pair(-0.999834, 70),
	    std::make_pair(-0.999834, 538),
	    std::make_pair(-0.999835, 490),
	    std::make_pair(-0.999835, 1105),
	    std::make_pair(-0.999835, 1889),
	    std::make_pair(-0.999835, 588),
	    std::make_pair(-0.999836, 1650),
	    std::make_pair(-0.999836, 659),
	    std::make_pair(-0.999836, 482),
	    std::make_pair(-0.999836, 1150),
	    std::make_pair(-0.999836, 1837),
	    std::make_pair(-0.999836, 42),
	    std::make_pair(-0.999836, 231),
	    std::make_pair(-0.999837, 28),
	    std::make_pair(-0.999837, 1213),
	    std::make_pair(-0.999837, 1813),
	    std::make_pair(-0.999837, 1166),
	    std::make_pair(-0.999838, 1493),
	    std::make_pair(-0.999838, 1277),
	    std::make_pair(-0.999838, 999),
	    std::make_pair(-0.999838, 280),
	    std::make_pair(-0.999839, 757),
	    std::make_pair(-0.999839, 65),
	    std::make_pair(-0.999839, 1922),
	    std::make_pair(-0.99984, 476),
	    std::make_pair(-0.99984, 1996),
	    std::make_pair(-0.99984, 899),
	    std::make_pair(-0.99984, 1820),
	    std::make_pair(-0.99984, 1961),
	    std::make_pair(-0.99984, 189),
	    std::make_pair(-0.999841, 543),
	    std::make_pair(-0.999841, 786),
	    std::make_pair(-0.999841, 1699),
	    std::make_pair(-0.999841, 1478),
	    std::make_pair(-0.999841, 352),
	    std::make_pair(-0.999841, 776),
	    std::make_pair(-0.999842, 1666),
	    std::make_pair(-0.999842, 1856),
	    std::make_pair(-0.999842, 39),
	    std::make_pair(-0.999842, 1163),
	    std::make_pair(-0.999842, 1627),
	    std::make_pair(-0.999843, 1678),
	    std::make_pair(-0.999843, 1092),
	    std::make_pair(-0.999844, 787),
	    std::make_pair(-0.999844, 1567),
	    std::make_pair(-0.999844, 1070),
	    std::make_pair(-0.999844, 1114),
	    std::make_pair(-0.999844, 1434),
	    std::make_pair(-0.999844, 1258),
	    std::make_pair(-0.999845, 1492),
	    std::make_pair(-0.999845, 978),
	    std::make_pair(-0.999845, 623),
	    std::make_pair(-0.999846, 1641),
	    std::make_pair(-0.999846, 311),
	    std::make_pair(-0.999846, 2030),
	    std::make_pair(-0.999846, 1973),
	    std::make_pair(-0.999846, 890),
	    std::make_pair(-0.999846, 1897),
	    std::make_pair(-0.999847, 290),
	    std::make_pair(-0.999847, 853),
	    std::make_pair(-0.999847, 841),
	    std::make_pair(-0.999847, 1324),
	    std::make_pair(-0.999847, 862),
	    std::make_pair(-0.999847, 940),
	    std::make_pair(-0.999847, 1849),
	    std::make_pair(-0.999847, 1715),
	    std::make_pair(-0.999847, 1394),
	    std::make_pair(-0.999848, 266),
	    std::make_pair(-0.999848, 1244),
	    std::make_pair(-0.999848, 1418),
	    std::make_pair(-0.999848, 1240),
	    std::make_pair(-0.999848, 1178),
	    std::make_pair(-0.999848, 1642),
	    std::make_pair(-0.999848, 613),
	    std::make_pair(-0.999849, 1312),
	    std::make_pair(-0.999849, 2005),
	    std::make_pair(-0.999849, 1192),
	    std::make_pair(-0.99985, 808),
	    std::make_pair(-0.99985, 1002),
	    std::make_pair(-0.99985, 31),
	    std::make_pair(-0.99985, 208),
	    std::make_pair(-0.999851, 1280),
	    std::make_pair(-0.999851, 1579),
	    std::make_pair(-0.999851, 527),
	    std::make_pair(-0.999851, 1243),
	    std::make_pair(-0.999851, 886),
	    std::make_pair(-0.999852, 273),
	    std::make_pair(-0.999852, 1329),
	    std::make_pair(-0.999852, 1933),
	    std::make_pair(-0.999853, 1044),
	    std::make_pair(-0.999853, 1377),
	    std::make_pair(-0.999853, 1966),
	    std::make_pair(-0.999853, 163),
	    std::make_pair(-0.999854, 489),
	    std::make_pair(-0.999854, 1006),
	    std::make_pair(-0.999854, 1379),
	    std::make_pair(-0.999854, 272),
	    std::make_pair(-0.999854, 1826),
	    std::make_pair(-0.999854, 331),
	    std::make_pair(-0.999854, 209),
	    std::make_pair(-0.999854, 240),
	    std::make_pair(-0.999854, 289),
	    std::make_pair(-0.999854, 309),
	    std::make_pair(-0.999855, 1161),
	    std::make_pair(-0.999855, 1662),
	    std::make_pair(-0.999855, 254),
	    std::make_pair(-0.999855, 526),
	    std::make_pair(-0.999855, 57),
	    std::make_pair(-0.999855, 673),
	    std::make_pair(-0.999855, 891),
	    std::make_pair(-0.999855, 817),
	    std::make_pair(-0.999856, 586),
	    std::make_pair(-0.999856, 408),
	    std::make_pair(-0.999856, 335),
	    std::make_pair(-0.999856, 511),
	    std::make_pair(-0.999856, 1740),
	    std::make_pair(-0.999856, 1363),
	    std::make_pair(-0.999857, 127),
	    std::make_pair(-0.999857, 1420),
	    std::make_pair(-0.999857, 758),
	    std::make_pair(-0.999857, 1816),
	    std::make_pair(-0.999858, 875),
	    std::make_pair(-0.999858, 188),
	    std::make_pair(-0.999858, 224),
	    std::make_pair(-0.999858, 1906),
	    std::make_pair(-0.999858, 1871),
	    std::make_pair(-0.999859, 1548),
	    std::make_pair(-0.999859, 1682),
	    std::make_pair(-0.999859, 34),
	    std::make_pair(-0.999859, 1236),
	    std::make_pair(-0.999859, 346),
	    std::make_pair(-0.999859, 2007),
	    std::make_pair(-0.99986, 881),
	    std::make_pair(-0.99986, 1622),
	    std::make_pair(-0.99986, 871),
	    std::make_pair(-0.99986, 50),
	    std::make_pair(-0.999861, 1189),
	    std::make_pair(-0.999861, 598),
	    std::make_pair(-0.999861, 516),
	    std::make_pair(-0.999862, 88),
	    std::make_pair(-0.999863, 1408),
	    std::make_pair(-0.999863, 1925),
	    std::make_pair(-0.999863, 1582),
	    std::make_pair(-0.999864, 269),
	    std::make_pair(-0.999864, 716),
	    std::make_pair(-0.999864, 772),
	    std::make_pair(-0.999864, 1444),
	    std::make_pair(-0.999864, 0),
	    std::make_pair(-0.999865, 1472),
	    std::make_pair(-0.999865, 641),
	    std::make_pair(-0.999865, 1269),
	    std::make_pair(-0.999865, 691),
	    std::make_pair(-0.999866, 1886),
	    std::make_pair(-0.999866, 732),
	    std::make_pair(-0.999866, 2031),
	    std::make_pair(-0.999866, 1186),
	    std::make_pair(-0.999867, 892),
	    std::make_pair(-0.999867, 66),
	    std::make_pair(-0.999867, 353),
	    std::make_pair(-0.999867, 517),
	    std::make_pair(-0.999867, 1232),
	    std::make_pair(-0.999867, 349),
	    std::make_pair(-0.999868, 1233),
	    std::make_pair(-0.999868, 1974),
	    std::make_pair(-0.999868, 1883),
	    std::make_pair(-0.999868, 1354),
	    std::make_pair(-0.999868, 1658),
	    std::make_pair(-0.999869, 1792),
	    std::make_pair(-0.999869, 1498),
	    std::make_pair(-0.999869, 923),
	    std::make_pair(-0.999869, 62),
	    std::make_pair(-0.999869, 973),
	    std::make_pair(-0.99987, 1335),
	    std::make_pair(-0.99987, 1139),
	    std::make_pair(-0.99987, 1610),
	    std::make_pair(-0.99987, 2000),
	    std::make_pair(-0.99987, 750),
	    std::make_pair(-0.999871, 1825),
	    std::make_pair(-0.999871, 859),
	    std::make_pair(-0.999871, 1808),
	    std::make_pair(-0.999871, 258),
	    std::make_pair(-0.999872, 1077),
	    std::make_pair(-0.999872, 1547),
	    std::make_pair(-0.999872, 319),
	    std::make_pair(-0.999872, 1302),
	    std::make_pair(-0.999873, 980),
	    std::make_pair(-0.999873, 1021),
	    std::make_pair(-0.999873, 1881),
	    std::make_pair(-0.999873, 645),
	    std::make_pair(-0.999873, 439),
	    std::make_pair(-0.999874, 1159),
	    std::make_pair(-0.999874, 316),
	    std::make_pair(-0.999874, 1857),
	    std::make_pair(-0.999874, 1543),
	    std::make_pair(-0.999874, 1430),
	    std::make_pair(-0.999874, 1689),
	    std::make_pair(-0.999874, 819),
	    std::make_pair(-0.999874, 426),
	    std::make_pair(-0.999875, 122),
	    std::make_pair(-0.999875, 1029),
	    std::make_pair(-0.999875, 1955),
	    std::make_pair(-0.999875, 804),
	    std::make_pair(-0.999876, 1248),
	    std::make_pair(-0.999876, 551),
	    std::make_pair(-0.999877, 873),
	    std::make_pair(-0.999877, 239),
	    std::make_pair(-0.999878, 857),
	    std::make_pair(-0.999878, 1583),
	    std::make_pair(-0.999878, 609),
	    std::make_pair(-0.999878, 1339),
	    std::make_pair(-0.999878, 40),
	    std::make_pair(-0.999879, 129),
	    std::make_pair(-0.999879, 1069),
	    std::make_pair(-0.999879, 1030),
	    std::make_pair(-0.999879, 1212),
	    std::make_pair(-0.999879, 831),
	    std::make_pair(-0.999879, 1260),
	    std::make_pair(-0.999879, 469),
	    std::make_pair(-0.999879, 1469),
	    std::make_pair(-0.99988, 1164),
	    std::make_pair(-0.99988, 1237),
	    std::make_pair(-0.99988, 692),
	    std::make_pair(-0.99988, 1314),
	    std::make_pair(-0.99988, 883),
	    std::make_pair(-0.99988, 1569),
	    std::make_pair(-0.99988, 878),
	    std::make_pair(-0.999881, 9),
	    std::make_pair(-0.999881, 22),
	    std::make_pair(-0.999881, 592),
	    std::make_pair(-0.999882, 823),
	    std::make_pair(-0.999882, 793),
	    std::make_pair(-0.999882, 1455),
	    std::make_pair(-0.999882, 1640),
	    std::make_pair(-0.999882, 569),
	    std::make_pair(-0.999882, 401),
	    std::make_pair(-0.999883, 1637),
	    std::make_pair(-0.999883, 530),
	    std::make_pair(-0.999883, 1927),
	    std::make_pair(-0.999883, 363),
	    std::make_pair(-0.999883, 1483),
	    std::make_pair(-0.999883, 355),
	    std::make_pair(-0.999884, 1892),
	    std::make_pair(-0.999884, 293),
	    std::make_pair(-0.999885, 295),
	    std::make_pair(-0.999885, 380),
	    std::make_pair(-0.999885, 1003),
	    std::make_pair(-0.999885, 852),
	    std::make_pair(-0.999885, 1938),
	    std::make_pair(-0.999886, 313),
	    std::make_pair(-0.999886, 1211),
	    std::make_pair(-0.999886, 359),
	    std::make_pair(-0.999886, 1538),
	    std::make_pair(-0.999886, 286),
	    std::make_pair(-0.999887, 204),
	    std::make_pair(-0.999887, 395),
	    std::make_pair(-0.999887, 1527),
	    std::make_pair(-0.999888, 981),
	    std::make_pair(-0.999888, 74),
	    std::make_pair(-0.999888, 924),
	    std::make_pair(-0.999888, 1687),
	    std::make_pair(-0.999888, 1117),
	    std::make_pair(-0.999888, 825),
	    std::make_pair(-0.999888, 2017),
	    std::make_pair(-0.999889, 325),
	    std::make_pair(-0.999889, 94),
	    std::make_pair(-0.999889, 1278),
	    std::make_pair(-0.999889, 583),
	    std::make_pair(-0.999889, 580),
	    std::make_pair(-0.999889, 1136),
	    std::make_pair(-0.999889, 558),
	    std::make_pair(-0.99989, 839),
	    std::make_pair(-0.99989, 573),
	    std::make_pair(-0.99989, 53),
	    std::make_pair(-0.99989, 1073),
	    std::make_pair(-0.99989, 1207),
	    std::make_pair(-0.999891, 1559),
	    std::make_pair(-0.999891, 874),
	    std::make_pair(-0.999891, 604),
	    std::make_pair(-0.999891, 1653),
	    std::make_pair(-0.999891, 1805),
	    std::make_pair(-0.999891, 1443),
	    std::make_pair(-0.999892, 798),
	    std::make_pair(-0.999892, 1516),
	    std::make_pair(-0.999892, 764),
	    std::make_pair(-0.999892, 1245),
	    std::make_pair(-0.999892, 267),
	    std::make_pair(-0.999892, 457),
	    std::make_pair(-0.999893, 105),
	    std::make_pair(-0.999893, 627),
	    std::make_pair(-0.999893, 717),
	    std::make_pair(-0.999893, 247),
	    std::make_pair(-0.999894, 506),
	    std::make_pair(-0.999894, 1223),
	    std::make_pair(-0.999894, 296),
	    std::make_pair(-0.999894, 84),
	    std::make_pair(-0.999894, 1068),
	    std::make_pair(-0.999894, 257),
	    std::make_pair(-0.999894, 775),
	    std::make_pair(-0.999895, 988),
	    std::make_pair(-0.999895, 458),
	    std::make_pair(-0.999895, 145),
	    std::make_pair(-0.999895, 821),
	    std::make_pair(-0.999895, 1214),
	    std::make_pair(-0.999896, 371),
	    std::make_pair(-0.999896, 160),
	    std::make_pair(-0.999896, 467),
	    std::make_pair(-0.999896, 466),
	    std::make_pair(-0.999897, 310),
	    std::make_pair(-0.999897, 1992),
	    std::make_pair(-0.999897, 1078),
	    std::make_pair(-0.999897, 2016),
	    std::make_pair(-0.999897, 1229),
	    std::make_pair(-0.999897, 636),
	    std::make_pair(-0.999897, 454),
	    std::make_pair(-0.999898, 287),
	    std::make_pair(-0.999898, 638),
	    std::make_pair(-0.999899, 1832),
	    std::make_pair(-0.999899, 1172),
	    std::make_pair(-0.999899, 1746),
	    std::make_pair(-0.999899, 181),
	    std::make_pair(-0.9999, 2025),
	    std::make_pair(-0.9999, 1669),
	    std::make_pair(-0.9999, 1285),
	    std::make_pair(-0.9999, 1845),
	    std::make_pair(-0.9999, 1101),
	    std::make_pair(-0.9999, 207),
	    std::make_pair(-0.9999, 2026),
	    std::make_pair(-0.999901, 1263),
	    std::make_pair(-0.999901, 279),
	    std::make_pair(-0.999901, 1940),
	    std::make_pair(-0.999901, 1330),
	    std::make_pair(-0.999901, 318),
	    std::make_pair(-0.999901, 365),
	    std::make_pair(-0.999901, 1080),
	    std::make_pair(-0.999902, 1373),
	    std::make_pair(-0.999902, 1771),
	    std::make_pair(-0.999902, 306),
	    std::make_pair(-0.999902, 983),
	    std::make_pair(-0.999902, 100),
	    std::make_pair(-0.999902, 606),
	    std::make_pair(-0.999902, 1403),
	    std::make_pair(-0.999903, 1209),
	    std::make_pair(-0.999903, 433),
	    std::make_pair(-0.999903, 1132),
	    std::make_pair(-0.999903, 1447),
	    std::make_pair(-0.999903, 1064),
	    std::make_pair(-0.999903, 1531),
	    std::make_pair(-0.999904, 1485),
	    std::make_pair(-0.999904, 1409),
	    std::make_pair(-0.999904, 20),
	    std::make_pair(-0.999904, 565),
	    std::make_pair(-0.999905, 1257),
	    std::make_pair(-0.999905, 714),
	    std::make_pair(-0.999905, 92),
	    std::make_pair(-0.999905, 505),
	    std::make_pair(-0.999905, 1899),
	    std::make_pair(-0.999905, 1835),
	    std::make_pair(-0.999905, 610),
	    std::make_pair(-0.999906, 265),
	    std::make_pair(-0.999906, 1031),
	    std::make_pair(-0.999906, 176),
	    std::make_pair(-0.999906, 1515),
	    std::make_pair(-0.999906, 1866),
	    std::make_pair(-0.999906, 1692),
	    std::make_pair(-0.999906, 1231),
	    std::make_pair(-0.999907, 1799),
	    std::make_pair(-0.999907, 621),
	    std::make_pair(-0.999907, 600),
	    std::make_pair(-0.999907, 11),
	    std::make_pair(-0.999907, 351),
	    std::make_pair(-0.999907, 824),
	    std::make_pair(-0.999908, 1511),
	    std::make_pair(-0.999908, 1667),
	    std::make_pair(-0.999908, 995),
	    std::make_pair(-0.999908, 327),
	    std::make_pair(-0.999908, 1616),
	    std::make_pair(-0.999908, 1859),
	    std::make_pair(-0.999909, 1620),
	    std::make_pair(-0.999909, 674),
	    std::make_pair(-0.999909, 1099),
	    std::make_pair(-0.999909, 792),
	    std::make_pair(-0.99991, 45),
	    std::make_pair(-0.99991, 2044),
	    std::make_pair(-0.99991, 1450),
	    std::make_pair(-0.99991, 1062),
	    std::make_pair(-0.99991, 807),
	    std::make_pair(-0.99991, 1593),
	    std::make_pair(-0.99991, 1680),
	    std::make_pair(-0.99991, 87),
	    std::make_pair(-0.999911, 1242),
	    std::make_pair(-0.999911, 485),
	    std::make_pair(-0.999911, 1294),
	    std::make_pair(-0.999911, 1708),
	    std::make_pair(-0.999911, 308),
	    std::make_pair(-0.999912, 1298),
	    std::make_pair(-0.999912, 832),
	    std::make_pair(-0.999912, 1524),
	    std::make_pair(-0.999912, 112),
	    std::make_pair(-0.999913, 844),
	    std::make_pair(-0.999913, 1657),
	    std::make_pair(-0.999913, 1156),
	    std::make_pair(-0.999913, 234),
	    std::make_pair(-0.999914, 1043),
	    std::make_pair(-0.999914, 1675),
	    std::make_pair(-0.999914, 198),
	    std::make_pair(-0.999914, 1587),
	    std::make_pair(-0.999914, 1501),
	    std::make_pair(-0.999915, 1271),
	    std::make_pair(-0.999915, 1022),
	    std::make_pair(-0.999915, 367),
	    std::make_pair(-0.999915, 1391),
	    std::make_pair(-0.999915, 1402),
	    std::make_pair(-0.999915, 1056),
	    std::make_pair(-0.999915, 1315),
	    std::make_pair(-0.999915, 888),
	    std::make_pair(-0.999915, 246),
	    std::make_pair(-0.999915, 1134),
	    std::make_pair(-0.999916, 437),
	    std::make_pair(-0.999916, 1371),
	    std::make_pair(-0.999916, 1440),
	    std::make_pair(-0.999916, 730),
	    std::make_pair(-0.999916, 1018),
	    std::make_pair(-0.999916, 41),
	    std::make_pair(-0.999917, 1333),
	    std::make_pair(-0.999917, 1728),
	    std::make_pair(-0.999917, 393),
	    std::make_pair(-0.999917, 1250),
	    std::make_pair(-0.999917, 1457),
	    std::make_pair(-0.999917, 334),
	    std::make_pair(-0.999917, 177),
	    std::make_pair(-0.999918, 699),
	    std::make_pair(-0.999918, 1520),
	    std::make_pair(-0.999918, 1821),
	    std::make_pair(-0.999918, 1458),
	    std::make_pair(-0.999918, 398),
	    std::make_pair(-0.999918, 1943),
	    std::make_pair(-0.999918, 305),
	    std::make_pair(-0.999918, 1599),
	    std::make_pair(-0.999918, 1905),
	    std::make_pair(-0.999919, 1621),
	    std::make_pair(-0.999919, 926),
	    std::make_pair(-0.999919, 1512),
	    std::make_pair(-0.999919, 1427),
	    std::make_pair(-0.999919, 1253),
	    std::make_pair(-0.999919, 260),
	    std::make_pair(-0.99992, 1807),
	    std::make_pair(-0.99992, 1489),
	    std::make_pair(-0.99992, 183),
	    std::make_pair(-0.99992, 436),
	    std::make_pair(-0.99992, 470),
	    std::make_pair(-0.999921, 664),
	    std::make_pair(-0.999921, 864),
	    std::make_pair(-0.999921, 815),
	    std::make_pair(-0.999921, 1273),
	    std::make_pair(-0.999921, 968),
	    std::make_pair(-0.999921, 396),
	    std::make_pair(-0.999922, 589),
	    std::make_pair(-0.999922, 133),
	    std::make_pair(-0.999922, 670),
	    std::make_pair(-0.999922, 282),
	    std::make_pair(-0.999923, 1143),
	    std::make_pair(-0.999923, 1016),
	    std::make_pair(-0.999923, 1742),
	    std::make_pair(-0.999923, 802),
	    std::make_pair(-0.999924, 657),
	    std::make_pair(-0.999924, 1706),
	    std::make_pair(-0.999924, 637),
	    std::make_pair(-0.999924, 784),
	    std::make_pair(-0.999925, 1776),
	    std::make_pair(-0.999926, 420),
	    std::make_pair(-0.999926, 268),
	    std::make_pair(-0.999926, 568),
	    std::make_pair(-0.999927, 488),
	    std::make_pair(-0.999927, 1873),
	    std::make_pair(-0.999927, 1054),
	    std::make_pair(-0.999928, 343),
	    std::make_pair(-0.999928, 1185),
	    std::make_pair(-0.999928, 1861),
	    std::make_pair(-0.999928, 1983),
	    std::make_pair(-0.999928, 1013),
	    std::make_pair(-0.999928, 626),
	    std::make_pair(-0.999929, 123),
	    std::make_pair(-0.999929, 1997),
	    std::make_pair(-0.999929, 29),
	    std::make_pair(-0.999929, 452),
	    std::make_pair(-0.999929, 782),
	    std::make_pair(-0.99993, 72),
	    std::make_pair(-0.99993, 1818),
	    std::make_pair(-0.99993, 1915),
	    std::make_pair(-0.99993, 1321),
	    std::make_pair(-0.99993, 749),
	    std::make_pair(-0.99993, 1459),
	    std::make_pair(-0.999931, 1089),
	    std::make_pair(-0.999931, 1051),
	    std::make_pair(-0.999931, 1042),
	    std::make_pair(-0.999931, 846),
	    std::make_pair(-0.999931, 560),
	    std::make_pair(-0.999931, 534),
	    std::make_pair(-0.999932, 1760),
	    std::make_pair(-0.999932, 777),
	    std::make_pair(-0.999932, 406),
	    std::make_pair(-0.999932, 546),
	    std::make_pair(-0.999932, 771),
	    std::make_pair(-0.999932, 1303),
	    std::make_pair(-0.999933, 666),
	    std::make_pair(-0.999933, 1550),
	    std::make_pair(-0.999934, 961),
	    std::make_pair(-0.999934, 300),
	    std::make_pair(-0.999934, 449),
	    std::make_pair(-0.999934, 574),
	    std::make_pair(-0.999934, 1998),
	    std::make_pair(-0.999935, 32),
	    std::make_pair(-0.999935, 1188),
	    std::make_pair(-0.999935, 1461),
	    std::make_pair(-0.999935, 1794),
	    std::make_pair(-0.999936, 835),
	    std::make_pair(-0.999936, 235),
	    std::make_pair(-0.999936, 1707),
	    std::make_pair(-0.999936, 1911),
	    std::make_pair(-0.999936, 1917),
	    std::make_pair(-0.999937, 1378),
	    std::make_pair(-0.999937, 928),
	    std::make_pair(-0.999938, 250),
	    std::make_pair(-0.999939, 1928),
	    std::make_pair(-0.999939, 1464),
	    std::make_pair(-0.99994, 205),
	    std::make_pair(-0.99994, 1098),
	    std::make_pair(-0.99994, 180),
	    std::make_pair(-0.99994, 294),
	    std::make_pair(-0.999941, 301),
	    std::make_pair(-0.999941, 1414),
	    std::make_pair(-0.999941, 1041),
	    std::make_pair(-0.999941, 1412),
	    std::make_pair(-0.999941, 1604),
	    std::make_pair(-0.999941, 559),
	    std::make_pair(-0.999941, 200),
	    std::make_pair(-0.999941, 625),
	    std::make_pair(-0.999941, 1206),
	    std::make_pair(-0.999942, 743),
	    std::make_pair(-0.999942, 1719),
	    std::make_pair(-0.999942, 387),
	    std::make_pair(-0.999943, 540),
	    std::make_pair(-0.999943, 934),
	    std::make_pair(-0.999943, 169),
	    std::make_pair(-0.999943, 1577),
	    std::make_pair(-0.999943, 219),
	    std::make_pair(-0.999944, 620),
	    std::make_pair(-0.999944, 974),
	    std::make_pair(-0.999944, 1344),
	    std::make_pair(-0.999945, 1108),
	    std::make_pair(-0.999945, 424),
	    std::make_pair(-0.999945, 2010),
	    std::make_pair(-0.999945, 700),
	    std::make_pair(-0.999946, 1748),
	    std::make_pair(-0.999946, 1537),
	    std::make_pair(-0.999946, 1596),
	    std::make_pair(-0.999947, 2028),
	    std::make_pair(-0.999947, 1718),
	    std::make_pair(-0.999947, 590),
	    std::make_pair(-0.999947, 687),
	    std::make_pair(-0.999947, 1833),
	    std::make_pair(-0.999947, 162),
	    std::make_pair(-0.999947, 1971),
	    std::make_pair(-0.999948, 414),
	    std::make_pair(-0.999949, 384),
	    std::make_pair(-0.999949, 1786),
	    std::make_pair(-0.999949, 175),
	    std::make_pair(-0.999949, 918),
	    std::make_pair(-0.99995, 650),
	    std::make_pair(-0.99995, 35),
	    std::make_pair(-0.99995, 508),
	    std::make_pair(-0.99995, 941),
	    std::make_pair(-0.999951, 1730),
	    std::make_pair(-0.999951, 863),
	    std::make_pair(-0.999951, 1347),
	    std::make_pair(-0.999951, 509),
	    std::make_pair(-0.999951, 1556),
	    std::make_pair(-0.999951, 1218),
	    std::make_pair(-0.999951, 171),
	    std::make_pair(-0.999952, 1173),
	    std::make_pair(-0.999952, 492),
	    std::make_pair(-0.999952, 1428),
	    std::make_pair(-0.999952, 1661),
	    std::make_pair(-0.999952, 1019),
	    std::make_pair(-0.999953, 1981),
	    std::make_pair(-0.999953, 1801),
	    std::make_pair(-0.999953, 1625),
	    std::make_pair(-0.999953, 143),
	    std::make_pair(-0.999954, 12),
	    std::make_pair(-0.999954, 1541),
	    std::make_pair(-0.999954, 440),
	    std::make_pair(-0.999955, 1079),
	    std::make_pair(-0.999955, 651),
	    std::make_pair(-0.999955, 1924),
	    std::make_pair(-0.999955, 769),
	    std::make_pair(-0.999955, 144),
	    std::make_pair(-0.999955, 1560),
	    std::make_pair(-0.999955, 975),
	    std::make_pair(-0.999955, 1356),
	    std::make_pair(-0.999955, 1174),
	    std::make_pair(-0.999955, 60),
	    std::make_pair(-0.999956, 712),
	    std::make_pair(-0.999956, 119),
	    std::make_pair(-0.999956, 1141),
	    std::make_pair(-0.999956, 330),
	    std::make_pair(-0.999957, 1803),
	    std::make_pair(-0.999957, 472),
	    std::make_pair(-0.999957, 1862),
	    std::make_pair(-0.999958, 1437),
	    std::make_pair(-0.999958, 132),
	    std::make_pair(-0.999958, 285),
	    std::make_pair(-0.999958, 242),
	    std::make_pair(-0.999958, 1591),
	    std::make_pair(-0.999958, 1052),
	    std::make_pair(-0.999958, 1584),
	    std::make_pair(-0.999959, 484),
	    std::make_pair(-0.999959, 971),
	    std::make_pair(-0.99996, 2036),
	    std::make_pair(-0.99996, 471),
	    std::make_pair(-0.99996, 1539),
	    std::make_pair(-0.99996, 1962),
	    std::make_pair(-0.99996, 705),
	    std::make_pair(-0.99996, 1438),
	    std::make_pair(-0.99996, 248),
	    std::make_pair(-0.999961, 21),
	    std::make_pair(-0.999961, 945),
	    std::make_pair(-0.999961, 202),
	    std::make_pair(-0.999961, 1628),
	    std::make_pair(-0.999961, 172),
	    std::make_pair(-0.999961, 1400),
	    std::make_pair(-0.999961, 1296),
	    std::make_pair(-0.999962, 1952),
	    std::make_pair(-0.999962, 1946),
	    std::make_pair(-0.999962, 591),
	    std::make_pair(-0.999962, 1351),
	    std::make_pair(-0.999962, 544),
	    std::make_pair(-0.999962, 1463),
	    std::make_pair(-0.999963, 930),
	    std::make_pair(-0.999963, 513),
	    std::make_pair(-0.999963, 1636),
	    std::make_pair(-0.999963, 1071),
	    std::make_pair(-0.999964, 1731),
	    std::make_pair(-0.999964, 827),
	    std::make_pair(-0.999965, 374),
	    std::make_pair(-0.999965, 288),
	    std::make_pair(-0.999965, 507),
	    std::make_pair(-0.999965, 1259),
	    std::make_pair(-0.999965, 612),
	    std::make_pair(-0.999965, 993),
	    std::make_pair(-0.999965, 1564),
	    std::make_pair(-0.999965, 1549),
	    std::make_pair(-0.999965, 701),
	    std::make_pair(-0.999966, 661),
	    std::make_pair(-0.999966, 275),
	    std::make_pair(-0.999966, 1032),
	    std::make_pair(-0.999966, 1072),
	    std::make_pair(-0.999966, 1540),
	    std::make_pair(-0.999966, 566),
	    std::make_pair(-0.999966, 223),
	    std::make_pair(-0.999966, 1226),
	    std::make_pair(-0.999967, 1960),
	    std::make_pair(-0.999967, 1028),
	    std::make_pair(-0.999967, 675),
	    std::make_pair(-0.999967, 937),
	    std::make_pair(-0.999967, 389),
	    std::make_pair(-0.999967, 1047),
	    std::make_pair(-0.999968, 1067),
	    std::make_pair(-0.999968, 512),
	    std::make_pair(-0.999968, 1782),
	    std::make_pair(-0.999968, 602),
	    std::make_pair(-0.999969, 1439),
	    std::make_pair(-0.999969, 1611),
	    std::make_pair(-0.999969, 1697),
	    std::make_pair(-0.999969, 820),
	    std::make_pair(-0.999969, 201),
	    std::make_pair(-0.999969, 277),
	    std::make_pair(-0.999969, 1929),
	    std::make_pair(-0.99997, 1590),
	    std::make_pair(-0.99997, 190),
	    std::make_pair(-0.99997, 1954),
	    std::make_pair(-0.99997, 1668),
	    std::make_pair(-0.99997, 1451),
	    std::make_pair(-0.99997, 766),
	    std::make_pair(-0.99997, 1532),
	    std::make_pair(-0.999971, 1009),
	    std::make_pair(-0.999971, 1495),
	    std::make_pair(-0.999971, 281),
	    std::make_pair(-0.999972, 634),
	    std::make_pair(-0.999972, 381),
	    std::make_pair(-0.999972, 450),
	    std::make_pair(-0.999972, 1558),
	    std::make_pair(-0.999972, 2027),
	    std::make_pair(-0.999972, 217),
	    std::make_pair(-0.999973, 939),
	    std::make_pair(-0.999973, 872),
	    std::make_pair(-0.999974, 214),
	    std::make_pair(-0.999974, 684),
	    std::make_pair(-0.999974, 1737),
	    std::make_pair(-0.999974, 27),
	    std::make_pair(-0.999974, 790),
	    std::make_pair(-0.999974, 155),
	    std::make_pair(-0.999975, 147),
	    std::make_pair(-0.999975, 640),
	    std::make_pair(-0.999975, 688),
	    std::make_pair(-0.999975, 104),
	    std::make_pair(-0.999975, 1265),
	    std::make_pair(-0.999975, 2001),
	    std::make_pair(-0.999976, 1122),
	    std::make_pair(-0.999976, 834),
	    std::make_pair(-0.999976, 1774),
	    std::make_pair(-0.999977, 957),
	    std::make_pair(-0.999977, 1900),
	    std::make_pair(-0.999977, 1573),
	    std::make_pair(-0.999977, 977),
	    std::make_pair(-0.999977, 17),
	    std::make_pair(-0.999978, 404),
	    std::make_pair(-0.999978, 1633),
	    std::make_pair(-0.999978, 167),
	    std::make_pair(-0.999978, 1503),
	    std::make_pair(-0.999978, 1125),
	    std::make_pair(-0.999978, 585),
	    std::make_pair(-0.999978, 1721),
	    std::make_pair(-0.999979, 763),
	    std::make_pair(-0.999979, 913),
	    std::make_pair(-0.999979, 399),
	    std::make_pair(-0.999979, 98),
	    std::make_pair(-0.999979, 1124),
	    std::make_pair(-0.999979, 1474),
	    std::make_pair(-0.999979, 1757),
	    std::make_pair(-0.99998, 1530),
	    std::make_pair(-0.99998, 800),
	    std::make_pair(-0.99998, 284),
	    std::make_pair(-0.999981, 633),
	    std::make_pair(-0.999981, 1090),
	    std::make_pair(-0.999981, 1659),
	    std::make_pair(-0.999982, 1410),
	    std::make_pair(-0.999982, 1119),
	    std::make_pair(-0.999982, 1898),
	    std::make_pair(-0.999982, 595),
	    std::make_pair(-0.999982, 1246),
	    std::make_pair(-0.999982, 1376),
	    std::make_pair(-0.999982, 518),
	    std::make_pair(-0.999983, 907),
	    std::make_pair(-0.999983, 1429),
	    std::make_pair(-0.999983, 1279),
	    std::make_pair(-0.999983, 1359),
	    std::make_pair(-0.999984, 1791),
	    std::make_pair(-0.999984, 1353),
	    std::make_pair(-0.999984, 1793),
	    std::make_pair(-0.999984, 707),
	    std::make_pair(-0.999984, 187),
	    std::make_pair(-0.999984, 241),
	    std::make_pair(-0.999985, 735),
	    std::make_pair(-0.999985, 196),
	    std::make_pair(-0.999985, 1877),
	    std::make_pair(-0.999985, 221),
	    std::make_pair(-0.999985, 1349),
	    std::make_pair(-0.999986, 1196),
	    std::make_pair(-0.999986, 146),
	    std::make_pair(-0.999987, 1578),
	    std::make_pair(-0.999987, 519),
	    std::make_pair(-0.999987, 498),
	    std::make_pair(-0.999987, 1991),
	    std::make_pair(-0.999987, 348),
	    std::make_pair(-0.999987, 465),
	    std::make_pair(-0.999988, 1617),
	    std::make_pair(-0.999988, 535),
	    std::make_pair(-0.999988, 1326),
	    std::make_pair(-0.999988, 388),
	    std::make_pair(-0.999988, 1979),
	    std::make_pair(-0.999989, 727),
	    std::make_pair(-0.999989, 1853),
	    std::make_pair(-0.999989, 173),
	    std::make_pair(-0.999989, 1194),
	    std::make_pair(-0.999989, 1264),
	    std::make_pair(-0.999989, 1526),
	    std::make_pair(-0.999989, 1713),
	    std::make_pair(-0.999989, 370),
	    std::make_pair(-0.99999, 1724),
	    std::make_pair(-0.99999, 307),
	    std::make_pair(-0.99999, 1288),
	    std::make_pair(-0.999991, 1181),
	    std::make_pair(-0.999991, 475),
	    std::make_pair(-0.999992, 443),
	    std::make_pair(-0.999992, 1507),
	    std::make_pair(-0.999992, 1686),
	    std::make_pair(-0.999992, 429),
	    std::make_pair(-0.999992, 1200),
	    std::make_pair(-0.999992, 2003),
	    std::make_pair(-0.999993, 1701),
	    std::make_pair(-0.999993, 1685),
	    std::make_pair(-0.999993, 718),
	    std::make_pair(-0.999994, 386),
	    std::make_pair(-0.999994, 1208),
	    std::make_pair(-0.999994, 1012),
	    std::make_pair(-0.999994, 689),
	    std::make_pair(-0.999994, 943),
	    std::make_pair(-0.999994, 3),
	    std::make_pair(-0.999994, 752),
	    std::make_pair(-0.999994, 135),
	    std::make_pair(-0.999995, 805),
	    std::make_pair(-0.999995, 932),
	    std::make_pair(-0.999995, 616),
	    std::make_pair(-0.999995, 856),
	    std::make_pair(-0.999996, 1561),
	    std::make_pair(-0.999996, 1944),
	    std::make_pair(-0.999996, 1703),
	    std::make_pair(-0.999996, 1727),
	    std::make_pair(-0.999996, 1923),
	    std::make_pair(-0.999996, 418),
	    std::make_pair(-0.999996, 1297),
	    std::make_pair(-0.999997, 1732),
	    std::make_pair(-0.999997, 987),
	    std::make_pair(-0.999997, 1091),
	    std::make_pair(-0.999998, 478),
	    std::make_pair(-0.999998, 1292),
	    std::make_pair(-0.999998, 383),
	    std::make_pair(-0.999998, 748),
	    std::make_pair(-0.999998, 1254),
	    std::make_pair(-0.999999, 1711),
	    std::make_pair(-0.999999, 1949),
	    std::make_pair(-0.999999, 1619),
	    std::make_pair(-0.999999, 1831),
	    std::make_pair(-0.999999, 854),
	    std::make_pair(-0.999999, 1286),
	    std::make_pair(-0.999999, 1631),
	    std::make_pair(-0.999999, 434),
	    std::make_pair(-1, 138),
	    std::make_pair(-1, 131)
	};

	for (idx_t i = 0; i < sample_count; i++) {
		double k_i = base_reservoir_sample->random.NextRandom(base_reservoir_sample->min_weight_threshold, 1);
		base_reservoir_sample->reservoir_weights.emplace(-k_i, i);
	}
}

void BaseReservoirSampling::InitializeReservoir(idx_t cur_size, idx_t sample_size) {
	//! 1: The first m items of V are inserted into R
	//! first we need to check if the reservoir already has "m" elements
	if (cur_size == sample_size) {
		//! 2. For each item vi ∈ R: Calculate a key ki = random(0, 1)
		//! we then define the threshold to enter the reservoir T_w as the minimum key of R
		//! we use a priority queue to extract the minimum key in O(1) time
		for (idx_t i = 0; i < sample_size; i++) {
			double k_i = random.NextRandom();
			reservoir_weights.emplace(-k_i, i);
		}
		SetNextEntry();
	}
}

void BaseReservoirSampling::SetNextEntry() {
	//! 4. Let r = random(0, 1) and Xw = log(r) / log(T_w)
	auto &min_key = reservoir_weights.top();
	double t_w = -min_key.first;
	double r = random.NextRandom();
	double x_w = log(r) / log(t_w);
	//! 5. From the current item vc skip items until item vi , such that:
	//! 6. wc +wc+1 +···+wi−1 < Xw <= wc +wc+1 +···+wi−1 +wi
	//! since all our weights are 1 (uniform sampling), we can just determine the amount of elements to skip
	min_weight_threshold = t_w;
	min_weighted_entry_index = min_key.second;
	next_index_to_sample = MaxValue<idx_t>(1, idx_t(round(x_w)));
	num_entries_to_skip_b4_next_sample = 0;
}

void BaseReservoirSampling::ReplaceElementWithIndex(duckdb::idx_t entry_index, double with_weight) {

	double r2 = with_weight;
	//! now we insert the new weight into the reservoir
	reservoir_weights.push(std::make_pair(-r2, entry_index));
	//! we update the min entry with the new min entry in the reservoir
	SetNextEntry();
}

void BaseReservoirSampling::ReplaceElement(double with_weight) {
	//! replace the entry in the reservoir
	//! pop the minimum entry
	reservoir_weights.pop();
	//! now update the reservoir
	//! 8. Let tw = Tw i , r2 = random(tw,1) and vi’s key: ki = (r2)1/wi
	//! 9. The new threshold Tw is the new minimum key of R
	//! we generate a random number between (min_weight_threshold, 1)
	double r2 = random.NextRandom(min_weight_threshold, 1);

	//! if we are merging two reservoir samples use the weight passed
	if (with_weight >= 0) {
		r2 = with_weight;
	}
	//! now we insert the new weight into the reservoir
	reservoir_weights.push(std::make_pair(-r2, min_weighted_entry_index));
	//! we update the min entry with the new min entry in the reservoir
	SetNextEntry();
}

std::pair<double, idx_t> BlockingSample::PopFromWeightQueue() {
	auto ret = base_reservoir_sample->reservoir_weights.top();
	base_reservoir_sample->reservoir_weights.pop();

	if (base_reservoir_sample->reservoir_weights.empty()) {
		// 1 is maximum weight
		base_reservoir_sample->min_weight_threshold = 1;
		return ret;
	}
	auto &min_key = base_reservoir_sample->reservoir_weights.top();
	base_reservoir_sample->min_weight_threshold = -min_key.first;
	return ret;
}

double BlockingSample::GetMinWeightThreshold() {
	return base_reservoir_sample->min_weight_threshold;
}

idx_t BlockingSample::GetPriorityQueueSize() {
	return base_reservoir_sample->reservoir_weights.size();
}

} // namespace duckdb
