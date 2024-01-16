#include "duckdb/execution/reservoir_sample.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

void ReservoirChunk::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<DataChunk>(100, "chunk", chunk);
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
	current_sample = make_uniq<ReservoirSample>(allocator, reservoir_sample_size, random.NextRandomInteger());
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
		current_sample = make_uniq<ReservoirSample>(allocator, reservoir_sample_size, random.NextRandomInteger());
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
		auto new_sample = make_uniq<ReservoirSample>(allocator, new_sample_size, random.NextRandomInteger());
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
