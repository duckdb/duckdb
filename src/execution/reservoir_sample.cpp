#include "duckdb/execution/reservoir_sample.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

ReservoirSample::ReservoirSample(Allocator &allocator, idx_t sample_count, int64_t seed)
    : BlockingSample(seed), sample_count(sample_count), reservoir(allocator) {
}

void ReservoirSample::AddToReservoir(DataChunk &input) {
	if (sample_count == 0) {
		return;
	}
	// Input: A population V of n weighted items
	// Output: A reservoir R with a size m
	// 1: The first m items of V are inserted into R
	// first we need to check if the reservoir already has "m" elements
	if (reservoir.Count() < sample_count) {
		if (FillReservoir(input) == 0) {
			// entire chunk was consumed by reservoir
			return;
		}
	}
	// find the position of next_index relative to current_count
	idx_t remaining = input.size();
	idx_t base_offset = 0;
	while (true) {
		idx_t offset = base_reservoir_sample.next_index - base_reservoir_sample.current_count;
		if (offset >= remaining) {
			// not in this chunk! increment current count and go to the next chunk
			base_reservoir_sample.current_count += remaining;
			return;
		}
		// in this chunk! replace the element
		ReplaceElement(input, base_offset + offset);
		// shift the chunk forward
		remaining -= offset;
		base_offset += offset;
	}
}

unique_ptr<DataChunk> ReservoirSample::GetChunk() {
	return reservoir.Fetch();
}

void ReservoirSample::ReplaceElement(DataChunk &input, idx_t index_in_chunk) {
	// replace the entry in the reservoir
	// 8. The item in R with the minimum key is replaced by item vi
	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		reservoir.SetValue(col_idx, base_reservoir_sample.min_entry, input.GetValue(col_idx, index_in_chunk));
	}
	base_reservoir_sample.ReplaceElement();
}

idx_t ReservoirSample::FillReservoir(DataChunk &input) {
	idx_t chunk_count = input.size();
	input.Flatten();

	// we have not: append to the reservoir
	idx_t required_count;
	if (reservoir.Count() + chunk_count >= sample_count) {
		// have to limit the count of the chunk
		required_count = sample_count - reservoir.Count();
	} else {
		// we copy the entire chunk
		required_count = chunk_count;
	}
	// instead of copying we just change the pointer in the current chunk
	input.SetCardinality(required_count);
	reservoir.Append(input);

	base_reservoir_sample.InitializeReservoir(reservoir.Count(), sample_count);

	// check if there are still elements remaining
	// this happens if we are on a boundary
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

ReservoirSamplePercentage::ReservoirSamplePercentage(Allocator &allocator, double percentage, int64_t seed)
    : BlockingSample(seed), allocator(allocator), sample_percentage(percentage / 100.0), current_count(0),
      is_finalized(false) {
	reservoir_sample_size = idx_t(sample_percentage * RESERVOIR_THRESHOLD);
	current_sample = make_unique<ReservoirSample>(allocator, reservoir_sample_size, random.NextRandomInteger());
}

void ReservoirSamplePercentage::AddToReservoir(DataChunk &input) {
	if (current_count + input.size() > RESERVOIR_THRESHOLD) {
		// we don't have enough space in our current reservoir
		// first check what we still need to append to the current sample
		idx_t append_to_current_sample_count = RESERVOIR_THRESHOLD - current_count;
		idx_t append_to_next_sample = input.size() - append_to_current_sample_count;
		if (append_to_current_sample_count > 0) {
			// we have elements remaining, first add them to the current sample
			input.Flatten();

			input.SetCardinality(append_to_current_sample_count);
			current_sample->AddToReservoir(input);
		}
		if (append_to_next_sample > 0) {
			// slice the input for the remainder
			SelectionVector sel(STANDARD_VECTOR_SIZE);
			for (idx_t i = 0; i < append_to_next_sample; i++) {
				sel.set_index(i, append_to_current_sample_count + i);
			}
			input.Slice(sel, append_to_next_sample);
		}
		// now our first sample is filled: append it to the set of finished samples
		finished_samples.push_back(move(current_sample));

		// allocate a new sample, and potentially add the remainder of the current input to that sample
		current_sample = make_unique<ReservoirSample>(allocator, reservoir_sample_size, random.NextRandomInteger());
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

unique_ptr<DataChunk> ReservoirSamplePercentage::GetChunk() {
	if (!is_finalized) {
		Finalize();
	}
	while (!finished_samples.empty()) {
		auto &front = finished_samples.front();
		auto chunk = front->GetChunk();
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
	if (current_count > 0) {
		// create a new sample
		auto new_sample_size = idx_t(round(sample_percentage * current_count));
		auto new_sample = make_unique<ReservoirSample>(allocator, new_sample_size, random.NextRandomInteger());
		while (true) {
			auto chunk = current_sample->GetChunk();
			if (!chunk || chunk->size() == 0) {
				break;
			}
			new_sample->AddToReservoir(*chunk);
		}
		finished_samples.push_back(move(new_sample));
	}
	is_finalized = true;
}

BaseReservoirSampling::BaseReservoirSampling(int64_t seed) : random(seed) {
	next_index = 0;
	min_threshold = 0;
	min_entry = 0;
	current_count = 0;
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
			reservoir_weights.push(std::make_pair(-k_i, i));
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
	min_threshold = t_w;
	min_entry = min_key.second;
	next_index = MaxValue<idx_t>(1, idx_t(round(x_w)));
	current_count = 0;
}

void BaseReservoirSampling::ReplaceElement() {
	//! replace the entry in the reservoir
	//! pop the minimum entry
	reservoir_weights.pop();
	//! now update the reservoir
	//! 8. Let tw = Tw i , r2 = random(tw,1) and vi’s key: ki = (r2)1/wi
	//! 9. The new threshold Tw is the new minimum key of R
	//! we generate a random number between (min_threshold, 1)
	double r2 = random.NextRandom(min_threshold, 1);
	//! now we insert the new weight into the reservoir
	reservoir_weights.push(std::make_pair(-r2, min_entry));
	//! we update the min entry with the new min entry in the reservoir
	SetNextEntry();
}

} // namespace duckdb
