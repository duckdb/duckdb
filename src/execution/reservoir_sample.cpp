#include "duckdb/execution/reservoir_sample.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

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
		idx_t offset = next_index - current_count;
		if (offset >= remaining) {
			// not in this chunk! increment current count and go to the next chunk
			current_count += remaining;
			return;
		}
		// in this chunk! replace the element
		ReplaceElement(input, base_offset + offset);
		// shift the chunk forward
		remaining -= offset;
		base_offset += offset;
	}
}

void ReservoirSample::SetNextEntry() {
	// 5. Let r = random(0, 1) and Xw = log(r) / log(T_w)
	auto &min_key = reservoir_weights.top();
	double T_w = -min_key.first;
	double r = random.NextRandom();
	double X_w = log(r) / log(T_w);
	// 6. From the current item vc skip items until item vi , such that:
	// 7. wc +wc+1 +···+wi−1 < Xw <= wc +wc+1 +···+wi−1 +wi
	// since all our weights are 1 (uniform sampling), we can just determine the amount of elements to skip
	min_threshold = T_w;
	min_entry = min_key.second;
	next_index = MaxValue<idx_t>(1, idx_t(round(X_w)));
	current_count = 0;
}

unique_ptr<DataChunk> ReservoirSample::GetChunk() {
	return reservoir.Fetch();
}

void ReservoirSample::ReplaceElement(DataChunk &input, idx_t index_in_chunk) {
	// replace the entry in the reservoir
	// 8. The item in R with the minimum key is replaced by item vi
	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		reservoir.SetValue(col_idx, min_entry, input.GetValue(col_idx, index_in_chunk));
	}
	// pop the minimum entry
	reservoir_weights.pop();
	// now update the reservoir
	// 9. Let tw = Tw i , r2 = random(tw,1) and vi’s key: ki = (r2)1/wi
	// 10. The new threshold Tw is the new minimum key of R
	// we generate a random number between (min_threshold, 1)
	double r2 = random.NextRandom(min_threshold, 1);
	// now we insert the new weight into the reservoir
	reservoir_weights.push(make_pair(-r2, min_entry));
	// we update the min entry with the new min entry in the reservoir
	SetNextEntry();
}

idx_t ReservoirSample::FillReservoir(DataChunk &input) {
	idx_t chunk_count = input.size();
	input.Normalify();

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

	if (reservoir.Count() == sample_count) {
		// our reservoir is full: initialize the actual reservoir
		// 2. For each item vi ∈ R: Calculate a key ki = random(0, 1)
		// we then define the threshold to enter the reservoir T_w as the minimum key of R
		// we use a priority queue to extract the minimum key in O(1) time
		for (idx_t i = 0; i < sample_count; i++) {
			double k_i = random.NextRandom();
			reservoir_weights.push(make_pair(-k_i, i));
		}
		// now that we have the sample, we start our replacement strategy
		// 4. Repeat Steps 5–10 until the population is exhausted
		SetNextEntry();
	}

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

ReservoirSamplePercentage::ReservoirSamplePercentage(double percentage, int64_t seed)
    : BlockingSample(seed), sample_percentage(percentage / 100.0), current_count(0), is_finalized(false) {
	reservoir_sample_size = idx_t(sample_percentage * RESERVOIR_THRESHOLD);
	current_sample = make_unique<ReservoirSample>(reservoir_sample_size, random.NextRandomInteger());
}

void ReservoirSamplePercentage::AddToReservoir(DataChunk &input) {
	if (current_count + input.size() > RESERVOIR_THRESHOLD) {
		// we don't have enough space in our current reservoir
		// first check what we still need to append to the current sample
		idx_t append_to_current_sample_count = RESERVOIR_THRESHOLD - current_count;
		idx_t append_to_next_sample = input.size() - append_to_current_sample_count;
		if (append_to_current_sample_count > 0) {
			// we have elements remaining, first add them to the current sample
			input.Normalify();

			input.SetCardinality(append_to_current_sample_count);
			current_sample->AddToReservoir(input);

			if (append_to_next_sample > 0) {
				// slice the input for the remainder
				SelectionVector sel(STANDARD_VECTOR_SIZE);
				for (idx_t i = 0; i < append_to_next_sample; i++) {
					sel.set_index(i, append_to_current_sample_count + i);
				}
				input.Slice(sel, append_to_next_sample);
			}
		}
		// now our first sample is filled: append it to the set of finished samples
		finished_samples.push_back(move(current_sample));

		// allocate a new sample, and potentially add the remainder of the current input to that sample
		current_sample = make_unique<ReservoirSample>(reservoir_sample_size, random.NextRandomInteger());
		if (append_to_next_sample > 0) {
			current_sample->AddToReservoir(input);
		}
		current_count = append_to_next_sample;
	} else {
		// we can just append to the current sample
		current_sample->AddToReservoir(input);
		current_count += input.size();
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
		auto new_sample = make_unique<ReservoirSample>(new_sample_size, random.NextRandomInteger());
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

} // namespace duckdb
