#include "duckdb/execution/reservoir_sample.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {
void ReservoirChunk::Serialize(Serializer &serializer) const {
	chunk.Serialize(serializer);
}

unique_ptr<ReservoirChunk> ReservoirChunk::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<ReservoirChunk>();
	result->chunk.Deserialize(deserializer);
	return result;
}

unique_ptr<ReservoirChunk> ReservoirChunk::Copy() const {
	auto copy = make_uniq<ReservoirChunk>();
	copy->chunk.Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());

	chunk.Copy(copy->chunk);
	return copy;
}

std::pair<double, idx_t> BlockingSample::PopFromWeightQueue() {
	D_ASSERT(base_reservoir_sample && !base_reservoir_sample->reservoir_weights.empty());
	auto ret = base_reservoir_sample->reservoir_weights.top();
	base_reservoir_sample->reservoir_weights.pop();

	base_reservoir_sample->UpdateMinWeightThreshold();
	D_ASSERT(base_reservoir_sample->min_weight_threshold > 0);
	return ret;
}

double BlockingSample::GetMinWeightThreshold() {
	return base_reservoir_sample->min_weight_threshold;
}

idx_t BlockingSample::GetPriorityQueueSize() {
	return base_reservoir_sample->reservoir_weights.size();
}

void BlockingSample::Destroy() {
	destroyed = true;
}

ReservoirSamplePercentage::ReservoirSamplePercentage(double percentage, int64_t seed, idx_t reservoir_sample_size)
    : BlockingSample(seed), allocator(Allocator::DefaultAllocator()), sample_percentage(percentage / 100.0),
      reservoir_sample_size(reservoir_sample_size), current_count(0), is_finalized(false) {
	current_sample =
	    make_uniq<ReservoirSample>(allocator, reservoir_sample_size, base_reservoir_sample->random.NextRandomInteger());
	type = SampleType::RESERVOIR_PERCENTAGE_SAMPLE;
}

ReservoirSamplePercentage::ReservoirSamplePercentage(Allocator &allocator, double percentage, int64_t seed)
    : BlockingSample(seed), allocator(allocator), sample_percentage(percentage / 100.0), current_count(0),
      is_finalized(false) {
	reservoir_sample_size = (idx_t)(sample_percentage * RESERVOIR_THRESHOLD);
	current_sample =
	    make_uniq<ReservoirSample>(allocator, reservoir_sample_size, base_reservoir_sample->random.NextRandomInteger());
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
		current_sample = make_uniq<ReservoirSample>(allocator, reservoir_sample_size,
		                                            base_reservoir_sample->random.NextRandomInteger());
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

unique_ptr<DataChunk> ReservoirSamplePercentage::GetChunk(idx_t offset, bool destroy) {
	// reservoir sample percentage should never stay
	D_ASSERT(destroy);
	if (!is_finalized) {
		Finalize();
	}
	while (!finished_samples.empty()) {
		auto &front = finished_samples.front();
		auto chunk = front->GetChunk(offset, destroy);
		if (chunk && chunk->size() > 0) {
			return chunk;
		}
		// move to the next sample
		finished_samples.erase(finished_samples.begin());
	}
	return nullptr;
}

unique_ptr<BlockingSample> ReservoirSamplePercentage::Copy() const {
	throw InternalException("calling copy on reservoir sample percentage");
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
	    static_cast<double>(current_count) > sample_percentage * RESERVOIR_THRESHOLD || finished_samples.empty();
	if (current_count > 0 && sampled_more_than_required) {
		// create a new sample
		auto new_sample_size = static_cast<idx_t>(round(sample_percentage * static_cast<double>(current_count)));
		auto new_sample =
		    make_uniq<ReservoirSample>(allocator, new_sample_size, base_reservoir_sample->random.NextRandomInteger());
		while (true) {
			auto chunk = current_sample->GetChunk(0, true);
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

void BlockingSample::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<unique_ptr<BaseReservoirSampling>>(100, "base_reservoir_sample",
	                                                                       base_reservoir_sample);
	serializer.WriteProperty<SampleType>(101, "type", type);
	serializer.WritePropertyWithDefault<bool>(102, "destroyed", destroyed);
}

unique_ptr<BlockingSample> BlockingSample::Deserialize(Deserializer &deserializer) {
	auto base_reservoir_sample =
	    deserializer.ReadPropertyWithDefault<unique_ptr<BaseReservoirSampling>>(100, "base_reservoir_sample");
	auto type = deserializer.ReadProperty<SampleType>(101, "type");
	auto destroyed = deserializer.ReadPropertyWithDefault<bool>(102, "destroyed");
	D_ASSERT(type == SampleType::RESERVOIR_SAMPLE);
	auto result = ReservoirSample::Deserialize(deserializer);
	D_ASSERT(result->type == SampleType::RESERVOIR_SAMPLE);
	result->type = type;
	result->base_reservoir_sample = std::move(base_reservoir_sample);
	result->destroyed = destroyed;
	if (result->type == SampleType::RESERVOIR_SAMPLE) {
		auto &wat = result->Cast<ReservoirSample>();
		wat.ExpandSerializedSample();
		wat.Verify();
	}
	return result;
}

void ReservoirSample::Serialize(Serializer &serializer) const {
	BlockingSample::Serialize(serializer);
	serializer.WritePropertyWithDefault<idx_t>(200, "sample_count", sample_count);
	serializer.WritePropertyWithDefault<unique_ptr<ReservoirChunk>>(201, "reservoir_chunk", reservoir_chunk);
}

unique_ptr<BlockingSample> ReservoirSample::Deserialize(Deserializer &deserializer) {
	auto sample_count = deserializer.ReadPropertyWithDefault<idx_t>(200, "sample_count");
	auto result = duckdb::unique_ptr<ReservoirSample>(new ReservoirSample(sample_count));
	deserializer.ReadPropertyWithDefault<unique_ptr<ReservoirChunk>>(201, "reservoir_chunk", result->reservoir_chunk);
	if (result->reservoir_chunk) {
		result->sel_size = result->reservoir_chunk->chunk.size();
		result->sel = SelectionVector(0, sample_count);
	}
	return std::move(result);
}

void ReservoirSamplePercentage::Serialize(Serializer &serializer) const {
	auto copy = Copy();
	auto &copy_percentage = copy->Cast<ReservoirSamplePercentage>();
	base_reservoir_sample->reservoir_weights.emplace(NumericLimits<double>::Maximum(),
	                                                 idx_t(copy_percentage.sample_percentage * 100));
}

unique_ptr<BlockingSample> ReservoirSamplePercentage::Deserialize(Deserializer &deserializer) {
	auto sample_percentage = deserializer.ReadProperty<double>(200, "sample_percentage");
	auto result = duckdb::unique_ptr<ReservoirSamplePercentage>(new ReservoirSamplePercentage(sample_percentage));
	deserializer.ReadPropertyWithDefault<idx_t>(201, "reservoir_sample_size", result->reservoir_sample_size);
	return std::move(result);
}

} // namespace duckdb
