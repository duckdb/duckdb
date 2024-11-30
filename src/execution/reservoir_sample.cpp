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
//
// void ReservoirSample::AddToReservoir(DataChunk &input) {
// 	if (sample_count == 0 || destroyed) {
// 		// sample count is 0, means no samples were requested
// 		// destroyed means the original table has been altered and the changes have not yet
// 		// been reflected within the sample reservoir. So we also don't add anything
// 		return;
// 	}
// 	base_reservoir_sample->num_entries_seen_total += input.size();
// 	// Input: A population V of n weighted items
// 	// Output: A reservoir R with a size m
// 	// 1: The first m items of V are inserted into R
// 	// first we need to check if the reservoir already has "m" elements
// 	if (!reservoir_chunk || Chunk().size() < sample_count) {
// 		if (FillReservoir(input) == 0) {
// 			// entire chunk was consumed by reservoir
// 			return;
// 		}
// 	}
// 	D_ASSERT(reservoir_chunk);
// 	D_ASSERT(Chunk().size() == sample_count);
// 	// Initialize the weights if we have collected sample_count rows and weights have not been initialized
// 	if (Chunk().size() == sample_count && GetPriorityQueueSize() == 0) {
// 		base_reservoir_sample->InitializeReservoirWeights(Chunk().size(), sample_count);
// 	}
// 	// find the position of next_index_to_sample relative to number of seen entries (num_entries_to_skip_b4_next_sample)
// 	idx_t remaining = input.size();
// 	idx_t base_offset = 0;
// 	while (true) {
// 		idx_t offset =
// 		    base_reservoir_sample->next_index_to_sample - base_reservoir_sample->num_entries_to_skip_b4_next_sample;
// 		if (offset >= remaining) {
// 			// not in this chunk! increment current count and go to the next chunk
// 			base_reservoir_sample->num_entries_to_skip_b4_next_sample += remaining;
// 			return;
// 		}
// 		// in this chunk! replace the element
// 		ReplaceElement(input, base_offset + offset);
// 		// shift the chunk forward
// 		remaining -= offset;
// 		base_offset += offset;
// 	}
// }
//
// unique_ptr<BlockingSample> ReservoirSample::Copy() const {
// 	throw InternalException("calling copy on reservoir sample");
// }
//
// struct ReplacementHelper {
// 	bool exists;
// 	std::pair<double, idx_t> pair;
// };
//
// unique_ptr<DataChunk> ReservoirSample::GetChunk(idx_t offset) {
//
// 	if (destroyed || !reservoir_chunk || Chunk().size() == 0 || offset >= Chunk().size()) {
// 		return nullptr;
// 	}
// 	auto ret = make_uniq<DataChunk>();
// 	idx_t ret_chunk_size = FIXED_SAMPLE_SIZE;
// 	if (offset + FIXED_SAMPLE_SIZE > Chunk().size()) {
// 		ret_chunk_size = Chunk().size() - offset;
// 	}
// 	auto reservoir_types = Chunk().GetTypes();
// 	SelectionVector sel(FIXED_SAMPLE_SIZE);
// 	for (idx_t i = offset; i < offset + ret_chunk_size; i++) {
// 		sel.set_index(i - offset, i);
// 	}
// 	ret->Initialize(allocator, reservoir_types, FIXED_SAMPLE_SIZE);
// 	ret->Slice(Chunk(), sel, FIXED_SAMPLE_SIZE);
// 	ret->SetCardinality(ret_chunk_size);
// 	return ret;
// }
//
// unique_ptr<DataChunk> ReservoirSample::GetChunkAndShrink() {
// 	if (!reservoir_chunk || Chunk().size() == 0 || destroyed) {
// 		return nullptr;
// 	}
// 	if (Chunk().size() > FIXED_SAMPLE_SIZE) {
// 		// get from the back
// 		auto ret = make_uniq<DataChunk>();
// 		auto samples_remaining = Chunk().size() - FIXED_SAMPLE_SIZE;
// 		auto reservoir_types = Chunk().GetTypes();
// 		SelectionVector sel(FIXED_SAMPLE_SIZE);
// 		for (idx_t i = samples_remaining; i < Chunk().size(); i++) {
// 			sel.set_index(i - samples_remaining, i);
// 		}
// 		ret->Initialize(allocator, reservoir_types, FIXED_SAMPLE_SIZE);
// 		ret->Slice(Chunk(), sel, FIXED_SAMPLE_SIZE);
// 		ret->SetCardinality(FIXED_SAMPLE_SIZE);
// 		// reduce capacity and cardinality of the sample data chunk
// 		Chunk().SetCardinality(samples_remaining);
// 		return ret;
// 	}
// 	auto ret = make_uniq<DataChunk>();
// 	ret->Initialize(allocator, Chunk().GetTypes());
// 	Chunk().Copy(*ret);
// 	reservoir_chunk = nullptr;
// 	return ret;
// }
//
// void ReservoirSample::Destroy() {
// 	BlockingSample::Destroy();
// 	reservoir_chunk = nullptr;
// }
//
// void ReservoirSample::ReplaceElement(DataChunk &input, idx_t index_in_chunk, double with_weight) {
// 	// replace the entry in the reservoir with Input[index_in_chunk]
// 	// If index_in_self_chunk is provided, then the
// 	// 8. The item in R with the minimum key is replaced by item vi
// 	D_ASSERT(input.ColumnCount() == Chunk().ColumnCount());
// 	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
// 		Chunk().SetValue(col_idx, base_reservoir_sample->min_weighted_entry_index,
// 		                 input.GetValue(col_idx, index_in_chunk));
// 	}
// 	base_reservoir_sample->ReplaceElement(with_weight);
// }
//
// void ReservoirSample::ReplaceElement(idx_t reservoir_chunk_index, DataChunk &input, idx_t index_in_input_chunk,
//                                      double with_weight) {
// 	// replace the entry in the reservoir with Input[index_in_chunk]
// 	// If index_in_self_chunk is provided, then the
// 	// 8. The item in R with the minimum key is replaced by item vi
// 	D_ASSERT(input.ColumnCount() == Chunk().ColumnCount());
// 	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
// 		Chunk().SetValue(col_idx, reservoir_chunk_index, input.GetValue(col_idx, index_in_input_chunk));
// 	}
// 	base_reservoir_sample->ReplaceElementWithIndex(reservoir_chunk_index, with_weight);
// }
//
// void ReservoirSample::CreateReservoirChunk(const vector<LogicalType> &types) {
// 	reservoir_chunk = make_uniq<ReservoirChunk>();
// 	Chunk().Initialize(allocator, types, sample_count);
// 	for (idx_t col_idx = 0; col_idx < Chunk().ColumnCount(); col_idx++) {
// 		FlatVector::Validity(Chunk().data[col_idx]).Initialize(sample_count);
// 	}
// }
//
// idx_t ReservoirSample::FillReservoir(DataChunk &input) {
// 	idx_t chunk_count = input.size();
// 	input.Flatten();
// 	auto num_added_samples = reservoir_chunk ? Chunk().size() : 0;
// 	D_ASSERT(num_added_samples <= sample_count);
//
// 	// required count is what we still need to add to the reservoir
// 	idx_t required_count;
// 	if (num_added_samples + chunk_count >= sample_count) {
// 		// have to limit the count of the chunk
// 		required_count = sample_count - num_added_samples;
// 	} else {
// 		// we copy the entire chunk
// 		required_count = chunk_count;
// 	}
// 	input.SetCardinality(required_count);
//
// 	// initialize the reservoir
// 	if (!reservoir_chunk) {
// 		CreateReservoirChunk(input.GetTypes());
// 	}
// 	Chunk().Append(input, false, nullptr, required_count);
// 	if (num_added_samples + required_count >= sample_count && GetPriorityQueueSize() == 0) {
// 		base_reservoir_sample->InitializeReservoirWeights(Chunk().size(), sample_count);
// 	}
//
// 	num_added_samples += required_count;
// 	Chunk().SetCardinality(num_added_samples);
// 	// check if there are still elements remaining in the Input data chunk that should be
// 	// randomly sampled and potentially added. This happens if we are on a boundary
// 	// for example, input.size() is 1024, but our sample size is 10
// 	if (required_count == chunk_count) {
// 		// we are done here
// 		return 0;
// 	}
// 	// we still need to process a part of the chunk
// 	// create a selection vector of the remaining elements
// 	SelectionVector sel(FIXED_SAMPLE_SIZE);
// 	for (idx_t i = required_count; i < chunk_count; i++) {
// 		sel.set_index(i - required_count, i);
// 	}
// 	// slice the input vector and continue
// 	input.Slice(sel, chunk_count - required_count);
// 	return input.size();
// }
//
// DataChunk &ReservoirSample::Chunk() {
// 	D_ASSERT(reservoir_chunk);
// 	return reservoir_chunk->chunk;
// }
//
// void ReservoirSample::Finalize() {
// 	return;
// }
//
// unique_ptr<IngestionSample> ReservoirSample::ConvertToIngestionSample() {
// 	auto ingestion_sample = make_uniq<IngestionSample>(sample_count);
//
// 	// first add the chunks
// 	auto chunk = GetChunkAndShrink();
// 	if (!chunk) {
// 		return nullptr;
// 	}
// 	D_ASSERT(chunk->size() <= FIXED_SAMPLE_SIZE);
// 	idx_t num_chunks_added = 0;
// 	// There should only be one chunk
// 	while (chunk) {
// 		num_chunks_added += 1;
// 		ingestion_sample->AddToReservoir(*chunk);
// 		chunk = GetChunkAndShrink();
// 	}
//
// 	if (num_chunks_added > 1) {
// 		throw InternalException("Error Converting Reservoir Sample to IngestionSample.");
// 	}
//
// 	// then assign the weights
// 	ingestion_sample->base_reservoir_sample = std::move(base_reservoir_sample);
//
// 	ingestion_sample->Verify();
// 	return ingestion_sample;
// }

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

unique_ptr<DataChunk> ReservoirSamplePercentage::GetChunk(idx_t offset) {
	if (!is_finalized) {
		Finalize();
	}
	idx_t finished_sample_index = 0;
	bool can_skip_finished_sample = true;
	while (can_skip_finished_sample && finished_sample_index < finished_samples.size()) {
		auto finished_sample_count = finished_samples.at(finished_sample_index)->reservoir_chunk->chunk.size();
		if (offset >= finished_sample_count) {
			offset -= finished_sample_count;
			finished_sample_index += 1;
		} else {
			can_skip_finished_sample = false;
		}
	}
	if (finished_sample_index >= finished_samples.size()) {
		return nullptr;
	}
	return finished_samples.at(finished_sample_index)->GetChunk(offset);
}

unique_ptr<BlockingSample> ReservoirSamplePercentage::Copy() const {
	throw InternalException("calling copy on reservoir sample percentage");
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
	    static_cast<double>(current_count) > sample_percentage * RESERVOIR_THRESHOLD || finished_samples.empty();
	if (current_count > 0 && sampled_more_than_required) {
		// create a new sample
		auto new_sample_size = static_cast<idx_t>(round(sample_percentage * static_cast<double>(current_count)));
		auto new_sample =
		    make_uniq<ReservoirSample>(allocator, new_sample_size, base_reservoir_sample->random.NextRandomInteger());
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

// void ReservoirSample::Verify() {
// 	if (destroyed) {
// 		return;
// 	}
// 	D_ASSERT(GetPriorityQueueSize() <= FIXED_SAMPLE_SIZE);
// 	auto base_reservoir_copy = base_reservoir_sample->Copy();
// 	unordered_set<idx_t> indexes;
// 	while (!base_reservoir_copy->reservoir_weights.empty()) {
// 		auto &pair = base_reservoir_copy->reservoir_weights.top();
// 		if (indexes.find(pair.second) == indexes.end()) {
// 			indexes.insert(pair.second);
// 			base_reservoir_copy->reservoir_weights.pop();
// 		} else {
// 			throw InternalException("found duplicate index when verifying sample");
// 		}
// 	}
// }

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
