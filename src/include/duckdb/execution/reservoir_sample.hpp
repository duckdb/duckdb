//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/reservoir_sample.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/types/data_chunk.hpp"

#include "duckdb/common/queue.hpp"

#define FIXED_SAMPLE_SIZE      STANDARD_VECTOR_SIZE
#define PERCENTAGE_SAMPLE_SIZE 1

namespace duckdb {

enum class SampleType : uint8_t {
	BLOCKING_SAMPLE = 0,
	RESERVOIR_SAMPLE = 1,
	RESERVOIR_PERCENTAGE_SAMPLE = 2,
	INGESTION_SAMPLE = 3
};

//! Resevoir sampling is based on the 2005 paper "Weighted Random Sampling" by Efraimidis and Spirakis
class BaseReservoirSampling {
public:
	explicit BaseReservoirSampling(int64_t seed);
	BaseReservoirSampling();

	void InitializeReservoirWeights(idx_t cur_size, idx_t sample_size, idx_t index_offset = 0);

	void SetNextEntry();

	void ReplaceElementWithIndex(idx_t entry_index, double with_weight, bool pop = true);
	void ReplaceElement(double with_weight = -1);

	void IncreaseNumEntriesSeenTotal(idx_t count);

	//! Go from the naive sampling to the reservoir sampling
	//! Naive samping will not collect weights, but when we serialize
	//! we need to serialize weights again.
	void FillWeights(vector<idx_t> &actual_sample_indexes);

	unique_ptr<BaseReservoirSampling> Copy();
	// BaseReservoirSampling Copy2();
	//! The random generator
	RandomEngine random;

	//! The next element to sample
	idx_t next_index_to_sample;
	//! The reservoir threshold of the current min entry
	double min_weight_threshold;
	//! The reservoir index of the current min entry
	idx_t min_weighted_entry_index;
	//! The current count towards next index (i.e. we will replace an entry in next_index - current_count tuples)
	//! The number of entries "seen" before choosing one that will go in our reservoir sample.
	idx_t num_entries_to_skip_b4_next_sample;
	//! when collecting a sample in parallel, we want to know how many values each thread has seen
	//! so we can collect the samples from the thread local states in a uniform manner
	idx_t num_entries_seen_total;
	//! Priority queue of [random element, index] for each of the elements in the sample
	std::priority_queue<std::pair<double, idx_t>> reservoir_weights;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<BaseReservoirSampling> Deserialize(Deserializer &deserializer);

	static unordered_map<idx_t, double> tuples_to_min_weight_map;
	// Blocking sample is a virtual class. It should be allowed to see the weights and
	// of tuples in the sample. The blocking sample can then easily maintain statisitcal properties
	// from the sample point of view.
	friend class BlockingSample;
};

class BlockingSample {
public:
	static constexpr const SampleType TYPE = SampleType::BLOCKING_SAMPLE;

	unique_ptr<BaseReservoirSampling> base_reservoir_sample;
	//! The sample type
	SampleType type;
	//! has the sample been destroyed due to updates to the referenced table
	bool destroyed;

public:
	explicit BlockingSample(int64_t seed = -1)
	    : base_reservoir_sample(make_uniq<BaseReservoirSampling>(seed)), type(SampleType::BLOCKING_SAMPLE),
	      destroyed(false) {
	}
	virtual ~BlockingSample() {
	}

	//! Add a chunk of data to the sample
	virtual void AddToReservoir(DataChunk &input) = 0;

	virtual unique_ptr<BlockingSample> Copy() const = 0;

	virtual void Finalize() = 0;

	//! Fetches a chunk from the sample. Note that this method is destructive and should only be used when
	//! querying from a live sample and not a table collected sample.
	virtual unique_ptr<DataChunk> GetChunkAndShrink() = 0;
	virtual unique_ptr<DataChunk> GetChunk(idx_t offset = 0) = 0;
	virtual void Destroy();

	virtual void Serialize(Serializer &serializer) const;
	static unique_ptr<BlockingSample> Deserialize(Deserializer &deserializer);

	//! Helper functions needed to merge two reservoirs while respecting weights of sampled rows
	std::pair<double, idx_t> PopFromWeightQueue();
	double GetMinWeightThreshold();
	idx_t GetPriorityQueueSize();

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE && TARGET::TYPE != SampleType::BLOCKING_SAMPLE) {
			throw InternalException("Failed to cast sample to type - sample type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE && TARGET::TYPE != SampleType::BLOCKING_SAMPLE) {
			throw InternalException("Failed to cast sample to type - sample type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class IngestionSample;

class ReservoirChunk {
public:
	ReservoirChunk() {
	}

	DataChunk chunk;
	void Serialize(Serializer &serializer) const;
	static unique_ptr<ReservoirChunk> Deserialize(Deserializer &deserializer);

	unique_ptr<ReservoirChunk> Copy() const;
};

//! The reservoir sample class maintains a streaming sample of fixed size "sample_count"
class ReservoirSample : public BlockingSample {
public:
	static constexpr const SampleType TYPE = SampleType::RESERVOIR_SAMPLE;

public:
	ReservoirSample(Allocator &allocator, idx_t sample_count, int64_t seed = 1);
	explicit ReservoirSample(idx_t sample_count, int64_t seed = 1);

	//! Add a chunk of data to the sample
	void AddToReservoir(DataChunk &input) override;

	unique_ptr<IngestionSample> ConvertToIngestionSample();

	unique_ptr<BlockingSample> Copy() const override;

	//! Fetches a chunk from the sample. Note that this method is destructive and should only be used after the
	//! sample is completely built.
	unique_ptr<DataChunk> GetChunkAndShrink() override;
	unique_ptr<DataChunk> GetChunk(idx_t offset = 0) override;
	void Destroy() override;
	void Finalize() override;
	void Verify();
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<BlockingSample> Deserialize(Deserializer &deserializer);

private:
	//! Replace a single element of the input
	void ReplaceElement(DataChunk &input, idx_t index_in_chunk, double with_weight = -1);
	void ReplaceElement(idx_t reservoir_chunk_index, DataChunk &input, idx_t index_in_input_chunk, double with_weight);

	void CreateReservoirChunk(const vector<LogicalType> &types);
	//! Fills the reservoir up until sample_count entries, returns how many entries are still required
	idx_t FillReservoir(DataChunk &input);

	DataChunk &Chunk();

public:
	Allocator &allocator;
	//! The size of the reservoir sample.
	//! when calculating percentages, it is set to reservoir_threshold * percentage
	//! when explicit number used, sample_count = number
	idx_t sample_count;
	//! The current reservoir
	unique_ptr<ReservoirChunk> reservoir_chunk;
};

//! The reservoir sample sample_size class maintains a streaming sample of variable size
class ReservoirSamplePercentage : public BlockingSample {
	constexpr static idx_t RESERVOIR_THRESHOLD = 100000;

public:
	static constexpr const SampleType TYPE = SampleType::RESERVOIR_PERCENTAGE_SAMPLE;

public:
	ReservoirSamplePercentage(Allocator &allocator, double percentage, int64_t seed = -1);
	ReservoirSamplePercentage(double percentage, int64_t seed, idx_t reservoir_sample_size);
	explicit ReservoirSamplePercentage(double percentage, int64_t seed = -1);

	//! Add a chunk of data to the sample
	void AddToReservoir(DataChunk &input) override;

	unique_ptr<BlockingSample> Copy() const override;

	//! Fetches a chunk from the sample. Note that this method is destructive and should only be used after the
	//! sample is completely built.
	unique_ptr<DataChunk> GetChunkAndShrink() override;
	//! Fetches a chunk from the sample. This method is not destructive
	unique_ptr<DataChunk> GetChunk(idx_t offset = 0) override;
	void Finalize() override;
	// void FromReservoirSample(unique_ptr<ReservoirSample> other);

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<BlockingSample> Deserialize(Deserializer &deserializer);

private:
	Allocator &allocator;
	//! The sample_size to sample
	double sample_percentage;
	//! The fixed sample size of the sub-reservoirs
	idx_t reservoir_sample_size;

	//! The current sample
	unique_ptr<ReservoirSample> current_sample;

	//! The set of finished samples of the reservoir sample
	vector<unique_ptr<ReservoirSample>> finished_samples;

	//! The amount of tuples that have been processed so far (not put in the reservoir, just processed)
	idx_t current_count = 0;
	//! Whether or not the stream is finalized. The stream is automatically finalized on the first call to
	//! GetChunkAndShrink();
	bool is_finalized;
};

// Ingestion sample needs to inherit from blocking sample.
// this way it can be serialized (Maybe? Need to figure that out)

// Special Sample type for ingestion
class IngestionSample : public BlockingSample {
public:
	static constexpr const SampleType TYPE = SampleType::INGESTION_SAMPLE;

	constexpr static idx_t FIXED_SAMPLE_SIZE_MULTIPLIER = 10;
	constexpr static idx_t FAST_TO_SLOW_THRESHOLD = 20;
	// how much of every chunk we consider for sampling.
	// This can be lowered to improve ingestion performance.
	constexpr static double CHUNK_SAMPLE_PERCENTAGE = 1.00;

	IngestionSample(Allocator &allocator, int64_t seed);
	explicit IngestionSample(idx_t sample_count, int64_t seed = 1);

	// TODO: this will need more info to initiliaze the correct sample type
	unique_ptr<BlockingSample> ConvertToReservoirSampleToSerialize();

	//! Shrink the Ingestion Sample to only contain the tuples that are in the
	//! reservoir weights or are in the "actual_indexes"
	void Shrink();

	void SimpleMerge(IngestionSample &other);

	// Helper methods for Shrink().
	// Shrink has different logic depending on if the IngestionSample is still in
	// "Fast" mode or in "Slow" mode
	unique_ptr<DataChunk> CreateNewSampleChunk(vector<LogicalType> &types);
	SelectionVector CreateSelectionVectorFromReservoirWeights(vector<std::pair<double, idx_t>> &weights_indexes) const;
	static SelectionVector CreateSelectionVectorFromSimpleVector(vector<idx_t> &actual_indexes);

	unique_ptr<BlockingSample> Copy() const override;
	unique_ptr<BlockingSample> Copy(bool for_serialization) const;
	void Merge(unique_ptr<BlockingSample> other);

	//! Update the sample by pushing new sample rows to the end of the sample_chunk.
	//! The new sample rows are the tuples rows resulting from applying sel to other
	void UpdateSampleAppend(DataChunk &other, SelectionVector &sel, idx_t sel_count);
	//! Actually appends the new tuples. TODO: rename function to AppendToSample
	void UpdateSampleWithTypes(DataChunk &other, SelectionVector &sel, idx_t source_count, idx_t source_offset,
	                           idx_t target_offset);
	//! ??? Honestly don't know what the difference between this function and UpdateSampleWithTypes is.
	void UpdateSampleCopy(DataChunk &other, SelectionVector &sel, idx_t source_offset, idx_t target_offset, idx_t size);

	idx_t GetTuplesSeen();
	static bool ValidSampleType(const LogicalType &type);
	idx_t NumSamplesCollected();
	//! Add a chunk of data to the sample
	void AddToReservoir(DataChunk &input) override;

	//! Fetches a chunk from the sample. Note that this method is destructive and should only be used after the
	//! sample is completely built.
	unique_ptr<DataChunk> GetChunkAndShrink() override;
	unique_ptr<DataChunk> GetChunk(idx_t offset = 0) override;
	void Destroy() override;
	void Finalize() override;

	void PrintWeightsInOrder();
	void Verify();

	// when replacing samples in the chunk, it's possible
	// that an index gets replaced twice because the new weight assigned
	// is relatively low. This function loops through the max heap that holds the sample index
	// map is [index in input chunk] -> [index in sample chunk]. Both are zero-based
	// index in sample chunk is incremented by 1
	// index in input chunk is incremented in random amounts.
	// The base_reservoir_sampling gets updated however, so the indexes point to sample_chunk_offset +
	// (index_in_sample_chunk) this data can then be used to make a selection vector to copy over the samples from the
	// input chunk to the sample chunk
	unordered_map<idx_t, idx_t> GetReplacementIndexes(idx_t sample_chunk_offset, idx_t theoretical_chunk_length);
	unordered_map<idx_t, idx_t> GetReplacementIndexesFast(idx_t sample_chunk_offset, idx_t theoretical_chunk_length);

	idx_t sample_count;
	Allocator &allocator;
	//! given the first chunk, create the first chunk
	//! called be AddToReservoir()
	idx_t FillReservoir(DataChunk &chunk);

	unique_ptr<DataChunk> sample_chunk;

	vector<idx_t> actual_sample_indexes;
};

} // namespace duckdb
