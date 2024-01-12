//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/reservoir_sample.hpp
//
//
//===----------------------------------------------------------------------===//

// whats the issue with sotring percentage samples in local state?
// mostly with merging
// if you have multiple unfinished samples during merging, then each unfinished sample needs to be merged
// The problem is if you have two samples that have seen below the reservoir threashold, but they have
// weights etc.
// I.e
// Sample 1 has 40,000 tuples after seeing 68640 rows.
// Sample 2 has 40,000 tuples after seeing 85600 rows.
//
// Desired result after merging is
// Sample (1+2) has 40,000 tuples after seeing 100,000 rows
// Sample 3 has 8640 tuples after seeing 54,240 rows
//           - all samples in sample 3 have the weights from when they were in sample 2.

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/types/data_chunk.hpp"

#include "duckdb/common/queue.hpp"

namespace duckdb {

class BaseReservoirSampling {
public:
	explicit BaseReservoirSampling(int64_t seed);
	BaseReservoirSampling();

	void InitializeReservoir(idx_t cur_size, idx_t sample_size);

	void SetNextEntry();

	void ReplaceElement(double with_weight = -1);
	//! The random generator
	RandomEngine random;
	//! Priority queue of [random element, index] for each of the elements in the sample
	std::priority_queue<std::pair<double, idx_t>> reservoir_weights;
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
};

class BlockingSample {
public:
	explicit BlockingSample(int64_t seed) : base_reservoir_sample(seed), random(base_reservoir_sample.random) {
	}
	virtual ~BlockingSample() {
	}

	//! Add a chunk of data to the sample
	virtual void AddToReservoir(DataChunk &input) = 0;

	virtual void Finalize() = 0;

	//! Fetches a chunk from the sample. Note that this method is destructive and should only be used when
	//! querying from a live sample and not a table collected sample.
	virtual unique_ptr<DataChunk> GetChunkAndShrink() = 0;
	virtual unique_ptr<DataChunk> GetChunk(idx_t offset = 0) = 0;
	BaseReservoirSampling base_reservoir_sample;

protected:
	//! The reservoir sampling
	RandomEngine &random;
};

//! The reservoir sample class maintains a streaming sample of fixed size "sample_count"
class ReservoirSample : public BlockingSample {
public:
	ReservoirSample(Allocator &allocator, idx_t sample_count, int64_t seed);

	//! Add a chunk of data to the sample
	void AddToReservoir(DataChunk &input) override;

	//! Fetches a chunk from the sample. Note that this method is destructive and should only be used after the
	//! sample is completely built.
	unique_ptr<DataChunk> GetChunkAndShrink() override;
	unique_ptr<DataChunk> GetChunk(idx_t offset = 0) override;
	void Finalize() override;

private:
	//! Replace a single element of the input
	void ReplaceElement(DataChunk &input, idx_t index_in_chunk, double with_weight = -1);
	void InitializeReservoir(DataChunk &input);
	//! Fills the reservoir up until sample_count entries, returns how many entries are still required
	idx_t FillReservoir(DataChunk &input);

public:
	Allocator &allocator;
	//! The size of the reservoir sample.
	//! when calculating percentages, it is set to reservoir_threshold * percentage
	//! when explicit number used, sample_count = number
	idx_t sample_count;
	bool reservoir_initialized;
	//! The current reservoir
	unique_ptr<DataChunk> reservoir_chunk;
};

//! The reservoir sample sample_size class maintains a streaming sample of variable size
class ReservoirSamplePercentage : public BlockingSample {
	constexpr static idx_t RESERVOIR_THRESHOLD = 100000;

public:
	ReservoirSamplePercentage(Allocator &allocator, double percentage, int64_t seed);

	//! Add a chunk of data to the sample
	void AddToReservoir(DataChunk &input) override;

	//! Fetches a chunk from the sample. Note that this method is destructive and should only be used after the
	//! sample is completely built.
	unique_ptr<DataChunk> GetChunkAndShrink() override;
	unique_ptr<DataChunk> GetChunk(idx_t offset = 0) override;
	void Finalize() override;

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

} // namespace duckdb
