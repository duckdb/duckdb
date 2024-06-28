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

namespace duckdb {

enum class SampleType : uint8_t { BLOCKING_SAMPLE = 0, RESERVOIR_SAMPLE = 1, RESERVOIR_PERCENTAGE_SAMPLE = 2 };

class BaseReservoirSampling {
public:
	explicit BaseReservoirSampling(int64_t seed);
	BaseReservoirSampling();

	void InitializeReservoir(idx_t cur_size, idx_t sample_size);

	void SetNextEntry();

	void ReplaceElement(double with_weight = -1);
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
	explicit BlockingSample(int64_t seed) : old_base_reservoir_sample(seed), random(old_base_reservoir_sample.random) {
		base_reservoir_sample = nullptr;
	}
	virtual ~BlockingSample() {
	}

	//! Add a chunk of data to the sample
	virtual void AddToReservoir(DataChunk &input) = 0;

	virtual void Finalize() = 0;
	//! Fetches a chunk from the sample. Note that this method is destructive and should only be used after the
	//! sample is completely built.
	virtual unique_ptr<DataChunk> GetChunk() = 0;
	BaseReservoirSampling old_base_reservoir_sample;

	virtual void Serialize(Serializer &serializer) const;
	static unique_ptr<BlockingSample> Deserialize(Deserializer &deserializer);

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
	//! The reservoir sampling
	RandomEngine &random;
};

class ReservoirChunk {
public:
	ReservoirChunk() {
	}

	DataChunk chunk;
	void Serialize(Serializer &serializer) const;
	static unique_ptr<ReservoirChunk> Deserialize(Deserializer &deserializer);
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

	//! Fetches a chunk from the sample. Note that this method is destructive and should only be used after the
	//! sample is completely built.
	unique_ptr<DataChunk> GetChunk() override;
	void Finalize() override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<BlockingSample> Deserialize(Deserializer &deserializer);

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
	unique_ptr<DataChunk> reservoir_data_chunk;
	unique_ptr<ReservoirChunk> reservoir_chunk;
};

//! The reservoir sample sample_size class maintains a streaming sample of variable size
class ReservoirSamplePercentage : public BlockingSample {
	constexpr static idx_t RESERVOIR_THRESHOLD = 100000;

public:
	static constexpr const SampleType TYPE = SampleType::RESERVOIR_PERCENTAGE_SAMPLE;

public:
	ReservoirSamplePercentage(Allocator &allocator, double percentage, int64_t seed = -1);
	explicit ReservoirSamplePercentage(double percentage, int64_t seed = -1);

	//! Add a chunk of data to the sample
	void AddToReservoir(DataChunk &input) override;

	//! Fetches a chunk from the sample. Note that this method is destructive and should only be used after the
	//! sample is completely built.
	unique_ptr<DataChunk> GetChunk() override;
	void Finalize() override;

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
	//! Whether or not the stream is finalized. The stream is automatically finalized on the first call to GetChunk();
	bool is_finalized;
};

} // namespace duckdb
