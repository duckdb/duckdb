//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/reservoir_sample.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include <random>
#include <queue>

namespace duckdb {

struct RandomEngine {
	std::mt19937 random_engine;
	RandomEngine() {
		std::random_device rd;
		random_engine.seed(rd());
	}

	//! Generate a random number between min and max
	double NextRandom(double min, double max) {
		std::uniform_real_distribution<double> dist(min, max);
		return dist(random_engine);
	}
	//! Generate a random number between 0 and 1
	double NextRandom() {
		return NextRandom(0, 1);
	}
};

class BlockingSample {
public:
	//! Add a chunk of data to the sample
	virtual void AddToReservoir(DataChunk &input) = 0;

	//! Fetches a chunk from the sample. Note that this method is destructive and should only be used after the
	// sample is completely built.
	virtual unique_ptr<DataChunk> GetChunk() = 0;
};

//! The reservoir sample class maintains a streaming sample of fixed size "sample_count"
class ReservoirSample : public BlockingSample {
public:
	ReservoirSample(idx_t sample_count) :
		sample_count(sample_count), scan_position(0) {}


	//! Add a chunk of data to the sample
	void AddToReservoir(DataChunk &input) override;

	//! Fetches a chunk from the sample. Note that this method is destructive and should only be used after the
	// sample is completely built.
	unique_ptr<DataChunk> GetChunk() override;
private:
	//! Sets the next index to insert into the reservoir based on the reservoir weights
	void SetNextEntry();

	//! Replace a single element of the input
	void ReplaceElement(DataChunk &input, idx_t index_in_chunk);

	//! Fills the reservoir up until sample_count entries, returns how many entries are still required
	idx_t FillReservoir(DataChunk &input);

private:
	//! The random generator
	RandomEngine random;
	//! The current reservoir
	ChunkCollection reservoir;
	//! Priority queue of [random element, index] for each of the elements in the sample
	std::priority_queue<std::pair<double, idx_t>> reservoir_weights;
	//! The next element to sample
	idx_t next_index;
	//! The reservoir threshold of the current min entry
	double min_threshold;
	//! The reservoir index of the current min entry
	idx_t min_entry;
	//! The current count towards next index (i.e. we will replace an entry in next_index - current_count tuples)
	idx_t current_count;
	//! The size of the reservoir sample
	idx_t sample_count;
	//! The position in the scan of this sample
	idx_t scan_position;
};


} // namespace duckdb
