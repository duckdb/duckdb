#include "duckdb/execution/operator/helper/physical_sample.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include <queue>
#include <cmath>
#include "duckdb/common/limits.hpp"
#include <random>

using namespace std;

namespace duckdb {

struct RandomEngine {
	std::mt19937 random_engine;
	RandomEngine() {
		random_device rd;
		random_engine.seed(rd());
	}

	//! Generate a random number between min and max
	double NextRandom(double min, double max) {
		uniform_real_distribution<double> dist(min, max);
		return dist(random_engine);
	}
	//! Generate a random number between 0 and 1
	double NextRandom() {
		return NextRandom(0, 1);
	}
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class SampleGlobalOperatorState : public GlobalOperatorState {
public:
	//! The lock for updating the global aggregate state
	mutex lock;
	//! The random generator
	RandomEngine random;
	//! The current reservoir
	ChunkCollection reservoir;
	//! Priority queue of [random element, index] for each of the elements in the sample
	priority_queue<std::pair<double, idx_t>> reservoir_weights;
	//! The next element to sample
	idx_t next_index;
	//! The reservoir threshold of the current min entry
	double min_threshold;
	//! The reservoir index of the current min entry
	idx_t min_entry;
	//! The current count towards next index (i.e. we will replace an entry in next_index - current_count tuples)
	idx_t current_count;

	//! Sets the next index to insert into the reservoir based on the reservoir weights
	void SetNextEntry() {
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
		next_index = idx_t(round(X_w));
		current_count = 0;
	}

	//! Replace a single element of the input
	void ReplaceElement(DataChunk &input, idx_t index_in_chunk) {
		// replace the entry in the reservoir
		// 8. The item in R with the minimum key is replaced by item vi
		for(idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
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
};

unique_ptr<GlobalOperatorState> PhysicalSample::GetGlobalState(ClientContext &context) {
	return make_unique<SampleGlobalOperatorState>();
}

void PhysicalSample::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                         DataChunk &input) {
	if (sample_count == 0) {
		return;
	}
	// we implement reservoir sampling without replacement and exponential jumps here
	// the algorithm is adopted from the paper Weighted random sampling with a reservoir by Pavlos S. Efraimidis et al.
	// note that the original algorithm is about weighted sampling; this is a simplified approach for uniform sampling
	auto &gstate = (SampleGlobalOperatorState &)state;
	lock_guard<mutex> glock(gstate.lock);

	// Input: A population V of n weighted items
	// Output: A reservoir R with a size m
	// 1: The first m items of V are inserted into R
	idx_t chunk_count = input.size();
	input.Normalify();
	// first we need to check if the reservoir already has "m" elements
	if (gstate.reservoir.Count() < sample_count) {
		// we have not: append to the reservoir
		idx_t required_count;
		if (gstate.reservoir.Count() + chunk_count >= sample_count) {
			// have to limit the count of the chunk
			required_count = sample_count - gstate.reservoir.Count();
		} else {
			// we copy the entire chunk
			required_count = chunk_count;
		}
		// instead of copying we just change the pointer in the current chunk
		input.SetCardinality(required_count);
		gstate.reservoir.Append(input);

		if (gstate.reservoir.Count() == sample_count) {
			// our reservoir is full: initialize the actual reservoir
			// 2. For each item vi ∈ R: Calculate a key ki = random(0, 1)
			// we then define the threshold to enter the reservoir T_w as the minimum key of R
			// we use a priority queue to extract the minimum key in O(1) time
			for(idx_t i = 0; i < sample_count; i++) {
				double k_i = gstate.random.NextRandom();
				gstate.reservoir_weights.push(make_pair(-k_i, i));
			}
			// now that we have the sample, we start our replacement strategy
			// 4. Repeat Steps 5–10 until the population is exhausted
			gstate.SetNextEntry();
		}

		// check if there are still elements remaining
		// this happens if we are on a boundary
		// for example, input.size() is 1024, but our sample size is 10
		if (required_count == chunk_count) {
			// we are done here
			return;
		}
		// we still need to process a part of the chunk
		// create a selection vector of the remaining elements
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		for(idx_t i = required_count; i < chunk_count; i++) {
			sel.set_index(i - required_count, i);
		}
		// slice the input vector and continue
		input.Slice(sel, chunk_count - required_count);
	}
	// find the position of gstate.next_index relative to gstate.current_count
	idx_t remaining = input.size();
	idx_t base_offset = 0;
	while(true) {
		idx_t offset = gstate.next_index - gstate.current_count;
		if (offset >= remaining) {
			// not in this chunk! increment current count and go to the next chunk
			gstate.current_count += remaining;
			return;
		}
		// in this chunk! replace the element
		gstate.ReplaceElement(input, base_offset + offset);
		// shift the chunk forward
		remaining -= offset;
		base_offset += offset;
	}
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalSampleOperatorState : public PhysicalOperatorState {
public:
	PhysicalSampleOperatorState(PhysicalOperator &op, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), position(0) {
	}

	idx_t position;
};

void PhysicalSample::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalSampleOperatorState *>(state_);
	auto &sink = (SampleGlobalOperatorState &)*this->sink_state;
	auto &chunks = sink.reservoir.Chunks();
	if (state->position >= chunks.size()) {
		return;
	}
	chunk.Reference(*chunks[state->position]);
	state->position++;
}

unique_ptr<PhysicalOperatorState> PhysicalSample::GetOperatorState() {
	return make_unique<PhysicalSampleOperatorState>(*this, children[0].get());
}

string PhysicalSample::ParamsToString() const {
	return to_string(sample_count);
}

} // namespace duckdb
