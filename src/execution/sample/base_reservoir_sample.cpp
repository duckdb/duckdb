#include "duckdb/execution/reservoir_sample.hpp"
#include <math.h>

namespace duckdb {

double BaseReservoirSampling::GetMinWeightFromTuplesSeen(idx_t rows_seen_total) {
	// this function was obtained using https://mycurvefit.com. Inputting multiple x, y values into
	// The
	switch (rows_seen_total) {
	case 0:
		return 0;
	case 1:
		return 0.000161;
	case 2:
		return 0.530136;
	case 3:
		return 0.693454;
	default: {
		return (0.99 - 0.355 * std::exp(-0.07 * static_cast<double>(rows_seen_total)));
	}
	}
}

BaseReservoirSampling::BaseReservoirSampling(int64_t seed) : random(seed) {
	next_index_to_sample = 0;
	min_weight_threshold = 0;
	min_weighted_entry_index = 0;
	num_entries_to_skip_b4_next_sample = 0;
	num_entries_seen_total = 0;
}

BaseReservoirSampling::BaseReservoirSampling() : BaseReservoirSampling(1) {
}

unique_ptr<BaseReservoirSampling> BaseReservoirSampling::Copy() {
	auto ret = make_uniq<BaseReservoirSampling>(1);
	ret->reservoir_weights = reservoir_weights;
	ret->next_index_to_sample = next_index_to_sample;
	ret->min_weight_threshold = min_weight_threshold;
	ret->min_weighted_entry_index = min_weighted_entry_index;
	ret->num_entries_to_skip_b4_next_sample = num_entries_to_skip_b4_next_sample;
	ret->num_entries_seen_total = num_entries_seen_total;
	return ret;
}

void BaseReservoirSampling::InitializeReservoirWeights(idx_t cur_size, idx_t sample_size) {
	//! 1: The first m items of V are inserted into R
	//! first we need to check if the reservoir already has "m" elements
	//! 2. For each item vi ∈ R: Calculate a key ki = random(0, 1)
	//! we then define the threshold to enter the reservoir T_w as the minimum key of R
	//! we use a priority queue to extract the minimum key in O(1) time
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
	D_ASSERT(!reservoir_weights.empty());
	//! 4. Let r = random(0, 1) and Xw = log(r) / log(T_w)
	auto &min_key = reservoir_weights.top();
	double t_w = -min_key.first;
	double r = random.NextRandom32();
	double x_w = log(r) / log(t_w);
	//! 5. From the current item vc skip items until item vi , such that:
	//! 6. wc +wc+1 +···+wi−1 < Xw <= wc +wc+1 +···+wi−1 +wi
	//! since all our weights are 1 (uniform sampling), we can just determine the amount of elements to skip
	min_weight_threshold = t_w;
	min_weighted_entry_index = min_key.second;
	next_index_to_sample = MaxValue<idx_t>(1, idx_t(round(x_w)));
	num_entries_to_skip_b4_next_sample = 0;
}

void BaseReservoirSampling::ReplaceElementWithIndex(idx_t entry_index, double with_weight, bool pop) {

	if (pop) {
		reservoir_weights.pop();
	}
	double r2 = with_weight;
	//! now we insert the new weight into the reservoir
	reservoir_weights.emplace(-r2, entry_index);
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
	reservoir_weights.emplace(-r2, min_weighted_entry_index);
	//! we update the min entry with the new min entry in the reservoir
	SetNextEntry();
}

void BaseReservoirSampling::UpdateMinWeightThreshold() {
	if (!reservoir_weights.empty()) {
		min_weight_threshold = -reservoir_weights.top().first;
		min_weighted_entry_index = reservoir_weights.top().second;
		return;
	}
	min_weight_threshold = 1;
}

void BaseReservoirSampling::FillWeights(SelectionVector &sel, idx_t &sel_size) {
	if (!reservoir_weights.empty()) {
		return;
	}
	D_ASSERT(reservoir_weights.empty());
	auto num_entries_seen_normalized = num_entries_seen_total / FIXED_SAMPLE_SIZE;
	auto min_weight = GetMinWeightFromTuplesSeen(num_entries_seen_normalized);
	for (idx_t i = 0; i < sel_size; i++) {
		auto weight = random.NextRandom(min_weight, 1);
		reservoir_weights.emplace(-weight, i);
	}
	D_ASSERT(reservoir_weights.size() <= sel_size);
	SetNextEntry();
}

} // namespace duckdb
