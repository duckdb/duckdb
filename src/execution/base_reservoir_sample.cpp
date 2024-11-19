#include "duckdb/execution/reservoir_sample.hpp"

namespace duckdb {

vector<double> BaseReservoirSampling::TuplesToMinWeightMap() {
	return vector<double> {
	    0,        0.000161, 0.530136, 0.693454, 0.768098, 0.813462, 0.842048, 0.864743, 0.880825, 0.894071, 0.905238,
	    0.912615, 0.919314, 0.925869, 0.930331, 0.934715, 0.938872, 0.942141, 0.945010, 0.947712, 0.949381, 0.952094,
	    0.954973, 0.956840, 0.958685, 0.960314, 0.961985, 0.963819, 0.965385, 0.966284, 0.966994, 0.968070, 0.969026,
	    0.969987, 0.970997, 0.971923, 0.972774, 0.973473, 0.974157, 0.974998, 0.975458, 0.975869, 0.976474, 0.976995,
	    0.977581, 0.978220, 0.978649, 0.979157, 0.979721, 0.980169, 0.980685, 0.981087, 0.981425, 0.981626, 0.981905,
	    0.982207, 0.982534, 0.982756, 0.983048, 0.983485, 0.983875, 0.984114, 0.984346, 0.984516, 0.984743, 0.985056,
	    0.985174, 0.985418, 0.985610, 0.985758, 0.985978, 0.986105, 0.986306, 0.986490, 0.986644, 0.986842, 0.987089,
	    0.987331, 0.987456, 0.987624, 0.987795, 0.987920, 0.988059, 0.988160, 0.988311, 0.988403, 0.988626, 0.988709,
	    0.988884, 0.989072, 0.989206, 0.989321, 0.989406, 0.989492, 0.989623, 0.989738, 0.989835, 0.989995, 0.990080,
	    0.990164, 0.990276, 0.990394, 0.990505, 0.990602, 0.990699, 0.990840, 0.990906, 0.991045, 0.991108, 0.991183,
	    0.991300, 0.991409, 0.991475, 0.991539, 0.991595, 0.991669, 0.991718, 0.991800, 0.991885, 0.991946, 0.991975};
}

ReservoirSample::ReservoirSample(Allocator &allocator, idx_t sample_count, int64_t seed)
    : BlockingSample(seed), allocator(allocator), sample_count(sample_count) {
	type = SampleType::RESERVOIR_SAMPLE;
}

ReservoirSample::ReservoirSample(idx_t sample_count, int64_t seed)
    : ReservoirSample(Allocator::DefaultAllocator(), sample_count, seed) {
}

void BaseReservoirSampling::IncreaseNumEntriesSeenTotal(idx_t count) {
	num_entries_seen_total += count;
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

void BaseReservoirSampling::InitializeReservoirWeights(idx_t cur_size, idx_t sample_size, idx_t index_offset) {
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
			idx_t index = i + index_offset;
			double k_i = random.NextRandom();
			reservoir_weights.emplace(-k_i, index);
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
		return;
	}
	min_weight_threshold = 1;
}

void BaseReservoirSampling::FillWeights(vector<idx_t> &actual_sample_indexes) {
	if (!reservoir_weights.empty()) {
		return;
	}
	D_ASSERT(actual_sample_indexes.size() <= FIXED_SAMPLE_SIZE);
	D_ASSERT(reservoir_weights.empty());
	auto min_weight_index = num_entries_seen_total / FIXED_SAMPLE_SIZE;
	auto tuples_to_min_weight_map = TuplesToMinWeightMap();
	if (tuples_to_min_weight_map.size() < min_weight_index) {
		min_weight_index = tuples_to_min_weight_map.size();
	}
	auto min_weight = tuples_to_min_weight_map[min_weight_index];
	for (auto &index : actual_sample_indexes) {
		auto weight = random.NextRandom(min_weight, 1);
		reservoir_weights.emplace(-weight, index);
	}
	D_ASSERT(reservoir_weights.size() <= FIXED_SAMPLE_SIZE);
	SetNextEntry();
}

} // namespace duckdb
