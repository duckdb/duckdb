#include "duckdb/execution/reservoir_sample.hpp"

namespace duckdb {

unordered_map<idx_t, double> BaseReservoirSampling::tuples_to_min_weight_map = {
    {0, 0},         {1, 0.000161},  {2, 0.530136},  {3, 0.693454},  {4, 0.768098},  {5, 0.813462},  {6, 0.842048},
    {7, 0.864743},  {8, 0.880825},  {9, 0.894071},  {10, 0.905238}, {11, 0.912615}, {12, 0.919314}, {13, 0.925869},
    {14, 0.930331}, {15, 0.934715}, {16, 0.938872}, {17, 0.942141}, {18, 0.945010}, {19, 0.947712}, {20, 0.949381},
    {21, 0.952094}, {22, 0.954973}, {23, 0.956840}, {24, 0.958685}, {25, 0.960314}, {26, 0.961985}, {27, 0.963819},
    {28, 0.965385}, {29, 0.966284}, {30, 0.966994}, {31, 0.968070}, {32, 0.969026}, {33, 0.969987}, {34, 0.970997},
    {35, 0.971923}, {36, 0.972774}, {37, 0.973473}, {38, 0.974157}, {39, 0.974998}, {40, 0.975458}, {41, 0.975869},
    {42, 0.976474}, {43, 0.976995}, {44, 0.977581}, {45, 0.978220}, {46, 0.978649}, {47, 0.979157}, {48, 0.979721},
    {49, 0.980169}, {50, 0.980685}, {51, 0.981087}, {52, 0.981425}, {53, 0.981626}, {54, 0.981905}, {55, 0.982207},
    {56, 0.982534}, {57, 0.982756}, {58, 0.983048}, {59, 0.983485}, {60, 0.983875}};

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

void BaseReservoirSampling::FillWeights(vector<idx_t> &actual_sample_indexes) {
	D_ASSERT(actual_sample_indexes.size() == FIXED_SAMPLE_SIZE);
	D_ASSERT(reservoir_weights.empty());
	auto min_weight_index = num_entries_seen_total / FIXED_SAMPLE_SIZE;
	auto min_weight = tuples_to_min_weight_map[min_weight_index];
	for (auto &index : actual_sample_indexes) {
		auto weight = random.NextRandom(min_weight, 1);
		reservoir_weights.emplace(-weight, index);
	}
	D_ASSERT(reservoir_weights.size() == FIXED_SAMPLE_SIZE);
	SetNextEntry();
	actual_sample_indexes.clear();
}

} // namespace duckdb
