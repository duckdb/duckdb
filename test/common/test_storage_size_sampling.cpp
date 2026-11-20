#include "catch.hpp"
#include "duckdb/storage/checkpoint/checkpointed_table_size.hpp"

#include <algorithm>
#include <cmath>
#include <iostream>

using namespace duckdb;

namespace {

struct StorageSamplingGroup {
	idx_t rows;
	double bytes;
};

enum class StorageSamplingWeight { ROWS, UNIFORM };

struct StorageSamplingDistribution {
	struct Outcome {
		double weight;
		double estimate;
	};

	StorageSamplingDistribution(const duckdb::vector<StorageSamplingGroup> &groups, idx_t budget,
	                            StorageSamplingWeight method)
	    : strata(MinValue<idx_t>(budget, groups.size())) {
		auto group_weight = [method](idx_t rows) {
			return method == StorageSamplingWeight::ROWS ? double(rows) : double(rows != 0);
		};
		idx_t begin = 0;
		for (idx_t i = 0; i < strata.size(); i++) {
			auto end = begin + groups.size() / strata.size() + (i < groups.size() % strata.size());
			double stratum_bytes = 0;
			double stratum_weight = 0;
			for (idx_t j = begin; j < end; j++) {
				stratum_bytes += groups[j].bytes;
				stratum_weight += group_weight(groups[j].rows);
			}
			for (idx_t j = begin; j < end; j++) {
				auto weight = group_weight(groups[j].rows);
				if (weight == 0) {
					continue;
				}
				auto estimate = stratum_weight * groups[j].bytes / weight;
				strata[i].push_back({weight, estimate});
				auto error = estimate - stratum_bytes;
				variance += weight / stratum_weight * error * error;
			}
			total_bytes += stratum_bytes;
			begin = end;
		}
	}

	double Draw(RandomEngine &random) const {
		double estimate = 0;
		for (auto &stratum : strata) {
			double total_weight = 0;
			double selected = 0;
			for (auto &outcome : stratum) {
				if (CheckpointedTableSize::SelectRowGroup(random, outcome.weight, total_weight)) {
					selected = outcome.estimate;
				}
			}
			estimate += selected;
		}
		return estimate;
	}

	duckdb::vector<duckdb::vector<Outcome>> strata;
	double total_bytes = 0;
	double variance = 0;
};

struct StorageSamplingFixture {
	string name;
	duckdb::vector<StorageSamplingGroup> groups;
};

static duckdb::vector<StorageSamplingFixture> StorageSamplingFixtures() {
	duckdb::vector<StorageSamplingFixture> fixtures {{"uniform", {}},
	                                                 {"compression_phases", {}},
	                                                 {"unaligned_phases", {}},
	                                                 {"short_groups", {}},
	                                                 {"equal_bytes_mixed_rows", {}},
	                                                 {"one_large_group", {}},
	                                                 {"mixed_rows_and_widths", {}}};
	const duckdb::vector<double> widths {1, 4, 16, 64};
	for (idx_t i = 0; i < 128; i++) {
		const idx_t rows = 8192;
		const auto mixed_rows = i % 4 == 0 ? idx_t(32) : rows;
		fixtures[0].groups.push_back({rows, double(rows * 8)});
		fixtures[1].groups.push_back({rows, double(rows) * widths[i / 32]});
		idx_t phase = i < 17 ? 0 : i < 61 ? 1 : i < 109 ? 2 : 3;
		fixtures[2].groups.push_back({rows, double(rows) * widths[phase]});
		fixtures[3].groups.push_back({mixed_rows, double(mixed_rows * 8)});
		fixtures[4].groups.push_back({mixed_rows, double(rows * 8)});
		fixtures[5].groups.push_back({rows, double(rows * 8) * (i == 37 ? 1000 : 1)});
		fixtures[6].groups.push_back({mixed_rows, double(mixed_rows) * widths[(i * 7) % 4] + 256});
	}
	return fixtures;
}

} // namespace

TEST_CASE("Single-item reservoir probabilities follow the weights", "[storage_size_sampling]") {
	RandomEngine random(42);
	const duckdb::vector<double> weights {0, 1, 3, 6};
	duckdb::vector<idx_t> selected_counts(weights.size(), 0);
	const idx_t trials = 20000;
	for (idx_t trial = 0; trial < trials; trial++) {
		double total_weight = 0;
		idx_t selected = 0;
		for (idx_t i = 0; i < weights.size(); i++) {
			if (CheckpointedTableSize::SelectRowGroup(random, weights[i], total_weight)) {
				selected = i;
			}
		}
		selected_counts[selected]++;
	}
	REQUIRE(selected_counts[0] == 0);
	for (idx_t i = 1; i < weights.size(); i++) {
		REQUIRE(std::abs(double(selected_counts[i]) / double(trials) - weights[i] / 10) < 0.02);
	}
}

TEST_CASE("Storage sampling is repeatable and estimates the population total", "[storage_size_sampling]") {
	const duckdb::vector<StorageSamplingGroup> groups {{0, 0}, {1, 10}, {3, 40}, {6, 90}};
	for (auto method : {StorageSamplingWeight::ROWS, StorageSamplingWeight::UNIFORM}) {
		StorageSamplingDistribution distribution(groups, 1, method);
		RandomEngine random(42);
		RandomEngine repeat(42);
		double sum = 0;
		double sum_squares = 0;
		const idx_t trials = 20000;
		for (idx_t trial = 0; trial < trials; trial++) {
			auto estimate = distribution.Draw(random);
			sum += estimate;
			auto error = estimate - distribution.total_bytes;
			sum_squares += error * error;
			if (trial < 20) {
				REQUIRE(estimate == distribution.Draw(repeat));
			}
		}
		REQUIRE(std::abs(sum / double(trials) - distribution.total_bytes) < 3);
		REQUIRE(sum_squares / double(trials) == Approx(distribution.variance).epsilon(0.05));
	}
}

TEST_CASE("Compare storage size sampling error distributions", "[.][storage_size_sampling_benchmark]") {
	const idx_t trials = 20000;
	const int64_t seed = 42;
	std::cout << "case,method,budget,trials,seed,bias_pct,rmse_pct,theoretical_rmse_pct,p50_abs_error_pct,p95_abs_"
	             "error_pct\n";
	for (auto &fixture : StorageSamplingFixtures()) {
		for (idx_t budget : {1, 4, 16, 64, 128}) {
			for (auto method : {StorageSamplingWeight::ROWS, StorageSamplingWeight::UNIFORM}) {
				StorageSamplingDistribution distribution(fixture.groups, budget, method);
				const auto actual = distribution.total_bytes;
				RandomEngine random(seed);
				double sum = 0;
				double sum_squares = 0;
				duckdb::vector<double> absolute_errors;
				absolute_errors.reserve(trials);
				for (idx_t trial = 0; trial < trials; trial++) {
					auto estimate = distribution.Draw(random);
					auto error = 100 * (estimate - actual) / actual;
					sum += error;
					sum_squares += error * error;
					absolute_errors.push_back(std::abs(error));
				}
				std::sort(absolute_errors.begin(), absolute_errors.end());
				auto name = method == StorageSamplingWeight::ROWS ? string("rows") : string("uniform");
				auto theoretical_rmse = 100 * std::sqrt(distribution.variance) / actual;
				std::cout << fixture.name << ',' << name << ',' << budget << ',' << trials << ',' << seed << ','
				          << sum / double(trials) << ',' << std::sqrt(sum_squares / double(trials)) << ','
				          << theoretical_rmse << ',' << absolute_errors[trials / 2] << ','
				          << absolute_errors[trials * 95 / 100] << '\n';
				if (budget == fixture.groups.size() || distribution.variance == 0) {
					REQUIRE(sum_squares == 0);
				}
			}
		}
	}
}
