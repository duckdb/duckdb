#include "duckdb/common/io_latency_model.hpp"
#include "duckdb/common/random_engine.hpp"
#include <cmath>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

namespace duckdb {

IoLatencyModel::IoLatencyModel(double mean_ms_p, double stddev_ms_p)
    : mean_ms(mean_ms_p), stddev_ms(stddev_ms_p), log_mean(0.0), log_stddev(0.0) {
	if (stddev_ms > 0.0 && mean_ms > 0.0) {
		double variance = stddev_ms * stddev_ms;
		double mean_squared = mean_ms * mean_ms;
		log_mean = std::log(mean_squared / std::sqrt(variance + mean_squared));
		log_stddev = std::sqrt(std::log(1.0 + variance / mean_squared));
	}
}

double IoLatencyModel::SampleLatency(RandomEngine &random) {
	if (stddev_ms <= 0.0) {
		return mean_ms;
	}
	double u1 = 0;
	do {
		u1 = random.NextRandom();
	} while (u1 <= 0.0);
	double u2 = random.NextRandom();
	double z = std::sqrt(-2.0 * std::log(u1)) * std::cos(2.0 * M_PI * u2);
	return std::exp(log_mean + log_stddev * z);
}

} // namespace duckdb
