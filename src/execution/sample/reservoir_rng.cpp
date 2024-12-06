#include "duckdb/execution/reservoir_sample.hpp"

namespace duckdb {

ReservoirRNG::ReservoirRNG(int64_t seed) : RandomEngine(seed) {};

ReservoirRNG::result_type ReservoirRNG::operator()() {
	return NextRandomInteger();
};

constexpr ReservoirRNG::result_type ReservoirRNG::min() {
	return NumericLimits<result_type>::Minimum();
};

constexpr ReservoirRNG::result_type ReservoirRNG::max() {
	return NumericLimits<result_type>::Maximum();
};

} // namespace duckdb
