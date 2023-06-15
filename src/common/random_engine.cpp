#include "duckdb/common/random_engine.hpp"
#include "pcg_random.hpp"
#include <random>

namespace duckdb {

struct RandomState {
	RandomState() {
	}

	pcg32 pcg;
};

RandomEngine::RandomEngine(int64_t seed) : random_state(make_uniq<RandomState>()) {
	if (seed < 0) {
		random_state->pcg.seed(pcg_extras::seed_seq_from<std::random_device>());
	} else {
		random_state->pcg.seed(seed);
	}
}

RandomEngine::~RandomEngine() {
}

double RandomEngine::NextRandom(double min, double max) {
	D_ASSERT(max >= min);
	return min + (NextRandom() * (max - min));
}

double RandomEngine::NextRandom() {
	return std::ldexp(random_state->pcg(), -32);
}
uint32_t RandomEngine::NextRandomInteger() {
	return random_state->pcg();
}

void RandomEngine::SetSeed(uint32_t seed) {
	random_state->pcg.seed(seed);
}

} // namespace duckdb
