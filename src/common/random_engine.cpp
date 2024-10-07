#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/numeric_utils.hpp"
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
		random_state->pcg.seed(NumericCast<uint64_t>(seed));
	}
}

RandomEngine::~RandomEngine() {
}

double RandomEngine::NextRandom(double min, double max) {
	D_ASSERT(max >= min);
	return min + (NextRandom() * (max - min));
}

double RandomEngine::NextRandom() {
	auto uint64 = NextRandomInteger64();
	return std::ldexp(uint64, -64);
}

double RandomEngine::NextRandom32(double min, double max) {
	D_ASSERT(max >= min);
	return min + (NextRandom32() * (max - min));
}

double RandomEngine::NextRandom32() {
	auto uint32 = NextRandomInteger();
	return std::ldexp(uint32, -32);
}

uint32_t RandomEngine::NextRandomInteger() {
	return random_state->pcg();
}

uint64_t RandomEngine::NextRandomInteger64() {
	return (static_cast<uint64_t>(NextRandomInteger()) << UINT64_C(32)) | static_cast<uint64_t>(NextRandomInteger());
}

uint32_t RandomEngine::NextRandomInteger(uint32_t min, uint32_t max) {
	return min + static_cast<uint32_t>(NextRandom() * double(max - min));
}

void RandomEngine::SetSeed(uint32_t seed) {
	random_state->pcg.seed(seed);
}

} // namespace duckdb
