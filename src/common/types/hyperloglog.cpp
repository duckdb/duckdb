#include "common/types/hyperloglog.hpp"

#include "common/exception.hpp"
#include "hyperloglog.h"

using namespace duckdb;
using namespace std;

HyperLogLog::HyperLogLog() : hll(nullptr) {
	hll = hll_create();
}

HyperLogLog::HyperLogLog(void *hll) : hll(hll) {
}

HyperLogLog::~HyperLogLog() {
	hll_destroy((robj *)hll);
}

void HyperLogLog::Add(uint8_t *element, size_t size) {
	if (hll_add((robj *)hll, element, size) == C_ERR) {
		throw Exception("Could not add to HLL?");
	}
}

size_t HyperLogLog::Count() {
	size_t result;
	if (hll_count((robj *)hll, &result) != C_OK) {
		throw Exception("Could not count HLL?");
	}
	return result;
}

unique_ptr<HyperLogLog> HyperLogLog::Merge(HyperLogLog &other) {
	robj *hlls[2];
	hlls[0] = (robj *)hll;
	hlls[1] = (robj *)other.hll;
	auto new_hll = hll_merge(hlls, 2);
	if (!new_hll) {
		throw Exception("Could not merge HLLs");
	}
	return unique_ptr<HyperLogLog>(new HyperLogLog((void *)new_hll));
}

unique_ptr<HyperLogLog> HyperLogLog::Merge(HyperLogLog logs[], size_t count) {
	robj *hlls[count];
	for (size_t i = 0; i < count; i++) {
		hlls[i] = (robj *)logs[i].hll;
	}
	auto new_hll = hll_merge(hlls, count);
	if (!new_hll) {
		throw Exception("Could not merge HLLs");
	}
	return unique_ptr<HyperLogLog>(new HyperLogLog((void *)new_hll));
}
