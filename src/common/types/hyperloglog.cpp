#include "duckdb/common/types/hyperloglog.hpp"

#include "duckdb/common/exception.hpp"
#include "hyperloglog.hpp"

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

void HyperLogLog::Add(data_ptr_t element, idx_t size) {
	if (hll_add((robj *)hll, element, size) == C_ERR) {
		throw Exception("Could not add to HLL?");
	}
}

idx_t HyperLogLog::Count() {
	size_t result; // exception from size_t ban
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

unique_ptr<HyperLogLog> HyperLogLog::Merge(HyperLogLog logs[], idx_t count) {
	auto hlls_uptr = unique_ptr<robj *[]> { new robj *[count] };
	auto hlls = hlls_uptr.get();
	for (idx_t i = 0; i < count; i++) {
		hlls[i] = (robj *)logs[i].hll;
	}
	auto new_hll = hll_merge(hlls, count);
	if (!new_hll) {
		throw Exception("Could not merge HLLs");
	}
	return unique_ptr<HyperLogLog>(new HyperLogLog((void *)new_hll));
}
