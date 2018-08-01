
#include "common/types/hash.hpp"
#include "common/exception.hpp"

using namespace std;

namespace duckdb {
int32_t Hash(bool val) { throw NotImplementedException(""); }

int32_t Hash(int8_t val) { throw NotImplementedException(""); }

int32_t Hash(int16_t val) { throw NotImplementedException(""); }

int32_t Hash(int32_t x) {
	x = ((x >> 16) ^ x) * 0x45d9f3b;
	x = ((x >> 16) ^ x) * 0x45d9f3b;
	return (x >> 16) ^ x;
}

int32_t Hash(int64_t val) { throw NotImplementedException(""); }

int32_t Hash(double val) { throw NotImplementedException(""); }

int32_t Hash(char *str) {
	int32_t hash = 5381;
	int32_t c;

	while (c = *str++) {
		hash = ((hash << 5) + hash) + c;
	}

	return hash;
}

int32_t Hash(uint64_t integer) { throw NotImplementedException(""); }
} // namespace duckdb
