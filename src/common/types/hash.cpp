
#include "common/types/hash.hpp"
#include "common/exception.hpp"

using namespace std;

namespace duckdb {

template <> int32_t Hash(uint64_t val) {
	return murmurhash64((uint32_t *)&val);
}

template <> int32_t Hash(int64_t val) { return murmurhash64((uint32_t *)&val); }

template <> int32_t Hash(double val) { return murmurhash64((uint32_t *)&val); }

template <> int32_t Hash(const char *str) {
	uint32_t hash = 5381;
	uint32_t c;

	while ((c = *str++)) {
		hash = ((hash << 5) + hash) + c;
	}

	return hash;
}

template <> int32_t Hash(char *val) { return Hash<const char *>(val); }

} // namespace duckdb
