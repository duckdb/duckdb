#include "common/types/hash.hpp"

#include "common/exception.hpp"

using namespace std;

namespace duckdb {

template <> uint64_t Hash(uint64_t val) {
	return murmurhash64((uint32_t *)&val);
}

template <> uint64_t Hash(int64_t val) {
	return murmurhash64((uint32_t *)&val);
}

template <> uint64_t Hash(double val) {
	return murmurhash64((uint32_t *)&val);
}

template <> uint64_t Hash(const char *str) {
	uint64_t hash = 5381;
	uint64_t c;

	while ((c = *str++)) {
		hash = ((hash << 5) + hash) + c;
	}

	return hash;
}

template <> uint64_t Hash(char *val) {
	return Hash<const char *>(val);
}

} // namespace duckdb
