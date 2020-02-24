#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/string_type.hpp"

#include "duckdb/common/exception.hpp"

#include <functional>

using namespace std;

namespace duckdb {

template <> uint64_t Hash(uint64_t val) {
	return murmurhash64(val);
}

template <> uint64_t Hash(int64_t val) {
	return murmurhash64((uint64_t)val);
}

template <> uint64_t Hash(float val) {
	return std::hash<float>{}(val);
}

template <> uint64_t Hash(double val) {
	return std::hash<double>{}(val);
}

template <> uint64_t Hash(const char *str) {
	uint64_t hash = 5381;
	uint64_t c;

	while ((c = *str++)) {
		hash = ((hash << 5) + hash) + c;
	}

	return hash;
}

template <> uint64_t Hash(string_t val) {
	return Hash(val.GetData(), val.GetSize());
}

template <> uint64_t Hash(char *val) {
	return Hash<const char *>(val);
}

uint64_t Hash(const char *val, size_t size) {
	uint64_t hash = 5381;

	for (size_t i = 0; i < size; i++) {
		hash = ((hash << 5) + hash) + val[i];
	}

	return hash;
}

uint64_t Hash(char *val, size_t size) {
	return Hash((const char *)val, size);
}

uint64_t Hash(uint8_t *val, size_t size) {
	return Hash((const char *)val, size);
}

} // namespace duckdb
