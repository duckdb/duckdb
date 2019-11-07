#include "duckdb/common/checksum.hpp"
#include "duckdb/common/types/hash.hpp"

using namespace std;

namespace duckdb {

uint64_t Checksum(uint8_t *buffer, size_t size) {
	uint64_t result = 5381;
	uint64_t *ptr = (uint64_t *)buffer;
	size_t i;
	// for efficiency, we first hash uint64_t values
	for (i = 0; i < size / 8; i++) {
		result ^= Hash(ptr[i]);
	}
	if (size - i * 8 > 0) {
		// the remaining 0-7 bytes we hash using a string hash
		result ^= Hash(buffer + i * 8, size - i * 8);
	}
	return result;
}

} // namespace duckdb
