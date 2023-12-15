#include "duckdb/common/constants.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {

constexpr const idx_t DConstants::INVALID_INDEX;
const row_t MAX_ROW_ID = 36028797018960000ULL;       // 2^55
const row_t MAX_ROW_ID_LOCAL = 72057594037920000ULL; // 2^56
const column_t COLUMN_IDENTIFIER_ROW_ID = (column_t)-1;
const sel_t ZERO_VECTOR[STANDARD_VECTOR_SIZE] = {0};
const double PI = 3.141592653589793;

const transaction_t TRANSACTION_ID_START = 4611686018427388000ULL;                // 2^62
const transaction_t MAX_TRANSACTION_ID = NumericLimits<transaction_t>::Maximum(); // 2^63
const transaction_t NOT_DELETED_ID = NumericLimits<transaction_t>::Maximum() - 1; // 2^64 - 1
const transaction_t MAXIMUM_QUERY_ID = NumericLimits<transaction_t>::Maximum();   // 2^64

bool IsPowerOfTwo(uint64_t v) {
	return (v & (v - 1)) == 0;
}

uint64_t NextPowerOfTwo(uint64_t v) {
	v--;
	v |= v >> 1;
	v |= v >> 2;
	v |= v >> 4;
	v |= v >> 8;
	v |= v >> 16;
	v |= v >> 32;
	v++;
	return v;
}

uint64_t PreviousPowerOfTwo(uint64_t v) {
	return NextPowerOfTwo((v / 2) + 1);
}

bool IsInvalidSchema(const string &str) {
	return str.empty();
}

bool IsInvalidCatalog(const string &str) {
	return str.empty();
}

bool IsRowIdColumnId(column_t column_id) {
	return column_id == COLUMN_IDENTIFIER_ROW_ID;
}

} // namespace duckdb
