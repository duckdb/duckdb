#include "duckdb/common/constants.hpp"

#include <limits>

using namespace duckdb;
using namespace std;

namespace duckdb {

const index_t INVALID_INDEX = (index_t)-1;
const row_t MAX_ROW_ID = 4611686018427388000ULL; // 2^62
const column_t COLUMN_IDENTIFIER_ROW_ID = (column_t)-1;
const sel_t ZERO_VECTOR[STANDARD_VECTOR_SIZE] = {0};
const double PI = 3.141592653589793;

const transaction_t TRANSACTION_ID_START = 4611686018427388000ULL;                  // 2^62
const transaction_t NOT_DELETED_ID = std::numeric_limits<transaction_t>::max() - 1; // 2^64 - 1
const transaction_t MAXIMUM_QUERY_ID = std::numeric_limits<transaction_t>::max();   // 2^64

} // namespace duckdb
