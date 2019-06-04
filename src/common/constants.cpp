#include "common/constants.hpp"
#include <cmath>

using namespace duckdb;
using namespace std;

namespace duckdb {

const index_t INVALID_INDEX = (index_t)-1;
const column_t COLUMN_IDENTIFIER_ROW_ID = (column_t)-1;
const sel_t ZERO_VECTOR[STANDARD_VECTOR_SIZE] = {0};

const double PI = 4 * atan((double)1.0);

} // namespace duckdb
