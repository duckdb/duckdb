#include "function/aggregate_function/first.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void first_update(Vector** inputs, index_t input_count, Vector &result ) {
	assert(input_count == 1 );
	VectorOperations::Scatter::SetFirst(*inputs[0], result);
}

} // namespace duckdb
