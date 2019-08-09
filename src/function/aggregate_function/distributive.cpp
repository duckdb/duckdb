#include "function/aggregate_function/distributive.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void gather_finalize(Vector &states, Vector &result) {
	VectorOperations::Gather::Set(states, result);
}

void null_state_initialize(data_ptr_t state, TypeId return_type) {
	SetNullValue(state, return_type);
}

} // namespace duckdb
