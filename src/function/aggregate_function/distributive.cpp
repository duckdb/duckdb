#include "function/aggregate_function/distributive.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include <cmath>

using namespace std;

namespace duckdb {

void gather_finalize(Vector& payloads, Vector &result) {
	VectorOperations::Gather::Set(payloads, result);
}

void null_payload_initialize(data_ptr_t payload, TypeId return_type) {
	SetNullValue(payload, return_type);
}

} // namespace duckdb
