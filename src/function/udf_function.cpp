#include "duckdb/function/udf_function.hpp"
#include "duckdb/common/types.hpp"

using namespace duckdb;

UDFWrapper::UDFWrapper(ClientContext &context): _context(context) {
}
