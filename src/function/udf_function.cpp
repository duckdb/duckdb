#include "duckdb/function/udf_function.hpp"

namespace duckdb {

UDFWrapper::UDFWrapper(ClientContext &context): _context(context) {
}
