#include "tpcds_errors.hpp"

#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"

namespace tpcds {

void ThrowInternalError(const std::string &message) {
	throw duckdb::cxx::Exception(DUCKDB_V2_ERROR_RUNTIME_INTERNAL, message);
}

void ThrowOutOfMemoryError(const std::string &message) {
	throw duckdb::cxx::Exception(DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY, message);
}

void ThrowInvalidInputError(const std::string &message) {
	throw duckdb::cxx::InvalidInputException(message);
}

} // namespace tpcds
