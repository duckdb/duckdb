//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/exception/conversion_exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/query_location.hpp"

namespace duckdb {
class ConversionException : public Exception {
public:
	DUCKDB_API explicit ConversionException(const string &msg);

	DUCKDB_API explicit ConversionException(QueryLocation error_location, const string &msg);

	DUCKDB_API ConversionException(const PhysicalType orig_type, const PhysicalType new_type);

	DUCKDB_API ConversionException(const LogicalType &orig_type, const LogicalType &new_type);

	template <typename... ARGS>
	explicit ConversionException(const string &msg, ARGS &&...params)
	    : ConversionException(ConstructMessage(msg, std::forward<ARGS>(params)...)) {
	}

	template <typename... ARGS>
	explicit ConversionException(QueryLocation error_location, const string &msg, ARGS &&...params)
	    : ConversionException(error_location, ConstructMessage(msg, std::forward<ARGS>(params)...)) {
	}
};
} // namespace duckdb
