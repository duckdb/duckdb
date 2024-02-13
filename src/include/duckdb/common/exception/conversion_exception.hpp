//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/exception/conversion_exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"

namespace duckdb {

class ConversionException : public Exception {
public:
	DUCKDB_API explicit ConversionException(const string &msg);
	DUCKDB_API ConversionException(const PhysicalType origType, const PhysicalType newType);
	DUCKDB_API ConversionException(const LogicalType &origType, const LogicalType &newType);

	template <typename... Args>
	explicit ConversionException(const string &msg, Args... params)
	    : ConversionException(ConstructMessage(msg, params...)) {
	}
};

} // namespace duckdb
