//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/numeric_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {
class Value;

//! NUMERIC is the exact numeric type whose physical representation is not fixed by a declared width and scale but
//! inferred from the values it holds. A NUMERIC column is bound to a concrete representation (TINYINT .. HUGEINT or
//! DECIMAL(width, scale)) that covers every value in the table; each row group stores its rows in the narrowest
//! representation that fits them. The rules below decide those representations.
struct NumericType {
	//! Whether values of this type can be stored in a NUMERIC column without loss (integers and decimals)
	DUCKDB_API static bool IsExact(const LogicalType &type);
	//! The representation of a NUMERIC column that holds no values
	DUCKDB_API static LogicalType DefaultType();
	//! The number of integer digits and the scale that an exact type can hold
	DUCKDB_API static void GetDigits(const LogicalType &type, uint8_t &integer_digits, uint8_t &scale);
	//! The narrowest representation that holds every value of both inputs (type-level widening, used for inserts)
	DUCKDB_API static LogicalType Widen(const LogicalType &left, const LogicalType &right);
	//! The narrowest representation for values in [min, max] with the given number of fractional digits
	//! (value-level, used for the bound type of a column and the storage type of a row group)
	DUCKDB_API static LogicalType ForRange(const Value &min, const Value &max, uint8_t scale);
	//! The number of integer digits of an exact numeric value (0 for |value| < 1)
	DUCKDB_API static uint8_t IntegerDigits(const Value &value);
	//! The widest supported representation of exact numbers with the given scale
	DUCKDB_API static LogicalType WidestType(uint8_t scale);
	//! Error thrown for values that cannot be stored in a NUMERIC column
	DUCKDB_API static string NotExactMessage(const LogicalType &type);
};

} // namespace duckdb
