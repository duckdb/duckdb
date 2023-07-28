//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/arrow_duck_schema.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>

#include "duckdb/common/types.hpp"

namespace duckdb {
//===--------------------------------------------------------------------===//
// Arrow Variable Size Types
//===--------------------------------------------------------------------===//
enum class ArrowVariableSizeType : uint8_t { FIXED_SIZE = 0, NORMAL = 1, SUPER_SIZE = 2 };

//===--------------------------------------------------------------------===//
// Arrow Time/Date Types
//===--------------------------------------------------------------------===//
enum class ArrowDateTimeType : uint8_t {
	MILLISECONDS = 0,
	MICROSECONDS = 1,
	NANOSECONDS = 2,
	SECONDS = 3,
	DAYS = 4,
	MONTHS = 5,
	MONTH_DAY_NANO = 6
};

class ArrowType {
public:
	// From a DuckDB type
	ArrowType(LogicalType type_p) : type(std::move(type_p)) {};

	// From a DuckDB type + fixed_size
	ArrowType(LogicalType type_p, idx_t fixed_size_p)
	    : type(std::move(type_p)), size_type(ArrowVariableSizeType::FIXED_SIZE), fixed_size(fixed_size_p) {};

	// From a DuckDB type + variable size type
	ArrowType(LogicalType type_p, ArrowVariableSizeType size_type_p)
	    : type(std::move(type_p)), size_type(size_type_p) {};

	// From a DuckDB type + datetime type
	ArrowType(LogicalType type_p, ArrowDateTimeType date_time_precision_p)
	    : type(std::move(type_p)), date_time_precision(date_time_precision_p) {};

	void AddChild(ArrowType &child);
	void AddChild(ArrowType &&child);

	void AssignChildren(vector<ArrowType> children);

	LogicalType &GetDuckType();

	ArrowVariableSizeType GetSizeType() const;

	idx_t FixedSize() const;

	ArrowDateTimeType GetDateTimeType() const;

	ArrowType &operator[](idx_t index);

private:
	LogicalType type;
	//! If we have a nested type, their children's type.
	vector<ArrowType> children;
	//! If its a variable size type (e.g., strings, blobs, lists) holds which type it is
	ArrowVariableSizeType size_type;
	//! If this is a date/time holds its precision
	ArrowDateTimeType date_time_precision;
	//! Only for size types with fixed size
	idx_t fixed_size = 0;
};

struct ArrowTableType {
public:
	vector<LogicalType> GetDuckDBTypes();
	void AddColumn(ArrowType &column);

private:
	vector<ArrowType> columns;
};
} // namespace duckdb
