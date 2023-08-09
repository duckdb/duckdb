//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/arrow_duck_schema.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"

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
	//! From a DuckDB type
	ArrowType(LogicalType type_p)
	    : type(std::move(type_p)), size_type(ArrowVariableSizeType::NORMAL),
	      date_time_precision(ArrowDateTimeType::DAYS) {};

	//! From a DuckDB type + fixed_size
	ArrowType(LogicalType type_p, idx_t fixed_size_p)
	    : type(std::move(type_p)), size_type(ArrowVariableSizeType::FIXED_SIZE),
	      date_time_precision(ArrowDateTimeType::DAYS), fixed_size(fixed_size_p) {};

	//! From a DuckDB type + variable size type
	ArrowType(LogicalType type_p, ArrowVariableSizeType size_type_p)
	    : type(std::move(type_p)), size_type(size_type_p), date_time_precision(ArrowDateTimeType::DAYS) {};

	//! From a DuckDB type + datetime type
	ArrowType(LogicalType type_p, ArrowDateTimeType date_time_precision_p)
	    : type(std::move(type_p)), size_type(ArrowVariableSizeType::NORMAL),
	      date_time_precision(date_time_precision_p) {};

	void AddChild(unique_ptr<ArrowType> child);

	void AssignChildren(vector<unique_ptr<ArrowType>> children);

	const LogicalType &GetDuckType() const;

	ArrowVariableSizeType GetSizeType() const;

	idx_t FixedSize() const;

	void SetDictionary(unique_ptr<ArrowType> dictionary);

	ArrowDateTimeType GetDateTimeType() const;

	const ArrowType &GetDictionary() const;

	const ArrowType &operator[](idx_t index) const;

private:
	LogicalType type;
	//! If we have a nested type, their children's type.
	vector<unique_ptr<ArrowType>> children;
	//! If its a variable size type (e.g., strings, blobs, lists) holds which type it is
	ArrowVariableSizeType size_type;
	//! If this is a date/time holds its precision
	ArrowDateTimeType date_time_precision;
	//! Only for size types with fixed size
	idx_t fixed_size = 0;
	//! Hold the optional type if the array is a dictionary
	unique_ptr<ArrowType> dictionary_type;
};

using arrow_column_map_t = unordered_map<idx_t, unique_ptr<ArrowType>>;

struct ArrowTableType {
public:
	void AddColumn(idx_t index, unique_ptr<ArrowType> type);
	const arrow_column_map_t &GetColumns() const;

private:
	arrow_column_map_t arrow_convert_data;
};

} // namespace duckdb
