//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/numeric_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/base_statistics.hpp"

namespace duckdb {

class StringStatistics : public BaseStatistics {
public:
	constexpr static uint32_t MAX_STRING_MINMAX_SIZE = 8;
public:
	StringStatistics();

	//! The minimum value of the segment, potentially truncated
	data_t min[MAX_STRING_MINMAX_SIZE];
	//! The maximum value of the segment, potentially truncated
	data_t max[MAX_STRING_MINMAX_SIZE];
	//! Whether or not the column is ASCII only
	bool has_unicode;
	//! The maximum string length in bytes
	uint32_t max_string_length;
	//! Whether or not the segment contains any big strings in overflow blocks
	bool has_overflow_strings;

public:
	void Update(const string_t &value);
	unique_ptr<BaseStatistics> Copy() override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<BaseStatistics> Deserialize(Deserializer &source);
	bool CheckZonemap(ExpressionType comparison_type, string value);
};

}
