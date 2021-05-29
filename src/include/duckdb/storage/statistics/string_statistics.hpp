//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/string_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"

namespace duckdb {

class StringStatistics : public BaseStatistics {
public:
	constexpr static uint32_t MAX_STRING_MINMAX_SIZE = 8;

public:
	explicit StringStatistics(LogicalType type);

	//! The minimum value of the segment, potentially truncated
	data_t min[MAX_STRING_MINMAX_SIZE];
	//! The maximum value of the segment, potentially truncated
	data_t max[MAX_STRING_MINMAX_SIZE];
	//! Whether or not the column can contain unicode characters
	bool has_unicode;
	//! The maximum string length in bytes
	uint32_t max_string_length;
	//! Whether or not the segment contains any big strings in overflow blocks
	bool has_overflow_strings;

public:
	void Update(const string_t &value);
	void Merge(const BaseStatistics &other) override;

	unique_ptr<BaseStatistics> Copy() override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<BaseStatistics> Deserialize(Deserializer &source, LogicalType type);
	void Verify(Vector &vector, idx_t count) override;

	FilterPropagateResult CheckZonemap(ExpressionType comparison_type, const string &value);

	string ToString() override;
};

} // namespace duckdb
