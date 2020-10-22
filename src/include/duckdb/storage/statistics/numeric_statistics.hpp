//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/numeric_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

class NumericStatistics : public BaseStatistics {
public:
	NumericStatistics(LogicalType type);
	NumericStatistics(LogicalType type, Value min, Value max);

	//! The minimum value of the segment
	Value min;
	//! The maximum value of the segment
	Value max;

public:
	void Merge(const BaseStatistics &other) override;
	bool CheckZonemap(ExpressionType comparison_type, Value constant);

	unique_ptr<BaseStatistics> Copy() override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<BaseStatistics> Deserialize(Deserializer &source, LogicalType type);

	string ToString() override;
};

}
