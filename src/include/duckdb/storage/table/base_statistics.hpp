//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/base_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/enums/expression_type.hpp"

namespace duckdb {
class Serializer;
class Deserializer;

class BaseStatistics {
public:
	BaseStatistics() : has_null(false) {}
	virtual ~BaseStatistics() {}

	//! Whether or not the segment has NULL values
	bool has_null;
public:
	virtual unique_ptr<BaseStatistics> Copy();
	virtual void Serialize(Serializer &serializer);
	static unique_ptr<BaseStatistics> Deserialize(Deserializer &source, PhysicalType type);
};

}
