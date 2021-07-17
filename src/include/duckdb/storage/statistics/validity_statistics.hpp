//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/validity_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {
class Serializer;
class Deserializer;
class Vector;

class ValidityStatistics : public BaseStatistics {
public:
	ValidityStatistics(bool has_null = false, bool has_no_null = true);

	//! Whether or not the segment can contain NULL values
	bool has_null;
	//! Whether or not the segment can contain values that are not null
	bool has_no_null;

public:
	void Merge(const BaseStatistics &other) override;

	bool IsConstant() override;

	unique_ptr<BaseStatistics> Copy() override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<BaseStatistics> Deserialize(Deserializer &source);
	void Verify(Vector &vector, idx_t count) override;

	static unique_ptr<BaseStatistics> Combine(const unique_ptr<BaseStatistics> &lstats,
	                                          const unique_ptr<BaseStatistics> &rstats);

	string ToString() override;
};

} // namespace duckdb
