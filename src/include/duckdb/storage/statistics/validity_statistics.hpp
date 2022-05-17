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
	explicit ValidityStatistics(bool has_null = false, bool has_no_null = true);

	//! Whether or not the segment can contain NULL values
	bool has_null;
	//! Whether or not the segment can contain values that are not null
	bool has_no_null;

public:
	void Merge(const BaseStatistics &other) override;

	bool IsConstant() const override;

	unique_ptr<BaseStatistics> Copy() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ValidityStatistics> Deserialize(FieldReader &reader);

	void Verify(Vector &vector, const SelectionVector &sel, idx_t count) const override;

	static unique_ptr<BaseStatistics> Combine(const unique_ptr<BaseStatistics> &lstats,
	                                          const unique_ptr<BaseStatistics> &rstats);

	string ToString() const override;
};

} // namespace duckdb
