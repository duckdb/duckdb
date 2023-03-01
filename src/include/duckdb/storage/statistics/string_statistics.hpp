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
	DUCKDB_API explicit StringStatistics(LogicalType type);

public:
	DUCKDB_API void Update(const string_t &value);
	DUCKDB_API void Merge(const BaseStatistics &other) override;

	//! Whether or not the statistics have a maximum string length defined
	DUCKDB_API bool HasMaxStringLength() const;
	//! Returns the maximum string length, or throws an exception if !HasMaxStringLength()
	DUCKDB_API uint32_t MaxStringLength() const;
	//! Whether or not the strings can contain unicode
	DUCKDB_API bool CanContainUnicode() const;

	//! Resets the max string length so HasMaxStringLength() is false
	DUCKDB_API void ResetMaxStringLength() {
		has_max_string_length = false;
	}
	//! FIXME: make this part of Set on statistics
	DUCKDB_API void SetContainsUnicode() {
		has_unicode = true;
	}

	unique_ptr<BaseStatistics> Copy() const override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<BaseStatistics> Deserialize(FieldReader &reader, LogicalType type);
	void Verify(Vector &vector, const SelectionVector &sel, idx_t count) const override;

	FilterPropagateResult CheckZonemap(ExpressionType comparison_type, const string &value) const;

	string ToString() const override;

private:
	//! The minimum value of the segment, potentially truncated
	data_t min[MAX_STRING_MINMAX_SIZE];
	//! The maximum value of the segment, potentially truncated
	data_t max[MAX_STRING_MINMAX_SIZE];
	//! Whether or not the column can contain unicode characters
	bool has_unicode;
	//! Whether or not the maximum string length is known
	bool has_max_string_length;
	//! The maximum string length in bytes
	uint32_t max_string_length;
};

} // namespace duckdb
