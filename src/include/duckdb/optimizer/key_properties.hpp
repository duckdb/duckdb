//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/key_properties.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

class LogicalGet;
class LogicalOperator;

enum class UniqueKeyProof : uint8_t { PRIMARY_KEY, UNIQUE_NOT_NULL, AGGREGATE_GROUP, KEY_PRESERVING_JOIN };

struct UniqueKeyProperty {
	UniqueKeyProof proof;
	optional_ptr<LogicalGet> base_scan;

	bool FunctionallyDetermines(LogicalOperator &owner, idx_t output_column) const;
};

//! Proves that the output columns form a complete non-NULL unique key.
optional<UniqueKeyProperty> GetUniqueKeyProperty(LogicalOperator &owner, const vector<idx_t> &output_columns);

} // namespace duckdb
