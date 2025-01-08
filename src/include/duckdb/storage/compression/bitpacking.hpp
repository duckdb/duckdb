//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/bitpacking.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string_util.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

enum class BitpackingMode : uint8_t { INVALID, AUTO, CONSTANT, CONSTANT_DELTA, DELTA_FOR, FOR };

BitpackingMode BitpackingModeFromString(const string &str);
string BitpackingModeToString(const BitpackingMode &mode);

class Serializer;

struct SerializedBitpackingSegmentState : public ColumnSegmentState {
public:
	SerializedBitpackingSegmentState();
	explicit SerializedBitpackingSegmentState(map<BitpackingMode, idx_t> counts_p);

public:
	void Serialize(Serializer &serializer) const override;

	map<BitpackingMode, idx_t> counts;
};

} // namespace duckdb
