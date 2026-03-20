//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/map_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

enum class MapInvalidReason : uint8_t { VALID, NULL_KEY, DUPLICATE_KEY, NOT_ALIGNED, INVALID_PARAMS };

struct MapVector {
	DUCKDB_API static const Vector &GetKeys(const Vector &vector);
	DUCKDB_API static const Vector &GetValues(const Vector &vector);
	DUCKDB_API static Vector &GetKeys(Vector &vector);
	DUCKDB_API static Vector &GetValues(Vector &vector);
	DUCKDB_API static MapInvalidReason
	CheckMapValidity(Vector &map, idx_t count, const SelectionVector &sel = *FlatVector::IncrementalSelectionVector());
	DUCKDB_API static void EvalMapInvalidReason(MapInvalidReason reason);
	DUCKDB_API static void MapConversionVerify(Vector &vector, idx_t count);
};

struct StructVector {
	DUCKDB_API static const vector<unique_ptr<Vector>> &GetEntries(const Vector &vector);
	DUCKDB_API static vector<unique_ptr<Vector>> &GetEntries(Vector &vector);
};

}
