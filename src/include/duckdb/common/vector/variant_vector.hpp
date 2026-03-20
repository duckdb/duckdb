//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/variant_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

struct VariantVector {
	//! Gets a reference to the 'keys' list (dictionary) of a Variant
	DUCKDB_API static Vector &GetKeys(Vector &vec);
	DUCKDB_API static Vector &GetKeys(const Vector &vec);
	//! Gets a reference to the 'children' list of a Variant
	DUCKDB_API static Vector &GetChildren(Vector &vec);
	DUCKDB_API static Vector &GetChildren(const Vector &vec);
	//! Gets a reference to the 'keys_index' inside the 'children' list of a Variant
	DUCKDB_API static Vector &GetChildrenKeysIndex(Vector &vec);
	DUCKDB_API static Vector &GetChildrenKeysIndex(const Vector &vec);
	//! Gets a reference to the 'values_index' inside the 'children' list of a Variant
	DUCKDB_API static Vector &GetChildrenValuesIndex(Vector &vec);
	DUCKDB_API static Vector &GetChildrenValuesIndex(const Vector &vec);
	//! Gets a reference to the 'values' list of a Variant
	DUCKDB_API static Vector &GetValues(Vector &vec);
	DUCKDB_API static Vector &GetValues(const Vector &vec);
	//! Gets a reference to the 'type_id' inside the 'values' list of a Variant
	DUCKDB_API static Vector &GetValuesTypeId(Vector &vec);
	DUCKDB_API static Vector &GetValuesTypeId(const Vector &vec);
	//! Gets a reference to the 'byte_offset' inside the 'values' list of a Variant
	DUCKDB_API static Vector &GetValuesByteOffset(Vector &vec);
	DUCKDB_API static Vector &GetValuesByteOffset(const Vector &vec);
	//! Gets a reference to the binary blob 'value', which encodes the data of the row
	DUCKDB_API static Vector &GetData(Vector &vec);
	DUCKDB_API static Vector &GetData(const Vector &vec);
};

}
