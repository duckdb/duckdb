//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/collation_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/enums/collation_type.hpp"

namespace duckdb {
struct MapCastInfo;
struct MapCastNode;
struct DBConfig;

//! Returns the (ordered) list of scalar functions that need to be applied to a value of the given type to make it
//! byte-comparable under its collation. Returns an empty list if no collation needs to be applied.
typedef vector<string> (*get_collation_functions_t)(ClientContext &context, const LogicalType &sql_type,
                                                    CollationType type);

struct CollationCallback {
	explicit CollationCallback(get_collation_functions_t get_collation_functions_p)
	    : get_collation_functions(get_collation_functions_p) {
	}

	get_collation_functions_t get_collation_functions;
};

class CollationBinding {
public:
	CollationBinding();

public:
	DUCKDB_API static CollationBinding &Get(ClientContext &context);
	DUCKDB_API static CollationBinding &Get(DatabaseInstance &db);
	//! Hashes the collation metadata of a type, including nested types.
	DUCKDB_API static hash_t HashCollations(const LogicalType &type);
	//! Checks whether two types have identical collation metadata, including nested types.
	DUCKDB_API static bool CollationsEqual(const LogicalType &left, const LogicalType &right);

	DUCKDB_API void RegisterCollation(CollationCallback callback);
	DUCKDB_API bool PushCollation(ClientContext &context, unique_ptr<Expression> &source, const LogicalType &sql_type,
	                              CollationType type) const;

private:
	vector<CollationCallback> collations;
};

} // namespace duckdb
