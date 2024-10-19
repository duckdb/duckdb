//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/collation_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/cast/default_casts.hpp"

namespace duckdb {
struct MapCastInfo;
struct MapCastNode;
struct DBConfig;

typedef bool (*try_push_collation_t)(ClientContext &context, unique_ptr<Expression> &source,
                                     const LogicalType &sql_type);

struct CollationCallback {
	explicit CollationCallback(try_push_collation_t try_push_collation_p) : try_push_collation(try_push_collation_p) {
	}

	try_push_collation_t try_push_collation;
};

class CollationBinding {
public:
	CollationBinding();

public:
	DUCKDB_API static CollationBinding &Get(ClientContext &context);
	DUCKDB_API static CollationBinding &Get(DatabaseInstance &db);

	DUCKDB_API void RegisterCollation(CollationCallback callback);
	DUCKDB_API bool PushCollation(ClientContext &context, unique_ptr<Expression> &source,
	                              const LogicalType &sql_type) const;

private:
	vector<CollationCallback> collations;
};

} // namespace duckdb
