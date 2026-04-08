//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/column_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <string>

#include "duckdb/common/common.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/table_index.hpp"
#include "duckdb/common/projection_index.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class Serializer;
class Deserializer;
class Expression;

struct ColumnBinding {
	ColumnBinding();
	ColumnBinding(TableIndex table, ProjectionIndex column);

	TableIndex table_index;
	ProjectionIndex column_index;

public:
	string ToString() const;

	bool operator==(const ColumnBinding &rhs) const;
	bool operator!=(const ColumnBinding &rhs) const;
	static ProjectionIndex PushExpression(vector<unique_ptr<Expression>> &expressions, unique_ptr<Expression> new_expr);

	void Serialize(Serializer &serializer) const;
	static ColumnBinding Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
