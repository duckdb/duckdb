//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_recursive_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/table_index.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class Deserializer;
class Serializer;

class LogicalRecursiveCTE : public LogicalCTE {
	LogicalRecursiveCTE();

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_RECURSIVE_CTE;

public:
	LogicalRecursiveCTE(string ctename_p, TableIndex table_index, idx_t column_count, bool union_all,
	                    vector<unique_ptr<Expression>> key_targets, unique_ptr<LogicalOperator> top,
	                    unique_ptr<LogicalOperator> bottom);

	bool union_all;
	// Flag if recurring table is referenced, if not we do not copy ht into ColumnDataCollection
	bool ref_recurring;
	vector<unique_ptr<Expression>> key_targets;
	vector<unique_ptr<Expression>> payload_aggregates;
	vector<LogicalType> internal_types;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, column_count);
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	vector<TableIndex> GetTableIndex() const override;
	string GetName() const override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
